# Flink Kafka App 信息补全设计

## 目标

在仓库根目录新增 Maven 工程 `flink_processer`。该工程使用 Apache Flink 从 Kafka
`flink_demo` topic 读取手机事件，以 `(udid, start_id)` 为业务 key，在 2 分钟处理时间
滚动窗口内找到 `app` 事件对应的应用名称和版本，并将这两个字段补充到同窗口、同 key
的非 `app` 事件顶层。成功补全的非 `app` 事件和原始 `app` 事件写入 Kafka
`flink_output` topic；任何未能完整补全的非 `app` 事件写入 Kafka `flink_error` topic。

## 输入契约

输入 value 是 UTF-8 JSON，沿用 `sender` 已有数据契约。处理逻辑依赖以下字段：

- 顶层 `protol_type`：事件类型，字段名保留既有拼写。
- 顶层 `udid`：设备标识字符串。
- 顶层 `start_id`：启动周期的 Unix 毫秒整数。
- 顶层 `sequence`：启动周期内的事件序号。
- `app` 事件的 `payload.self_package_name`：当前应用包名。
- `app` 事件的 `payload.apps`：安装应用数组。
- `payload.apps[].package_name`、`app_name`、`version_name`：用于匹配和补全。

缺少 `udid`、`start_id`、`protol_type` 或 JSON 无法解析时，处理函数抛出异常，使 Flink
按作业的失败恢复策略处理，而不是静默丢弃数据。

## 窗口与状态语义

解析后的事件使用 `udid + start_id` 组成不可歧义的业务 key，然后进入长度为 2 分钟的
Processing Time Tumbling Window。窗口使用 Flink 管理的窗口状态缓存其中的事件，并在固定
窗口结束时一次性处理和清理。

滚动窗口具有固定、互不重叠的时间边界。相同业务 key 的消息如果跨越窗口边界，将进入
不同窗口；后一个窗口内的 `app` 事件不能补全前一个已经关闭窗口中的非 `app` 事件。
这是已接受的行为，不增加跨窗口事件缓存、滑动窗口、去重状态或更新流来弥补。

窗口按处理时间而不是 `event_timestamp` 运行，因此不需要 watermark，也不会因为生产端
时钟偏差延迟窗口关闭。代价是消息属于哪个窗口由 Flink 收到它的时间决定。

## 补全逻辑

窗口触发后执行以下步骤：

1. 在窗口事件中查找 `protol_type = "app"` 的记录。
2. 读取该记录的 `payload.self_package_name`。
3. 在同一记录的 `payload.apps` 中查找 `package_name` 完全相等的元素。
4. 将匹配元素的 `app_name` 作为输出顶层 `app_name`，将 `version_name` 作为输出顶层
   `app_version`。
5. 对窗口中的所有非 `app` 事件复制原 JSON，并设置这两个顶层字段。
6. `app` 事件不增加这两个顶层字段，按原 JSON 输出。

正常发送器每个启动周期只有一条 `app` 事件。若窗口中异常出现多条，则选择
`sequence` 最大的 `app` 事件；若 `sequence` 相同，则选择 `event_timestamp` 最大的记录。
这样结果不依赖窗口迭代顺序。

若窗口中没有 `app` 事件、`self_package_name` 在 `apps` 中没有匹配项，或匹配项缺少
非空的 `app_name`/`version_name`，则对应非 `app` 事件仍增加顶层 `app_name` 和
`app_version` 字段，无法取得的字段值为 JSON `null`，并路由到 `flink_error`。只要任一字段
为 `null` 或空字符串，就视为补全失败。不会使用 `start.payload.app_version` 代替安装列表中的
`version_name`。

`app` 事件本身不属于待补全数据，无论其安装列表能否匹配，都保持原 JSON 并写入
`flink_output`，不会因为缺少顶层 `app_name`/`app_version` 被路由到 `flink_error`。

窗口内输出按顶层 `sequence` 升序排列；序号相同的记录按 `event_timestamp`、`event_id`
依次排序，以提供确定性的单 key 输出顺序。

## Kafka 数据流

作业使用 Flink DataStream API：

```text
KafkaSource(flink_demo)
  -> JSON 解析
  -> keyBy(udid + start_id)
  -> 2 分钟处理时间滚动窗口
  -> 窗口补全
  -> 成功流 -> JSON 序列化 -> KafkaSink(flink_output)
  -> 失败侧输出 -> JSON 序列化 -> KafkaSink(flink_error)
```

输入 topic 默认是 `flink_demo`，输出 topic 默认是 `flink_output`。Kafka bootstrap servers、
错误 topic 默认是 `flink_error`。Kafka bootstrap servers、consumer group、输入 topic、正常
输出 topic、错误 topic 和作业并行度通过命令行参数配置，默认值集中在配置类中。Kafka
Source 默认从 consumer group 已提交 offset 开始；不存在已提交 offset 时从最早位置开始。
两个 Kafka Sink 均使用至少一次投递语义，并启用周期性 checkpoint；下游应继续以
`event_id` 作为事件幂等标识。每条输入事件只进入一个输出 topic，不同时写入正常和错误
topic。

## 工程结构

`flink_processer` 使用 Java、Maven 和 Apache Flink 2.2.0。代码按职责拆分：

- 作业入口负责读取配置、组装 Source、窗口处理和 Sink。
- 配置类负责命令行参数解析与校验。
- JSON 事件模型负责解析、业务 key、类型判断和稳定排序字段。
- 窗口函数只负责选择 App 元数据及生成补全后的 JSON。

项目提供 Maven Shade 打包配置，生成可提交到 Flink 集群的 fat JAR；Flink 运行时依赖使用
`provided` scope，Kafka connector 与 JSON 依赖按集群部署要求打入作业 JAR。

## 错误处理与可观测性

- 无法解析或缺少业务 key 的输入使作业失败，不静默提交其 Kafka offset。
- App 信息缺失属于可处理的业务数据问题，输出带 `null` 字段的原事件到 `flink_error`，
  不使作业失败。
- 作业入口为 Source、窗口算子和 Sink 设置稳定名称与 UID，便于从 checkpoint/savepoint
  恢复和定位指标。
- 日志不打印完整安装列表，只记录异常摘要和业务 key，避免大消息造成日志膨胀。

## 测试范围

自动化测试覆盖：

- 相同 key 且位于同一窗口时，`app` 晚于非 `app` 到达仍能完成补全。
- 使用 `self_package_name` 精确匹配安装列表，并把 `version_name` 映射为顶层
  `app_version`。
- 不同 `udid` 或不同 `start_id` 的数据不会互相补全。
- `app` 事件保持原样，非 `app` 事件保留原字段并增加两个顶层字段。
- 缺少 App、包名不匹配、App 元数据缺失或字段为空时，非 `app` 事件只写入
  `flink_error`，并以 `null` 表示无法取得的字段。
- 成功补全的非 `app` 事件和原始 `app` 事件只写入 `flink_output`。
- 每条事件只路由到一个 topic。
- 多条 App 事件和输入乱序时结果保持确定。
- 处理时间跨越滚动窗口边界时，两侧数据不会跨窗口关联，记录已接受的边界行为。
- 窗口触发后释放窗口状态。
- 作业拓扑使用预期的 Kafka topic、消费起点和至少一次 Sink 语义。

## 不在范围内

- 跨滚动窗口补全。
- 将无法解析的数据写入死信 topic。
- 修改 `sender` 的事件顺序或数据结构。
- 使用事件时间、水位线、滑动窗口或会话窗口。
- 对输出 topic 实现事务性精确一次投递。
