# Flink Common 状态补全设计

## 目标

修改 `flink_processer`，不再从 `app` 事件安装列表提取应用名称和版本。作业改为把每个
`(udid, start_id)` 启动周期的首条 `common` 事件作为公共元数据来源，使用 Flink keyed
`ValueState` 保存六个字段，并立即补全后续所有非 `common` 事件。

输出 Kafka record key 从 `<udid>:<start_id>` 改为 `<udid>_<start_id>`。连续 30 秒没有收到
同一 key 的新事件时，清除该 key 的公共元数据状态和定时器状态。

## 输入契约

输入仍为 Kafka `flink_demo` topic 中的 UTF-8 JSON。所有事件继续要求顶层存在：

- 非空字符串 `udid`。
- 64 位整数 `start_id`。
- 非空字符串 `protol_type`。

`common` 事件的 `payload` 必须包含六个非空字符串：

- `self_app_package`
- `self_app_name`
- `model`
- `brand`
- `os`
- `os_version`

上游保证同一个 `udid`、同一次 `start_id` 的 `common` 必定第一条发送。Flink 不增加等待、
乱序缓存或回补机制；如果实际输入违反该保证，早于 `common` 到达的非 `common` 事件按
缺少状态处理并立即进入错误流。

JSON 无法解析、根节点不是对象，或缺少合法业务 key/类型时，解析器继续抛出异常，由 Flink
失败恢复机制处理，不写入业务错误 topic。

## 分组与状态

解析后的事件使用 `<udid>_<start_id>` 作为 Flink keyed stream 的 key。该字符串也直接作为
两个输出 Kafka topic 的 record key。

每个 key 使用两个托管状态：

1. `ValueState<CommonMetadata>`：保存六个公共元数据字段。
2. `ValueState<Long>`：保存当前已注册的处理时间清理定时器时间戳。

每收到任意类型事件时，状态函数都执行以下定时器续期步骤：

1. 读取旧定时器时间戳；若存在，则调用 `deleteProcessingTimeTimer` 删除旧定时器。
2. 使用 `currentProcessingTime() + 30_000` 计算新时间戳。
3. 注册新处理时间定时器，并把时间戳写入定时器状态。

`onTimer` 触发时，仅当回调时间戳等于定时器状态中的当前时间戳，才清除
`CommonMetadata` 和定时器状态。该判断防止已经被续期取代的旧定时器误删新状态。

删除现有 2 分钟 Processing Time Tumbling Window、`ProcessWindowFunction` 和窗口内全量事件
缓存。非 `common` 事件在到达时立即处理，不再产生最长 2 分钟的窗口等待，也不存在跨窗口
补全边界。

## Common 处理规则

收到 `protol_type = "common"` 时，从 `payload` 提取六个字段。

字段全部是非空字符串时：

- 用该值完整覆盖当前 `CommonMetadata` 状态。
- 不写入 `flink_output`。
- 不写入 `flink_error`。

任一字段缺失、不是字符串、为 `null` 或空字符串时：

- 不创建或覆盖 `CommonMetadata` 状态，避免用不完整数据污染已有状态。
- 复制原始 JSON，在顶层设置全部六个字段；有效字段使用提取值，无效字段使用 JSON
  `null`。
- 该事件只写入 `flink_error`，不写入 `flink_output`。
- 仍按“任意消息”规则续期 30 秒定时器。

同一 key 异常出现多条合法 `common` 时，后到的数据覆盖先前状态。由于上游保证首条发送，
正常数据流每个启动周期只需要第一次写入。

## 非 Common 补全与路由

收到 `start`、`devinfo`、`environment`、`app` 或其他非 `common` 类型时：

1. 读取当前 `CommonMetadata`。
2. 复制原始 JSON，保留所有已有字段。
3. 在顶层写入六个公共字段；已有同名顶层字段会被状态值覆盖。

状态存在且六个字段完整时，事件只写入 `flink_output`。状态不存在时，六个顶层字段全部写为
JSON `null`，事件只写入 `flink_error`。每条事件只能进入一个输出 topic。

`app` 不再具有特殊处理：

- 不读取 `payload.self_package_name`。
- 不遍历或匹配 `payload.apps[]`。
- 不生成或覆盖顶层 `app_name`、`app_version`。
- 如果原始输入本身包含未知的顶层 `app_name`/`app_version`，按保留原始未知字段的规则原样
  保留，但其值不参与成功/错误判定。

## Kafka 数据流

```text
KafkaSource(flink_demo)
  -> JSON 解析
  -> keyBy(<udid>_<start_id>)
  -> CommonStateEnrichmentFunction
       -> 主输出：完整补全的非 common 事件
       -> 错误侧输出：不完整 common 或无 common 状态的非 common 事件
  -> KafkaSink(flink_output) / KafkaSink(flink_error)
```

两个 Sink 继续使用至少一次投递语义和 checkpoint。所有输出 record 的 key 严格等于对应 JSON
顶层字段组成的 `<udid>_<start_id>`；不沿用输入 Kafka record key。

## 组件调整

- `EventRecord.recordKey()`：分隔符从冒号改为下划线。
- 新增 `CommonMetadata`：可被 Flink 序列化的六字段状态对象。
- 用 `CommonStateEnrichmentFunction` 替换 `AppEnrichmentWindowFunction` 和
  `WindowEnricher`。
- 保留 `EnrichmentBatch` 与窗口相关类型没有价值，应删除。
- `EventEnrichmentJob`：移除滚动窗口，改为 `keyBy(...).process(...)`，保持主输出和错误侧输出
  的两个 Kafka Sink。
- `OutputEventKafkaSerializer`：继续序列化 `OutputEvent.key()`，测试更新为下划线 key。

## 错误处理与恢复

- 输入解析错误使作业失败，Kafka offset 不被静默提交。
- 业务元数据错误进入 `flink_error`，不使作业失败。
- 状态和处理时间定时器由 checkpoint 恢复；恢复后的当前定时器仍决定状态清理时间。
- 定时器时间戳使用 `Math.addExact(currentProcessingTime, 30_000)`，极端溢出时显式失败，避免
  注册错误的过去时间戳。
- 输出继续以至少一次语义运行，下游使用 `event_id` 做幂等处理。

## 测试范围

自动化测试覆盖：

- `common` 六字段完整时写入状态且不产生任何输出。
- 后续每种非 `common` 事件都从状态补齐六个顶层字段并进入正常流。
- 顶层已有同名字段时被状态值覆盖，其他未知字段保持不变。
- 完全删除 App 安装列表匹配依赖；`app` 与普通非 `common` 事件行为一致，不新增
  `app_name`/`app_version`。
- 不完整 `common` 不更新状态，只进入错误流，有效字段保留、无效字段写为 `null`。
- 无状态非 `common` 只进入错误流，六字段均为 `null`。
- 同 key 后续合法 `common` 覆盖先前状态；不同 key 状态严格隔离。
- 每条消息把清理时间顺延 30 秒并删除旧定时器。
- 旧定时器回调不能误删续期后的状态；当前定时器触发后两个状态均被清除。
- record key 严格使用 `<udid>_<start_id>`，两个 Kafka Sink 一致。
- 作业拓扑不再包含滚动窗口，并保持固定 operator UID、checkpoint 与双 Sink 配置。

## 文档与兼容性

更新 `flink_processer/README.md` 和仓库根 `README.md`：

- 删除基于 `app` 安装列表补全 `app_name/app_version` 的说明。
- 说明 `common` 是状态来源且不会进入正常输出。
- 说明六个顶层字段、错误路由、30 秒不活跃清理和即时输出。
- 把输出 Kafka record key 示例改为 `<udid>_<start_id>`。

这是输出契约的不兼容变更：下游不能再依赖 Flink 生成的顶层 `app_name`、`app_version`，并且
必须同步更新对 Kafka record key 分隔符的解析。
