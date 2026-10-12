# Flink Kafka App 信息补全作业

`flink_processer` 从 Kafka 读取手机事件，按 `(udid, start_id)` 组合分组，在 2 分钟处理时间滚动窗口关闭时补全非 `app` 事件的顶层 `app_name` 和 `app_version`。成功事件写入 `flink_output`，补全失败的非 `app` 事件写入 `flink_error`。

## 环境与构建

- Java 17（本工程按 Java 17 编译）。
- Maven 3.9.x。
- 已启动且可提交作业的 Apache Flink 2.2.0 集群，`flink` 命令位于 `PATH`。
- Flink 集群可访问 Kafka；输入、成功输出、错误输出三个 topic 必须互不相同，并具有相应读写权限。

从仓库根目录构建：

```bash
mvn -f flink_processer/pom.xml clean package
```

也可在本目录执行 `mvn clean package`。产物为 `flink_processer/target/flink-processer-1.0.0.jar`，是包含 Kafka connector/client 和 JSON 依赖的 fat JAR；Flink runtime 核心依赖由集群提供。工程使用 Flink 2.2.0 和 Kafka connector `5.0.0-2.2`。

## 提交作业

从仓库根目录使用全部默认参数：

```bash
flink run --detached flink_processer/target/flink-processer-1.0.0.jar
```

显式指定入口类和应用参数：

```bash
flink run --detached \
  -c com.example.flinkprocesser.EventEnrichmentJob \
  flink_processer/target/flink-processer-1.0.0.jar \
  --bootstrap-servers 192.168.250.106:9092 \
  --input-topic flink_demo \
  --output-topic flink_output \
  --error-topic flink_error \
  --group-id flink-processer \
  --parallelism 1 \
  --checkpoint-interval-ms 60000
```

`--detached` 和 `-c` 是 Flink CLI 参数，JAR 路径之后的是作业参数。入口类也已写入 JAR manifest，因此默认示例可省略 `-c`。提交语法参考 [Flink 2.2 CLI 文档](https://github.com/apache/flink/blob/release-2.2.0/docs/content/docs/deployment/cli.md)。

全部作业参数如下：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--bootstrap-servers` | `192.168.250.106:9092` | Kafka bootstrap servers，多个地址用逗号分隔 |
| `--input-topic` | `flink_demo` | 输入 topic |
| `--output-topic` | `flink_output` | 成功输出 topic |
| `--error-topic` | `flink_error` | 补全失败输出 topic |
| `--group-id` | `flink-processer` | Kafka consumer group |
| `--parallelism` | `1` | 作业并行度，必须为正整数 |
| `--checkpoint-interval-ms` | `60000` | Checkpoint 间隔，单位毫秒，必须为正整数 |

字符串参数不能留空，三个 topic 不能相同。2 分钟窗口长度固定，不能通过参数调整。新消费任务默认从 consumer group 已提交的 offset 开始；没有已提交 offset 时从最早位置开始。通过 checkpoint/savepoint 恢复时使用恢复的状态。

## 输入字段

每条 Kafka value 是 UTF-8 JSON 对象，完整数据契约见 [sender 文档](../sender/README.md)。输入 record key 不参与分组，作业使用 JSON 顶层字段重新生成业务 key。

| 字段 | 类型 | 用途 |
| --- | --- | --- |
| `udid` | 非空白 string | 设备标识，与 `start_id` 一起分组 |
| `start_id` | 64 位 integer | 启动周期的 Unix epoch 毫秒值 |
| `protol_type` | 非空白 string | 事件类型，沿用既定拼写；`app` 是元数据事件，其余类型均按非 `app` 处理 |
| `sequence` | integer | 启动周期内序号，用于选择 App 事件和输出排序 |
| `event_timestamp` | integer | Unix epoch 毫秒值，用作序号相同时的排序条件，不用于窗口分配 |
| `event_id` | string | 事件标识，用作最后的输出排序条件和下游幂等标识 |
| `payload` | object | 事件数据；非 `app` 事件保留原值 |
| `payload.self_package_name` | string | `app` 事件的当前应用包名 |
| `payload.apps` | array | `app` 事件的安装应用列表 |
| `payload.apps[].package_name` | string | 与当前应用包名精确匹配 |
| `payload.apps[].app_name` | string | 映射为顶层 `app_name` |
| `payload.apps[].version_name` | string | 映射为顶层 `app_version` |

JSON 无法解析、不是对象，或缺少有效的 `udid`、`start_id`、`protol_type` 时，作业抛出异常，由 Flink 的失败恢复策略处理。这类解析错误不写入 `flink_error`，也不会静默丢弃。发送器提供全部排序字段；若 `sequence` 或 `event_timestamp` 无法转换为整数，解析默认值为 `Long.MAX_VALUE`，若 `event_id` 缺失则默认为空字符串。

## 补全与路由规则

窗口关闭时，对同一业务 key 的事件执行以下规则：

1. 在本窗口的 `app` 事件中选择 `sequence` 最大的一条；序号相同则选择 `event_timestamp` 最大的一条。
2. 用选中记录的 `payload.self_package_name`，精确匹配同一记录 `payload.apps` 中的 `package_name`，取首个匹配元素。
3. 将匹配元素的 `app_name` 和 `version_name` 分别写入每条非 `app` 事件的顶层 `app_name` 和 `app_version`。保留其余 JSON 字段；已有的这两个顶层字段会被覆盖。不使用 `start.payload.app_version` 作为回退值。
4. 两个值都是非空字符串时，非 `app` 事件只写入成功 topic。没有 App、包名不匹配或任一元数据字段缺失、为空或非字符串时，无法取得的字段设为 JSON `null`，该非 `app` 事件只写入错误 topic；已取得的另一个字段仍保留。
5. 所有 `app` 事件保留输入原 JSON，只写入成功 topic，即使其元数据不完整也不进入错误 topic。

每条输入事件在一次窗口处理中只进入一个输出 topic，成功与错误路由互斥。各路输出按 `sequence`、`event_timestamp`、`event_id` 升序排列；两个 topic 之间没有统一消费顺序。

两个输出 topic 的每条 Kafka record key 都是从 JSON 顶层字段生成的 UTF-8 字符串 `<udid>:<start_id>`，例如 `device-1:1791614142000`，冒号为固定分隔符。这与 Flink 的组合分组 key 相同，保证同一启动周期在各自输出 topic 内进入同一分区。发送器输入 key 为 `udid`，输出不会沿用该 key。

## 窗口延迟与限制

窗口采用固定、互不重叠的 2 分钟处理时间滚动窗口。Flink 接收事件的处理时间决定窗口归属，`event_timestamp` 不决定窗口边界，也不使用 watermark。窗口内缓存事件，等关闭时一次性输出，因此正常运行时仅窗口等待就会带来最多 2 分钟的输出延迟；调度、背压、Kafka 写入和故障恢复还可能增加实际端到端延迟。

即使 `(udid, start_id)` 相同，跨滚动窗口边界的记录也不能相互补全。例如非 `app` 事件进入前一个窗口，`app` 进入下一个窗口，前一个窗口会将无法补全的非 `app` 事件写入错误 topic，不会事后更新。窗口关闭后由 Flink 自动清理窗口状态；作业不保留跨窗口 App 缓存。

Checkpoint 和两个 Kafka Sink 均使用至少一次语义，故障恢复可能重复输出事件。下游可使用 `event_id` 做幂等处理，路由互斥不代表精确一次投递。

## 验证

从仓库根目录执行：

```bash
mvn -f flink_processer/pom.xml clean verify
jar tf flink_processer/target/flink-processer-1.0.0.jar \
  | rg 'EventEnrichmentJob|OutputEventKafkaSerializer|org/apache/kafka'
```

自动化测试验证解析、默认参数、App 匹配、排序、互斥路由、输出 record key、窗口分配和作业拓扑；JAR 检查验证作业及 Kafka 依赖已打包。这些检查不替代真实 Flink/Kafka 集群联调。
