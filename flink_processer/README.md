# Flink Kafka common 状态补全作业

`flink_processer` 从 Kafka 读取手机事件，按 `(udid, start_id)` 分组。首条完整的 `common` 建立该启动周期的状态；后续非 `common` 事件在收到时立即补全六个顶层字段。成功事件写入 `flink_output`，缺少完整状态或 `common` 元数据不完整的事件写入 `flink_error`。

## 环境与构建

- Java 17（本工程按 Java 17 编译）。
- Maven 3.9.x。
- 已启动且可提交作业的 Apache Flink 2.3.0 集群，`flink` 命令位于 `PATH`。
- Flink 集群可访问 Kafka；输入、成功输出、错误输出三个 topic 必须互不相同，并具有相应读写权限。

从仓库根目录构建：

```bash
mvn -f flink_processer/pom.xml clean package
```

也可在本目录执行 `mvn clean package`。产物为 `flink_processer/target/flink-processer-1.0.0.jar`，是包含 Kafka connector/client 和 JSON 依赖的 fat JAR；Flink runtime 核心依赖由集群提供。工程使用 Flink 2.3.0 和 Kafka connector `5.0.0-2.2`。

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

`--detached` 和 `-c` 是 Flink CLI 参数，JAR 路径之后的是作业参数。入口类也已写入 JAR manifest，因此默认示例可省略 `-c`。提交语法参考 [Flink 2.3 CLI 文档](https://github.com/apache/flink/blob/release-2.3.0/docs/content/docs/deployment/cli.md)。

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
| `--checkpoint-storage-uri` | `file:///home/zhangdi/workspace/flink_data/checkpoints` | Checkpoint 本地存储目录，必须是绝对 `file:` URI |
| `--savepoint-storage-uri` | `file:///home/zhangdi/workspace/flink_data/savepoints` | Savepoint 本地存储目录，必须是绝对 `file:` URI |

字符串参数不能留空，三个 topic 不能相同。新消费任务默认优先从 consumer group 已提交的 offset 开始；没有已提交 offset 时从最早位置开始。恢复 checkpoint/savepoint 时，恢复状态中记录的 Kafka source offset 优先，consumer group 已提交 offset 和此初始 offset 策略只用于没有可恢复 source 状态的新启动。新作业处理算子的 UID 为 `common-state-enrichment`，与旧窗口算子的 UID 不同；旧 savepoint 中的窗口状态不能迁移到新算子。升级时应从新部署启动，或在确认旧状态可以丢弃后使用 Flink 的未恢复状态选项。

## 事件时间与状态清理

作业解析输入时检查 `event_timestamp`。只有时间戳严格晚于解析时刻 5 秒以上（`event_timestamp > 当前时间 + 5000 毫秒`）才会被校正为当前时间；恰好晚 5 秒不校正。校正后的时间戳也会写回事件 JSON，保证水位线和输出记录使用一致的值。水位线允许 5 秒乱序；Kafka 分区连续 1 分钟没有记录后会被标记为空闲，避免空闲分区持续拖住其他分区的水位线。

每个 `(udid, start_id)` 的状态清理截止时间是该 key 已观察到的最大事件时间加 30 秒。后续时间较早的事件不会把截止时间往回移动。清理通过事件时间定时器触发，所以需要水位线越过截止时间。分区空闲策略只处理部分分区无数据的情况；如果全流停滞，没有新事件推动水位线，事件时间清理也会停住，已过期状态会暂时保留，直到流量恢复并推进水位线。

## Checkpoint 与恢复

默认本地存储目录为 Ubuntu 主机上的 `file:///home/zhangdi/workspace/flink_data/checkpoints` 和 `file:///home/zhangdi/workspace/flink_data/savepoints`。提交作业的 Flink 用户需要对这些目录有读写权限；其他环境可通过 `--checkpoint-storage-uri` 和 `--savepoint-storage-uri` 指定绝对本地 `file:` URI。作业每 60 秒 checkpoint，并设置 `RETAIN_ON_CANCELLATION`，取消作业时保留外部化 checkpoint。

从保留的 checkpoint 恢复时，将 `_metadata` 文件路径传给 Flink CLI 的 `-s`：

```bash
flink run --detached \
  -s file:///home/zhangdi/workspace/flink_data/checkpoints/checkpoint-directory/_metadata \
  flink_processer/target/flink-processer-1.0.0.jar
```

该 source 与两个 Kafka sink 都是至少一次语义。失败恢复可能重放 checkpoint 之后已处理或已写出的记录，造成 Kafka 输出重复；`event_id` 可供下游幂等去重，不能据此认为端到端精确一次。

## 输入字段

每条 Kafka value 是 UTF-8 JSON 对象，完整数据契约见 [sender 文档](../sender/README.md)。发送器的输入 Kafka record key 是 `udid`；Flink 不使用该 record key 分组，而是从 JSON 顶层的 `udid` 和 `start_id` 生成组合业务 key。

| 字段 | 类型 | 用途 |
| --- | --- | --- |
| `udid` | 非空白 string | 设备标识，与 `start_id` 一起分组 |
| `start_id` | 64 位 integer | 启动周期的 Unix epoch 毫秒值 |
| `protol_type` | 非空白 string | 事件类型，沿用既定拼写；只有 `common` 更新状态，其余类型（包括 `app`）按非 `common` 事件路由 |
| `sequence` | integer | 启动周期内序号，由发送器提供；不参与补全选择 |
| `event_timestamp` | 必填 64 位 integer | Unix epoch 毫秒值，由发送器提供；参与事件时间水位线和每 key 状态清理截止时间计算 |
| `event_id` | string | 事件标识，供下游幂等处理 |
| `payload` | object | 事件类型对应的数据；输出保留原 `payload` |

JSON 无法解析、不是对象，或缺少有效的 `udid`、`start_id`、`protol_type`、`event_timestamp` 时，作业抛出异常，由 Flink 的失败恢复策略处理。这类解析错误不写入 `flink_error`，也不会静默丢弃。`event_timestamp` 必须是可转换为 64 位整数的 JSON 整数；缺失、非整数或超出 64 位范围都会导致解析失败，不会默认成 `Long.MAX_VALUE`。缺少或无法转换的 `sequence` 使用 `Long.MAX_VALUE`；缺少 `event_id` 则默认为空字符串。

`common.payload` 必须包含下表六个非空白字符串；完整字段定义见 [sender 文档](../sender/README.md)。补全会将它们写入非 `common` 输出事件的同名顶层字段，并覆盖事件原有的同名顶层值。

| `common.payload` 字段 | 输出顶层字段 |
| --- | --- |
| `self_app_package` | `self_app_package` |
| `self_app_name` | `self_app_name` |
| `model` | `model` |
| `brand` | `brand` |
| `os` | `os` |
| `os_version` | `os_version` |

## 补全与路由规则

对同一 `(udid, start_id)`，每条消息按到达时的状态处理：

1. 完整 `common` 只更新六字段状态，不进入 `flink_output` 或 `flink_error`。后续完整 `common` 可更新该 key 的状态。
2. 不完整 `common` 只进入 `flink_error`，缺失、非字符串或空白的字段在其输出顶层为 JSON `null`；它不会覆盖此前保存的完整状态。
3. 任意非 `common` 事件（包括 `app`）在收到时立即处理。有完整状态时将六字段补全至顶层，只进入 `flink_output`；没有状态时六个顶层字段均为 JSON `null`，只进入 `flink_error`。不会等待未来的 `common` 再回填已输出事件。

成功与错误路由互斥。作业不再从 `app.payload.self_package_name` 或 `app.payload.apps[]` 提取 `app_name`、`app_version`；发送器仍可生成这些原始 `app` 数据。

两个输出 topic 的每条 Kafka record key 都是从 JSON 顶层字段生成的 UTF-8 字符串 `<udid>_<start_id>`，例如 `device-1_1791614142000`，下划线为固定分隔符。同一启动周期的输出在各自 topic 内进入同一分区。输入 record key 仍为发送器写入的 `udid`，输出不会沿用它。

## 状态清理与限制

每个组合 key 的状态清理截止时间为已观察到的最大 `event_timestamp` 加 30 秒。较早时间戳的后续记录不会缩短截止时间；水位线越过截止时间时，定时器清除 common 元数据状态和清理计时状态。分区 idleness 不能推进全流停滞时的水位线，因此全流停滞会延迟清理。状态清理后到达的非 `common` 事件按无状态规则进入错误 topic。

Checkpoint 和两个 Kafka Sink 均使用至少一次语义，故障恢复可能重复输出事件。下游可使用 `event_id` 做幂等处理，路由互斥不代表精确一次投递。

## 验证

从仓库根目录执行：

```bash
mvn -f flink_processer/pom.xml clean verify
git diff --check
jar tf flink_processer/target/flink-processer-1.0.0.jar \
  | rg 'EventRecordKafkaDeserializer|EventTimePolicy|CommonStateEnrichmentFunction|org/apache/kafka'
```

自动化测试验证解析、默认参数、common 元数据提取、状态补全与路由、输出 record key 和作业拓扑；JAR 检查验证事件时间解析类、策略类、状态函数及 Kafka 依赖已打包。这些检查不替代真实 Flink/Kafka 集群联调；本文档对应的是本地构建验证，不表示已在远端 Flink/Kafka 集群验证。
