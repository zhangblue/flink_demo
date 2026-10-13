# Flink Demo：手机事件模拟发送器

本仓库包含 Rust/Tokio 手机 App 事件模拟发送器和 Java/Flink 事件补全作业。发送器持续生成多台 Android/iOS 设备的启动与运行数据并写入 Kafka；Flink 作业由首条完整 `common` 建立状态，在收到后续事件时即时补全，并将成功与错误事件分别写入 Kafka。

## 核心能力

- 同一设备在进程生命周期内保持固定的 `udid`。
- 每次 App 启动使用启动时刻的 Unix 毫秒值作为 `start_id`，该周期内的所有事件共享此 ID。
- 每个启动周期严格按 `common → start → devinfo → environment/app（随机顺序）` 发送五条事件。
- 每次启动从设备安装列表随机选择当前 App；`start.payload.app_package` 与
  `app.payload.self_package_name` 在同一启动周期内保持一致。
- 多设备通过 Tokio 并发运行，所有设备共享一个可配置的全局发送速率。
- 发送器写入输入 topic 时，Kafka record key 使用 `udid`，确保同一设备的事件进入同一分区并保持顺序。
- JSON 只使用 Unix 毫秒时间戳字段 `event_timestamp`，不包含 `event_time`。
- 支持优雅关闭、发送队列背压、投递确认和 QueueFull 限时重试。

## 项目结构

```text
.
├── README.md
├── docs/
│   └── superpowers/        # 设计方案与实现计划
├── flink_processer/
│   ├── pom.xml             # Java 17 / Flink 2.2 Maven 工程
│   ├── README.md           # 构建、运行参数与补全规则
│   └── src/                # Kafka 作业实现与测试
└── sender/
    ├── Cargo.toml
    ├── config.toml         # 运行配置
    ├── README.md           # 完整 JSON 数据契约
    ├── src/                # 生成、限速与 Kafka 发送实现
    └── tests/              # Kafka 集成测试
```

## 快速开始

### 环境要求

- Rust stable 工具链
- 可访问的 Kafka 集群
- 系统安装 `librdkafka`

### 默认配置

默认配置文件为 [`sender/config.toml`](sender/config.toml)：

```toml
[kafka]
brokers = "192.168.250.106:9092"
topic = "flink_demo"
compression = "lz4"
message_timeout_ms = 5000

[simulation]
device_count = 100
messages_per_second = 500
startup_interval_min_secs = 30
startup_interval_max_secs = 120
installed_apps_min = 20
installed_apps_max = 80
channel_capacity = 10000
```

`messages_per_second` 是所有设备共享的全局速度，而不是单台设备的速度。

### 构建与运行

从项目根目录执行：

```bash
cargo build --manifest-path sender/Cargo.toml --release
cargo run --manifest-path sender/Cargo.toml --release -- --config sender/config.toml
```

设置日志级别：

```bash
RUST_LOG=debug cargo run --manifest-path sender/Cargo.toml -- --config sender/config.toml
```

按 `Ctrl+C` 后，程序停止创建新的启动周期，排空已进入发送流程的消息并退出。

### 构建 Flink 补全作业

安装 Java 17、Maven 和 Flink 2.2.0 后，从项目根目录执行：

```bash
mvn -f flink_processer/pom.xml clean package
flink run --detached flink_processer/target/flink-processer-1.0.0.jar
```

提交前需启动 Flink 集群并确保其可访问 Kafka。作业默认消费 `flink_demo`：完整 `common` 仅更新状态，不进入输出 topic；不完整 `common` 只进入 `flink_error`，且不覆盖旧状态；包括 `app` 在内的非 `common` 事件收到时立即补全，有完整状态只进入 `flink_output`，无状态时六个顶层字段为 `null`、只进入 `flink_error`。两个输出 topic 的 record key 均为 `<udid>_<start_id>`（如 `device-1_1791614142000`），与发送器输入 record key `udid` 不同。全部参数、输入字段和状态清理规则见 [Flink 补全作业使用说明](flink_processer/README.md)。

## 事件数据

每条 Kafka value 是一个 JSON 对象，公共字段如下：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `event_id` | string | 单条事件的 UUID v4 |
| `protol_type` | string | `common`、`start`、`devinfo`、`environment` 或 `app` |
| `udid` | string | 设备 UUID v4，同一设备固定 |
| `start_id` | integer | 启动周期开始时的 Unix epoch 毫秒值，同一周期固定；Flink 使用 `BIGINT` |
| `event_timestamp` | integer | 单条消息生成时的 Unix epoch 毫秒值，与 `start_id` 独立 |
| `sequence` | integer | 启动周期内从 1 开始递增的序号 |
| `payload` | object | 对应事件类型的数据对象 |

字段名 `protol_type` 是既定数据契约的拼写。五种 `payload` 的字段定义、取值范围、完整 JSON 示例和 Flink 消费约定见 [`sender/README.md`](sender/README.md)。

## 事件顺序

一次启动周期固定包含 5 条消息：

```text
common → start → devinfo → environment/app（随机顺序）
```

不同设备的消息可以交错，也可能具有相同的毫秒 `start_id`。下游应按 `udid` 与 `start_id` 组合分组，并依据 `sequence` 还原周期内顺序。多个事件可能在同一毫秒生成，因此不要使用 `event_timestamp` 代替 `sequence` 排序。

## 验证

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
cargo build --manifest-path sender/Cargo.toml --release
mvn -f flink_processer/pom.xml clean verify
```

真实 Kafka 集成测试默认被忽略，可显式执行：

```bash
KAFKA_BROKERS=192.168.250.106:9092 \
KAFKA_TOPIC=flink_demo \
cargo test --manifest-path sender/Cargo.toml --test kafka_integration -- --ignored --nocapture
```

该命令会向指定 topic 写入并消费一个包含 5 条事件的完整启动周期。

## 详细文档

- [发送器配置与完整数据契约](sender/README.md)
- [Flink 补全作业使用说明](flink_processer/README.md)
- [Flink 事件补全设计](docs/superpowers/specs/2026-10-12-flink-kafka-event-enrichment-design.md)
- [系统设计方案](docs/superpowers/specs/2026-10-10-mobile-event-sender-design.md)
- [实现计划](docs/superpowers/plans/2026-10-10-mobile-event-sender.md)
