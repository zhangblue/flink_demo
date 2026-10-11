# Kafka Rust 客户端迁移实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 完全移除 `rdkafka`，统一使用 `kafka` 0.10.0，并通过专用阻塞工作线程在不阻塞 Tokio 网络任务的前提下保持发送顺序、背压、交付确认和关闭排空。

**架构：** Tokio dispatcher 将序列化后的事件非阻塞写入有界 FIFO 工作队列；单个标准线程独占同步 `kafka::producer::Producer`，依次发送并通过 oneshot 返回 broker 确认结果。配置层负责 broker 列表和压缩算法校验，真实集成测试改用 `kafka::consumer::Consumer`。

**技术栈：** Rust 2024、Tokio、`kafka` 0.10.0、serde/serde_json、标准库同步通道与线程、Cargo 测试。

---

## 文件结构

- 修改：`sender/Cargo.toml`、`sender/Cargo.lock` — 引入 `kafka` 0.10.0，最终删除 `rdkafka`/`rdkafka-sys`。
- 修改：`sender/src/config.rs` — broker 列表、压缩算法解析和配置错误。
- 修改：`sender/config.toml` — 默认压缩算法由 `lz4` 改为 `snappy`。
- 创建：`sender/src/kafka_worker.rs` — 同步 Producer、FIFO 工作线程、入队回执、屏障和关闭。
- 修改：`sender/src/lib.rs` — 注册内部 Kafka worker 模块。
- 修改：`sender/src/sink.rs` — `KafkaSink` 接入 worker，保留 `EventSink` 和 dispatcher 契约。
- 修改：`sender/src/runner.rs` — 把经过验证的工作队列容量传给 `KafkaSink`。
- 修改：`sender/tests/kafka_integration.rs` — 使用 `kafka` crate 完成真实 Kafka 往返验证。
- 修改：`README.md`、`sender/README.md` — 更新依赖、配置和非幂等发送语义。
- 生成但不提交：`runner/mobile-event-sender` — 更新 release 可执行文件。

### 任务 1：建立依赖与配置契约

**文件：**
- 修改：`sender/Cargo.toml`
- 修改：`sender/Cargo.lock`
- 修改：`sender/src/config.rs`
- 修改：`sender/config.toml`

- [ ] **步骤 1：先添加 broker 和压缩配置失败测试**

在 `sender/src/config.rs` 的测试模块加入：

```rust
#[test]
fn parses_trimmed_broker_hosts() {
    let mut config = valid_config();
    config.kafka.brokers = " kafka-a:9092, kafka-b:9092 ".into();
    assert_eq!(
        config.kafka.broker_hosts().unwrap(),
        ["kafka-a:9092".to_owned(), "kafka-b:9092".to_owned()]
    );
}

#[test]
fn rejects_empty_broker_item() {
    let mut config = valid_config();
    config.kafka.brokers = "kafka-a:9092,,kafka-b:9092".into();
    assert!(matches!(config.validate(), Err(ConfigError::EmptyBrokerItem)));
}

#[test]
fn accepts_supported_compression_values() {
    for (raw, expected) in [
        ("none", KafkaCompression::None),
        ("gzip", KafkaCompression::Gzip),
        ("snappy", KafkaCompression::Snappy),
    ] {
        let mut config = valid_config();
        config.kafka.compression = raw.into();
        assert_eq!(config.kafka.compression_kind().unwrap(), expected);
        assert!(config.validate().is_ok());
    }
}

#[test]
fn rejects_unsupported_compression() {
    let mut config = valid_config();
    config.kafka.compression = "lz4".into();
    assert!(matches!(
        config.validate(),
        Err(ConfigError::UnsupportedCompression { value }) if value == "lz4"
    ));
}
```

- [ ] **步骤 2：运行测试确认红灯**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml config::tests::parses_trimmed_broker_hosts
cargo test --manifest-path sender/Cargo.toml config::tests::accepts_supported_compression_values
```

预期：编译失败，指出 `KafkaCompression`、`broker_hosts` 和 `compression_kind` 尚不存在。

- [ ] **步骤 3：加入 `kafka` 依赖并定义配置类型**

在 `sender/Cargo.toml` 中加入：

```toml
kafka = { version = "0.10.0", default-features = false, features = ["gzip", "snappy"] }
```

本任务暂时保留 `rdkafka`，因为真实集成测试仍使用它；任务 3 替换测试后删除。

在 `sender/src/config.rs` 定义：

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaCompression {
    None,
    Gzip,
    Snappy,
}
```

新增错误：

```rust
#[error("kafka.brokers contains an empty broker item")]
EmptyBrokerItem,
#[error("kafka.compression must be one of none, gzip, snappy; got {value}")]
UnsupportedCompression { value: String },
```

为 `KafkaConfig` 增加：

```rust
pub fn broker_hosts(&self) -> Result<Vec<String>, ConfigError> {
    let hosts: Vec<_> = self.brokers.split(',').map(str::trim).collect();
    if hosts.is_empty() || hosts.iter().any(|host| host.is_empty()) {
        return Err(ConfigError::EmptyBrokerItem);
    }
    Ok(hosts.into_iter().map(str::to_owned).collect())
}

pub fn compression_kind(&self) -> Result<KafkaCompression, ConfigError> {
    match self.compression.as_str() {
        "none" => Ok(KafkaCompression::None),
        "gzip" => Ok(KafkaCompression::Gzip),
        "snappy" => Ok(KafkaCompression::Snappy),
        value => Err(ConfigError::UnsupportedCompression { value: value.to_owned() }),
    }
}
```

`AppConfig::validate` 在非空字符串检查后调用这两个方法。保留原 `EmptyBrokers`
对完全为空或全空白字符串的错误语义。

- [ ] **步骤 4：修改默认配置**

把 `sender/config.toml` 的 `compression = "lz4"` 改为：

```toml
compression = "snappy"
```

- [ ] **步骤 5：运行配置测试和依赖检查**

```bash
cargo test --manifest-path sender/Cargo.toml config::tests
cargo tree --manifest-path sender/Cargo.toml -i kafka
git diff --check
```

预期：配置测试全部通过；依赖树显示 `kafka v0.10.0`；默认配置有效。

- [ ] **步骤 6：提交配置基础**

```bash
git add sender/Cargo.toml sender/Cargo.lock sender/src/config.rs sender/config.toml
git commit -m "feat(Kafka): 增加 kafka crate 配置契约"
```

### 任务 2：实现同步 Kafka 工作线程并迁移生产发送

**文件：**
- 创建：`sender/src/kafka_worker.rs`
- 修改：`sender/src/lib.rs`
- 修改：`sender/src/sink.rs`
- 修改：`sender/src/runner.rs`

- [ ] **步骤 1：编写工作线程的失败测试**

在新文件 `sender/src/kafka_worker.rs` 中先建立测试模块。测试用
`FakeProducer` 实现内部 `BlockingProducer` trait，并覆盖：

```rust
#[test]
fn worker_sends_in_fifo_order_and_barrier_waits_for_prior_messages() {
    // 固定入队 key-1、key-2，等待两个回执和 barrier；记录必须严格同序。
}

#[test]
fn bounded_worker_queue_reports_full() {
    // 用同步闸门阻塞第一次 send；第一条发送中、第二条占队列时，第三条返回 Full。
}

#[test]
fn delivery_error_reaches_ack() {
    // FakeProducer 返回 injected send failure；回执必须保留错误文本。
}

#[test]
fn startup_error_is_returned_to_caller() {
    // factory 返回 injected startup failure；start 必须同步返回错误。
}

#[test]
fn disconnected_worker_is_visible() {
    // worker 退出后再次入队，必须返回 Disconnected 而不是悬挂。
}
```

用通道或 Condvar 控制线程状态，禁止依赖 sleep 猜测调度。

- [ ] **步骤 2：运行工作线程测试确认红灯**

```bash
cargo test --manifest-path sender/Cargo.toml kafka_worker::tests -- --nocapture
```

预期：编译失败，因为 worker 类型和方法尚未实现。

- [ ] **步骤 3：实现专用工作线程**

在 `sender/src/lib.rs` 注册：

```rust
mod kafka_worker;
```

内部接口：

```rust
pub(crate) trait BlockingProducer: Send + 'static {
    fn send(&mut self, topic: &str, key: &str, value: &str) -> Result<(), String>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkerStartError {
    #[error("failed to create Kafka producer: {0}")]
    Producer(String),
    #[error("Kafka worker exited during startup")]
    Disconnected,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WorkerEnqueueError { Full, Disconnected }

#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkerBarrierError {
    #[error("Kafka worker queue is full")]
    Full,
    #[error("Kafka worker is unavailable")]
    Disconnected,
    #[error("Kafka worker barrier timed out")]
    Timeout,
}
```

命令：

```rust
enum Command {
    Send {
        topic: String,
        key: String,
        value: String,
        ack: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Barrier(std::sync::mpsc::Sender<()>),
    Shutdown,
}
```

`WorkerHandle::start` 签名：

```rust
pub(crate) fn start<F>(capacity: usize, factory: F) -> Result<Self, WorkerStartError>
where
    F: FnOnce() -> Result<Box<dyn BlockingProducer>, String> + Send + 'static;
```

实现要求：使用 `sync_channel(capacity)`；容量必须大于零；worker 内运行 factory
并通过启动握手返回结果；`try_send` 返回 oneshot receiver；循环串行处理命令；
`barrier(timeout)` 使用 `try_send` 与 `recv_timeout`；Drop 发送 Shutdown 并 join；
worker panic 或回执 sender 丢失必须成为可见错误。

- [ ] **步骤 4：实现真实 kafka Producer adapter**

```rust
struct KafkaProducer { inner: kafka::producer::Producer }

impl BlockingProducer for KafkaProducer {
    fn send(&mut self, topic: &str, key: &str, value: &str) -> Result<(), String> {
        self.inner
            .send(&kafka::producer::Record::from_key_value(topic, key, value))
            .map_err(|error| error.to_string())
    }
}
```

生产 factory 必须在 worker 内构造：

```rust
Producer::from_hosts(hosts)
    .with_required_acks(kafka::client::RequiredAcks::All)
    .with_ack_timeout(message_timeout)
    .with_compression(match compression {
        KafkaCompression::None => kafka::client::Compression::NONE,
        KafkaCompression::Gzip => kafka::client::Compression::GZIP,
        KafkaCompression::Snappy => kafka::client::Compression::SNAPPY,
    })
    .create()
```

- [ ] **步骤 5：把 KafkaSink 接到 worker**

构造函数改为：

```rust
pub fn new(config: &KafkaConfig, queue_capacity: usize) -> Result<Self, SinkError>
```

`KafkaSink` 只保存 worker、topic 和 timeout。`SinkError` 删除 rdkafka 错误依赖，加入：

```rust
#[error("failed to start Kafka worker: {0}")]
WorkerStart(String),
#[error("Kafka worker is unavailable")]
WorkerUnavailable,
#[error("Kafka worker barrier timed out")]
FlushTimeout,
```

`enqueue` 将 Full 映射到 `QueueFull`、Disconnected 映射到 `WorkerUnavailable`；ack
把同步发送错误映射成 `Delivery`。`flush` 使用 FIFO barrier。`runner.rs` 改为：

```rust
let sink = KafkaSink::new(&config.kafka, config.simulation.channel_capacity)?;
```

- [ ] **步骤 6：运行 worker、sink 和 runner 测试**

```bash
cargo test --manifest-path sender/Cargo.toml kafka_worker::tests
cargo test --manifest-path sender/Cargo.toml sink::tests
cargo test --manifest-path sender/Cargo.toml runner::tests
cargo clippy --manifest-path sender/Cargo.toml --lib --tests -- -D warnings
```

预期：全部通过且不连接真实 Kafka；原有限速、QueueFull 截止时间、错误计数和关闭排空
测试保持通过。

- [ ] **步骤 7：提交生产发送迁移**

```bash
git add sender/src/kafka_worker.rs sender/src/lib.rs sender/src/sink.rs sender/src/runner.rs
git commit -m "refactor(Kafka): 使用同步工作线程发送消息"
```

### 任务 3：迁移真实 Kafka 测试、删除 rdkafka 并更新交付物

**文件：**
- 修改：`sender/tests/kafka_integration.rs`
- 修改：`sender/Cargo.toml`
- 修改：`sender/Cargo.lock`
- 修改：`README.md`
- 修改：`sender/README.md`

- [ ] **步骤 1：先替换集成测试 imports 和生产者配置**

删除 rdkafka imports，改为：

```rust
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
```

使用：

```rust
let kafka_config = KafkaConfig {
    brokers: brokers.clone(),
    topic: topic.clone(),
    compression: "snappy".to_owned(),
    message_timeout_ms: 5_000,
};
let hosts = kafka_config.broker_hosts().expect("parse broker hosts");
let sink = KafkaSink::new(&kafka_config, 4).expect("create test producer");
```

- [ ] **步骤 2：运行集成测试编译确认红灯**

```bash
cargo test --manifest-path sender/Cargo.toml --test kafka_integration --no-run
```

预期：编译失败，指向旧 `StreamConsumer`、`recv`、Message trait 或 offset API。

- [ ] **步骤 3：使用同步 Consumer 重写消费逻辑**

测试保持 `#[ignore]`，可改为普通 `#[test]`。在发送前创建：

```rust
let mut consumer = Consumer::from_hosts(hosts)
    .with_topic(topic.clone())
    .with_group(format!("mobile-event-sender-test-{}", Uuid::new_v4()))
    .with_fallback_offset(FetchOffset::Latest)
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .with_fetch_max_wait_time(Duration::from_millis(200))
    .create()
    .expect("create test consumer");
```

consumer 必须在发送前创建。用 `futures::executor::block_on` 等待每个 delivery ack。
轮询用 `std::time::Instant` 的 45 秒截止时间；遍历 `messagesets.iter()`，读取
partition、offset、key、value，过滤本轮 udid/event_id；标记每个 messageset 已消费并
调用 `commit_consumed`。保留顺序、key、partition、offset、完整 JSON 和
`self_package_name` 的所有现有断言。

- [ ] **步骤 4：删除 rdkafka 并检查锁文件**

从 `sender/Cargo.toml` 删除 rdkafka 依赖，然后运行：

```bash
cargo check --manifest-path sender/Cargo.toml --all-targets
! rg -n "rdkafka|rdkafka-sys" sender/Cargo.toml sender/Cargo.lock sender/src sender/tests
```

预期：全部 target 编译，搜索无匹配。

- [ ] **步骤 5：更新 README**

根 README 和 sender README 均需：删除 librdkafka 安装要求；说明使用纯 Rust
`kafka` crate 和专用工作线程；把 `lz4` 改成 `snappy`；列出合法压缩值
`none/gzip/snappy`；说明 `acks=all` 不提供幂等生产，异常重试可能重复，消费者以
`event_id` 去重。事件 JSON 与 Flink schema 不变。

- [ ] **步骤 6：完整静态检查和普通测试**

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
git diff --check
```

预期：全部退出码为 0；真实 Kafka 测试默认 ignored；普通测试无失败。

- [ ] **步骤 7：运行真实 Kafka 往返测试**

```bash
KAFKA_BROKERS=192.168.250.106:9092 \
KAFKA_TOPIC=flink_demo \
cargo test --manifest-path sender/Cargo.toml --test kafka_integration -- --ignored --nocapture
```

预期：`1 passed; 0 failed`，四条消息的内容和顺序断言全部成立。

- [ ] **步骤 8：刷新 runner 二进制**

```bash
cargo build --manifest-path sender/Cargo.toml --release
cp sender/target/release/mobile-event-sender runner/mobile-event-sender
shasum -a 256 sender/target/release/mobile-event-sender runner/mobile-event-sender
```

预期：两个 SHA-256 相同，runner 仍被 Git 忽略。

- [ ] **步骤 9：提交最终迁移**

```bash
git add sender/Cargo.toml sender/Cargo.lock sender/tests/kafka_integration.rs README.md sender/README.md
git commit -m "refactor(Kafka): 完成 kafka crate 迁移"
```

- [ ] **步骤 10：最终核对**

```bash
git status --short
git log --oneline -5
! rg -n "rdkafka|rdkafka-sys|librdkafka|compression = \"lz4\"" \
  README.md sender/Cargo.toml sender/Cargo.lock sender/config.toml sender/src sender/tests sender/README.md
```

预期：工作区干净；源码、测试、锁文件和文档中不再引用旧客户端或旧默认压缩值。
