# 手机 App 模拟数据发送器实现计划

> **数据契约迁移说明：** 本计划记录初始 UUID `start_id` 实现。当前契约已由 `docs/superpowers/specs/2026-10-11-millisecond-start-id-design.md` 和 `docs/superpowers/plans/2026-10-11-millisecond-start-id.md` 更新为 Unix 毫秒整数；涉及 `start_id` 的旧代码片段仅作历史记录。

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在 `sender/` 中实现一个基于 Tokio 的多设备手机 App 数据模拟器，按全局可配置速率将满足固定 JSON 契约和启动顺序的消息写入 Kafka。

**架构：** 每个模拟设备由独立 Tokio 任务维护固定身份和启动周期，通过有界 `mpsc` channel 向单一调度器提交事件。调度器使用 Tokio interval 进行全局限速，并按接收顺序将以 `udid` 为 key 的 JSON record 入队 rust-rdkafka `FutureProducer`，同时异步跟踪投递确认。配置、数据契约、生成器、限速器、Kafka sink 和运行编排保持独立，便于单元测试。

**技术栈：** Rust 2024、Tokio 1.x、rust-rdkafka 0.38、Serde/serde_json、TOML、Clap、UUID、rand、tracing、futures

---

## 文件结构

- `sender/Cargo.toml`：crate 元数据、运行依赖与测试特性。
- `sender/config.toml`：可直接运行的默认配置，连接 `192.168.250.106:9092/flink_demo`。
- `sender/src/lib.rs`：导出各职责模块。
- `sender/src/config.rs`：TOML 配置类型、读取与完整校验。
- `sender/src/model.rs`：公共消息信封、四种 payload 和序列化契约。
- `sender/src/generator.rs`：设备档案、稳定身份、启动周期及合理随机数据生成。
- `sender/src/rate.rs`：全局 Tokio interval 限速器。
- `sender/src/sink.rs`：可测试的 `EventSink` 边界、Kafka 实现和投递调度器。
- `sender/src/runner.rs`：设备任务生命周期、channel、关闭信号和排空逻辑。
- `sender/src/main.rs`：CLI、日志初始化、配置加载和 Ctrl+C 入口。
- `sender/tests/kafka_integration.rs`：默认忽略的真实 Kafka 生产/消费顺序测试。
- `sender/README.md`：运行方式、配置以及供 Flink 使用的完整 JSON 数据契约。

## 参考 API 决策

Context7 当前文档确认：

- Tokio 有界 `mpsc` 在所有 sender clone 被释放后，receiver 可通过 `while let Some(...)` 排空；适合优雅退出。
- `tokio::time::interval` 支持 `MissedTickBehavior::Delay`，避免调度停顿后突发补发；`test-util` 支持暂停时间测试。
- `FutureProducer::send_result` 可同步按调用顺序将 record 入队并返回投递 future；因此调度器无需等待每条 Kafka ack 就能维持设定吞吐，同时同一 key 的入队顺序保持稳定。
- `FutureProducer::flush` 用于关闭前等待待发送消息，且必须处理超时或 flush 错误。

### 任务 1：建立 crate 与配置边界

**文件：**
- 创建：`sender/Cargo.toml`
- 创建：`sender/config.toml`
- 创建：`sender/src/lib.rs`
- 创建：`sender/src/config.rs`

- [ ] **步骤 1：建立最小 crate 清单**

创建 `sender/Cargo.toml`，使用以下依赖边界：

```toml
[package]
name = "mobile-event-sender"
version = "0.1.0"
edition = "2024"

[dependencies]
anyhow = "1"
clap = { version = "4", features = ["derive"] }
futures = "0.3"
rand = "0.9"
rdkafka = { version = "0.38", features = ["cmake-build", "tokio"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "sync", "time"] }
toml = "0.9"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
uuid = { version = "1", features = ["serde", "v4"] }

[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt", "sync", "test-util", "time"] }
```

`sender/src/lib.rs` 先只声明 `pub mod config;`。

- [ ] **步骤 2：先写配置校验失败测试**

在 `sender/src/config.rs` 的测试模块定义一个有效配置 helper，并依次添加最小测试：

```rust
#[test]
fn rejects_zero_device_count() {
    let mut config = valid_config();
    config.simulation.device_count = 0;
    assert!(matches!(config.validate(), Err(ConfigError::ZeroDeviceCount)));
}

#[test]
fn rejects_reversed_startup_interval() {
    let mut config = valid_config();
    config.simulation.startup_interval_min_secs = 121;
    config.simulation.startup_interval_max_secs = 120;
    assert!(matches!(config.validate(), Err(ConfigError::InvalidStartupInterval { .. })));
}

#[test]
fn rejects_reversed_app_count_range() {
    let mut config = valid_config();
    config.simulation.installed_apps_min = 81;
    config.simulation.installed_apps_max = 80;
    assert!(matches!(config.validate(), Err(ConfigError::InvalidAppCountRange { .. })));
}

#[test]
fn parses_complete_toml() {
    let config: AppConfig = toml::from_str(include_str!("../config.toml")).unwrap();
    config.validate().unwrap();
    assert_eq!(config.kafka.topic, "flink_demo");
}
```

同文件还要覆盖空 `brokers`、空 `topic`、零 `message_timeout_ms`、零发送速率、零启动间隔、零 App 数量和零 channel capacity。

- [ ] **步骤 3：运行测试并确认因配置类型缺失而失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml config::tests -- --nocapture
```

预期：编译失败，提示 `AppConfig`、`ConfigError` 或 `validate` 尚未定义；失败必须来自待实现行为，而不是 TOML 拼写错误。

- [ ] **步骤 4：实现最小配置类型与校验**

实现以下公开接口：

```rust
#[derive(Clone, Debug, serde::Deserialize)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub simulation: SimulationConfig,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub compression: String,
    pub message_timeout_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct SimulationConfig {
    pub device_count: usize,
    pub messages_per_second: u64,
    pub startup_interval_min_secs: u64,
    pub startup_interval_max_secs: u64,
    pub installed_apps_min: usize,
    pub installed_apps_max: usize,
    pub channel_capacity: usize,
}

impl AppConfig {
    pub fn from_path(path: &std::path::Path) -> Result<Self, ConfigLoadError>;
    pub fn validate(&self) -> Result<(), ConfigError>;
}
```

`from_path` 只负责读取、TOML 反序列化、调用 `validate` 并保留路径上下文。错误类型使用 `thiserror`，每种非法输入返回可定位字段的明确错误。

- [ ] **步骤 5：写入并验证默认配置**

创建 `sender/config.toml`：

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

运行前述配置测试，预期全部通过。

- [ ] **步骤 6：Commit**

若项目已成为 Git 仓库：

```bash
git add sender/Cargo.toml sender/config.toml sender/src/lib.rs sender/src/config.rs
git commit -m "feat(sender): add validated simulator configuration"
```

若仍非 Git 仓库，记录无法提交并继续，不擅自初始化仓库。

### 任务 2：定义并锁定 JSON 数据契约

**文件：**
- 创建：`sender/src/model.rs`
- 修改：`sender/src/lib.rs`

- [ ] **步骤 1：先写四类序列化契约测试**

每个测试构造固定值事件并转为 `serde_json::Value`。以 `start` 为例：

```rust
#[test]
fn start_event_serializes_with_stable_contract() {
    let event = fixture_start_event();
    let value = serde_json::to_value(event).unwrap();

    assert_eq!(value["protol_type"], "start");
    assert_eq!(value["event_timestamp"], 1_791_614_142_381_i64);
    assert!(value.get("event_time").is_none());
    assert_eq!(value["sequence"], 1);
    assert_eq!(value["payload"]["app_package"], "com.example.demo");
    assert_eq!(value["payload"]["app_build"], 30_801);
    assert!(value["payload"]["foreground"].is_boolean());
}
```

另外三个测试逐字段断言设计文档中的全部字段：

- `devinfo`：13 个 payload 字段；
- `environment`：12 个 payload 字段；
- `app`：`collected_at`、`apps` 以及每个 App 的 7 个字段。

测试同时断言顶层 key 集合严格等于 `event_id/protol_type/udid/start_id/event_timestamp/sequence/payload`，防止误加 `event_time`。

- [ ] **步骤 2：运行测试并确认模型缺失导致失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml model::tests -- --nocapture
```

预期：编译失败，提示事件和 payload 类型未定义。

- [ ] **步骤 3：实现公共信封和四类 payload**

使用强类型枚举而不是自由 `serde_json::Value`：

```rust
#[derive(Clone, Debug, serde::Serialize)]
pub struct MobileEvent {
    pub event_id: uuid::Uuid,
    pub protol_type: ProtocolType,
    pub udid: uuid::Uuid,
    pub start_id: uuid::Uuid,
    pub event_timestamp: i64,
    pub sequence: u32,
    pub payload: EventPayload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolType { Start, Devinfo, Environment, App }

#[derive(Clone, Debug, serde::Serialize)]
#[serde(untagged)]
pub enum EventPayload {
    Start(StartPayload),
    Devinfo(DevinfoPayload),
    Environment(EnvironmentPayload),
    App(AppPayload),
}
```

为四类 payload 与 `InstalledApp` 定义设计文档中的全部字段及对应 Rust 类型：字符串使用 `String`，时间用 `i64`，计数/容量用 `u32` 或 `u64`，布尔值用 `bool`，列表用 `Vec<InstalledApp>`。提供只读 helper `MobileEvent::kafka_key() -> String`。

- [ ] **步骤 4：运行契约测试并确认通过**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml model::tests -- --nocapture
```

预期：四种消息 JSON 契约测试全部通过，且输出中无 warning。

- [ ] **步骤 5：Commit**

```bash
git add sender/src/lib.rs sender/src/model.rs
git commit -m "feat(sender): define mobile event JSON contract"
```

### 任务 3：实现设备身份与启动周期生成器

**文件：**
- 创建：`sender/src/generator.rs`
- 修改：`sender/src/lib.rs`

- [ ] **步骤 1：先写身份和顺序测试**

使用固定种子的 `StdRng`，避免随机测试不稳定：

```rust
#[test]
fn one_cycle_starts_with_start_then_devinfo() {
    let mut device = fixture_device(7);
    let cycle = device.next_cycle(1_791_614_142_381);
    assert_eq!(cycle.len(), 4);
    assert_eq!(cycle[0].protol_type, ProtocolType::Start);
    assert_eq!(cycle[1].protol_type, ProtocolType::Devinfo);
    assert_eq!(cycle.iter().map(|e| e.sequence).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
}

#[test]
fn udid_is_stable_and_start_id_changes_each_cycle() {
    let mut device = fixture_device(9);
    let first = device.next_cycle(1000);
    let second = device.next_cycle(2000);
    assert!(first.iter().all(|e| e.udid == first[0].udid));
    assert_eq!(first[0].udid, second[0].udid);
    assert_ne!(first[0].start_id, second[0].start_id);
    assert!(second.iter().all(|e| e.start_id == second[0].start_id));
}
```

继续添加合理性测试：电量 0～100、剩余内存/存储不超过设备总量、App 数量在配置范围、更新时间不早于安装时间、平台档案组合有效、第三/第四条恰好覆盖 `environment` 和 `app`。

- [ ] **步骤 2：运行生成器测试并确认失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml generator::tests -- --nocapture
```

预期：因 `SimulatedDevice` 和 `next_cycle` 缺失而失败。

- [ ] **步骤 3：实现设备档案与可注入随机源**

核心接口：

```rust
pub struct SimulatedDevice<R = rand::rngs::StdRng> {
    udid: uuid::Uuid,
    profile: DeviceProfile,
    installed_apps: Vec<InstalledApp>,
    rng: R,
}

impl<R: rand::Rng> SimulatedDevice<R> {
    pub fn new(rng: R, app_count: std::ops::RangeInclusive<usize>) -> Self;
    pub fn udid(&self) -> uuid::Uuid;
    pub fn next_cycle(&mut self, base_timestamp: i64) -> Vec<MobileEvent>;
}
```

建立少量明确的 Android/iOS `DeviceProfile` 模板，每个模板同时包含厂商、品牌、型号、系统、CPU、内存、存储和屏幕信息。App 模板按平台区分 package/bundle ID；按配置范围选取并补充合成项，确保数量要求可满足。

- [ ] **步骤 4：实现单周期生成规则**

`next_cycle` 每次生成新 UUID `start_id`，固定构造前两条，再随机交换后两条。所有事件使用唯一 `event_id`；`event_timestamp` 从传入基准开始单调递增至少 1ms；`sequence` 始终为 1～4。环境数据从设备总资源派生，禁止生成超界值。

- [ ] **步骤 5：运行生成器与契约测试**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml generator::tests model::tests -- --nocapture
```

预期：身份、顺序、字段范围和序列化契约测试全部通过。

- [ ] **步骤 6：Commit**

```bash
git add sender/src/lib.rs sender/src/generator.rs
git commit -m "feat(sender): generate ordered device startup cycles"
```

### 任务 4：实现全局 Tokio 限速器

**文件：**
- 创建：`sender/src/rate.rs`
- 修改：`sender/src/lib.rs`

- [ ] **步骤 1：先写暂停时间测试**

```rust
#[tokio::test(start_paused = true)]
async fn rate_limiter_spaces_global_permits() {
    let mut limiter = GlobalRateLimiter::new(10).unwrap();
    limiter.acquire().await;

    let second = limiter.acquire();
    tokio::pin!(second);
    assert!(futures::poll!(&mut second).is_pending());

    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    assert!(futures::poll!(&mut second).is_ready());
}

#[test]
fn rate_limiter_rejects_zero_rate() {
    assert!(GlobalRateLimiter::new(0).is_err());
}
```

- [ ] **步骤 2：运行限速测试并确认失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml rate::tests -- --nocapture
```

预期：`GlobalRateLimiter` 缺失导致失败。

- [ ] **步骤 3：实现基于 interval 的最小限速器**

```rust
pub struct GlobalRateLimiter {
    interval: tokio::time::Interval,
}

impl GlobalRateLimiter {
    pub fn new(messages_per_second: u64) -> Result<Self, RateLimitError>;
    pub async fn acquire(&mut self);
}
```

周期使用 `Duration::from_secs_f64(1.0 / rate as f64)`，拒绝导致纳秒周期为 0 的极端速率。设置 `MissedTickBehavior::Delay`，使运行时暂停后不会短时间补发积压 tick。第一个许可立即可用，后续许可按间隔发放。

- [ ] **步骤 4：运行限速测试并确认通过**

运行前述测试，预期全部通过。

- [ ] **步骤 5：Commit**

```bash
git add sender/src/lib.rs sender/src/rate.rs
git commit -m "feat(sender): add global tokio rate limiter"
```

### 任务 5：实现 Kafka sink 与投递调度器

**文件：**
- 创建：`sender/src/sink.rs`
- 修改：`sender/src/lib.rs`

- [ ] **步骤 1：先写可观察行为测试**

定义测试用 `RecordingSink`，记录 enqueue 顺序和 key，并可配置某一条投递失败。测试行为：

```rust
#[tokio::test(start_paused = true)]
async fn dispatcher_uses_udid_as_key_and_keeps_processing_after_delivery_failure() {
    let events = fixture_events(3);
    let sink = RecordingSink::fail_delivery_at(1);
    let report = dispatch_events(receiver_with(events.clone()), sink.clone(), 100, 16).await;

    assert_eq!(sink.keys(), events.iter().map(MobileEvent::kafka_key).collect::<Vec<_>>());
    assert_eq!(report.enqueued, 3);
    assert_eq!(report.delivered, 2);
    assert_eq!(report.failed, 1);
}
```

再添加测试：JSON value 可被反序列化且包含原 `event_id`；enqueue 暂时返回 queue full 时，在一个 in-flight delivery 完成后重试当前事件；receiver 关闭后等待全部投递 future 完成。

- [ ] **步骤 2：运行发送器测试并确认失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml sink::tests -- --nocapture
```

预期：`EventSink`、`KafkaSink` 或 `dispatch_events` 缺失导致失败。

- [ ] **步骤 3：实现 sink 接口与投递句柄**

接口将“按当前调用顺序入队”和“稍后等待 ack”分开：

```rust
pub type DeliveryAck = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(), SinkError>> + Send + 'static>
>;

pub trait EventSink: Send + Sync + 'static {
    fn enqueue(&self, event: &MobileEvent) -> Result<DeliveryAck, SinkError>;
    fn flush(&self, timeout: std::time::Duration) -> Result<(), SinkError>;
}
```

`KafkaSink::new(&KafkaConfig)` 使用 `ClientConfig` 设置：

- `bootstrap.servers`
- `message.timeout.ms`
- `compression.type`
- `enable.idempotence=true`
- `acks=all`

`enqueue` 将事件序列化为 owned JSON 字符串，以 `event.kafka_key()` 为 key 构造 `FutureRecord`，调用 `FutureProducer::send_result`，把返回的 delivery future 转为 `DeliveryAck`。调用发生在单一调度器中，因此 enqueue 顺序确定；Kafka 的幂等 producer 与同 key 分区共同保护顺序。

- [ ] **步骤 4：实现有限 in-flight 调度**

```rust
pub async fn dispatch_events<S: EventSink>(
    mut receiver: tokio::sync::mpsc::Receiver<MobileEvent>,
    sink: S,
    messages_per_second: u64,
    max_in_flight: usize,
) -> DispatchReport;
```

调度器每次从 receiver 取事件、等待全局许可、调用 `enqueue`，并把 ack 放入 `FuturesUnordered`。当 in-flight 达到 `max_in_flight` 时先等待至少一个 ack；这里传入 `channel_capacity`，不增加新的配置项。queue full 时先回收一个 ack 再重试当前事件；不可恢复的序列化/配置错误记录失败并继续。receiver 关闭后等待全部 ack，再调用 `flush(message_timeout)`。日志必须带 `event_id/udid/start_id/topic`。

- [ ] **步骤 5：运行发送器及限速测试**

```bash
cargo test --manifest-path sender/Cargo.toml sink::tests rate::tests -- --nocapture
```

预期：key、顺序、速率、失败继续处理和排空测试全部通过。

- [ ] **步骤 6：Commit**

```bash
git add sender/src/lib.rs sender/src/sink.rs
git commit -m "feat(sender): dispatch rate-limited events to Kafka"
```

### 任务 6：实现设备任务、优雅退出与 CLI

**文件：**
- 创建：`sender/src/runner.rs`
- 创建：`sender/src/main.rs`
- 修改：`sender/src/lib.rs`

- [ ] **步骤 1：先写设备任务关闭测试**

```rust
#[tokio::test(start_paused = true)]
async fn device_task_sends_one_ordered_cycle_then_stops_on_shutdown() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let device = fixture_device(42);

    let task = tokio::spawn(run_device(
        device,
        tx,
        shutdown_rx,
        30..=120,
    ));

    let cycle = collect_n(&mut rx, 4).await;
    assert_eq!(cycle[0].protol_type, ProtocolType::Start);
    assert_eq!(cycle[1].protol_type, ProtocolType::Devinfo);
    shutdown_tx.send(true).unwrap();
    task.await.unwrap();
}
```

添加测试：关闭发生在随机等待期间时任务立即退出；发送 channel 被关闭时任务退出而不是循环报错；多个设备的所有事件最终由一个 receiver 获取。

- [ ] **步骤 2：运行 runner 测试并确认失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml runner::tests -- --nocapture
```

预期：`run_device` 或 `run` 缺失导致失败。

- [ ] **步骤 3：实现单设备循环**

```rust
pub async fn run_device<R: rand::Rng + Send + 'static>(
    mut device: SimulatedDevice<R>,
    sender: tokio::sync::mpsc::Sender<MobileEvent>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    startup_interval: std::ops::RangeInclusive<u64>,
);
```

每轮先检查 shutdown，再调用系统时间生成 Unix 毫秒基准，顺序发送四条事件。等待下一启动周期时用 `tokio::select!` 同时等待 `sleep` 与 `shutdown.changed()`。发送失败说明 receiver 已关闭，应结束任务。

- [ ] **步骤 4：实现总编排函数**

`runner::run(config)` 完成：

1. 构造一个有界 channel 和 watch shutdown channel；
2. 创建 `device_count` 个独立设备并 spawn；
3. 创建 `KafkaSink` 并运行单一 `dispatch_events`；
4. 等待外部 shutdown future；
5. 广播 shutdown，等待所有设备任务结束，释放最后一个 channel sender；
6. 等 receiver 排空、所有 delivery ack 完成和 Kafka flush；
7. 返回汇总的成功/失败计数。

为可测试性，将等待信号作为 future 参数注入内部 `run_until(config, shutdown_future)`，生产入口传 `tokio::signal::ctrl_c()`。

- [ ] **步骤 5：实现命令行入口**

```rust
#[derive(clap::Parser)]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化 tracing；解析 CLI；加载并校验配置；运行到 Ctrl+C。
}
```

路径约定：用户在 `sender/` 下执行时默认读取 `config.toml`；仓库根目录执行时使用 `--config sender/config.toml`。日志默认 `info`，可由 `RUST_LOG` 覆盖。

- [ ] **步骤 6：运行全部单元测试**

```bash
cargo test --manifest-path sender/Cargo.toml --lib -- --nocapture
```

预期：配置、契约、生成器、限速、sink 和 runner 测试全部通过。

- [ ] **步骤 7：Commit**

```bash
git add sender/src/lib.rs sender/src/runner.rs sender/src/main.rs
git commit -m "feat(sender): run concurrent devices with graceful shutdown"
```

### 任务 7：添加真实 Kafka 集成测试与使用文档

**文件：**
- 创建：`sender/tests/kafka_integration.rs`
- 创建：`sender/README.md`

- [ ] **步骤 1：先写默认忽略的集成测试**

测试必须显式标记，避免普通测试写入共享 Kafka：

```rust
#[tokio::test]
#[ignore = "writes to the configured real Kafka cluster"]
async fn sends_and_consumes_one_ordered_startup_cycle() {
    let brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "192.168.250.106:9092".to_owned());
    let topic = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "flink_demo".to_owned());
    // 使用唯一 udid/start_id 和唯一 consumer group；生产四条后在超时内
    // 只收集该 udid 的消息，断言 sequence 为 1,2,3,4 且前两类固定。
}
```

测试 consumer 使用 `auto.offset.reset=latest`、唯一 group ID，订阅完成后再生产；用 `tokio::time::timeout` 限制总等待时间，避免 Kafka 不可达时永久挂起。

- [ ] **步骤 2：运行测试清单确认集成测试默认不执行**

```bash
cargo test --manifest-path sender/Cargo.toml --test kafka_integration -- --list
cargo test --manifest-path sender/Cargo.toml --test kafka_integration
```

预期：第一条列出测试；第二条报告 1 个 ignored、0 个失败，且不连接 Kafka。

- [ ] **步骤 3：编写 README 并完整复制数据契约**

`sender/README.md` 必须包含：

- 构建、启动、`--config` 与 `RUST_LOG` 示例；
- 配置项表及默认 Kafka 地址/topic；
- 顶层公共字段表；
- `start`、`devinfo`、`environment`、`app` 的完整 JSON 示例和逐字段类型表；
- `udid/start_id/sequence` 的稳定性与顺序保证；
- Flink 应按 `protol_type` 分流，并将 `event_timestamp` 作为 epoch milliseconds；
- 普通测试与真实 Kafka 测试的明确区分。

真实测试命令：

```bash
KAFKA_BROKERS=192.168.250.106:9092 \
KAFKA_TOPIC=flink_demo \
cargo test --manifest-path sender/Cargo.toml \
  --test kafka_integration -- --ignored --nocapture
```

- [ ] **步骤 4：按需执行真实 Kafka 测试**

先运行只读连通性/元数据检查；只有在用户已授权向 `flink_demo` 写入测试事件时，才运行 ignored 集成测试。预期：消费到同一唯一 `udid` 的四条消息，sequence 为 1～4，前两类依次为 `start`、`devinfo`。

- [ ] **步骤 5：Commit**

```bash
git add sender/tests/kafka_integration.rs sender/README.md
git commit -m "docs(sender): document contract and Kafka integration test"
```

### 任务 8：完成前全量验证

**文件：**
- 检查：`sender/` 全部文件
- 对照：`docs/superpowers/specs/2026-10-10-mobile-event-sender-design.md`

- [ ] **步骤 1：格式检查**

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
```

预期：退出码 0，无格式差异。

- [ ] **步骤 2：Clippy 严格检查**

```bash
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
```

预期：退出码 0，无 warning。

- [ ] **步骤 3：运行全部默认测试**

```bash
cargo test --manifest-path sender/Cargo.toml --all-features
```

预期：所有单元测试通过；真实 Kafka 测试显示 ignored。

- [ ] **步骤 4：构建 release 二进制**

```bash
cargo build --manifest-path sender/Cargo.toml --release
```

预期：退出码 0，生成 `sender/target/release/mobile-event-sender`。

- [ ] **步骤 5：逐项核对需求**

对照设计文档确认：固定 `udid`、每启动新 `start_id`、`start -> devinfo`、四类完整 payload、全局速率、多设备、Kafka key、默认 broker/topic、Tokio、优雅退出、README 数据契约全部有自动测试或明确代码路径支撑。

- [ ] **步骤 6：最终 Commit**

若验证过程产生修正：

```bash
git add sender docs/superpowers/specs docs/superpowers/plans
git commit -m "feat(sender): complete mobile event simulator"
```

若目录仍不是 Git 仓库，不初始化、不提交，只在交付报告中说明。
