# Kafka 实时投递速度打印实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 发送器运行期间每秒在命令行打印最近 1 秒内收到 Kafka broker 成功确认的消息数量。

**架构：** 在现有 dispatcher 内维护一个共享原子周期计数器，每次成功 delivery acknowledgement 同时更新累计报告和周期计数。独立 Tokio 任务使用延迟首 tick 的 1 秒 interval 定时 `swap(0)` 并输出 `INFO` 日志；dispatcher 退出前显式停止并等待该任务。

**技术栈：** Rust 2024、Tokio 1.x (`time`、`sync`、`test-util`)、tracing 0.1、rdkafka 0.38。

**API 依据：** Tokio 官方文档确认 `interval_at(start, period)` 的首个 tick 在 `start` 到达时发生，`MissedTickBehavior::Skip` 跳过错过的 tick；`#[tokio::test(start_paused = true)]` 由现有 dev-dependency 的 `test-util` feature 支持。tracing 官方文档确认 `tracing::info!("… {}", value)` 生成可由 subscriber 级别过滤的 INFO event。

---

## 文件结构

- 修改 `sender/src/sink.rs`：实现速度报告任务、成功确认计数、生命周期管理及暂停时钟测试。
- 修改 `sender/README.md`：说明默认每秒速度日志、统计口径和 `RUST_LOG` 过滤行为。
- 修改 `README.md`：在运行说明中补充实时速度示例。

### 任务 1：实现成功投递速度计数与每秒日志

**文件：**
- 修改：`sender/src/sink.rs:1-265`
- 测试：`sender/src/sink.rs:264-720`

- [ ] **步骤 1：编写失败的成功/失败确认计数测试**

该测试要抓住的破坏是：把失败确认计入速度，或只更新累计报告却不更新周期计数。在测试模块导入 `AtomicU64` 和 `Ordering`，新增：

```rust
#[test]
fn delivery_speed_counts_only_successful_acknowledgements() {
    let mut report = DispatchReport::default();
    let speed_count = AtomicU64::new(0);

    record_delivery(&mut report, Ok(()), &speed_count);
    record_delivery(
        &mut report,
        Err(SinkError::Delivery("injected".into())),
        &speed_count,
    );

    assert_eq!((report.delivered, report.failed), (1, 1));
    assert_eq!(speed_count.load(Ordering::Relaxed), 1);
}
```

- [ ] **步骤 2：运行测试并确认因接口尚未支持计数器而失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml sink::tests::delivery_speed_counts_only_successful_acknowledgements
```

预期：编译失败，指出 `record_delivery` 尚不接受速度计数器参数；不能接受测试拼写或 fixture 引起的无关失败。

- [ ] **步骤 3：编写失败的 1 秒窗口、清零和关闭测试**

在 sink 测试模块加入只用于测试的内存 writer，沿用仓库现有 tracing 捕获模式：

```rust
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
```

新增暂停时钟测试。该测试要抓住启动瞬间打印、没有周期清零、空闲周期不打印，以及关闭后任务仍存活：

```rust
#[tokio::test(start_paused = true)]
async fn speed_reporter_waits_one_second_resets_and_stops() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let captured = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || CaptureWriter(captured.clone()))
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let speed_count = Arc::new(AtomicU64::new(0));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let reporter = tokio::spawn(report_delivery_speed(speed_count.clone(), shutdown_rx));
    tokio::task::yield_now().await;
    speed_count.fetch_add(3, Ordering::Relaxed);

    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    let before_first_tick = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(!before_first_tick.contains("当前发送速度"));

    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    let first_tick = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(first_tick.contains("当前发送速度 3 条/秒"), "{first_tick}");

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    let second_tick = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(second_tick.contains("当前发送速度 0 条/秒"), "{second_tick}");

    shutdown_tx.send(()).unwrap();
    reporter.await.unwrap();
    let length_after_shutdown = output.lock().unwrap().len();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(output.lock().unwrap().len(), length_after_shutdown);
}
```

- [ ] **步骤 4：运行报告任务测试并确认因函数不存在而失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml sink::tests::speed_reporter_waits_one_second_resets_and_stops
```

预期：编译失败，指出 `report_delivery_speed` 尚不存在。

- [ ] **步骤 5：实现最小速度报告器**

在生产代码导入：

```rust
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout_at};
```

添加固定周期和私有报告函数：

```rust
const SPEED_REPORT_INTERVAL: Duration = Duration::from_secs(1);

async fn report_delivery_speed(
    delivered_in_window: Arc<AtomicU64>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let first_tick = Instant::now() + SPEED_REPORT_INTERVAL;
    let mut interval = interval_at(first_tick, SPEED_REPORT_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            _ = interval.tick() => {
                let speed = delivered_in_window.swap(0, Ordering::Relaxed);
                tracing::info!("当前发送速度 {speed} 条/秒");
            }
        }
    }
}
```

把 `record_delivery` 改为接收 `&AtomicU64`，只在 `result.is_ok()` 分支同时执行：

```rust
report.delivered += 1;
delivered_in_window.fetch_add(1, Ordering::Relaxed);
```

更新 dispatcher 内所有 `record_delivery` 调用点，传入同一个周期计数器。

- [ ] **步骤 6：在 dispatcher 中管理报告任务生命周期**

只在 `GlobalRateLimiter::new` 成功后创建报告任务，避免无效配置也启动日志循环：

```rust
let delivered_in_window = Arc::new(AtomicU64::new(0));
let (speed_shutdown_tx, speed_shutdown_rx) = oneshot::channel();
let speed_reporter = tokio::spawn(report_delivery_speed(
    delivered_in_window.clone(),
    speed_shutdown_rx,
));
```

完成 pending acknowledgement 排空与 `sink.flush` 后关闭并等待报告任务：

```rust
let _ = speed_shutdown_tx.send(());
if let Err(error) = speed_reporter.await {
    tracing::error!(%error, "delivery speed reporter failed");
}
```

关闭失败不改变 `DispatchReport` 的公开字段或既有 Kafka 错误优先级。若 `dispatch_events` future 被取消，oneshot sender 被丢弃，receiver 同样就绪并使报告任务退出。

- [ ] **步骤 7：运行目标测试与 sink 全套测试**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml sink::tests::delivery_speed_counts_only_successful_acknowledgements
cargo test --manifest-path sender/Cargo.toml sink::tests::speed_reporter_waits_one_second_resets_and_stops
cargo test --manifest-path sender/Cargo.toml sink::tests
```

预期：两个新增测试和全部 sink 测试通过；现有累计计数、背压、排空和失败测试不回归。

- [ ] **步骤 8：提交速度统计实现与测试**

```bash
git add sender/src/sink.rs
git commit -m "feat(发送器): 每秒打印 Kafka 成功投递速度"
```

### 任务 2：同步运行文档并完成验证

**文件：**
- 修改：`sender/README.md:3-20`
- 修改：`README.md:3-75`

- [ ] **步骤 1：更新发送器运行说明**

在 `sender/README.md` 的运行章节补充：

```text
程序默认每秒输出一次最近 1 秒内收到 Kafka broker 成功确认的消息数量，例如 `当前发送速度 1000 条/秒`。进入本地 producer 队列但尚未确认、投递失败或超时的消息不计入；空闲周期打印 0。该消息使用 INFO 级别，若通过 RUST_LOG 屏蔽 INFO 日志则不会显示。
```

不得把该数字描述为 `simulation.messages_per_second` 的配置值；后者是全局目标限速，前者是实际成功确认速度。

- [ ] **步骤 2：更新仓库级运行说明**

在根 `README.md` 的核心能力与运行章节中补充同一语义，并给出一行示例：

```text
当前发送速度 1000 条/秒
```

保持构建命令、Kafka JSON 契约和 Flink 文档不变。

- [ ] **步骤 3：执行文档一致性检查**

运行：

```bash
rg -n '当前发送速度|成功确认|messages_per_second|INFO' README.md sender/README.md
git diff --check
```

预期：两个 README 均说明实际成功确认速度；没有把目标限速写成实时速度；无空白错误。

- [ ] **步骤 4：运行完整静态检查、测试和构建**

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
cargo build --manifest-path sender/Cargo.toml --release
```

预期：所有命令退出码为 0；全部 Rust 测试通过，真实 Kafka 集成测试默认保持 ignored；Release 构建成功。

- [ ] **步骤 5：在 Kafka 可达时执行真实往返回归测试**

```bash
KAFKA_BROKERS=192.168.250.106:9092 \
KAFKA_TOPIC=flink_demo \
cargo test --manifest-path sender/Cargo.toml \
  --test kafka_integration -- --ignored --nocapture
```

预期：五条消息成功生产并消费。该测试验证 Kafka 路径未回归；每秒日志本身由暂停时钟测试确定性验证。若集群不可达，应如实记录环境阻塞，不得声称本项通过。

- [ ] **步骤 6：提交文档并复核工作树**

```bash
git add README.md sender/README.md
git commit -m "docs(发送器): 说明实时投递速度日志"
git status --short
git log --oneline -5
```

预期：worktree 无未提交变更；最近提交分别覆盖规格、计划、速度实现与文档。
