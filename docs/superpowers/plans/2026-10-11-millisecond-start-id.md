# 毫秒 start_id 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:executing-plans 在当前会话内执行，并使用 superpowers:test-driven-development 完成生产代码变更。步骤使用复选框（`- [ ]`）语法跟踪进度。

**目标：** 将 `start_id` 从 UUID 字符串改为启动时刻的 Unix 毫秒整数，并保持 `event_timestamp` 为独立的消息生成时间。

**架构：** `MobileEvent.start_id` 使用 `i64`。生成器接收互相独立的启动时间和事件生成时间：启动时间经单设备单调化后成为周期 ID，事件生成时间用于该批事件和 App 采集时间，不从周期 ID 派生。运行器分别读取两个系统时间值。

**技术栈：** Rust 2024、Serde JSON、Tokio、rust-rdkafka、Cargo test。

---

### 任务 1：迁移 start_id 数据契约与生成逻辑

**文件：**
- 修改：`sender/src/model.rs`
- 修改：`sender/src/generator.rs`
- 修改：`sender/src/runner.rs`
- 修改：`sender/src/sink.rs`
- 修改：`sender/tests/kafka_integration.rs`
- 修改：`sender/README.md`
- 修改：`README.md`
- 修改：`docs/superpowers/specs/2026-10-10-mobile-event-sender-design.md`
- 测试：`sender/src/model.rs`
- 测试：`sender/src/generator.rs`
- 测试：`sender/src/runner.rs`
- 测试：`sender/src/sink.rs`
- 测试：`sender/tests/kafka_integration.rs`

- [ ] **步骤 1：把模型测试改为数字 start_id**

将固定事件的 `start_id` 设为 `1_791_614_142_000_i64`，并断言序列化结果是 JSON number：

```rust
assert!(value["start_id"].is_i64());
assert_eq!(value["start_id"], 1_791_614_142_000_i64);
```

- [ ] **步骤 2：增加生成器时间语义测试**

调用新的双时间参数接口：

```rust
let events = device.next_cycle(1_700_000_000_000, 1_700_000_000_123);
assert!(events.iter().all(|event| event.start_id == 1_700_000_000_000));
assert!(events.iter().all(|event| event.event_timestamp == 1_700_000_000_123));
```

再用相同或回拨的启动时间生成下一周期，断言新 `start_id` 等于上一值加 1，而第二个参数仍独立成为 `event_timestamp`。

- [ ] **步骤 3：运行定向测试并确认红灯**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml --lib
```

预期：编译失败，因为生产模型仍使用 UUID，且生成器仍只接受一个时间参数。失败原因必须与缺少新契约一致。

- [ ] **步骤 4：实现最小模型与生成器变更**

将模型字段改为：

```rust
pub start_id: i64,
```

生成器保存 `last_start_id: Option<i64>`，接口改为：

```rust
pub fn next_cycle(
    &mut self,
    start_timestamp_ms: i64,
    event_timestamp_ms: i64,
) -> Vec<MobileEvent>
```

周期 ID 使用 `max(start_timestamp_ms, last_start_id + 1)`；溢出时明确报 `start_id timestamp exhausted`。4 条事件的 `event_timestamp` 使用独立参数，App 的 `collected_at` 与 App 事件时间一致。

- [ ] **步骤 5：适配运行器、发送器和集成测试**

运行器在周期开始时分别读取启动时刻与事件生成时刻并调用双参数接口。更新所有测试 fixture，把 UUID `start_id` 改为整数。Kafka 集成测试直接比较 JSON 数字：

```rust
let start_id = events[0].start_id;
assert_eq!(value["start_id"].as_i64(), Some(start_id));
```

- [ ] **步骤 6：运行定向测试并确认绿灯**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml model::tests
cargo test --manifest-path sender/Cargo.toml generator::tests
cargo test --manifest-path sender/Cargo.toml runner::tests
cargo test --manifest-path sender/Cargo.toml sink::tests
```

预期：全部通过。

- [ ] **步骤 7：同步数据契约文档**

把根 README、发送器 README 和 canonical spec 中的 `start_id` 类型更新为 integer/Flink `BIGINT`，所有 JSON 示例使用不带引号的 13 位毫秒值。明确说明 `event_timestamp` 是单条消息生成时间，不从 `start_id` 派生，周期唯一键为 `(udid, start_id)`。

- [ ] **步骤 8：完整验证**

运行：

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
cargo build --manifest-path sender/Cargo.toml --release
git diff --check
```

预期：格式、Clippy、全部单元测试、非网络测试、release 构建和 diff 检查均通过；真实 Kafka 测试仍默认忽略。

- [ ] **步骤 9：提交本次变更**

仅暂存上述实现、测试和文档文件，不包含用户现有的 `.gitignore` 修改：

```bash
git commit -m "feat(数据契约): 使用毫秒级 start_id"
```
