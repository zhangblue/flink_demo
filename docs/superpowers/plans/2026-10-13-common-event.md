# Common 类型事件实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为每个设备启动周期新增第一条 `common` 事件，携带一对匹配但与其他事件不绑定的 App 标识，以及稳定的设备档案字段。

**架构：** 扩展现有 `ProtocolType`/`EventPayload` 代数类型，并由 `SimulatedDevice::next_cycle` 从同一设备安装列表独立抽取 `common` App。周期仍由单个生成器一次性构造，因此五条事件天然共享 `udid`、`start_id` 与生成时刻，Kafka、Tokio 调度和全局限速层无需新增分支。

**技术栈：** Rust 2024、Serde/serde_json、rand、Tokio、rdkafka；Java 17/Flink 2.2 仅做现有通用解析兼容性验证。

---

## 文件结构

- 修改 `sender/src/model.rs`：定义 `CommonPayload`、`ProtocolType::Common` 和 `EventPayload::Common`，并锁定 JSON 序列化契约。
- 修改 `sender/src/generator.rs`：生成 `common` App/设备状态并把周期扩展为五条有序事件；调整受事件数量和索引影响的生成器测试。
- 修改 `sender/src/runner.rs`：调整 Tokio 设备任务测试，使优雅关闭验证完整排空五条事件。
- 修改 `sender/tests/kafka_integration.rs`：真实 Kafka 往返期望五条事件并验证 `common` 位于第一条。
- 修改 `sender/README.md`：增加完整 `common` JSON、字段表、五类事件和新顺序。
- 修改 `README.md`：更新仓库概览、公共类型列表、周期数量和验证说明。
- 修改 `flink_processer/README.md`：明确 `common` 由现有通用解析器作为非 `app` 事件处理，无需更改 Flink 生产代码。

### 任务 1：扩展 Rust 数据模型与 JSON 契约

**文件：**
- 修改：`sender/src/model.rs:19-68`
- 测试：`sender/src/model.rs:95-291`

- [ ] **步骤 1：编写失败的 `common` 序列化测试**

该测试要抓住的破坏是：缺少 `common` 协议分支、字段漏序列化、字段名错误或 payload 出现额外字段。在 `model.rs` 的测试模块新增：

```rust
#[test]
fn common_event_serializes_with_stable_contract() {
    let event = fixture_event(
        ProtocolType::Common,
        1,
        EventPayload::Common(CommonPayload {
            self_app_package: "com.eg.android.AlipayGphone".into(),
            self_app_name: "支付宝".into(),
            model: "23127PN0CC".into(),
            brand: "Xiaomi".into(),
            os: "Android".into(),
            os_version: "15".into(),
        }),
    );

    let value = serde_json::to_value(event).unwrap();
    assert_envelope(&value, "common", 1);
    assert_eq!(
        value["payload"],
        json!({
            "self_app_package": "com.eg.android.AlipayGphone",
            "self_app_name": "支付宝",
            "model": "23127PN0CC",
            "brand": "Xiaomi",
            "os": "Android",
            "os_version": "15"
        })
    );
}
```

- [ ] **步骤 2：运行测试并确认因缺少模型而失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml model::tests::common_event_serializes_with_stable_contract
```

预期：编译失败，指出 `ProtocolType::Common`、`EventPayload::Common` 或 `CommonPayload` 尚不存在。不能接受与测试拼写或 fixture 无关的失败。

- [ ] **步骤 3：添加最小模型实现**

在 `ProtocolType` 和 `EventPayload` 中分别添加 `Common` 分支，并在 payload 结构体区域添加：

```rust
#[derive(Clone, Debug, Serialize)]
pub struct CommonPayload {
    pub self_app_package: String,
    pub self_app_name: String,
    pub model: String,
    pub brand: String,
    pub os: String,
    pub os_version: String,
}
```

`ProtocolType` 已使用 `#[serde(rename_all = "lowercase")]`，因此 `Common` 必须序列化为 `common`，不要添加单独的字符串映射层。

- [ ] **步骤 4：运行模型测试确认通过**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml model::tests::common_event_serializes_with_stable_contract
cargo test --manifest-path sender/Cargo.toml model::tests
```

预期：新增测试及全部模型测试通过，且原四类 payload 的序列化结果未改变。

- [ ] **步骤 5：提交模型与契约测试**

```bash
git add sender/src/model.rs
git commit -m "feat(数据契约): 增加 common 事件模型"
```

### 任务 2：生成五条有序事件并更新运行路径测试

**文件：**
- 修改：`sender/src/generator.rs:1-310`
- 测试：`sender/src/generator.rs:420-880`
- 测试：`sender/src/runner.rs:285-350`
- 测试：`sender/tests/kafka_integration.rs:44-175`

- [ ] **步骤 1：先把周期、状态和独立 App 行为写成失败测试**

该组测试要抓住的破坏包括：`common` 不在第一条、周期仍只有四条、package/name 不匹配、设备字段来自错误档案，以及错误复用 `start` 选择结果。

将 `one_cycle_starts_with_start_then_devinfo` 改为 `one_cycle_starts_with_common_start_then_devinfo`，核心断言为：

```rust
assert_eq!(cycle.len(), 5);
assert_eq!(cycle[0].protol_type, ProtocolType::Common);
assert_eq!(cycle[1].protol_type, ProtocolType::Start);
assert_eq!(cycle[2].protol_type, ProtocolType::Devinfo);
assert_eq!(
    cycle.iter().map(|event| event.sequence).collect::<Vec<_>>(),
    vec![1, 2, 3, 4, 5]
);
```

扩展 payload/协议配对断言，使 `ProtocolType::Common` 只能搭配 `EventPayload::Common`。后置顺序测试应读取索引 `3`，并确认固定种子集合中 `environment` 与 `app` 都能成为第四条。

新增状态测试，从同一周期取出 `common`、`devinfo` 和 `app` payload：

```rust
assert!(app.apps.iter().any(|installed| {
    installed.package_name == common.self_app_package
        && installed.app_name == common.self_app_name
}));
assert_eq!(common.model, devinfo.model);
assert_eq!(common.brand, devinfo.brand);
assert_eq!(common.os, devinfo.os);
assert_eq!(common.os_version, devinfo.os_version);
```

新增固定种子的多周期测试，收集 64 个周期，至少观察到一次 `common.self_app_package != start.app_package`。这会抓住把 `common` 直接绑定到 `start` 选择结果的错误；它不要求每个周期都不同，也允许随机情况下相同。

同时把唯一 ID 测试的 10 个周期期望值从 `40` 改为 `50`，把依赖固定位置的 `devinfo` 索引从 `1` 改为 `2`。不要只批量替换数字；每处索引都按新的顺序语义核对。

- [ ] **步骤 2：运行生成器测试并确认行为测试失败**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml generator::tests
```

预期：测试因周期仍为四条、首条仍为 `start` 或找不到 `common` payload 而失败。若出现与新需求无关的编译错误，先修正测试本身。

- [ ] **步骤 3：在生成器中加入最小实现**

导入 `CommonPayload`。在每次 `next_cycle` 中先独立随机选一条完整安装记录，再单独执行现有启动 App 包名选择：

```rust
let common_app = self.installed_apps
    [self.rng.random_range(0..self.installed_apps.len())]
    .clone();
let selected_package = self.installed_apps
    [self.rng.random_range(0..self.installed_apps.len())]
    .package_name
    .clone();
```

用 `common_app.package_name` 和 `common_app.app_name` 构造 payload；设备字段使用同一个 `profile`：

```rust
let common = EventPayload::Common(CommonPayload {
    self_app_package: common_app.package_name,
    self_app_name: common_app.app_name,
    model: profile.model.into(),
    brand: profile.brand.into(),
    os: profile.os.into(),
    os_version: profile.os_version.into(),
});
```

将最终 payload 顺序改为：

```rust
let payloads = [
    (ProtocolType::Common, common),
    (ProtocolType::Start, start),
    (ProtocolType::Devinfo, devinfo),
    third,
    fourth,
];
```

保留现有 `enumerate()` 生成 sequence 的方式，这会自然得到 1 至 5。不要修改 Kafka sink、速率限制或配置结构。

- [ ] **步骤 4：运行生成器测试确认通过**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml generator::tests
```

预期：所有生成器测试通过，包括五条顺序、匹配 App 对、设备档案一致和独立抽取行为。

- [ ] **步骤 5：先更新 Tokio 关闭测试并验证它能抓住不完整周期**

在 `device_task_completes_started_cycle_after_shutdown` 中断言第一条为 `Common`，关闭信号发出后继续收取其余 4 条，并断言前 3 种类型及 `[1, 2, 3, 4, 5]`。在休眠期间关闭测试中收取完整 5 条。

为证明测试有效，临时把 `collect_n(&mut receiver, 4)` 保持为旧值运行目标测试，确认它对周期不完整或队列未排空失败；随后改成正确的 5 条/剩余 4 条断言。

运行：

```bash
cargo test --manifest-path sender/Cargo.toml runner::tests::device_task_completes_started_cycle_after_shutdown
cargo test --manifest-path sender/Cargo.toml runner::tests::device_task_exits_immediately_when_shutdown_arrives_during_sleep
```

预期：最终两个测试均通过，证明收到关闭信号后不会截断已开始的五条周期。

- [ ] **步骤 6：更新真实 Kafka 往返契约**

在 `sender/tests/kafka_integration.rs` 中：

- 将唯一事件数和接收循环目标改为 `5`。
- 将 sequence 期望改为 `[1, 2, 3, 4, 5]`。
- 断言前三条依次为 `common`、`start`、`devinfo`。
- 在索引 `3..` 中确认 `environment` 与 `app` 都存在。
- 不新增 `common` 与 `start` App 相等断言；只校验收到的 JSON 与生成器产物逐条相等。

先运行列表和默认测试，确认该测试仍被显式忽略且可以编译：

```bash
cargo test --manifest-path sender/Cargo.toml --test kafka_integration -- --list
cargo test --manifest-path sender/Cargo.toml --test kafka_integration
```

预期：列出一个测试；默认运行显示一个 ignored，不连接 Kafka。

- [ ] **步骤 7：运行完整 Rust 测试并提交生成路径改动**

```bash
cargo test --manifest-path sender/Cargo.toml --all-features
git add sender/src/generator.rs sender/src/runner.rs sender/tests/kafka_integration.rs
git commit -m "feat(生成器): 每次启动首先发送 common 事件"
```

### 任务 3：同步数据契约文档并完成全量验证

**文件：**
- 修改：`sender/README.md:55-265`
- 修改：`README.md:1-140`
- 修改：`flink_processer/README.md:60-90`

- [ ] **步骤 1：更新发送器完整契约**

在 `sender/README.md` 中：

- 将 `protol_type` 枚举扩展为 `common`、`start`、`devinfo`、`environment`、`app`。
- 将“同一周期四条消息”统一改为“五条消息”。
- 在 `start` 章节之前加入规格文档中的完整 `common` JSON 和六个 payload 字段说明。
- 把示例 sequence 调整为：`common=1`、`start=2`、`devinfo=3`，`environment/app=4/5`。
- 明确 `common` 的 package/name 来自安装列表同一条记录，但不要求等于 `start.payload.app_package` 或 `app.payload.self_package_name`。
- 将 Kafka 集成测试说明从四条改为五条。

- [ ] **步骤 2：更新仓库级与 Flink 消费说明**

在根 `README.md` 中把核心能力和事件顺序更新为：

```text
common → start → devinfo → environment/app（随机顺序）
```

将公共类型列表、周期数量和真实集成测试说明更新为五类/五条。

在 `flink_processer/README.md` 输入契约中说明 `common` 是合法的非 `app` 类型：现有解析器无需枚举分支即可保留其 payload，并按当前规则参与非 `app` 事件补全。不要改变 Flink 作业的 enrichment 语义。

- [ ] **步骤 3：进行文档一致性扫描**

运行：

```bash
rg -n '四条|4 条|包含 4 条|四种 payload|start.*sequence 1|devinfo.*sequence 2' README.md sender/README.md flink_processer/README.md
```

预期：不再出现描述旧周期数量或旧 sequence 的命中；若命中历史兼容性说明，人工确认其上下文后保留。

再运行：

```bash
rg -n 'common|五条|5 条' README.md sender/README.md flink_processer/README.md
```

预期：三个文档均明确新类型或其消费行为。

- [ ] **步骤 4：运行完整验证**

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
cargo build --manifest-path sender/Cargo.toml --release
mvn -f flink_processer/pom.xml clean verify
git diff --check
```

预期：所有命令退出码为 0；Rust 测试无失败，Kafka 真实测试保持 ignored；Flink 全部测试通过。

- [ ] **步骤 5：在集群可达时运行真实 Kafka 往返测试**

```bash
KAFKA_BROKERS=192.168.250.106:9092 \
KAFKA_TOPIC=flink_demo \
cargo test --manifest-path sender/Cargo.toml \
  --test kafka_integration -- --ignored --nocapture
```

预期：生产并消费同一设备、同一启动周期的五条消息，顺序为 `common`、`start`、`devinfo`、后置两类。若网络、broker advertised listener 或 consumer group 协调不可达，应保留其他验证结果并准确报告该环境阻塞，不得把本项记为通过。

- [ ] **步骤 6：提交文档并复核变更范围**

```bash
git add README.md sender/README.md flink_processer/README.md
git commit -m "docs(数据契约): 说明 common 事件格式与顺序"
git status --short
git log --oneline -5
```

预期：worktree 无未提交变更；最近提交仅覆盖模型、生成器/测试和文档三个独立交付物。
