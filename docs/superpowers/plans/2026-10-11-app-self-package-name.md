# App 消息 self_package_name 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为 `app` 消息增加必填 `self_package_name`，并保证它与同一次启动的 `start.payload.app_package` 一致且来自该设备的安装列表。

**架构：** `SimulatedDevice::next_cycle` 每次调用时只从 `installed_apps` 等概率随机选择一次包名，然后把同一个字符串分别写入 `StartPayload` 和 `AppPayload`。序列化结构只新增一个字段，不改变消息信封、事件顺序、设备安装列表或 Kafka key。

**技术栈：** Rust 2024、serde/serde_json、rand 0.9、Tokio、rust-rdkafka、Cargo 测试。

---

## 文件结构

- 修改：`sender/src/model.rs` — 扩展 `AppPayload` 数据契约与序列化测试。
- 修改：`sender/src/generator.rs` — 每个启动周期选择包名并写入 start/app 两类 payload，增加生成不变量测试。
- 修改：`sender/tests/kafka_integration.rs` — 在真实 Kafka 往返测试中验证新增字段及跨消息一致性。
- 修改：`sender/README.md` — 更新 start/app JSON 示例、字段表与 Flink 消费约定。
- 修改：`README.md` — 在事件能力说明中注明启动包名的一致性规则。
- 生成但不提交：`runner/mobile-event-sender` — 从验证通过的 release 构建复制，供当前运行目录直接使用。

### 任务 1：实现数据契约与每周期包名选择

**文件：**
- 修改：`sender/src/model.rs:83-86`
- 测试：`sender/src/model.rs:232-260`
- 修改：`sender/src/generator.rs:185-268`
- 测试：`sender/src/generator.rs:690-850`

- [ ] **步骤 1：先修改序列化测试，要求新增字段**

把 `app_event_serializes_with_stable_contract` 的 fixture 改为：

```rust
EventPayload::App(AppPayload {
    self_package_name: "com.tencent.mm".into(),
    collected_at: 1_791_614_142_417,
    apps: vec![InstalledApp {
        package_name: "com.tencent.mm".into(),
        app_name: "WeChat".into(),
        version_name: "8.0.62".into(),
        version_code: 2800,
        is_system_app: false,
        first_installed_at: 1_735_689_600_000,
        last_updated_at: 1_789_000_000_000,
    }],
})
```

期望 payload 改为：

```rust
json!({
    "self_package_name": "com.tencent.mm",
    "collected_at": 1791614142417_i64,
    "apps": [{
        "package_name": "com.tencent.mm",
        "app_name": "WeChat",
        "version_name": "8.0.62",
        "version_code": 2800,
        "is_system_app": false,
        "first_installed_at": 1735689600000_i64,
        "last_updated_at": 1789000000000_i64
    }]
})
```

- [ ] **步骤 2：编写跨消息一致性的行为测试**

在 generator 测试模块加入：

```rust
#[test]
fn selected_package_belongs_to_apps_and_matches_start() {
    for seed in 0..64 {
        let mut device = SimulatedDevice::new(StdRng::seed_from_u64(seed), 7..=7);
        for cycle_index in 0..4 {
            let cycle = device.next_cycle(1_000 + cycle_index, 2_000 + cycle_index);
            let EventPayload::Start(start) = &cycle[0].payload else {
                panic!("start payload")
            };
            let app = cycle
                .iter()
                .find_map(|event| match &event.payload {
                    EventPayload::App(value) => Some(value),
                    _ => None,
                })
                .expect("app payload");

            assert_eq!(start.app_package, app.self_package_name);
            assert!(
                app.apps
                    .iter()
                    .any(|installed| installed.package_name == app.self_package_name)
            );
        }
    }
}
```

- [ ] **步骤 3：运行测试并确认红灯**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml model::tests::app_event_serializes_with_stable_contract
```

预期：编译失败，明确指出 `AppPayload` 不存在字段 `self_package_name`。这是预期红灯；
行为测试将在同一最小实现完成后一起转绿，避免提交无法编译的中间状态。

- [ ] **步骤 4：扩展 AppPayload**

```rust
#[derive(Clone, Debug, Serialize)]
pub struct AppPayload {
    pub self_package_name: String,
    pub collected_at: i64,
    pub apps: Vec<InstalledApp>,
}
```

- [ ] **步骤 5：实现每周期一次随机选择**

在 `next_cycle` 构造 `start` 之前选择一次：

```rust
let selected_package = self.installed_apps
    [self.rng.random_range(0..self.installed_apps.len())]
    .package_name
    .clone();
```

`installed_apps_min >= 1` 已由配置校验和构造函数约束保证，因此索引范围非空。
随后复用同一个值：

```rust
let start = EventPayload::Start(StartPayload {
    app_package: selected_package.clone(),
    app_version: "3.8.1".into(),
    app_build: 30_801,
    launch_type: "cold".into(),
    foreground: true,
    battery_level,
    network_type: network_type.clone(),
});
```

```rust
let app = EventPayload::App(AppPayload {
    self_package_name: selected_package,
    collected_at,
    apps,
});
```

不要把选择结果保存到 `SimulatedDevice` 字段中；下一次 `next_cycle` 必须重新抽取。

- [ ] **步骤 6：运行目标测试和生成器回归测试**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml model::tests::app_event_serializes_with_stable_contract
cargo test --manifest-path sender/Cargo.toml generator::tests::selected_package_belongs_to_apps_and_matches_start
cargo test --manifest-path sender/Cargo.toml generator::tests
```

预期：全部通过；新字段被序列化，start/app 包名相同且包名存在于安装列表，原有
平台匹配、安装历史、时间戳、顺序与资源边界测试保持通过。

- [ ] **步骤 7：提交数据结构与生成逻辑**

```bash
git add sender/src/model.rs sender/src/generator.rs
git commit -m "feat(数据契约): 增加启动 App 包名关联"
```

### 任务 2：更新 Kafka 往返断言、文档和运行产物

**文件：**
- 修改：`sender/tests/kafka_integration.rs:80-145`
- 修改：`sender/README.md:90-250`
- 修改：`README.md:5-15`

- [ ] **步骤 1：增强真实 Kafka 测试的断言**

在收齐四条消息并确认顺序后加入：

```rust
let start = &received[0].2["payload"];
let app = received
    .iter()
    .find_map(|(_, _, value)| {
        (value["protol_type"] == "app").then_some(&value["payload"])
    })
    .expect("app payload");
let selected = app["self_package_name"]
    .as_str()
    .expect("self_package_name string");

assert_eq!(start["app_package"], selected);
assert!(
    app["apps"]
        .as_array()
        .expect("apps array")
        .iter()
        .any(|installed| installed["package_name"] == selected)
);
```

- [ ] **步骤 2：运行普通测试，确认集成测试可编译且默认忽略**

运行：

```bash
cargo test --manifest-path sender/Cargo.toml --all-features
```

预期：所有普通测试通过；真实 Kafka 测试显示 ignored，而不是编译失败。

- [ ] **步骤 3：更新 sender 数据契约**

在 `sender/README.md` 中完成以下修改：

- `start` 示例的 `app_package` 改为示例安装列表中存在的 `com.tencent.mm`。
- `start.app_package` 语义注明：来自本次启动 `app.payload.apps`，与
  `app.payload.self_package_name` 相同。
- `app` 示例加入 `"self_package_name": "com.tencent.mm"`。
- `app` payload 字段表增加 `self_package_name | string`，注明每个启动周期重新随机选择。
- Flink 建议注明该字段为必填 `STRING`，并可用安装列表校验其存在性。

- [ ] **步骤 4：更新仓库概览**

在根 `README.md` 的核心能力中加入：

```markdown
- 每次启动从设备安装列表随机选择当前 App；`start.payload.app_package` 与
  `app.payload.self_package_name` 在同一启动周期内保持一致。
```

- [ ] **步骤 5：格式、静态检查和完整测试**

依次运行：

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
git diff --check
```

预期：所有命令退出码为 0，无格式、Clippy、测试或空白错误。

- [ ] **步骤 6：运行真实 Kafka 集成测试**

运行：

```bash
KAFKA_BROKERS=192.168.250.106:9092 \
KAFKA_TOPIC=flink_demo \
cargo test --manifest-path sender/Cargo.toml --test kafka_integration -- --ignored --nocapture
```

预期：`sends_and_consumes_one_ordered_startup_cycle ... ok`，结果为
`1 passed; 0 failed`，消费到的 app 消息满足新增字段不变量。

- [ ] **步骤 7：构建并刷新运行目录二进制**

运行：

```bash
cargo build --manifest-path sender/Cargo.toml --release
cp sender/target/release/mobile-event-sender runner/mobile-event-sender
```

验证：

```bash
shasum -a 256 sender/target/release/mobile-event-sender runner/mobile-event-sender
```

预期：两个 SHA-256 完全相同。`runner/` 已由 `.gitignore` 忽略，因此二进制不进入 Git 提交。

- [ ] **步骤 8：提交测试和文档**

```bash
git add sender/tests/kafka_integration.rs sender/README.md README.md
git commit -m "docs(数据契约): 说明启动 App 包名关联"
```

- [ ] **步骤 9：最终核对提交范围**

运行：

```bash
git status --short
git log --oneline -4
```

预期：工作区干净；提交仅包含模型、生成器、集成测试与文档变更，`runner/`
产物不出现在 Git 状态中。
