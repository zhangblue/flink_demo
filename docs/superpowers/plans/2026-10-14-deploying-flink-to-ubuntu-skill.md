# Ubuntu Flink 部署 Skill 与 Kafka offset 回退实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 将 Kafka 无可用 offset 时的回退策略改为最新位置，并创建一个需要逐次授权、可选择是否从 savepoint 恢复的项目级 Ubuntu Flink 部署 Skill。

**架构：** Kafka Source 继续使用 committed-offset initializer，只把缺失 committed offset 时的 reset strategy 改为 `LATEST`，checkpoint/savepoint 恢复优先级保持不变。项目级 Skill 负责用户交互与权限边界，Shell 脚本只执行可测试的机械部署步骤；脚本默认只读预检，变更模式必须同时收到显式确认标志和恢复模式。

**技术栈：** Java 17、Apache Flink 2.3.0、Flink Kafka connector `5.0.0-2.2`、Maven、Bash、SSH/SCP、Codex Agent Skills。

---

## 文件结构

- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java` — 使用 Kafka committed-offset + `LATEST` 回退策略。
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java` — 从实际 SourceTransformation 验证 offset initializer 的回退语义。
- 修改：`flink_processer/README.md` — 说明无快照、无 committed offset 时从最新位置消费。
- 创建：`.agents/skills/deploying-flink-to-ubuntu/SKILL.md` — 项目级部署 Skill 的触发条件、两次询问、停止条件与结果报告。
- 创建：`.agents/skills/deploying-flink-to-ubuntu/agents/openai.yaml` — Skill UI 元数据，保留隐式发现。
- 创建：`.agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh` — 可重复、默认只读、显式授权后执行的部署脚本。
- 创建：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh` — 使用假的本地/远端命令验证部署脚本，不接触 Ubuntu。
- 创建（忽略目录）：`.superpowers/sdd/2026-10-14-deploying-flink-to-ubuntu-skill/skill-baseline.md` — 记录无 Skill 时的基线行为和失败模式。
- 创建（忽略目录）：`.superpowers/sdd/2026-10-14-deploying-flink-to-ubuntu-skill/skill-forward-test.md` — 记录加载 Skill 后的独立行为验证。

### 任务 1：Kafka 无 offset 时回退到最新位置

**文件：**
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java`
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java`
- 修改：`flink_processer/README.md`

- [ ] **步骤 1：编写失败的 offset initializer 测试**

在 `EventEnrichmentJobTest` 添加窄范围表征测试，从实际 `SourceTransformation` 取得 `KafkaSource`，再读取当前 connector 版本没有公开 getter 的 `startingOffsetsInitializer`：

```java
@Test
void sourceUsesCommittedOffsetsWithLatestFallback() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EventEnrichmentJob.buildPipeline(env,
            new JobConfig("localhost:9092", "input", "output", "errors", "test-group", 1, 1500L,
                    "file:///tmp/checkpoints", "file:///tmp/savepoints"));
    SourceTransformation<?, ?, ?> source = (SourceTransformation<?, ?, ?>) env.getTransformations().stream()
            .flatMap(transformation -> transformation.getTransitivePredecessors().stream())
            .filter(transformation -> "kafka-source".equals(transformation.getUid()))
            .findFirst().orElseThrow();
    KafkaSource<?> kafkaSource = (KafkaSource<?>) source.getSource();
    Field field = KafkaSource.class.getDeclaredField("startingOffsetsInitializer");
    field.setAccessible(true);
    OffsetsInitializer initializer = (OffsetsInitializer) field.get(kafkaSource);

    assertThat(initializer.getAutoOffsetResetStrategy())
            .isEqualTo(OffsetResetStrategy.LATEST);
}
```

增加 `java.lang.reflect.Field`、`KafkaSource`、`OffsetsInitializer` 和 `OffsetResetStrategy` import。反射字段名被明确限定为本工程锁定的 connector `5.0.0-2.2`；测试检查的是实际接入 pipeline 的 source，而不是平行测试辅助函数。它应抓住把回退策略保留为 `EARLIEST` 或改成 `earliest()` 的回归。

- [ ] **步骤 2：运行测试并确认 RED**

运行：

```bash
mvn -f flink_processer/pom.xml \
  -Dtest=EventEnrichmentJobTest#sourceUsesCommittedOffsetsWithLatestFallback test
```

预期：断言失败，实际 reset strategy 为 `EARLIEST`。不得先改生产代码让测试立即通过。

- [ ] **步骤 3：实现最小 offset 策略修改**

把 `EventEnrichmentJob` builder 中的：

```java
.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
```

替换为：

```java
.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
```

更新相邻中文注释，明确：快照状态优先；没有可恢复状态时读取 committed offset；缺失 committed offset 才回退到最新位置。

- [ ] **步骤 4：运行定向测试并确认 GREEN**

运行：

```bash
mvn -f flink_processer/pom.xml -Dtest=EventEnrichmentJobTest test
```

预期：`EventEnrichmentJobTest` 全部通过。

- [ ] **步骤 5：同步 README**

将“没有已提交 offset 时从最早位置开始”改为以下语义：

```text
新作业优先使用 consumer group 已提交的 offset；某分区没有有效 committed offset 时从该分区最新位置开始。
从 checkpoint/savepoint 恢复时，快照中的 Kafka source offset 优先于 consumer group 和该回退策略。
```

不得暗示 `LATEST` 会覆盖 checkpoint/savepoint 中的 source offset。

- [ ] **步骤 6：运行模块回归并提交**

运行：

```bash
mvn -f flink_processer/pom.xml test
git diff --check
```

预期：全部测试通过，差异检查无输出。

提交：

```bash
git add flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java \
  flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java \
  flink_processer/README.md
git commit -m "fix(Flink): 无 offset 时从最新位置消费"
```

### 任务 2：以 TDD 创建安全的部署脚本

**文件：**
- 创建：`.agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh`
- 创建：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh`
- 创建（忽略目录）：`.superpowers/sdd/2026-10-14-deploying-flink-to-ubuntu-skill/skill-baseline.md`

- [ ] **步骤 1：运行无 Skill 基线场景并记录失败模式**

使用一个全新子代理，不向它提供目标 Skill，只提供下列安全场景，要求它只给出下一步动作而不调用工具：

```text
这个仓库的 Flink JAR 已构建完成，请部署到 ubuntu。服务器上可能已有同名运行作业。
只说明你接下来会询问什么和执行什么，不要真正连接服务器或修改文件。
```

记录它是否遗漏以下任一要求：逐次部署确认、单独询问是否从 savepoint 恢复、恢复模式不使用 `--drain`、无状态模式不读取 savepoint、多个同名作业时停止。把逐字结果和基线缺口写入 `skill-baseline.md`。这是 Skill 的 RED；不要先写 `SKILL.md`。

- [ ] **步骤 2：先编写失败的 Shell 行为测试**

`deploy_test.sh` 在临时目录创建假的 `mvn`、`jar`、`git`、`shasum`、`ssh`、`scp` 和 `sleep`，通过 `PATH` 注入并把所有调用记录到日志。测试至少包含这些独立用例：

```bash
test_preflight_never_mutates_remote
test_execute_requires_confirmation_flag
test_multiple_matching_jobs_abort_before_upload
test_savepoint_mode_stops_without_drain_and_restores_with_s
test_stateless_mode_cancels_and_submits_without_s
test_checksum_mismatch_keeps_old_job_running
test_missing_running_job_requires_explicit_savepoint_path
test_new_job_not_running_reports_failure_without_automatic_rollback
```

关键断言使用实际命令日志，而不是检查脚本文本。例如 savepoint 模式必须观察到：

```bash
assert_log_contains "flink stop --savepointPath /home/zhangdi/workspace/flink_data/savepoints <old-job-id>"
assert_log_not_contains "--drain"
assert_log_contains "flink run --detached -s file:///home/zhangdi/workspace/flink_data/savepoints/<generated>"
```

无状态模式必须观察到 `flink cancel <old-job-id>`，且整个提交命令不含 ` -s `。

- [ ] **步骤 3：运行测试并确认 RED**

运行：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
```

预期：失败，因为 `scripts/deploy.sh` 尚未实现所需接口或行为。确认失败来自缺少部署行为，而不是测试夹具语法错误。

- [ ] **步骤 4：实现最小部署脚本**

脚本接口固定为：

```text
deploy.sh
deploy.sh --execute --confirmed --restore-mode savepoint [--savepoint-path <file-uri>]
deploy.sh --execute --confirmed --restore-mode stateless
```

无参数为只读预检。`--execute` 缺少 `--confirmed` 或有效 `--restore-mode` 时必须在上传、停止、取消、提交之前失败。

脚本使用以下固定配置：

```bash
SSH_HOST="ubuntu"
REMOTE_DATA_DIR="/home/zhangdi/workspace/flink_data"
REMOTE_FLINK_HOME="/home/zhangdi/works/flink-2.3.0"
REMOTE_JAVA_HOME="/home/zhangdi/works/jdk-21.0.12.1+1"
REMOTE_SAVEPOINT_DIR="$REMOTE_DATA_DIR/savepoints"
JOB_NAME="mobile-event-common-enrichment"
LOCAL_JAR="flink_processer/target/flink-processer-1.0.0.jar"
```

版本化名称使用 `git rev-parse --short=12 HEAD`。主要函数保持单一职责：

```bash
parse_args
verify_local_build
read_running_jobs
upload_and_verify
stop_with_savepoint
cancel_without_restore
submit_job
wait_until_running
```

执行顺序必须是：本地 `mvn clean verify` 与 fat JAR 检查 → 远端只读预检 → 参数/作业数量门禁 → 上传临时文件并核对 SHA-256 → 原子移动为版本化 JAR → 停止或取消旧作业 → 提交新作业 → 最多 30 秒轮询新 Job ID 是否进入 `RUNNING`。

savepoint 模式规则：

- 有一个旧作业：运行 `flink stop --savepointPath ... <job-id>`，解析 `Savepoint completed. Path:`；不得传 `--drain`。
- 没有旧作业：必须提供 `--savepoint-path`，并在远端验证其 `_metadata` 可读；不自动选择最新目录。
- 新提交命令在 JAR 前加入 `-s <savepoint-uri>`。

stateless 模式规则：

- 有一个旧作业：运行 `flink cancel <job-id>`。
- 没有旧作业：直接提交。
- 不查找、不验证、不传递 savepoint。

任何模式发现多个同名运行作业都在上传前停止。上传摘要不一致时删除本次临时上传文件并停止，不触碰旧作业。新作业失败时只报告旧作业状态和回滚所需信息，不自动执行回滚。

远端提交显式指定入口类和现有业务参数，恢复参数只在 savepoint 模式加入 JAR 之前：

```bash
flink run --detached \
  ${restore_args[@]} \
  -c com.example.flinkprocesser.EventEnrichmentJob \
  "/home/zhangdi/workspace/flink_data/flink-processer-${short_sha}.jar" \
  --bootstrap-servers 192.168.250.106:9092 \
  --input-topic flink_demo \
  --output-topic flink_output \
  --error-topic flink_error \
  --group-id flink-processer \
  --parallelism 1 \
  --checkpoint-interval-ms 60000 \
  --checkpoint-storage-uri file:///home/zhangdi/workspace/flink_data/checkpoints \
  --savepoint-storage-uri file:///home/zhangdi/workspace/flink_data/savepoints
```

实现不得使用 `eval`。解析出的 Job ID 必须匹配 `[0-9a-f]{32}`；用户提供或命令返回的 savepoint URI 必须位于 `file:///home/zhangdi/workspace/flink_data/savepoints/` 下，路径字符限制为字母、数字、点、下划线、斜杠和连字符。所有动态值先验证再进入 SSH 命令。

- [ ] **步骤 5：运行 Shell 测试并确认 GREEN**

运行：

```bash
bash -n .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh
bash .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
```

预期：语法检查成功，所有场景通过，假的命令日志中没有未授权远端变更。

- [ ] **步骤 6：提交部署脚本与测试**

```bash
git add .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh \
  .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
git commit -m "feat(Flink): 添加 Ubuntu 部署脚本"
```

不要提交 `.superpowers/sdd` 下的基线记录。

### 任务 3：编写并前向验证项目级部署 Skill

**文件：**
- 创建：`.agents/skills/deploying-flink-to-ubuntu/SKILL.md`
- 创建：`.agents/skills/deploying-flink-to-ubuntu/agents/openai.yaml`
- 创建（忽略目录）：`.superpowers/sdd/2026-10-14-deploying-flink-to-ubuntu-skill/skill-forward-test.md`

- [ ] **步骤 1：根据基线缺口编写最小 Skill**

frontmatter 使用：

```yaml
---
name: deploying-flink-to-ubuntu
description: Use when deploying this repository's flink_processer build to the configured Ubuntu standalone Flink service, including upgrades, savepoint recovery, or stateless replacement.
---
```

正文必须给未来代理一个确定的交互契约：

1. 运行只读预检后，询问“是否确认部署到 Ubuntu？”；没有本次明确确认，不得使用 `--execute`。
2. 确认部署后，单独询问“新程序是否从 savepoint 恢复？”；不得从上下文猜测。
3. 选择恢复时传 `--restore-mode savepoint`；无运行作业时再要求用户提供准确 savepoint URI。
4. 选择不恢复时传 `--restore-mode stateless`；不检查或使用任何旧 savepoint。
5. 只有两个回答都明确后，运行 `scripts/deploy.sh --execute --confirmed ...`。
6. 多个同名作业、构建失败、摘要不一致、savepoint 失败、Job ID/路径无法解析或新作业未运行时停止并报告。
7. 不自动启动/停止整个 Flink 集群，不删除历史产物，不自动回滚。
8. 成功报告新 Job ID、远端版本化 JAR、恢复选择、实际 savepoint 路径（若有）和运行验证。

Skill 应引用同目录脚本，而不是复制完整 SSH 命令。不得把用户此前某一次“确认”解释为后续部署的持续授权。

- [ ] **步骤 2：生成并校验 Skill UI 元数据**

运行：

```bash
python3 /Users/zhangdi/.codex/skills/.system/skill-creator/scripts/generate_openai_yaml.py \
  .agents/skills/deploying-flink-to-ubuntu \
  --interface display_name="Deploy Flink to Ubuntu" \
  --interface short_description="安全部署本项目 Flink 作业到 Ubuntu" \
  --interface default_prompt='Use $deploying-flink-to-ubuntu to deploy the verified Flink processor to Ubuntu.'
```

保持默认隐式发现，不添加外部 MCP 依赖。随后运行：

```bash
python3 /Users/zhangdi/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  .agents/skills/deploying-flink-to-ubuntu
```

预期：校验通过，无 scaffold 占位符。检查 `agents/openai.yaml` 中字符串均有引号，`default_prompt` 明确包含 `$deploying-flink-to-ubuntu`。

- [ ] **步骤 3：用全新子代理进行 Skill GREEN 测试**

让新的独立子代理读取 `.agents/skills/deploying-flink-to-ubuntu/SKILL.md`，再给它与基线完全相同的部署请求，要求只说明下一步而不执行工具。它必须先提出当次部署确认，不能直接部署或把已有确认当作持续授权。

在它收到模拟的“确认部署”后，再发送后续输入；它必须单独询问是否从 savepoint 恢复。分别测试回答“是”和“否”：

- “是”映射到 savepoint 模式，明确不使用 `--drain`；
- “否”映射到 stateless 模式，不查找也不传 `-s`。

把实际输出、判定和与基线的差异记录到 `skill-forward-test.md`。如果子代理绕过任一门禁，只针对观察到的漏洞收紧 `SKILL.md` 并重新测试。

- [ ] **步骤 4：复跑脚本与 Skill 校验并提交**

运行：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
python3 /Users/zhangdi/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  .agents/skills/deploying-flink-to-ubuntu
git diff --check
```

提交：

```bash
git add .agents/skills/deploying-flink-to-ubuntu/SKILL.md \
  .agents/skills/deploying-flink-to-ubuntu/agents/openai.yaml
git commit -m "feat(Flink): 添加 Ubuntu 部署技能"
```

不要提交两份 `.superpowers/sdd` 行为验证记录。

### 任务 4：完整集成验证与交付

**文件：**
- 验证所有已修改和新增文件；除非验证暴露问题，否则不增加范围。

- [ ] **步骤 1：运行完整 Maven 构建**

```bash
mvn -f flink_processer/pom.xml clean verify
```

预期：全部测试通过，`BUILD SUCCESS`。

- [ ] **步骤 2：验证 fat JAR 内容**

```bash
jar tf flink_processer/target/flink-processer-1.0.0.jar \
  | rg 'EventRecordKafkaDeserializer|EventTimePolicy|CommonStateEnrichmentFunction|org/apache/kafka'
```

预期：三个项目类和 Kafka 客户端类均存在。

- [ ] **步骤 3：运行 Skill 与脚本完整验证**

```bash
bash -n .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh
bash .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
python3 /Users/zhangdi/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  .agents/skills/deploying-flink-to-ubuntu
```

预期：全部退出 0。此步骤必须使用假的命令环境，不连接 Ubuntu，也不产生真实部署。

- [ ] **步骤 4：检查仓库状态与规格覆盖**

```bash
git diff --check
git status -sb
git log --oneline --decorate -5
```

逐项核对：offset 回退为 `LATEST`；Skill 每次询问部署确认；恢复选择单独询问；savepoint/stateless 两条路径互斥；版本化 JAR；多个作业停止；无自动集群启停、删除或回滚。

- [ ] **步骤 5：最终审查**

请求一个只读审查者检查设计规格到分支 HEAD 的完整 diff，重点审查部署授权是否可绕过、Shell 引号/命令注入、Job ID/savepoint 解析、失败后旧作业安全性、测试是否只测真实可观察行为。若有 Critical/Important，使用 TDD 修复后做一次定向复审。

- [ ] **步骤 6：交付但不部署**

报告分支、提交、测试数、Skill 路径和脚本接口。明确说明：本计划的实现与验证不等于已部署；真实上传、停止旧作业和提交新作业必须在使用 Skill 时再次取得用户确认。
