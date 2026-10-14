# Flink 部署同名 JAR 覆盖实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 让 Ubuntu Flink 部署脚本在远端版本化 JAR 同名但摘要不同时，经临时上传和双重 SHA-256 校验后原子覆盖目标文件，并保证任何覆盖前校验失败都不会停止旧作业或提交新作业。

**架构：** 保留现有构建、预检、人工确认、作业停止和提交顺序，只替换 `upload_and_verify` 的发布策略：摘要相同继续复用，摘要不同或目标不存在则上传到同目录临时文件，校验临时文件后使用 `mv -f` 发布，再校验最终目标。fake PATH 测试分别模拟上传、临时摘要、原子移动和最终摘要读取，证明旧作业只会在最终 JAR 验证成功后被触碰。

**技术栈：** Bash、Maven、Flink 2.3.0、SSH/SCP、SHA-256、fake PATH Shell 测试、Codex Skill 校验器

---

## 文件结构

- 修改：`.agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh` — 实现临时上传、原子覆盖和最终目标摘要校验。
- 修改：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh` — 用隔离 fake PATH 覆盖同名 JAR 的成功与失败路径，并验证命令顺序。
- 修改：`.agents/skills/deploying-flink-to-ubuntu/SKILL.md` — 同步部署行为、停止条件和成功判定，避免 Skill 仍把同名摘要冲突描述为不可恢复错误。
- 参考：`docs/superpowers/specs/2026-10-14-flink-deploy-overwrite-design.md` — 已确认的安全边界与部署门禁；实现不得扩大范围。

### 任务 1：用失败测试定义原子覆盖契约

**文件：**
- 修改：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh:18-122`
- 修改：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh:291-315`
- 测试：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh`

- [ ] **步骤 1：扩展 fake SSH，使临时摘要和最终目标摘要可分别失败**

把 `setup` 中默认环境变量拆成临时文件与最终文件两个开关：

```bash
export FAKE_TEMP_CHECKSUM=ok FAKE_FINAL_CHECKSUM=ok
```

保留 `FAKE_EXISTING=absent|same|different|error` 负责初次目标探测；在 fake `ssh` 的 `sha256sum` 分支中按命令路径区分结果。`dispatch` 是独立子进程，而测试文件中的 `DATA` 没有导出，因此模式必须使用完整远端路径：

```bash
*'sha256sum /home/zhangdi/workspace/flink_data/.flink-processer-'*)
  if [[ "$FAKE_TEMP_CHECKSUM" == error ]]; then
    exit 1
  elif [[ "$FAKE_TEMP_CHECKSUM" == ok ]]; then
    printf '%064d  upload\n' 0
  else
    printf '%064d  upload\n' 1
  fi ;;
*'sha256sum /home/zhangdi/workspace/flink_data/flink-processer-'*)
  if [[ "$FAKE_FINAL_CHECKSUM" == error ]]; then
    exit 1
  elif [[ "$FAKE_FINAL_CHECKSUM" == ok ]]; then
    printf '%064d  installed\n' 0
  else
    printf '%064d  installed\n' 1
  fi ;;
```

初次 `if test -e ...; then sha256sum ...` 探测分支必须继续位于普通 `sha256sum` 分支之前，确保 `FAKE_EXISTING` 仍决定目标是否存在及原摘要。

在 `assert_before` 后增加一个比较“最后一次左侧命令”和“第一次右侧命令”的断言，避免初次目标探测被误认为发布后的最终校验：

```bash
assert_last_before() {
  local left right
  left=$(grep -nF -- "$1" "$CALL_LOG" | tail -1 | cut -d: -f1)
  right=$(grep -nF -- "$2" "$CALL_LOG" | head -1 | cut -d: -f1)
  [[ -n "$left" && -n "$right" && "$left" -lt "$right" ]] || { cat "$CALL_LOG"; return 1; }
}

assert_before_last() {
  local left right
  left=$(grep -nF -- "$1" "$CALL_LOG" | head -1 | cut -d: -f1)
  right=$(grep -nF -- "$2" "$CALL_LOG" | tail -1 | cut -d: -f1)
  [[ -n "$left" && -n "$right" && "$left" -lt "$right" ]] || { cat "$CALL_LOG"; return 1; }
}
```

- [ ] **步骤 2：把“摘要冲突必须终止”测试改成“摘要冲突必须安全覆盖”**

用下列测试替换 `test_existing_conflicting_version_aborts_before_stopping_old_job` 和 `test_version_install_never_overwrites_existing_artifact`：

```bash
test_existing_conflicting_version_is_atomically_overwritten_before_old_job_stops() {
  export FAKE_EXISTING=different
  execute stateless; assert_ok
  assert_log_contains "scp -o BatchMode=yes -o ConnectTimeout=10"
  assert_log_contains "mv -f $DATA/.flink-processer-abcdef123456-"
  assert_log_contains "sha256sum $DATA/flink-processer-abcdef123456.jar"
  assert_log_not_contains 'ln '
  assert_before 'scp ' 'mv -f '
  assert_before_last 'mv -f ' "sha256sum $DATA/flink-processer-abcdef123456.jar"
  [[ $(grep -c "sha256sum $DATA/flink-processer-abcdef123456.jar" "$CALL_LOG") == 2 ]]
  assert_last_before "sha256sum $DATA/flink-processer-abcdef123456.jar" "flink cancel $OLD"
}

test_absent_version_is_atomically_installed_and_verified() {
  execute stateless; assert_ok
  assert_log_contains "mv -f $DATA/.flink-processer-abcdef123456-"
  assert_log_contains "sha256sum $DATA/flink-processer-abcdef123456.jar"
  assert_log_not_contains 'ln '
  assert_before 'scp ' 'mv -f '
  assert_before_last 'mv -f ' "sha256sum $DATA/flink-processer-abcdef123456.jar"
  [[ $(grep -c "sha256sum $DATA/flink-processer-abcdef123456.jar" "$CALL_LOG") == 2 ]]
  assert_last_before "sha256sum $DATA/flink-processer-abcdef123456.jar" "flink cancel $OLD"
}
```

- [ ] **步骤 3：补充覆盖移动失败、最终摘要不一致和最终摘要读取失败测试**

加入以下测试，明确这些失败均发生在旧作业停止之前：

```bash
test_overwrite_move_failure_keeps_old_job_running() {
  export FAKE_EXISTING=different FAKE_MOVE=bad
  execute stateless; assert_failed
  assert_log_contains 'scp '
  assert_log_contains 'mv -f '
  assert_log_contains "rm -f $DATA/.flink-processer-abcdef123456-"
  assert_log_not_contains 'flink cancel '
  assert_log_not_contains 'flink stop '
  assert_log_not_contains 'flink run '
}

test_final_checksum_mismatch_keeps_old_job_running() {
  export FAKE_EXISTING=different FAKE_FINAL_CHECKSUM=bad
  execute stateless; assert_failed
  assert_log_contains 'mv -f '
  assert_log_not_contains 'flink cancel '
  assert_log_not_contains 'flink stop '
  assert_log_not_contains 'flink run '
}

test_final_checksum_read_failure_keeps_old_job_running() {
  export FAKE_EXISTING=different FAKE_FINAL_CHECKSUM=error
  execute stateless; assert_failed
  assert_log_contains 'mv -f '
  assert_log_not_contains 'flink cancel '
  assert_log_not_contains 'flink stop '
  assert_log_not_contains 'flink run '
}
```

把现有 `test_checksum_mismatch_keeps_old_job_running` 改为设置 `FAKE_TEMP_CHECKSUM=bad`；保留“相同摘要不上传、不移动”的测试，并额外断言只出现一次目标 `sha256sum`（来自初次探测）：

```bash
[[ $(grep -c "sha256sum $DATA/flink-processer-abcdef123456.jar" "$CALL_LOG") == 1 ]]
```

- [ ] **步骤 4：运行 Shell 测试，确认新契约在旧实现上失败**

运行：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
```

预期：至少以下用例 FAIL：

- `test_existing_conflicting_version_is_atomically_overwritten_before_old_job_stops`，因为旧实现遇到不同摘要立即退出；
- `test_absent_version_is_atomically_installed_and_verified`，因为旧实现使用 `ln -T` 且没有最终摘要校验；
- 两个最终摘要失败用例，因旧实现不会读取发布后的目标摘要。

- [ ] **步骤 5：提交仅包含失败测试的变更**

```bash
git add .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
git commit -m "test(Flink): 定义同名 JAR 安全覆盖契约"
```

### 任务 2：实现临时上传、原子覆盖和最终摘要校验

**文件：**
- 修改：`.agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh:174-211`
- 测试：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh`

- [ ] **步骤 1：保留相同摘要快速返回，并让不同摘要进入上传路径**

将现有目标探测逻辑改为只在摘要合法且与本地一致时复用；目标摘要格式非法仍失败，合法但不同则打印覆盖提示并继续：

```bash
if [[ "$existing" != absent ]]; then
  remote_hash=${existing%% *}
  [[ "$remote_hash" =~ ^[0-9a-f]{64}$ ]] || fail 'invalid existing versioned JAR SHA-256; old job was not stopped'
  if [[ "$remote_hash" == "$local_hash" ]]; then
    printf 'Reusing verified versioned JAR: %s\n' "$VERSIONED_JAR"
    return 0
  fi
  printf 'Existing versioned JAR differs and will be atomically replaced: %s\n' "$VERSIONED_JAR"
fi
```

- [ ] **步骤 2：用同目录 `mv -f` 发布，并在发布后验证最终目标**

临时文件摘要通过后，用以下逻辑替换 `ln -T` 发布；移动成功后清空 `TEMP_JAR`，避免后续清理误把最终路径当作临时文件：

```bash
remote "mv -f $TEMP_JAR $VERSIONED_JAR" || {
  cleanup_temp_upload
  fail 'atomic JAR replacement failed; old job was not stopped'
}
TEMP_JAR=''

if ! remote_result=$(remote "sha256sum $VERSIONED_JAR"); then
  fail 'final versioned JAR SHA-256 read failed; old job was not stopped'
fi
remote_hash=${remote_result%% *}
if [[ ! "$remote_hash" =~ ^[0-9a-f]{64}$ || "$remote_hash" != "$local_hash" ]]; then
  fail 'final versioned JAR SHA-256 mismatch; old job was not stopped'
fi
printf 'Installed and verified versioned JAR: %s\n' "$VERSIONED_JAR"
```

同时让清理函数在空路径时直接返回，保证失败处理不会执行 `rm -f` 空参数：

```bash
cleanup_temp_upload() {
  [[ -n "$TEMP_JAR" ]] || return 0
  remote "rm -f $TEMP_JAR" || printf 'Temporary upload cleanup failed: %s\n' "$TEMP_JAR" >&2
}
```

- [ ] **步骤 3：运行语法检查和部署脚本测试，确认全部通过**

运行：

```bash
bash -n .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh
bash -n .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
bash .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
```

预期：两个 `bash -n` 命令退出码为 0；测试汇总为 `0 failed`。日志顺序必须证明最终目标摘要校验早于 `flink stop` 或 `flink cancel`。

- [ ] **步骤 4：检查变更范围和空白错误**

运行：

```bash
git diff -- .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
git diff --check
```

预期：仅包含已确认的发布策略和相应 fake 测试；`git diff --check` 无输出。不得修改 `sender/config.toml`。

- [ ] **步骤 5：提交实现**

```bash
git add .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
git commit -m "fix(Flink): 安全覆盖同名部署 JAR"
```

### 任务 3：同步项目级部署 Skill 并完成全量验证

**文件：**
- 修改：`.agents/skills/deploying-flink-to-ubuntu/SKILL.md`
- 测试：`.agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh`
- 验证：`flink_processer/pom.xml`

- [ ] **步骤 1：更新 Skill 的执行说明和停止条件**

在 `SKILL.md` 的执行阶段明确写入以下行为：

```markdown
- 远端版本化 JAR 已存在且 SHA-256 相同时直接复用。
- 同名 JAR 的 SHA-256 不同时，脚本先上传到同目录临时文件，校验临时摘要，再用 `mv -f` 原子覆盖，并再次校验最终目标摘要。
- 上传、临时摘要、原子移动或最终摘要校验失败时，部署停止；旧作业不得停止，新作业不得提交。
```

从停止条件中删除“同名版本化 JAR 摘要不同必须停止”的旧描述，改为只在目标探测失败、摘要格式非法、上传失败、临时摘要失败、移动失败或最终摘要失败时停止。保留以下门禁原文含义：每次真实部署前都要重新只读预检、询问“确认部署”，并单独询问“是否从 savepoint 恢复”。

- [ ] **步骤 2：运行部署测试和 Skill 结构校验**

运行：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/tests/deploy_test.sh
PYTHONPATH=/private/tmp/movie-harbor-skill-validation-20261010/lib/python3.9/site-packages \
  python3 /Users/zhangdi/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  .agents/skills/deploying-flink-to-ubuntu
```

预期：部署测试汇总为 `0 failed`；Skill 校验输出成功。若该固定 PyYAML 路径在执行环境中不存在，先运行 `python3 -c 'import yaml; print(yaml.__file__)'`；只有本机已有可导入的 `yaml` 时才改用对应父目录，不安装依赖、不访问网络。

- [ ] **步骤 3：运行 Flink Maven 全量验证**

运行：

```bash
mvn -f flink_processer/pom.xml clean verify
```

预期：`BUILD SUCCESS`，现有 Flink 测试全部通过；本任务不修改 Java 业务代码，因此任何失败都必须先记录原始错误并判断是否由本变更引入。

- [ ] **步骤 4：执行最终仓库检查**

运行：

```bash
git diff --check
git status --short
git log --oneline -5
```

预期：`git diff --check` 无输出；状态中允许存在用户原有的 ` M sender/config.toml`，但它不出现在本任务任何 commit 中；最近提交包含设计、失败测试、实现和 Skill 文档变更。

- [ ] **步骤 5：提交 Skill 文档**

```bash
git add .agents/skills/deploying-flink-to-ubuntu/SKILL.md
git commit -m "docs(Flink): 更新同名 JAR 覆盖说明"
```

- [ ] **步骤 6：审查、合并并推送后重新执行真实部署门禁**

代码审查确认测试与设计一致后，按用户授权合并到 `main` 并推送 GitHub。真实 Ubuntu 部署不能复用上一轮确认，必须依次执行：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh
```

只读预检成功后暂停并询问用户是否确认部署；用户确认后再单独询问“是否从 savepoint 恢复”。只有得到这两个本轮新答复，才可选择以下一个命令执行：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh \
  --execute --confirmed --restore-mode stateless
```

如果预检发现一个活跃旧作业且用户选择从 savepoint 恢复，则脚本会停止该作业、生成 savepoint 并自动使用其精确 URI：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh \
  --execute --confirmed --restore-mode savepoint
```

如果没有活跃旧作业但用户选择从 savepoint 恢复，必须先取得并再次向用户确认一个真实、可读且位于 `/home/zhangdi/workspace/flink_data/savepoints/` 下的精确 `file://` URI，再把该原值作为 `--savepoint-path` 参数；不得猜测目录名。

成功判定必须同时满足：脚本输出最终目标 SHA-256 验证成功、提交得到合法 Job ID、新作业连续 30 秒保持 `RUNNING`、旧作业不再活跃；失败时不得自动重试或自动回滚。
