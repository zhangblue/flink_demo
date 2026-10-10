# 仓库 README 与 GitHub 发布实现计划

> **面向 AI 代理的工作者：** 必需子技能：在当前会话中内联执行；提交前使用 superpowers:verification-before-completion，发布时使用 superpowers:finishing-a-development-branch。

**目标：** 创建根 README 和安全忽略规则，并把已实现的发送器发布到指定 GitHub 仓库。

**架构：** 根 README 作为快速入口，`sender/README.md` 保持完整数据契约。Git 仓库使用 `main` 分支和单一初始提交，普通推送到空远端。

**技术栈：** Markdown、Git、Rust、Cargo、GitHub。

---

### 任务 1：创建仓库入口文档并发布

**文件：**
- 创建：`README.md`
- 创建：`.gitignore`
- 创建：`docs/superpowers/specs/2026-10-10-repository-readme-design.md`
- 创建：`docs/superpowers/plans/2026-10-10-repository-readme.md`

- [ ] **步骤 1：创建根 README**

写明项目能力、快速开始、配置入口、事件契约摘要、验证命令，并链接 `sender/README.md`。

- [ ] **步骤 2：添加忽略规则**

忽略 `target/`、`.DS_Store`、编辑器目录、日志和本地环境文件；保留 `Cargo.lock`。

- [ ] **步骤 3：验证文档引用与代码**

运行：

```bash
test -f sender/README.md
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
cargo build --manifest-path sender/Cargo.toml --release
```

预期：文件存在，所有 Cargo 命令退出码为 0。

- [ ] **步骤 4：初始化并检查提交内容**

```bash
git init -b main
git remote add origin https://github.com/zhangblue/flink_demo.git
git add -A
git status --short
git diff --cached --check
```

预期：仅源代码、配置和文档进入暂存区，不包含 `sender/target`。

- [ ] **步骤 5：提交并推送**

```bash
git commit -m "feat(sender): 添加手机事件模拟发送器"
git push -u origin main
```

预期：提交成功，远端 `main` 与本地 `HEAD` 一致。
