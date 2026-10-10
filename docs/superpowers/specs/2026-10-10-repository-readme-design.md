# 仓库 README 与 GitHub 发布设计

## 目标

在项目根目录提供简洁的中文入口文档，并将当前 Rust/Tokio 手机事件模拟器安全提交到空的 GitHub 仓库 `zhangblue/flink_demo`。

## 文档结构

根 `README.md` 面向首次访问仓库的开发者，包含项目用途、核心能力、目录结构、运行命令、默认 Kafka 配置、四类事件及关键顺序约束、验证命令。字段级 JSON 契约不在根文档重复维护，而是链接到 `sender/README.md` 作为唯一详细说明。

## 仓库发布

新增根 `.gitignore` 忽略 Rust `target/`、编辑器文件与本地系统文件。确认测试和格式检查通过后，在当前目录初始化 `main` 分支，配置 `origin`，创建一个 Conventional Commit，并以普通 fast-forward push 发布；不使用强制推送。目标远端当前没有分支或提交。

## 验收

- 根 README 的链接和命令对应现有文件。
- `cargo fmt --check`、Clippy、测试和 release 构建通过。
- Git 提交不包含 `sender/target` 或本地临时文件。
- `origin/main` 指向本地提交。
