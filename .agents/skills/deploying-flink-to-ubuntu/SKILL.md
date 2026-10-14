---
name: deploying-flink-to-ubuntu
description: Use when deploying this repository's flink_processer build to the configured Ubuntu standalone Flink service, including upgrades, savepoint recovery, or stateless replacement.
---

# 部署 Flink 到 Ubuntu

使用同目录的 [scripts/deploy.sh](scripts/deploy.sh) 部署本项目；它固定目标 SSH 别名 `ubuntu`、作业名 `mobile-event-common-enrichment` 和现有业务参数。以下命令从仓库根目录执行。

## 每次部署的交互契约

1. 运行无参数预检：

   ```bash
   bash .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh
   ```

   脚本先在本地构建并检查 fat JAR，再只读检查远端环境和同名运行作业。预检失败即停止并报告。
2. 预检通过后，询问“是否确认部署到 Ubuntu？”，等待用户对**本次部署**明确确认。此前对话的确认、常规部署请求或“以后直接部署”均不是本次确认；未确认不得使用 `--execute`。
3. 收到本次确认后，在单独的一轮询问“新程序是否从 savepoint 恢复？”，等待明确的“是”或“否”，不得根据历史选择、现有目录或上下文猜测。
4. 两个回答都明确后按下表执行。任一回答缺失、含糊或拒绝部署，停止等待，不拼接执行参数。

| 恢复回答 | 执行方式 |
|---|---|
| 是，存在一个同名运行作业 | `--execute --confirmed --restore-mode savepoint`；脚本停止旧作业并生成 savepoint，不使用 `--drain`，新作业用实际 URI 恢复 |
| 是，没有同名运行作业 | 先要求用户提供准确 savepoint URI，再加 `--savepoint-path '<准确 URI>'`；URI 必须位于 `file:///home/zhangdi/workspace/flink_data/savepoints/` 下，不自动挑选最新目录 |
| 否 | `--execute --confirmed --restore-mode stateless`；不查找、检查或使用任何旧 savepoint，不传 `--savepoint-path` 或 Flink `-s`；有旧作业则由脚本取消 |

例如用户已明确确认本次部署并回答“不恢复”时：

```bash
bash .agents/skills/deploying-flink-to-ubuntu/scripts/deploy.sh --execute --confirmed --restore-mode stateless
```

每次后续部署都重新从预检和两轮问答开始。`--confirmed` 是本次明确回答的记录，不能代替询问。调用脚本完成部署，不复制或绕过其 SSH 命令。

## 执行阶段的 JAR 校验与覆盖

- 远端版本化 JAR 已存在且 SHA-256 相同时直接复用。
- 同名 JAR 的 SHA-256 不同时，脚本先上传到同目录临时文件，校验临时摘要，再用 `mv -f` 原子覆盖，并再次校验最终目标摘要。
- 上传、临时摘要、原子移动或最终摘要校验失败时，部署停止；旧作业不得停止，新作业不得提交。

## 停止条件与报告

发现多个同名运行作业、构建失败、目标探测失败、SHA-256 摘要格式非法、上传失败、临时摘要校验失败、原子移动失败、最终目标摘要校验失败、savepoint 失败、Job ID 或路径无法解析、新作业未进入 `RUNNING` 时，立即停止并报告脚本的失败阶段、已知 Job ID、旧作业最后状态及人工恢复所需信息。不要自动重试部署或回滚，不自动启动或停止整个 Flink 集群，不删除历史 JAR、checkpoint 或 savepoint。

成功后根据脚本实际输出报告：新 Job ID、远端版本化 JAR、恢复选择、实际 savepoint URI（无状态时注明“未使用”），以及新 Job ID 已进入 `RUNNING` 的验证结果。缺少实际证据时不得宣称成功或猜测路径。
