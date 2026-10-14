# Ubuntu Flink 部署 Skill 设计

## 目标

在仓库中新增项目级 Skill，统一 `flink_processer` 的构建、上传和 Ubuntu 单机 Flink 部署流程。未来完成 Flink 代码修改后，可使用该 Skill 将验证通过的版本化 JAR 上传到服务器，并在用户明确确认后替换当前作业。

本次同时调整 Kafka Source 的无 offset 启动策略：优先恢复 Flink checkpoint/savepoint 中的 source offset，其次使用 consumer group 已提交 offset；只有两者都不可用时，从 Kafka 各分区的最新位置开始，不回放此前历史消息。

Skill 只服务本仓库，不修改 Ubuntu 的全局 Flink 配置，也不自动清理旧 JAR、checkpoint 或 savepoint。

## 固定环境

- 项目级 Skill：`.agents/skills/deploying-flink-to-ubuntu/`
- 本地模块：`flink_processer`
- 本地产物：`flink_processer/target/flink-processer-1.0.0.jar`
- SSH 主机别名：`ubuntu`
- 远端上传目录：`/home/zhangdi/workspace/flink_data`
- 远端 Flink：`/home/zhangdi/works/flink-2.3.0`
- 远端 Java：`/home/zhangdi/works/jdk-21.0.12.1+1`
- 远端 checkpoint：`/home/zhangdi/workspace/flink_data/checkpoints`
- 远端 savepoint：`/home/zhangdi/workspace/flink_data/savepoints`
- Flink 作业名：`mobile-event-common-enrichment`

上传后的文件名使用当前 Git 提交的短 SHA：`flink-processer-<short-sha>.jar`。旧版本保留，供人工回滚。

## Kafka offset 回退规则

`KafkaSource` 使用 committed offsets 初始化器，并把缺失 committed offset 时的回退策略由 `EARLIEST` 改为 `LATEST`。该配置只影响没有可恢复 source 状态且 consumer group 对相应分区没有有效已提交 offset 的启动：

1. 从 checkpoint/savepoint 恢复时，使用快照内记录的 source offset；
2. 无快照状态但存在 consumer group committed offset 时，从 committed offset 继续；
3. 两者都不可用时，从部署时 Kafka 分区末尾开始，只消费之后到达的新数据。

实现需同步更新拓扑测试和 `flink_processer/README.md`，避免文档继续声称新 group 会从最早位置回放。

## 组成

### `SKILL.md`

负责触发条件、授权边界、部署决策和异常停止条件。每次部署前必须询问用户是否确认部署；未获得当次明确确认时，只允许本地构建和远端只读检查。

部署确认后还必须询问：**“新程序是否从 savepoint 恢复？”** 该选择决定旧作业的停止方式和新作业的提交参数。

### `scripts/deploy.sh`

封装重复且易错的机械步骤，包括：

- 本地 Maven 验证；
- 版本化 JAR 命名与 SHA-256 计算；
- 远端目录和 Flink CLI 检查；
- JAR 上传与两端摘要核对；
- 按作业名识别运行作业；
- 根据恢复选择停止旧作业；
- 提交新作业并验证状态。

脚本默认执行只读预检并展示计划。只有 Skill 已获得用户当次部署确认后，才可使用脚本的执行模式。恢复选择必须作为显式参数传入，不能使用隐藏默认值。

## 部署流程

### 共同阶段

1. 在仓库根目录运行 `mvn -f flink_processer/pom.xml clean verify`。
2. 检查 fat JAR 包含作业类、事件时间类、状态函数和 Kafka 依赖。
3. 生成版本化远端文件名和本地 SHA-256。
4. 通过 SSH 只读检查 Flink CLI、Java、目标目录、集群状态及同名运行作业。
5. 若发现多个同名运行作业，立即停止并报告，不选择其中任何一个。
6. 展示将要上传的版本、远端路径、当前作业和部署模式，询问用户是否确认部署。
7. 询问新程序是否从 savepoint 恢复。
8. 获得回答后上传 JAR，并核对本地与远端 SHA-256。摘要不一致时停止。

上传应在停止旧作业之前完成，以缩短停机时间。

### 从 savepoint 恢复

如果有且仅有一个同名运行作业：

1. 使用 `flink stop --savepointPath /home/zhangdi/workspace/flink_data/savepoints <job-id>` 生成最终 savepoint 并停止旧作业。
2. 不使用 `--drain`，避免推进最大 watermark 后再恢复造成事件时间语义变化。
3. 从成功输出中取得明确的 savepoint 路径。
4. 使用 `flink run --detached -s <savepoint-path> ...` 提交版本化 JAR。

如果没有同名运行作业，则要求用户提供明确的已有 savepoint 路径。Skill 不自动选择“最新”目录。远端验证该路径及 `_metadata` 可读后再提交。

### 不从 savepoint 恢复

如果有且仅有一个同名运行作业，使用 Flink CLI 取消该作业。提交新 JAR 时不传 `-s`，也不查找或选择任何旧 savepoint。已有 checkpoint/savepoint 文件保持不动，但不参与本次部署。

如果没有同名运行作业，直接以无状态方式提交新 JAR。

## 提交参数

新作业使用 `com.example.flinkprocesser.EventEnrichmentJob` 入口类和仓库当前默认业务参数。checkpoint 与 savepoint URI继续使用：

- `file:///home/zhangdi/workspace/flink_data/checkpoints`
- `file:///home/zhangdi/workspace/flink_data/savepoints`

恢复模式仅在用户选择恢复时增加 `-s <savepoint-path>`。Flink CLI 参数位于 JAR 之前，作业参数位于 JAR 之后。

## 成功条件

- 本地完整测试成功；
- fat JAR 内容检查成功；
- 上传文件摘要一致；
- 新提交命令返回 Job ID；
- 通过 Flink CLI 或 REST 确认该 Job ID 进入 `RUNNING`；
- 旧作业不再处于运行状态；
- 最终报告包含新 Job ID、远端 JAR 路径、是否恢复、实际 savepoint 路径和验证结果。

## 失败与回滚边界

- 本地构建、远端预检或摘要校验失败时，不停止旧作业。
- savepoint 生成失败时，不提交新作业。
- 新作业提交或启动验证失败时，报告旧作业状态和可用 savepoint 路径，给出回滚命令，但未经再次确认不自动回滚。
- 多个同名作业、无法可靠解析 Job ID 或 savepoint 路径时立即停止。
- 不自动启动或停止整个 Flink 集群；如果集群不可用，报告状态并等待用户决定。
- 不删除旧 JAR、checkpoint、savepoint 或 Kafka 数据。

## 验证

- 使用 Skill 初始化器/校验器检查目录、frontmatter 和未完成占位符。
- 通过拓扑测试验证 Kafka Source 的 committed-offset 缺失回退策略为 `LATEST`，并验证 README 与实现一致。
- 对部署脚本运行 shell 语法检查和只读 dry-run。
- 使用隔离的命令替身验证以下行为：
  - 未提供执行模式时不会上传、停止或提交；
  - 多个同名运行作业时中止；
  - 选择恢复时使用 `stop --savepointPath` 且提交带 `-s`；
  - 选择不恢复时取消旧作业且提交不带 `-s`；
  - 摘要不一致、savepoint 失败和新作业未运行时停止；
  - 未取得用户部署确认时，Skill 不允许执行脚本的变更模式。

真实服务器验证必须再次取得用户部署确认，并使用实际 Flink 作业状态进行验收。
