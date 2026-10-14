# Kafka 实时投递速度打印设计

## 背景与目标

手机事件发送器目前只在停止时输出累计入队、成功投递和失败数量，运行过程中无法直接观察 Kafka 的实时接收速度。

新增运行时速度打印：程序正常发送期间，每隔 1 秒在命令行输出上一统计周期内收到 Kafka broker 成功确认的消息数量，例如：

```text
当前发送速度 1000 条/秒
```

## 统计口径

- 仅当 Kafka delivery acknowledgement 返回成功时计数。
- 已进入本地 producer 队列但尚未得到 broker 确认的消息不计数。
- 入队失败、投递失败和超时消息不计数。
- 每次打印后清零周期计数，因此输出是最近一个 1 秒窗口的成功投递量，而不是配置项 `messages_per_second` 的目标值。
- 统计窗口内没有成功投递时仍打印 `当前发送速度 0 条/秒`。
- 累计 `DispatchReport.delivered` 的语义保持不变；实时速度计数是独立的周期计数。

## 架构

在 Kafka dispatcher 内创建一个共享的原子计数器，并启动独立的 Tokio 速度报告任务：

1. dispatcher 收到成功 delivery acknowledgement 时，继续增加累计 `delivered`，并把周期原子计数器加 1。
2. 速度报告任务启动后等待完整的 1 秒，再执行第一次打印。
3. 每次 tick 使用原子 `swap(0)` 取得并清零上一周期成功数。
4. 使用现有 `tracing` 日志系统按 `INFO` 级别输出中文消息，默认日志配置下直接显示在命令行。
5. dispatcher 完成接收、排空所有 delivery acknowledgement 并执行 producer flush 后，通知报告任务退出并等待其结束，避免遗留后台任务。

速度报告任务与 dispatcher 主循环分离，避免限速、QueueFull 重试、等待输入或排空确认消息时阻塞定时打印。该任务只依赖原子计数器和关闭信号，不接触 Kafka producer 或事件 payload。

## 时间与调度行为

- 固定周期为 1 秒，不新增配置项。
- 第一次输出发生在任务启动约 1 秒后，不在启动瞬间输出 0。
- 正常运行期间每秒输出一次，包括空闲周期。
- 定时器采用跳过错过 tick 的策略，运行时调度延迟后不会瞬间补打一串历史日志。
- 关闭信号优先于同一时刻到达的 tick；dispatcher 退出后不再打印速度。

## 输出格式

消息正文固定为：

```text
当前发送速度 {count} 条/秒
```

其中 `{count}` 为非负整数。该正文由 `tracing::info!` 输出，因此命令行仍会带现有 tracing subscriber 配置的时间、级别等前缀。若用户通过 `RUST_LOG` 屏蔽 `INFO` 日志，则速度消息也会被屏蔽。

## 错误与边界处理

- 计数器使用 `u64` 饱和安全假设：单秒内不可能产生接近 `u64::MAX` 的成功确认；采用原子加法不会引入锁竞争。
- 失败确认只增加既有失败累计值，不增加实时速度。
- dispatcher 因无效限速配置在启动阶段立即返回时，不启动速度报告任务。
- 报告任务的关闭信号发送失败仅表示任务已经退出，不覆盖 dispatcher 的主要结果。
- 报告任务若异常结束，dispatcher 仍应完成 Kafka 排空与返回现有 `DispatchReport`；异常应记录为错误日志，不改变当前公开返回类型。

## 测试设计

使用 Tokio 暂停时钟和内存日志 writer，不连接 Kafka：

1. 启动后不足 1 秒时不打印速度。
2. 第一个完整 1 秒后打印一次，数字等于该周期成功确认数量。
3. 下一秒没有成功确认时打印 `0`，证明周期计数已清零。
4. 失败 delivery acknowledgement 不增加速度。
5. 关闭后继续推进虚拟时间，不再出现新的速度日志。
6. 现有 dispatcher 测试继续验证累计 `enqueued`、`delivered`、`failed` 和优雅排空行为。

完整验证包括 Rust 格式检查、Clippy、全部测试、Release 构建，以及在 Kafka 可达时运行真实往返测试。真实 Kafka 测试用于确认发送路径未回归；速度日志的确定性由暂停时钟测试负责。

## 文档与兼容性

- 在 `sender/README.md` 和根 `README.md` 中说明默认每秒打印实时成功投递速度及其统计口径。
- 不修改 `config.toml` 结构，不新增 CLI 参数。
- 不修改 Kafka 消息 JSON、key、topic、发送顺序或全局限速语义。
- 保持现有 `rdkafka`、Tokio 和 tracing 技术栈。
