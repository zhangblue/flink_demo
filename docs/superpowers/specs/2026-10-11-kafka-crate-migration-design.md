# Kafka Rust 客户端迁移设计

## 背景与目标

当前发送器使用 `rdkafka` 和 librdkafka 的异步生产者。此次改动将 Kafka
生产、测试消费等所有 Rust 客户端代码统一迁移到 crates.io 上名为 `kafka`
的 crate，版本采用当前稳定版 `0.10.0`，并从依赖中完全移除 `rdkafka`。

迁移后继续使用 Tokio 驱动设备模拟、全局限速、关闭信号和任务协调。消息 JSON
结构、Kafka key、设备启动周期、同设备消息顺序和全局发送速度保持不变。

## 已确认的约束

- `rdkafka` 必须从生产代码、测试代码和 `Cargo.toml` 中完全移除。
- 生产者与真实 Kafka 集成测试消费者统一使用 `kafka` crate。
- Kafka 的同步网络 I/O 不得直接阻塞 Tokio 工作线程。
- 同一设备的事件继续使用 `udid` 作为 Kafka key，并保持入队顺序。
- 保留有界背压、每条消息的成功或失败统计、关闭时排空以及可观测错误日志。
- 保留现有 `config.toml` 中的 `brokers`、`topic`、`compression` 和
  `message_timeout_ms` 配置入口。
- `compression` 支持 `none`、`gzip` 和 `snappy`，其他值在配置校验阶段拒绝。
- 使用 `RequiredAcks::All` 和配置的 ack timeout。
- `kafka` crate 不提供 librdkafka 的幂等生产者能力，因此迁移后不承诺
  exactly-once；异常重试可能产生重复消息，消费者应继续以 `event_id` 去重。

## 方案选择

### 采用：单个专用阻塞工作线程

`KafkaSink` 创建一个专用工作线程，该线程独占一个同步
`kafka::producer::Producer`。Tokio 调度器只把已经序列化的消息写入有界命令队列，
不会直接执行同步 Kafka 网络调用。工作线程严格按命令到达顺序调用生产者，从而避免
多个 `spawn_blocking` 任务竞争锁时可能产生的顺序变化。

未采用的方案：

- 每条消息单独 `spawn_blocking`：线程调度和 Producer 锁竞争使顺序控制更复杂。
- 在 Tokio 调度器中直接同步发送：Kafka 网络延迟会阻塞异步运行时。

## 组件设计

### Kafka 配置

`KafkaConfig.brokers` 继续接受逗号分隔的 broker 地址。创建生产者前将其按逗号拆分、
去除空白并形成 `Vec<String>`；任何空 broker 项均视为配置错误。

配置映射如下：

| 当前配置 | `kafka` crate 行为 |
| --- | --- |
| `brokers` | `Producer::from_hosts` / `Consumer::from_hosts` |
| `topic` | `Record::from_key_value` 的 topic |
| `compression = "none"` | 不设置压缩 |
| `compression = "gzip"` | `Compression::GZIP` |
| `compression = "snappy"` | `Compression::SNAPPY` |
| `message_timeout_ms` | producer ack timeout、队列重试截止时间、flush 屏障等待时间 |

新增明确的配置错误类型，用于报告空 broker 项和不支持的压缩值。默认配置文件字段名与
启动命令保持不变。

### KafkaSink 与工作线程

`KafkaSink` 保存以下状态：

- topic；
- message timeout；
- 有界工作命令发送端；
- 工作线程状态或关闭句柄。

工作命令包括：

1. `Send`：包含 key、JSON value 和 Tokio oneshot 回执发送端；
2. `Barrier`：包含同步回执，用于确认此前的所有 `Send` 已处理；
3. `Shutdown`：结束工作循环，由资源释放路径使用。

工作线程启动后创建并独占 `Producer`，按照命令顺序调用同步 `send`。生产者创建失败
必须反馈给 `KafkaSink::new`，不能让程序在后台静默启动失败。

### 入队和确认

`EventSink::enqueue` 的外部契约保持不变：

1. 将事件序列化为 JSON，并取 `udid` 作为 key；
2. 通过非阻塞 `try_send` 写入有界工作队列；
3. 队列已满映射为 `SinkError::QueueFull`，让现有截止时间重试逻辑继续工作；
4. 工作线程断开映射为明确的 worker unavailable 错误；
5. 返回的 `DeliveryAck` 等待 oneshot，并把同步 `Producer::send` 结果映射为成功或
   `SinkError::Delivery`。

每条消息只有在 broker 按 `RequiredAcks::All` 确认后才计入 delivered。序列化失败、
队列超时、工作线程断开和 Kafka 发送失败分别保留可区分的错误信息。

### Flush 与关闭

`flush` 向同一 FIFO 队列发送 `Barrier`，并最多等待 `message_timeout_ms`。收到屏障回执
意味着屏障之前的发送命令都已完成，因为只有一个工作线程顺序处理命令。

正常关闭流程继续先停止设备任务、关闭事件输入、等待全部 delivery ack，再执行
`flush`。`KafkaSink` 被释放时发送关闭命令并回收工作线程；不得因为工作线程异常退出
而无限等待。

### Tokio 边界

设备任务、全局限速器、dispatcher、mpsc 事件通道和 shutdown 协调器仍由 Tokio
运行。只有 `kafka` crate 的 Producer 创建、发送和屏障处理位于专用阻塞线程。

## 集成测试迁移

真实 Kafka 测试改用 `kafka::consumer::Consumer`：

- 使用唯一 consumer group；
- fallback offset 使用 latest，避免消费历史数据；
- 发送测试周期后轮询目标 topic；
- 从 message set 读取 partition、offset、key 和 value；
- 标记消息已消费并提交 offset；
- 保留现有四条启动周期消息、顺序、Kafka key、`start_id`、JSON 完整内容以及
  `self_package_name` 关联断言。

Consumer 的同步 poll 放到阻塞上下文中，避免阻塞 Tokio 测试运行时。测试仍默认
`#[ignore]`，显式提供真实 Kafka 配置时执行。

## 测试策略

### 单元测试

- broker 列表拆分和空项校验；
- compression 三种合法值及非法值；
- worker 保持 FIFO 顺序；
- 工作队列已满仍返回 `QueueFull`；
- 每条同步发送结果正确传到 delivery ack；
- worker 断开和发送失败可见；
- barrier 只在此前发送全部完成后成功；
- barrier 超时和 worker 异常不会无限阻塞；
- 现有全局限速、重试截止时间、关闭排空和报告计数测试保持通过。

生产者测试通过内部可替换的工作线程发送接口验证，不要求单元测试连接 Kafka，也不在
生产接口中暴露测试专用能力。

### 验证

- `cargo fmt --check`；
- `cargo clippy --all-targets --all-features -- -D warnings`；
- `cargo test --all-features`；
- 真实 Kafka 往返测试连接 `192.168.250.106:9092` 的 `flink_demo`；
- release 构建并刷新 `runner/mobile-event-sender`，确认 SHA-256 一致；
- `Cargo.lock` 和源码中不再出现 `rdkafka` / `rdkafka-sys`。

## 文档和兼容性

README 更新依赖与发送语义，明确：

- 程序仍用 Tokio，但 Kafka I/O 在专用同步工作线程执行；
- `acks=all` 不等于幂等生产，异常重试可能重复；
- `event_id` 是消费者去重依据；
- 配置字段名不变，压缩值限制为 `none`、`gzip`、`snappy`。

本次不修改任何事件 JSON 字段，因此 Flink 表结构无需变化。
