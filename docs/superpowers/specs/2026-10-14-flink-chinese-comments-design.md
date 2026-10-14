# Flink 代码中文注释设计

## 目标

为 `flink_processer` 的 Java 生产代码补充详细、准确的中文注释，使维护者能够直接从源码理解作业拓扑、数据流、状态生命周期、事件时间语义和 Kafka 输入输出契约。

本次只修改注释，不添加运行日志、不引入日志依赖，也不改变任何业务逻辑、配置默认值、状态描述符、算子 UID、序列化格式或 Kafka topic 行为。

## 注释范围

采用“关键路径详细注释”方案：

- 类级 JavaDoc 说明每个类在 Flink 作业中的职责及其上下游关系。
- 关键公共方法说明调用方、输入输出和异常语义。
- 复杂业务分支使用行内注释说明状态更新、路由、时间校正及容错目的。
- 简单 getter/setter 不添加重复注释，避免注释噪声掩盖业务逻辑。

覆盖以下生产类：

- `EventEnrichmentJob`：作业入口、执行环境、checkpoint、Kafka Source、watermark、keyBy、状态补全和双 Sink 拓扑。
- `CommonStateEnrichmentFunction`：托管状态初始化、逐条处理、错误侧输出、事件时间定时器续期和清理。
- `EventRecordKafkaDeserializer`：Kafka Source 反序列化回调及产出类型声明。
- `EventRecordParser`：Flink `MapFunction` 回调、JSON 校验及未来时间校正。
- `OutputEventKafkaSerializer`：Kafka Sink 序列化回调和 record key/value 生成。
- `EventTimePolicy`：事件时间、水位线、乱序容忍和空闲分区策略。
- `JobConfig`、`CommonMetadataJson`、`CommonMetadata`、`EventRecord`、`OutputEvent`：配置、状态数据及流转模型的关键契约。

## Flink 回调注释规则

凡是实现 Flink 接口或覆盖 Flink 抽象类的方法，JavaDoc 必须明确标注“触发条件”和“作用”。

| 方法 | 触发条件 | 作用重点 |
|---|---|---|
| `CommonStateEnrichmentFunction.open` | 每个并行算子实例初始化或故障恢复后打开时调用一次，早于处理输入 | 获取由 Flink 管理并参与 checkpoint/savepoint 的 keyed state |
| `processElement` | keyed stream 每到达一条消息时调用 | 在当前 key 上续期定时器、更新/读取 common 状态并互斥路由主输出或错误侧输出 |
| `onTimer` | watermark 越过已注册的事件时间定时器时，在对应 key 上调用 | 仅在回调仍是最新截止时间时清理状态，忽略陈旧回调 |
| `EventRecordParser.map` | DataStream map 算子每收到一条字符串时调用；当前生产拓扑主要在 Source 反序列化阶段直接复用解析逻辑 | 解析并校验事件信封字段 |
| `EventRecordKafkaDeserializer.deserialize` | Kafka Source 拉取到一条 ConsumerRecord 时调用 | 把 value 按 UTF-8 解析为 `EventRecord` 并交给下游 |
| `getProducedType` | Flink 构建类型系统、序列化器或执行图时查询 | 显式声明反序列化输出类型，避免泛型擦除导致类型推断不足 |
| `OutputEventKafkaSerializer.serialize` | Kafka Sink 写出每个 `OutputEvent` 前调用 | 生成目标 topic、组合 record key 和 JSON value |

这些说明以 Flink 2.3.0 项目依赖和现有接口签名为准。对于 checkpoint 恢复、watermark 或异常传播，只描述当前代码能够保证的行为，不暗示未实现的容错语义。

## 注释内容原则

1. 注释解释“为什么”和运行时语义，不逐字复述代码。
2. 触发条件区分“每个算子实例一次”“每条消息一次”“watermark 到达时”以及“Flink 查询类型时”。
3. 状态注释明确 keyed state 按 `udid + start_id` 隔离，并由 checkpoint/savepoint 持久化。
4. Kafka 注释明确输入 offset 的优先级为：恢复快照中的 source 状态、consumer group committed offset、缺失时最新位置。
5. 时间注释明确 5 秒未来时间校正、5 秒乱序容忍、1 分钟分区空闲检测和最新事件时间后 30 秒清理。
6. 不记录敏感配置值，不把注释写成部署操作手册。
7. 不添加与实现不一致的承诺；若注释检查暴露实现与文档冲突，本任务只报告，不顺带修改业务行为。

## 验证

- 使用源码检查确认所有 Flink 接口或抽象类重写方法均包含触发条件与作用说明。
- 运行 `mvn -f flink_processer/pom.xml clean verify`，证明注释修改没有破坏编译、测试和打包。
- 检查 fat JAR 仍包含既有入口及业务类。
- 运行 `git diff --check`，确认没有格式问题。
- 审查最终 diff，确保生产代码变化仅为注释和必要的排版，不包含可执行语句变化。
