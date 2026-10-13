# Flink 事件时间清理与单机 Checkpoint 恢复实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 使用校正后的 `event_timestamp`、watermark 和事件时间定时器清理 keyed state，并把 Kafka offset、状态和定时器持久化到 Ubuntu 本机 checkpoint 目录以支持重新提交恢复。

**架构：** Kafka Source 直接反序列化并校正为 `EventRecord`，Source 级 `WatermarkStrategy` 提供 5 秒乱序容忍和 1 分钟 idleness。`CommonStateEnrichmentFunction` 只向后续期事件时间定时器；作业使用 filesystem checkpoint storage 和取消后保留策略。

**技术栈：** Java 17、Apache Flink 2.3.0 DataStream API、Flink Kafka Connector 5.0.0-2.2、Jackson 2.18.3、JUnit 5、AssertJ、Maven。

---

## 文件结构

- 修改 `flink_processer/pom.xml`：将 Flink 核心依赖升级为 2.3.0。
- 创建 `flink_processer/src/main/java/com/example/flinkprocesser/EventTimePolicy.java`：集中定义时间常量和 watermark 策略。
- 修改 `flink_processer/src/main/java/com/example/flinkprocesser/EventRecordParser.java`：严格校验并校正事件时间。
- 创建 `flink_processer/src/main/java/com/example/flinkprocesser/EventRecordKafkaDeserializer.java`：让 Kafka Source 直接产生 `EventRecord`。
- 修改 `flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java`：接入 watermark 和持久化 checkpoint。
- 修改 `flink_processer/src/main/java/com/example/flinkprocesser/JobConfig.java`：提供并校验 checkpoint/savepoint URI。
- 修改 `flink_processer/src/main/java/com/example/flinkprocesser/CommonStateEnrichmentFunction.java`：改用事件时间定时器。
- 修改相应测试，并创建 checkpoint 恢复测试。
- 修改 `flink_processer/README.md`：记录部署和恢复操作。

### 任务 1：严格解析并校正事件时间

**文件：**
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventTimePolicy.java`
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecordParser.java`
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventRecordParserTest.java`

- [ ] **步骤 1：编写失败的解析测试**

增加确定性入口 `parseAt(String json, long currentTimeMillis)` 的测试：

```java
EventRecord corrected = parser.parseAt(eventJson(106_001L), 100_000L);
assertThat(corrected.getEventTimestamp()).isEqualTo(100_000L);
assertThat(MAPPER.readTree(corrected.getJson()).path("event_timestamp").longValue())
        .isEqualTo(100_000L);

EventRecord boundary = parser.parseAt(eventJson(105_000L), 100_000L);
assertThat(boundary.getEventTimestamp()).isEqualTo(105_000L);
assertThat(boundary.getJson()).isEqualTo(eventJson(105_000L));
```

把缺失、字符串、小数和超出 `long` 范围的 `event_timestamp` 加入非法输入测试，删除原 `Long.MAX_VALUE` 回退断言。

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn -f flink_processer/pom.xml -Dtest=EventRecordParserTest test`

预期：FAIL，`parseAt` 不存在且缺失时间戳仍被接受。

- [ ] **步骤 3：实现最少代码**

`EventTimePolicy` 定义 `5_000L` 未来容忍、`Duration.ofSeconds(5)` 乱序、`Duration.ofMinutes(1)` 空闲和 `30_000L` 清理间隔。解析器实现：

```java
EventRecord parseAt(String json, long now) throws Exception {
    ObjectNode root = requireObject(MAPPER.readTree(json));
    long timestamp = requiredLong(root, "event_timestamp");
    long futureLimit = Math.addExact(now, EventTimePolicy.FUTURE_TOLERANCE_MILLIS);
    if (timestamp > futureLimit) {
        timestamp = now;
        root.put("event_timestamp", now);
        json = MAPPER.writeValueAsString(root);
    }
    return createEvent(root, json, timestamp);
}
```

公开的 `map` 使用 `System.currentTimeMillis()` 调用 `parseAt`。

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn -f flink_processer/pom.xml -Dtest=EventRecordParserTest test`

预期：全部通过。

- [ ] **步骤 5：Commit**

```bash
git add flink_processer/src/main/java/com/example/flinkprocesser/EventTimePolicy.java flink_processer/src/main/java/com/example/flinkprocesser/EventRecordParser.java flink_processer/src/test/java/com/example/flinkprocesser/EventRecordParserTest.java
git commit -m "feat(Flink): 校正事件时间字段"
```

### 任务 2：在 Kafka Source 生成 watermark

**文件：**
- 修改：`flink_processer/pom.xml`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecordKafkaDeserializer.java`
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/EventRecordKafkaDeserializerTest.java`
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java`
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java`

- [ ] **步骤 1：编写失败的反序列化与拓扑测试**

```java
ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        "input", 0, 1L, null, validJson.getBytes(StandardCharsets.UTF_8));
List<EventRecord> output = new ArrayList<>();
deserializer.deserialize(record, output::add);
assertThat(output).singleElement().extracting(EventRecord::recordKey)
        .isEqualTo("device-1_1000");
```

拓扑测试断言 Source 使用事件时间策略，并保留 `kafka-source`、`parse-json`、`common-state-enrichment` 和两个 Sink UID。`parse-json` 变成 typed source 后的 identity map，仅用于保持既有算子 UID 和拓扑兼容性。

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn -f flink_processer/pom.xml -Dtest=EventRecordKafkaDeserializerTest,EventEnrichmentJobTest test`

预期：FAIL，新反序列化器不存在，Source 仍使用 `noWatermarks()`。

- [ ] **步骤 3：实现 typed Source 与 watermark**

将 `flink.version` 改为 `2.3.0`，保留 Maven Central 最新 connector `5.0.0-2.2`。实现 `KafkaRecordDeserializationSchema<EventRecord>`，拒绝 null value，并用 UTF-8 value 调用解析器。Source 改为：

```java
KafkaSource<EventRecord> source = KafkaSource.<EventRecord>builder()
        .setBootstrapServers(config.bootstrapServers())
        .setTopics(config.inputTopic())
        .setGroupId(config.groupId())
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setDeserializer(new EventRecordKafkaDeserializer())
        .build();

env.fromSource(source, EventTimePolicy.watermarkStrategy(), "Kafka input")
        .uid("kafka-source")
        .map(event -> event)
        .returns(EventRecord.class)
        .uid("parse-json")
        .keyBy(EventRecord::recordKey)
        .process(new CommonStateEnrichmentFunction())
        .uid("common-state-enrichment");
```

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn -f flink_processer/pom.xml -Dtest=EventRecordKafkaDeserializerTest,EventEnrichmentJobTest test`

预期：通过。若 connector 与 2.3.0 编译 API 不兼容，停止并报告具体错误，不猜测不存在的版本。

- [ ] **步骤 5：Commit**

```bash
git add flink_processer/pom.xml flink_processer/src/main/java/com/example/flinkprocesser/EventRecordKafkaDeserializer.java flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java flink_processer/src/test/java/com/example/flinkprocesser/EventRecordKafkaDeserializerTest.java flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java
git commit -m "feat(Flink): 按事件时间生成水位线"
```

### 任务 3：把状态清理改为事件时间定时器

**文件：**
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/CommonStateEnrichmentFunction.java`
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/CommonStateEnrichmentFunctionTest.java`

- [ ] **步骤 1：编写失败的事件时间测试**

```java
harness.processElement(common("device-1", 1L, 1_000L), 1_000L);
assertThat(harness.numEventTimeTimers()).isEqualTo(1);
assertThat(harness.numProcessingTimeTimers()).isZero();
harness.processElement(nonCommon("device-1", 1L, "app", 20_000L), 20_000L);
harness.processElement(nonCommon("device-1", 1L, "devinfo", 10_000L), 10_000L);
harness.processWatermark(new Watermark(49_999L));
assertThat(harness.numKeyedStateEntries()).isEqualTo(2);
harness.processWatermark(new Watermark(50_000L));
assertThat(harness.numKeyedStateEntries()).isZero();
```

陈旧 callback 改用 `onEventTime`，并验证截止时间加法溢出时不注册 timer、不输出。

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn -f flink_processer/pom.xml -Dtest=CommonStateEnrichmentFunctionTest test`

预期：FAIL，当前实现注册处理时间 timer。

- [ ] **步骤 3：实现最少事件时间逻辑**

```java
long candidate = Math.addExact(event.getEventTimestamp(),
        EventTimePolicy.STATE_CLEANUP_DELAY_MILLIS);
Long previous = cleanupTimerState.value();
if (previous == null || candidate > previous) {
    if (previous != null) {
        timerService.deleteEventTimeTimer(previous);
    }
    timerService.registerEventTimeTimer(candidate);
    cleanupTimerState.update(candidate);
}
```

同步把类、字段、方法和 `onTimer` 的详细中文注释改成事件时间语义。

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn -f flink_processer/pom.xml -Dtest=CommonStateEnrichmentFunctionTest test`

预期：全部通过，processing timer 数量始终为零。

- [ ] **步骤 5：Commit**

```bash
git add flink_processer/src/main/java/com/example/flinkprocesser/CommonStateEnrichmentFunction.java flink_processer/src/test/java/com/example/flinkprocesser/CommonStateEnrichmentFunctionTest.java
git commit -m "feat(Flink): 按事件时间清理状态"
```

### 任务 4：配置单机持久化 checkpoint 与 savepoint

**文件：**
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/JobConfig.java`
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java`
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/JobConfigTest.java`
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java`

- [ ] **步骤 1：编写失败的配置测试**

```java
assertThat(config.checkpointStorageUri())
        .isEqualTo("file:///home/zhangdi/workspace/flink_data/checkpoints");
assertThat(config.savepointStorageUri())
        .isEqualTo("file:///home/zhangdi/workspace/flink_data/savepoints");
assertThat(env.getCheckpointConfig().getExternalizedCheckpointRetention())
        .isEqualTo(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
```

参数测试拒绝空值、相对路径、`file://relative` 和非 `file` scheme；拓扑测试还读取 `CheckpointingOptions.CHECKPOINTS_DIRECTORY` 与 `SAVEPOINT_DIRECTORY`。

- [ ] **步骤 2：运行测试验证失败**

运行：`mvn -f flink_processer/pom.xml -Dtest=JobConfigTest,EventEnrichmentJobTest test`

预期：FAIL，配置字段和 retained checkpoint 尚不存在。

- [ ] **步骤 3：实现 URI 与 checkpoint 配置**

`JobConfig` 增加两个 URI 字符串，使用 `URI.create` 验证 scheme 为 `file` 且 path 为绝对路径。`buildPipeline` 配置：

```java
Configuration flinkConfig = new Configuration();
flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointStorageUri());
flinkConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, config.savepointStorageUri());
env.configure(flinkConfig);
env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.AT_LEAST_ONCE);
env.getCheckpointConfig().setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
```

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn -f flink_processer/pom.xml -Dtest=JobConfigTest,EventEnrichmentJobTest test`

预期：全部通过。

- [ ] **步骤 5：Commit**

```bash
git add flink_processer/src/main/java/com/example/flinkprocesser/JobConfig.java flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java flink_processer/src/test/java/com/example/flinkprocesser/JobConfigTest.java flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java
git commit -m "feat(Flink): 持久化单机检查点"
```

### 任务 5：验证 checkpoint 恢复状态和 timer

**文件：**
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/CommonStateCheckpointRestoreTest.java`

- [ ] **步骤 1：编写 snapshot/restore 测试**

第一个 harness 输入完整 `common` 后执行 `snapshot(1L, 1L)` 并关闭；第二个全新 harness 执行 `setup()`、`initializeState(snapshot)`、`open()`，再输入非 `common`：

```java
restored.processElement(nonCommon("device-1", 1L, "app", 20_000L), 20_000L);
assertThat(restored.extractOutputValues()).hasSize(1);
assertThat(restored.numEventTimeTimers()).isEqualTo(1);
restored.processWatermark(new Watermark(50_000L));
assertThat(restored.numKeyedStateEntries()).isZero();
```

- [ ] **步骤 2：运行恢复测试**

运行：`mvn -f flink_processer/pom.xml -Dtest=CommonStateCheckpointRestoreTest test`

预期：PASS，并且测试从 `OperatorSubtaskState` 初始化新 harness，不复用旧 harness 的内存状态。

- [ ] **步骤 3：运行全部测试**

运行：`mvn -f flink_processer/pom.xml clean verify`

预期：全部通过。

- [ ] **步骤 4：Commit**

```bash
git add flink_processer/src/test/java/com/example/flinkprocesser/CommonStateCheckpointRestoreTest.java
git commit -m "test(Flink): 验证检查点状态恢复"
```

### 任务 6：更新部署文档并完成本地验证

**文件：**
- 修改：`flink_processer/README.md`

- [ ] **步骤 1：更新 README**

明确记录未来 5 秒校正、5 秒乱序、1 分钟 idleness、最大事件时间加 30 秒、两个 `/home/zhangdi/workspace/flink_data` 默认目录、`-s .../_metadata` 恢复命令、全流空闲限制和至少一次重复风险。

- [ ] **步骤 2：执行完整验证**

```bash
mvn -f flink_processer/pom.xml clean verify
git diff --check
jar tf flink_processer/target/flink-processer-1.0.0.jar | rg 'EventRecordKafkaDeserializer|EventTimePolicy|CommonStateEnrichmentFunction|org/apache/kafka'
```

预期：构建成功，`git diff --check` 无输出，JAR 包含新类与 Kafka 类。

- [ ] **步骤 3：Commit**

```bash
git add flink_processer/README.md
git commit -m "docs(Flink): 说明事件时间与检查点恢复"
```

### 任务 7：Ubuntu Flink 2.3.0 冒烟验证

**文件：**
- 不修改服务器全局配置，也不删除用户创建的根目录。

- [ ] **步骤 1：经用户授权后上传并提交 JAR**

上传到 `/home/zhangdi/workspace/flink_data/flink-processer-1.0.0.jar`，使用 `/home/zhangdi/works/flink-2.3.0/bin/flink run --detached` 提交。非交互 SSH 若缺少 `JAVA_HOME`，先只读查询正在运行的 Flink 进程或启动脚本使用的 Java 路径，不猜测。

- [ ] **步骤 2：验证 checkpoint 与恢复**

等待成功 checkpoint，记录 `_metadata`；取消作业并确认 checkpoint 保留；使用 `-s` 重新提交；发送同 key 的非 `common` 数据，确认恢复前的 common 状态仍可补全且从快照 offset 继续消费。

- [ ] **步骤 3：停止冒烟作业并记录兼容结果**

停止测试作业。保留恢复证据 checkpoint，除非用户明确要求删除。最终说明 Flink 2.3.0 与 Kafka connector 5.0.0-2.2 的实际运行结果。
