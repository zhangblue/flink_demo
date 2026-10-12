# Flink Kafka App 信息补全实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 新增 Maven 工程 `flink_processer`，将 `flink_demo` 中同一 2 分钟处理时间滚动窗口、同一 `udid + start_id` 的事件进行 App 信息补全，并按成功或失败分别写入 `flink_output`、`flink_error`。

**架构：** Kafka value 先解析为保留原始 JSON 的轻量 POJO，以 `<udid>:<start_id>` 分组后进入 2 分钟 Processing Time Tumbling Window。纯 Java 补全器在窗口关闭时选择 App 元数据、排序和路由，`ProcessWindowFunction` 将成功记录发往主输出、失败记录发往侧输出，两个 Kafka Sink 使用相同组合 key 序列化策略。

**技术栈：** Java 17、Maven 3.9、Apache Flink 2.2.0 DataStream API、Flink Kafka Connector 5.0.0-2.2、Jackson、JUnit 5、AssertJ、Maven Shade Plugin。

---

## 文件结构

- 创建：`flink_processer/pom.xml` — 依赖、Java 17、测试和 fat JAR 配置。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/JobConfig.java` — CLI 默认值与校验。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecord.java` — 输入事件 POJO 与组合 key。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecordParser.java` — JSON 解析和必填字段校验。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/OutputEvent.java` — Kafka 输出 key/value。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EnrichmentBatch.java` — 成功、失败两组结果。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/WindowEnricher.java` — App 选择、补全、排序和路由。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/AppEnrichmentWindowFunction.java` — 主输出与错误侧输出适配。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/OutputEventKafkaSerializer.java` — topic、组合 key、JSON value 序列化。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java` — Source、滚动窗口、checkpoint、双 Sink。
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/*Test.java` — 配置、解析、补全、序列化和拓扑测试。
- 创建：`flink_processer/README.md` — 构建、运行、参数和数据契约。
- 修改：`README.md` — 增加 Flink 工程入口。

### 任务 1：建立 Maven 工程、配置和输入边界

**文件：**
- 创建：`flink_processer/pom.xml`
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/JobConfigTest.java`
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/EventRecordParserTest.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/JobConfig.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecord.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecordParser.java`

- [ ] **步骤 1：创建仅含依赖与测试插件的 pom**

设置 `flink.version=2.2.0`、`flink.kafka.version=5.0.0-2.2` 和 Java release 17。`flink-streaming-java`、`flink-clients` 使用 `provided` scope；Kafka connector 与 Jackson 打入作业 JAR；测试使用 JUnit Jupiter、AssertJ 和 Surefire。暂不创建生产类。

- [ ] **步骤 2：编写配置与解析失败测试**

`JobConfigTest` 要求默认值为：

```java
assertThat(config.bootstrapServers()).isEqualTo("192.168.250.106:9092");
assertThat(config.inputTopic()).isEqualTo("flink_demo");
assertThat(config.outputTopic()).isEqualTo("flink_output");
assertThat(config.errorTopic()).isEqualTo("flink_error");
assertThat(config.groupId()).isEqualTo("flink-processer");
assertThat(config.parallelism()).isEqualTo(1);
assertThat(config.checkpointIntervalMs()).isEqualTo(60_000L);
```

分别断言空 brokers/topic/group、重复 topic、非正并行度和非正 checkpoint 间隔失败。

`EventRecordParserTest` 用手工 JSON 断言：

```java
EventRecord event = parser.map(json);
assertThat(event.getUdid()).isEqualTo("device-1");
assertThat(event.getStartId()).isEqualTo(1_791_614_142_000L);
assertThat(event.recordKey()).isEqualTo("device-1:1791614142000");
assertThat(event.getProtolType()).isEqualTo("start");
```

无效 JSON、缺少 `udid`、缺少整数 `start_id`、缺少 `protol_type` 必须失败。

- [ ] **步骤 3：运行测试并确认红灯**

运行：`mvn -f flink_processer/pom.xml test`

预期：测试编译失败，指出 `JobConfig`、`EventRecordParser`、`EventRecord` 不存在，而不是依赖或测试发现错误。

- [ ] **步骤 4：实现最小配置对象**

使用 `ParameterTool.fromArgs(args)` 读取 `--bootstrap-servers`、`--input-topic`、`--output-topic`、`--error-topic`、`--group-id`、`--parallelism`、`--checkpoint-interval-ms`。使用 Java record 保存结果，字符串 trim 后非空，三个 topic 互不相同，数值必须大于零。

- [ ] **步骤 5：实现最小事件模型与解析器**

`EventRecord` 是带无参构造器及 getter/setter 的 Flink POJO，字段为 `json`、`udid`、`startId`、`protolType`、`sequence`、`eventTimestamp`、`eventId`；`recordKey()` 返回 `udid + ":" + startId`。解析器用 Jackson `readTree`，要求业务 key 和类型；排序辅助字段缺失时使用 `Long.MAX_VALUE`/空字符串。

- [ ] **步骤 6：运行目标测试并确认绿灯**

运行：`mvn -f flink_processer/pom.xml test -Dtest=JobConfigTest,EventRecordParserTest`

预期：全部通过。

- [ ] **步骤 7：提交输入边界**

```bash
git add flink_processer/pom.xml flink_processer/src
git commit -m "feat(Flink): 建立事件处理输入边界"
```

### 任务 2：实现窗口补全和成功/错误路由

**文件：**
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/WindowEnricherTest.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/OutputEvent.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EnrichmentBatch.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/WindowEnricher.java`

- [ ] **步骤 1：编写成功补全的失败测试**

fixture 顺序设为 `start, devinfo, app, environment`。app 的 `self_package_name=com.tencent.mm`，安装列表同时含 other 和 `{package_name:com.tencent.mm,app_name:WeChat,version_name:8.0.62}`。断言三个非 app 成功输出顶层字段为 `WeChat`/`8.0.62`，app 原 JSON 进入成功流且不新增字段，错误流为空，四条输出按 sequence 升序且 key 都是 `device-1:1791614142000`。

- [ ] **步骤 2：编写失败路由与确定性选择测试**

表驱动覆盖：无 app、包名不匹配、匹配项缺 `app_name`、缺 `version_name`、任一字段为空。非 app 只进入错误流，无法取得的字段为 JSON null；app 仍只进入成功流。两条 app 时选择 sequence 最大者，sequence 相同选 `event_timestamp` 最大者；反转输入后结果不变。混入另一 record key 时抛出异常。

- [ ] **步骤 3：运行测试并确认红灯**

运行：`mvn -f flink_processer/pom.xml test -Dtest=WindowEnricherTest`

预期：测试编译失败，指出补全类型不存在。

- [ ] **步骤 4：实现最小补全逻辑**

`WindowEnricher.enrich(key, events)` 先校验每条记录 key，按 sequence、event timestamp、event id 排序，再用以下规则选 app：

```java
Optional<EventRecord> selectedApp = ordered.stream()
    .filter(EventRecord::isApp)
    .max(Comparator.comparingLong(EventRecord::getSequence)
        .thenComparingLong(EventRecord::getEventTimestamp));
```

只从选中 app 的 `payload.apps` 中按 `self_package_name == package_name` 提取非空 `app_name`、`version_name`。成功时对非 app 的 `ObjectNode.deepCopy()` 设置两个顶层字段；失败时设置可取得值、缺失值显式 `putNull` 并加入错误结果。app 永远使用原 JSON 加入成功结果。

- [ ] **步骤 5：运行目标测试和回归测试**

运行：`mvn -f flink_processer/pom.xml test -Dtest=WindowEnricherTest,EventRecordParserTest,JobConfigTest`

预期：全部通过，任何事件不会同时出现在成功和错误结果。

- [ ] **步骤 6：提交业务补全器**

```bash
git add flink_processer/src
git commit -m "feat(Flink): 实现窗口内 App 信息补全"
```

### 任务 3：接入 Flink 滚动窗口与双 Kafka Sink

**文件：**
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/OutputEventKafkaSerializerTest.java`
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/AppEnrichmentWindowFunction.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/OutputEventKafkaSerializer.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java`
- 修改：`flink_processer/pom.xml`

- [ ] **步骤 1：编写 Kafka record 契约的失败测试**

分别用两个 topic 构造 serializer，断言：

```java
assertThat(record.topic()).isEqualTo(topic);
assertThat(new String(record.key(), UTF_8)).isEqualTo("device-1:1791614142000");
assertThat(new String(record.value(), UTF_8)).isEqualTo(outputEvent.json());
```

空 topic 或空 key 必须失败。

- [ ] **步骤 2：编写滚动窗口与拓扑契约的失败测试**

调用 package-private `createWindowAssigner()`，断言是 `TumblingProcessingTimeWindows`，处理时间 `119_999` 属于 `[0,120_000)`、`120_000` 属于 `[120_000,240_000)`。调用 `buildPipeline(env, config)` 后检查稳定 UID：`kafka-source`、`parse-json`、`app-enrichment-window`、`kafka-output-sink`、`kafka-error-sink`，并断言并行度和 checkpoint interval 来自配置。

- [ ] **步骤 3：运行测试并确认红灯**

运行：`mvn -f flink_processer/pom.xml test -Dtest=OutputEventKafkaSerializerTest,EventEnrichmentJobTest`

预期：测试编译失败，指出 serializer、window function 或 job 不存在。

- [ ] **步骤 4：实现窗口适配器和 serializer**

定义 `new OutputTag<OutputEvent>("app-enrichment-errors") {}`。`process` 将纯补全器成功结果 collect 到主输出，将错误结果 `context.output` 到侧输出，不创建额外 keyed/global state。serializer 实现 `KafkaRecordSerializationSchema<OutputEvent>`：

```java
return new ProducerRecord<>(topic,
    event.key().getBytes(StandardCharsets.UTF_8),
    event.json().getBytes(StandardCharsets.UTF_8));
```

- [ ] **步骤 5：实现作业拓扑**

Source 使用 `KafkaSource<String>`、`SimpleStringSchema` 和 `OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)`。核心拓扑为：

```java
SingleOutputStreamOperator<OutputEvent> output = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka input").uid("kafka-source")
    .map(new EventRecordParser()).uid("parse-json")
    .keyBy(EventRecord::recordKey)
    .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(2)))
    .process(new AppEnrichmentWindowFunction()).uid("app-enrichment-window");
```

从固定 OutputTag 获取错误流。两个 `KafkaSink` 设置对应 topic serializer 和 `DeliveryGuarantee.AT_LEAST_ONCE`。环境设置并行度和 `enableCheckpointing(interval, CheckpointingMode.AT_LEAST_ONCE)`。main 解析配置、构建拓扑并执行 `mobile-event-app-enrichment`。

- [ ] **步骤 6：配置 Shade Plugin 并验证绿灯**

Shade 合并 `META-INF/services`、设置 main class、排除签名文件且不生成 dependency-reduced POM。运行：`mvn -f flink_processer/pom.xml test`

预期：全部测试通过。

- [ ] **步骤 7：提交 Flink 作业拓扑**

```bash
git add flink_processer
git commit -m "feat(Flink): 接入滚动窗口和双 Kafka 输出"
```

### 任务 4：完善文档并执行完整验证

**文件：**
- 创建：`flink_processer/README.md`
- 修改：`README.md`

- [ ] **步骤 1：编写使用文档**

包含 Java 17/Maven/Flink 2.2 环境要求、`mvn clean package`、JAR 路径、`flink run` 示例、全部参数和默认值、输入字段、路由规则、`<udid>:<start_id>` record key、最多 2 分钟输出延迟、跨滚动窗口无法补全的限制。

- [ ] **步骤 2：更新仓库概览**

根 README 项目结构加入 `flink_processer/`，快速开始加入 Flink 构建与文档链接，不改变 sender 运行方式。

- [ ] **步骤 3：运行 Maven 测试和打包**

运行：`mvn -f flink_processer/pom.xml clean verify`

预期：所有测试通过并生成 `flink_processer/target/flink-processer-1.0.0.jar`。

- [ ] **步骤 4：检查 fat JAR 内容**

运行：

```bash
jar tf flink_processer/target/flink-processer-1.0.0.jar | rg 'EventEnrichmentJob|OutputEventKafkaSerializer|org/apache/kafka'
```

预期：包含作业类、serializer、Kafka connector/client，不包含 Flink runtime 核心类。

- [ ] **步骤 5：执行仓库完整回归验证**

```bash
cargo fmt --manifest-path sender/Cargo.toml --check
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
mvn -f flink_processer/pom.xml clean verify
git diff --check
git status --short
```

预期：Rust 格式、Clippy 和 83 个普通测试通过，真实 Kafka 测试保持 ignored；Maven 测试和打包通过；无空白错误。

- [ ] **步骤 6：提交文档**

```bash
git add README.md flink_processer/README.md
git commit -m "docs(Flink): 添加事件补全作业使用说明"
```

- [ ] **步骤 7：最终需求核对**

确认目录名、三个默认 topic、2 分钟处理时间滚动窗口、组合分组、App 匹配规则、成功/错误互斥路由、两个输出 topic 的 `<udid>:<start_id>` record key、跨窗口不补全和窗口关闭自动清理全部符合规格。
