# Flink Common 状态补全实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 将 Flink 作业从 2 分钟 App 窗口补全改为使用 `common` 事件维护 keyed `ValueState`，即时补全六个顶层公共字段，并在 30 秒无新消息后清理状态。

**架构：** Kafka JSON 仍先解析为保留原文的 `EventRecord`，随后以 `<udid>_<start_id>` 分组进入 `KeyedProcessFunction`。函数把完整 `common` 写入 `ValueState<CommonMetadata>`，用另一个 `ValueState<Long>` 管理可续期的处理时间定时器；非 `common` 事件即时补全后走主输出，无状态事件和不完整 `common` 走错误侧输出。

**技术栈：** Java 17、Maven 3.9、Apache Flink 2.2.0 DataStream API、Flink Kafka Connector 5.0.0-2.2、Jackson、Flink operator test harness、JUnit 5、AssertJ。

---

## 文件结构

- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/CommonMetadata.java` — 可由 Flink 序列化的六字段状态对象及完整性判断。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/CommonMetadataJson.java` — 从 `common.payload` 提取字段，并把六字段写入输出 JSON。
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/CommonStateEnrichmentFunction.java` — keyed state、30 秒处理时间定时器、主输出和错误侧输出路由。
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/CommonMetadataJsonTest.java` — 字段提取、覆盖、显式 null 和旧 App 逻辑移除测试。
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/CommonStateEnrichmentFunctionTest.java` — 使用 Flink test harness 验证状态隔离、路由、续期和清理。
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecord.java` — 组合 key 改为下划线格式。
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java` — 删除滚动窗口并接入 keyed process function。
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventRecordParserTest.java` — 更新组合 key 契约。
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/OutputEventKafkaSerializerTest.java` — 验证两个 topic 都使用下划线组合 key。
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java` — 验证无窗口拓扑、稳定 UID、checkpoint 和双 Sink。
- 修改：`flink_processer/pom.xml` — 增加 Flink test harness 所需测试依赖。
- 删除：`flink_processer/src/main/java/com/example/flinkprocesser/AppEnrichmentWindowFunction.java` — 被 keyed process function 替代。
- 删除：`flink_processer/src/main/java/com/example/flinkprocesser/WindowEnricher.java` — 删除 App 安装列表匹配逻辑。
- 删除：`flink_processer/src/main/java/com/example/flinkprocesser/EnrichmentBatch.java` — 不再批量输出窗口结果。
- 删除：`flink_processer/src/test/java/com/example/flinkprocesser/WindowEnricherTest.java` — 被 common 元数据及状态函数测试替代。
- 修改：`flink_processer/README.md` — 记录 common 契约、即时路由、30 秒状态清理和部署兼容性。
- 修改：`README.md` — 更新仓库概览和 Flink 作业说明。

### 任务 1：切换组合 key 契约

**文件：**
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventRecordParserTest.java`
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/OutputEventKafkaSerializerTest.java`
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventRecord.java`

- [ ] **步骤 1：先把解析与 Kafka 序列化测试改为下划线 key**

把断言改成精确字符串，并让两个输出 topic 共用同一组参数化测试：

```java
assertThat(event.recordKey()).isEqualTo("device-1_1791614142000");

OutputEvent event = new OutputEvent(
        "device-1_1791614142000",
        "{\"self_app_name\":\"微信\"}");
assertThat(new String(record.key(), UTF_8))
        .isEqualTo("device-1_1791614142000");
```

- [ ] **步骤 2：运行目标测试并确认红灯**

运行：

```bash
mvn -f flink_processer/pom.xml test \
  -Dtest=EventRecordParserTest,OutputEventKafkaSerializerTest
```

预期：`EventRecordParserTest` 因实际 key 仍使用冒号而失败；serializer 的 topic、value 和非法参数测试保持通过。

- [ ] **步骤 3：用最小修改实现新 key**

```java
public String recordKey() {
    return udid + "_" + startId;
}
```

同步把方法注释改为“状态分组和两个输出 Kafka record 共用的启动周期组合 key”。

- [ ] **步骤 4：重新运行目标测试并确认绿灯**

运行：

```bash
mvn -f flink_processer/pom.xml test \
  -Dtest=EventRecordParserTest,OutputEventKafkaSerializerTest
```

预期：两个测试类全部通过。

- [ ] **步骤 5：提交 key 契约**

```bash
git add flink_processer/src/main/java/com/example/flinkprocesser/EventRecord.java \
  flink_processer/src/test/java/com/example/flinkprocesser/EventRecordParserTest.java \
  flink_processer/src/test/java/com/example/flinkprocesser/OutputEventKafkaSerializerTest.java
git commit -m "feat(Flink): 更新启动周期组合 key"
```

### 任务 2：实现 Common 元数据 JSON 边界

**文件：**
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/CommonMetadataJsonTest.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/CommonMetadata.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/CommonMetadataJson.java`

- [ ] **步骤 1：编写完整 common 提取和输出覆盖的失败测试**

构造以下 common payload：

```java
String commonJson = """
        {"udid":"device-1","start_id":1791614142000,"protol_type":"common",
         "payload":{"self_app_package":"com.example.demo","self_app_name":"Demo",
                    "model":"Pixel 10","brand":"Google","os":"Android",
                    "os_version":"17"}}
        """;
CommonMetadata metadata = CommonMetadataJson.extract(commonJson);
assertThat(metadata.isComplete()).isTrue();
assertThat(metadata.getSelfAppPackage()).isEqualTo("com.example.demo");
assertThat(metadata.getSelfAppName()).isEqualTo("Demo");
assertThat(metadata.getModel()).isEqualTo("Pixel 10");
assertThat(metadata.getBrand()).isEqualTo("Google");
assertThat(metadata.getOs()).isEqualTo("Android");
assertThat(metadata.getOsVersion()).isEqualTo("17");
```

再用带有旧顶层六字段和 `unknown:true` 的非 common JSON 调用
`CommonMetadataJson.enrich(json, metadata)`，断言六字段均被状态值覆盖，`unknown` 和原 `payload`
保持不变。

- [ ] **步骤 2：编写不完整字段和 App 逻辑移除的失败测试**

使用参数化输入分别把六字段设为缺失、`null`、数字、空字符串和只含空白的字符串，断言对应
getter 返回 `null` 且 `isComplete()` 为 `false`。将该部分元数据写入错误 JSON 时，断言全部六个
顶层字段存在，有效字段保留、无效字段为 JSON `null`。

另构造旧格式 App payload：

```java
String appJson = """
        {"udid":"device-1","start_id":1791614142000,"protol_type":"app",
         "app_name":"input-name","app_version":"input-version",
         "payload":{"self_package_name":"com.old",
                    "apps":[{"package_name":"com.old","app_name":"Old","version_name":"1.0"}]}}
        """;
```

调用 `enrich` 后断言 `app_name`/`app_version` 仍为输入值，只新增或覆盖 common 的六字段，证明
代码没有读取 `self_package_name` 或 `apps[]`。

- [ ] **步骤 3：运行测试并确认红灯**

运行：`mvn -f flink_processer/pom.xml test -Dtest=CommonMetadataJsonTest`

预期：测试编译失败，指出 `CommonMetadata` 和 `CommonMetadataJson` 尚不存在。

- [ ] **步骤 4：实现可序列化状态对象**

`CommonMetadata` 实现 `Serializable`，提供公开无参构造器、六字段构造器、getter/setter 和完整性判断：

```java
public boolean isComplete() {
    return selfAppPackage != null
            && selfAppName != null
            && model != null
            && brand != null
            && os != null
            && osVersion != null;
}
```

所有空值归一化在 JSON 提取边界完成，状态对象只判断 `null`，避免业务函数重复校验。

- [ ] **步骤 5：实现 JSON 提取与顶层补全**

`CommonMetadataJson.extract(String)` 读取根对象的 `payload`，只接受非空白字符串：

```java
private static String nonblankText(JsonNode node) {
    return node != null && node.isTextual() && !node.textValue().isBlank()
            ? node.textValue() : null;
}
```

`enrich(String, CommonMetadata)` 对根 `ObjectNode` 做 `deepCopy()`，依次写入
`self_app_package`、`self_app_name`、`model`、`brand`、`os`、`os_version`；值为 `null` 时使用
`putNull`，其他未知顶层字段不改动。不要在该类中出现 `self_package_name`、`apps`、`app_name`
或 `app_version` 的匹配分支。

- [ ] **步骤 6：运行目标测试并确认绿灯**

运行：`mvn -f flink_processer/pom.xml test -Dtest=CommonMetadataJsonTest`

预期：完整、不完整、覆盖、未知字段保留和 App 旧字段保留测试全部通过。

- [ ] **步骤 7：提交 Common JSON 边界**

```bash
git add flink_processer/src/main/java/com/example/flinkprocesser/CommonMetadata.java \
  flink_processer/src/main/java/com/example/flinkprocesser/CommonMetadataJson.java \
  flink_processer/src/test/java/com/example/flinkprocesser/CommonMetadataJsonTest.java
git commit -m "feat(Flink): 建立 common 元数据边界"
```

### 任务 3：实现 keyed 状态、互斥路由和 30 秒续期定时器

**文件：**
- 修改：`flink_processer/pom.xml`
- 创建：`flink_processer/src/test/java/com/example/flinkprocesser/CommonStateEnrichmentFunctionTest.java`
- 创建：`flink_processer/src/main/java/com/example/flinkprocesser/CommonStateEnrichmentFunction.java`

- [ ] **步骤 1：加入 Flink operator test harness 测试依赖**

在测试 scope 加入与运行时一致的版本：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-test-utils</artifactId>
    <version>${flink.version}</version>
    <scope>test</scope>
</dependency>
```

该依赖只用于测试，不得把 test harness 打入生产 JAR。

- [ ] **步骤 2：建立 keyed process function 测试夹具**

```java
private KeyedOneInputStreamOperatorTestHarness<String, EventRecord, OutputEvent> harness;

@BeforeEach
void openHarness() throws Exception {
    harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
            new CommonStateEnrichmentFunction(),
            EventRecord::recordKey,
            Types.STRING);
    harness.open();
}

@AfterEach
void closeHarness() throws Exception {
    harness.close();
}
```

测试辅助方法从 `harness.extractOutputValues()` 读取主输出，从
`harness.getSideOutput(CommonStateEnrichmentFunction.ERROR_OUTPUT_TAG)` 的 `StreamRecord` 提取错误值。

- [ ] **步骤 3：编写状态写入、即时补全和互斥路由失败测试**

覆盖以下序列：

1. 完整 common：主输出和错误侧输出都为空。
2. 同 key 的 `start`、`devinfo`、`environment`、`app`：每条立即出现在主输出，六字段完整。
3. 无 common 的非 common：只在错误侧输出，六字段全部为 JSON `null`。
4. 不完整 common：只在错误侧输出，有效字段保留、无效字段为 `null`，且不建立状态。
5. 已有完整状态后收到不完整 common：错误 common 不覆盖旧状态，下一条非 common 仍使用旧值。
6. 同 key 第二条完整 common：下一条事件使用后到的六字段。
7. 只给 device-1 写 common：device-2 同 `start_id` 的事件进入错误流，证明 keyed state 隔离。

每个输入事件的 `OutputEvent.key()` 都断言为 `<udid>_<start_id>`；每条非 common 只出现在一条
输出分支中。

- [ ] **步骤 4：编写处理时间定时器失败测试**

```java
harness.setProcessingTime(1_000L);
harness.processElement(common("device-1", 1L), 1_000L);
assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

harness.setProcessingTime(20_000L);
harness.processElement(nonCommon("device-1", 1L, "start"), 20_000L);
assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

harness.setProcessingTime(31_000L);
assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

harness.setProcessingTime(50_000L);
assertThat(harness.numProcessingTimeTimers()).isZero();
harness.processElement(nonCommon("device-1", 1L, "environment"), 50_000L);
assertLastErrorOutputHasNullMetadata();
```

这里第二条消息把清理时刻从 `31_000` 顺延到 `50_000`；到达旧时刻时状态仍存在，到达当前
时刻后状态清除。`start` 已在清理前进入主输出；`environment` 在清理后只进入错误输出。再用
不完整 common 作为第二条消息重复该测试，证明“任意消息”都会续期。另把处理时间设置为
`Long.MAX_VALUE - 10_000L` 后处理消息，断言 `Math.addExact` 触发 `ArithmeticException`，避免注册
溢出后的过去时间戳。

- [ ] **步骤 5：运行测试并确认红灯**

运行：`mvn -f flink_processer/pom.xml test -Dtest=CommonStateEnrichmentFunctionTest`

预期：测试编译失败，指出 `CommonStateEnrichmentFunction` 尚不存在；test harness 依赖本身可解析。

- [ ] **步骤 6：实现两个 ValueState 和状态初始化**

```java
private transient ValueState<CommonMetadata> metadataState;
private transient ValueState<Long> cleanupTimerState;

@Override
public void open(OpenContext openContext) throws Exception {
    metadataState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("common-metadata", CommonMetadata.class));
    cleanupTimerState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("cleanup-timer", Long.class));
}
```

错误侧输出固定定义为：

```java
public static final OutputTag<OutputEvent> ERROR_OUTPUT_TAG =
        new OutputTag<OutputEvent>("common-enrichment-errors") { };
```

- [ ] **步骤 7：实现逐条处理与互斥路由**

`processElement` 首先续期定时器，再分支处理：

```java
refreshCleanupTimer(ctx.timerService());
if ("common".equals(event.getProtolType())) {
    CommonMetadata candidate = CommonMetadataJson.extract(event.getJson());
    if (candidate.isComplete()) {
        metadataState.update(candidate);
    } else {
        ctx.output(ERROR_OUTPUT_TAG, new OutputEvent(
                event.recordKey(), CommonMetadataJson.enrich(event.getJson(), candidate)));
    }
    return;
}

CommonMetadata metadata = metadataState.value();
String json = CommonMetadataJson.enrich(
        event.getJson(), metadata == null ? new CommonMetadata() : metadata);
OutputEvent result = new OutputEvent(event.recordKey(), json);
if (metadata == null) {
    ctx.output(ERROR_OUTPUT_TAG, result);
} else {
    out.collect(result);
}
```

合法 common 不输出；不合法 common 和无状态非 common 只走错误侧输出；完整状态下所有
非 common 类型（包括 app）只走主输出。

- [ ] **步骤 8：实现 30 秒定时器续期和防陈旧回调清理**

```java
private void refreshCleanupTimer(TimerService timerService) throws Exception {
    Long previous = cleanupTimerState.value();
    if (previous != null) {
        timerService.deleteProcessingTimeTimer(previous);
    }
    long next = Math.addExact(timerService.currentProcessingTime(), 30_000L);
    timerService.registerProcessingTimeTimer(next);
    cleanupTimerState.update(next);
}

@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutputEvent> out)
        throws Exception {
    Long current = cleanupTimerState.value();
    if (current != null && current == timestamp) {
        metadataState.clear();
        cleanupTimerState.clear();
    }
}
```

比较时依靠拆箱后的 long 值，不能用 `Long` 对象引用相等。`onTimer` 不产生业务输出。

- [ ] **步骤 9：运行目标测试和基础回归测试**

运行：

```bash
mvn -f flink_processer/pom.xml test \
  -Dtest=CommonMetadataJsonTest,CommonStateEnrichmentFunctionTest,EventRecordParserTest,OutputEventKafkaSerializerTest
```

预期：全部通过；test harness 最终只保留每个活跃 key 的一个处理时间定时器。

- [ ] **步骤 10：提交状态补全函数**

```bash
git add flink_processer/pom.xml \
  flink_processer/src/main/java/com/example/flinkprocesser/CommonStateEnrichmentFunction.java \
  flink_processer/src/test/java/com/example/flinkprocesser/CommonStateEnrichmentFunctionTest.java
git commit -m "feat(Flink): 使用 common 状态即时补全事件"
```

### 任务 4：替换窗口拓扑并删除 App 补全实现

**文件：**
- 修改：`flink_processer/src/test/java/com/example/flinkprocesser/EventEnrichmentJobTest.java`
- 修改：`flink_processer/src/main/java/com/example/flinkprocesser/EventEnrichmentJob.java`
- 删除：`flink_processer/src/test/java/com/example/flinkprocesser/WindowEnricherTest.java`
- 删除：`flink_processer/src/main/java/com/example/flinkprocesser/AppEnrichmentWindowFunction.java`
- 删除：`flink_processer/src/main/java/com/example/flinkprocesser/WindowEnricher.java`
- 删除：`flink_processer/src/main/java/com/example/flinkprocesser/EnrichmentBatch.java`

- [ ] **步骤 1：把拓扑测试改为 keyed process 契约**

删除窗口分配测试和手工 `ProcessWindowFunction.Context` 测试。保留环境配置断言，并检查：

```java
assertThat(graph.getStreamNodes())
        .extracting(StreamNode::getTransformationUID)
        .contains("kafka-source", "parse-json", "common-state-enrichment",
                  "kafka-output-sink", "kafka-error-sink");

assertThat(graph.getStreamNodes())
        .allSatisfy(node -> assertThat(node.getOperatorName())
                .doesNotContainIgnoringCase("window"));
```

同时断言并行度为配置值、checkpoint interval 为配置值、模式为 `AT_LEAST_ONCE`，且图中有两个
Kafka Sink。

- [ ] **步骤 2：运行拓扑测试并确认红灯**

运行：`mvn -f flink_processer/pom.xml test -Dtest=EventEnrichmentJobTest`

预期：测试因仍存在 `app-enrichment-window` UID 和窗口 operator 而失败。

- [ ] **步骤 3：用 keyed process function 替换滚动窗口**

核心拓扑改为：

```java
SingleOutputStreamOperator<OutputEvent> output = env
        .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka input").uid("kafka-source")
        .map(new EventRecordParser()).uid("parse-json")
        .keyBy(EventRecord::recordKey)
        .process(new CommonStateEnrichmentFunction()).uid("common-state-enrichment");

output.sinkTo(createSink(config, config.outputTopic())).uid("kafka-output-sink");
output.getSideOutput(CommonStateEnrichmentFunction.ERROR_OUTPUT_TAG)
        .sinkTo(createSink(config, config.errorTopic())).uid("kafka-error-sink");
```

删除 `Duration`、`TumblingProcessingTimeWindows` import 和 `createWindowAssigner()`。更新类注释与
作业名称为 `mobile-event-common-enrichment`。

- [ ] **步骤 4：删除已经失效的窗口实现与测试**

删除列出的四个文件，并执行：

```bash
rg -n 'AppEnrichmentWindowFunction|WindowEnricher|EnrichmentBatch|TumblingProcessingTimeWindows' \
  flink_processer/src
```

预期：无匹配，命令退出码为 1。

- [ ] **步骤 5：运行全部 Java 测试并确认绿灯**

运行：`mvn -f flink_processer/pom.xml test`

预期：所有解析、配置、JSON 补全、状态/定时器、序列化和拓扑测试通过。

- [ ] **步骤 6：提交无窗口拓扑**

```bash
git add -A flink_processer/src
git commit -m "refactor(Flink): 用 keyed 状态替换滚动窗口"
```

### 任务 5：更新运行文档并完成全量验证

**文件：**
- 修改：`flink_processer/README.md`
- 修改：`README.md`

- [ ] **步骤 1：改写 Flink 数据契约和路由说明**

`flink_processer/README.md` 明确写出：

- `common.payload` 六个必需非空字符串字段及其顶层映射。
- 完整 common 只更新状态，不进入 `flink_output` 或 `flink_error`。
- 不完整 common 只进入 `flink_error`，不覆盖已有状态。
- 非 common 在收到时立即补全；有完整状态进入 `flink_output`，无状态时六字段为 `null` 并进入
  `flink_error`。
- 每条任意类型消息把当前 key 的处理时间清理时刻顺延 30 秒。
- 两个输出 topic 的 record key 都是 `<udid>_<start_id>`。
- `app` 不再从 `payload.self_package_name`/`apps[]` 提取 `app_name`/`app_version`。
- checkpoint 和 Sink 仍为至少一次语义，下游继续用 `event_id` 幂等。
- 新 operator UID 与旧窗口 UID 不同，旧 savepoint 的窗口状态不能迁移；升级时从新部署启动，或在
  已评估旧状态可丢弃后使用 Flink 的未恢复状态选项。

- [ ] **步骤 2：更新仓库根 README**

把“2 分钟 App 窗口补全”改为“由首条 common 建立状态并即时补全”。快速开始仍保留 Maven 构建和
默认三个 topic，成功/错误说明改为新互斥路由，输出 key 示例使用下划线格式。

- [ ] **步骤 3：执行文档陈旧内容扫描**

运行：

```bash
rg -n '2 分钟|滚动窗口|<udid>:<start_id>|App 信息补全|app-enrichment-window' \
  flink_processer/README.md README.md
```

预期：无旧 Flink 行为匹配；如果根 README 在发送器语境仍使用 App 一词，只保留与发送器数据生成
直接相关的描述。

- [ ] **步骤 4：执行完整构建、测试和产物检查**

运行：

```bash
mvn -f flink_processer/pom.xml clean verify
jar tf flink_processer/target/flink-processer-1.0.0.jar \
  | rg 'CommonStateEnrichmentFunction|CommonMetadata|OutputEventKafkaSerializer|org/apache/kafka'
git diff --check
git status --short
```

预期：Maven `BUILD SUCCESS`；测试全部通过；fat JAR 含新状态函数、元数据类、serializer 和 Kafka
依赖，不含已删除的窗口类；`git diff --check` 无输出；状态仅显示本任务预期的 README 修改。

- [ ] **步骤 5：提交文档和最终验证结果**

```bash
git add README.md flink_processer/README.md
git commit -m "docs(Flink): 说明 common 状态补全规则"
```

提交后再次运行：

```bash
git status --short
git log --oneline -5
```

预期：工作区干净，最近五条提交按任务顺序包含 key、Common JSON 边界、状态函数、拓扑替换和文档。
