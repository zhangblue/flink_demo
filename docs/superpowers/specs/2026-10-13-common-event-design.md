# Common 类型事件设计

## 背景与目标

当前每个设备启动周期生成 `start`、`devinfo`、`environment` 和 `app` 四条消息。新增 `common` 类型事件，用于在其他事件之前发送本次启动所需的公共设备与 App 信息。

每个启动周期改为生成五条消息，且 `common` 必须是第一条。现有 Kafka key、全局限速、设备并发及发送机制保持不变。

## 消息结构

`common` 继续使用现有统一消息信封：

```json
{
  "event_id": "00000000-0000-4000-8000-00000000002a",
  "protol_type": "common",
  "udid": "ad3eb36b-2145-4e70-8000-00000000002a",
  "start_id": 1791614142000,
  "event_timestamp": 1791614142380,
  "sequence": 1,
  "payload": {
    "self_app_package": "com.eg.android.AlipayGphone",
    "self_app_name": "支付宝",
    "model": "23127PN0CC",
    "brand": "Xiaomi",
    "os": "Android",
    "os_version": "15"
  }
}
```

顶层字段语义沿用现有契约：

- `event_id`：单条消息的唯一 UUID。
- `protol_type`：固定为 `common`。字段名继续使用既定拼写 `protol_type`。
- `udid`：设备唯一标识；同一设备保持不变。
- `start_id`：本次启动开始时的 Unix epoch 毫秒值；同一启动周期的五条消息保持一致。
- `event_timestamp`：生成本批事件时采集的 Unix epoch 毫秒值，与 `start_id` 独立。
- `sequence`：固定为 `1`。

`payload` 字段定义：

| 字段 | JSON 类型 | 语义 |
| --- | --- | --- |
| `self_app_package` | string | 本次 `common` 事件独立选中的 App package name 或 bundle ID |
| `self_app_name` | string | 与 `self_app_package` 对应的可读 App 名称 |
| `model` | string | 当前设备的厂商型号标识 |
| `brand` | string | 当前设备品牌 |
| `os` | string | `Android` 或 `iOS` |
| `os_version` | string | 当前设备操作系统版本 |

## App 选择规则

每个启动周期从当前设备的完整安装列表中为 `common` 独立随机选择一条 App，同时读取该条目的 `package_name` 和 `app_name`，保证二者匹配。

该选择与 `start.payload.app_package`、`app.payload.self_package_name` 没有一致性约束。它们可以不同，也允许随机选择时恰好相同。不会为 `common` 维护第二份 App 候选表，也不会新增配置项。

## 设备状态来源

`model`、`brand`、`os` 和 `os_version` 直接来自生成该设备时确定的设备档案。因此：

- 同一设备的这些字段跨启动周期保持一致。
- Android 与 iOS 的品牌、型号、系统和版本组合继续遵循现有设备档案约束。
- `common` 中的四个字段与同一周期 `devinfo` 中的对应字段一致。

## 启动周期与顺序

每次启动生成五条消息：

1. `common`，`sequence = 1`。
2. `start`，`sequence = 2`。
3. `devinfo`，`sequence = 3`。
4. `environment` 或 `app`，`sequence = 4`。
5. 另一条 `environment` 或 `app`，`sequence = 5`。

`environment` 与 `app` 的相对顺序继续随机。五条消息共享同一个 `udid` 和 `start_id`，每条消息拥有唯一 `event_id`。Kafka key 仍使用 `udid`，确保同一设备的消息进入同一分区并保持发送顺序。

现有“先发送 `start`，再发送 `devinfo`”约束仍成立，但二者之前新增一条 `common`。

## 代码边界

- 在协议枚举中新增 `Common`，序列化为 `common`。
- 新增 `CommonPayload`，并将其加入 `EventPayload`。
- 设备生成器在创建每个启动周期时生成 `common` payload，并把五条事件按上述顺序组装。
- Kafka producer、全局限速器、Tokio 任务结构和配置文件不需要新增行为或参数。
- Flink 下游需把 `common` 视为新的 `protol_type` 分支；原有四类 JSON payload 字段保持不变。

## 兼容性影响

本变更是数据契约扩展，同时改变每个启动周期的事件数量和现有类型的 `sequence`：

- 每周期事件数由 4 增加为 5。
- `start` 的 `sequence` 由 1 调整为 2。
- `devinfo` 的 `sequence` 由 2 调整为 3。
- `environment` 与 `app` 的 `sequence` 由 3/4 调整为 4/5。
- 未识别 `common` 的消费者应在上线前增加分流处理，或明确忽略未知类型。

`start`、`devinfo`、`environment`、`app` 各自现有 payload 不新增、删除或更名字段。

## 测试与验收标准

自动化测试必须覆盖：

1. `common` JSON 序列化字段完整、类型正确，且无额外字段。
2. 每个周期生成五条消息，第一条为 `common`，随后严格为 `start`、`devinfo`。
3. `common`、`start`、`devinfo`、`environment`、`app` 的 `sequence` 为连续的 1 至 5。
4. `environment` 与 `app` 两种后置顺序仍都可生成。
5. 五条消息共享相同 `udid` 和 `start_id`，且 `event_id` 各不相同。
6. `common.payload.self_app_package` 与 `self_app_name` 对应设备安装列表中的同一条记录。
7. `common` 的设备字段与同一周期 `devinfo` 的对应字段一致。
8. `common` 的 App 与 `start`/`app` App 之间不建立相等断言。
9. Kafka 真实往返测试期望接收五条消息，并验证新的顺序及 JSON 内容。
10. README 中的公共契约、完整示例、顺序说明和 Flink 消费说明同步更新。

完成实现后运行格式检查、Clippy、全部测试和 release 构建。真实 Kafka 集成测试在集群可达时执行；若环境不可达，应明确报告而不能声明该项通过。
