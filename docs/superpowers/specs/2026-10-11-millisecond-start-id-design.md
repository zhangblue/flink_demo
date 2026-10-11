# 毫秒 start_id 数据契约设计

## 目标

将顶层 `start_id` 从 UUID 字符串改为 Unix epoch 毫秒整数，使 Flink 可将其直接声明为 `BIGINT`。`start_id` 标识设备的一次启动周期；`event_timestamp` 继续表示单条消息的生成时间，两者具有独立语义。

## 数据契约

- `start_id` 的 JSON 类型为 integer，对应 Rust `i64` 和 Flink `BIGINT`。
- 同一设备同一启动周期的 4 条事件共享同一个 `start_id`。
- 同一设备后续启动周期的 `start_id` 必须严格递增，即使系统时钟回拨或两个周期落在同一毫秒。
- 不同设备允许出现相同 `start_id`；启动周期的业务标识是 `(udid, start_id)`。
- `event_timestamp` 的 JSON 类型仍为 integer，对应单条事件生成时的 Unix epoch 毫秒值。
- `event_timestamp` 不从 `start_id` 加序号计算，也不要求与 `start_id` 相等。
- 同一周期内多个事件可能在同一毫秒生成，因此 `event_timestamp` 允许相同；事件顺序以 `sequence` 为准。

示例：

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "protol_type": "start",
  "udid": "ad3eb36b-2145-4e70-a8cd-adc193ca865f",
  "start_id": 1791614142000,
  "event_timestamp": 1791614142381,
  "sequence": 1,
  "payload": {}
}
```

## 实现

`MobileEvent.start_id` 改为 `i64`。设备生成器单独维护上一次启动 ID：每次使用调用方提供的启动时刻；若该值不大于上一周期，则使用上一值加 1。事件时间由独立的事件时间基准生成，不引用 `start_id`；四条消息保持非递减并与 `sequence` 对应。

运行器在周期开始时读取一次系统时间作为启动时刻，再独立读取一次系统时间作为事件生成时刻，将两者分别传给生成器。测试可直接提供两个不同的时间值，证明两个字段没有派生关系。

## 兼容性

这是数据契约的不兼容变更。Flink 表或反序列化模型必须把 `start_id` 从 `STRING` 改为 `BIGINT`。`event_id` 和 `udid` 仍为 UUID 字符串；Kafka key 仍为 `udid`。

## 验证

- 序列化测试断言 `start_id` 是 JSON number。
- 生成器测试断言同周期一致、跨周期严格递增、时钟回拨时仍递增。
- 独立时间测试使用不同的启动时刻和事件时刻，断言 `event_timestamp` 不由 `start_id` 派生。
- Kafka 集成测试按数字读取并比较 `start_id`。
- README 和设计文档中的所有字段表与 JSON 示例同步更新。
