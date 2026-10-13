# Flink 事件时间清理与单机 Checkpoint 恢复设计

## 目标

将 `flink_processer` 的状态清理依据从处理时间改为消息顶层的 `event_timestamp`，并让单机 Ubuntu 上的 Flink 作业在取消、服务重启或重新提交后，可以从保留的 checkpoint 恢复 Kafka offset、公共元数据状态和事件时间定时器。

部署环境固定为：

- Ubuntu 单机 Flink 2.3.0，安装目录 `/home/zhangdi/works/flink-2.3.0`；
- checkpoint 目录 `file:///home/zhangdi/workspace/flink_data/checkpoints`；
- savepoint 目录 `file:///home/zhangdi/workspace/flink_data/savepoints`；
- 两个目录由用户在部署前创建，并保证 Flink 运行用户可读写；
- checkpoint 和 savepoint 不写入版本化的 Flink 安装目录，也不写入 `/tmp`。

## 输入时间校验与校正

`event_timestamp` 成为事件时间处理的必填字段，必须是可转换为 Java `long` 的 JSON 整数。字段缺失、类型错误或超出 64 位整数范围时，解析器抛出异常，交给 Flink 的失败恢复策略处理，不把这类数据写入 `flink_error`。

每条消息反序列化时读取一次当前系统时间。若：

```text
event_timestamp > 当前系统时间 + 5 秒
```

则把内部 `EventRecord.eventTimestamp` 和原始 JSON 顶层 `event_timestamp` 同时改为该次读取的当前系统时间。未超过阈值的消息保留原始 JSON 和事件时间。比较与 30 秒截止时间计算均使用溢出检查，避免极端时间戳回绕成过去时间。

## Kafka Source 与 Watermark

Kafka Source 直接反序列化为 `EventRecord`，保证时间校正发生在 timestamp assigner 之前。Source 使用以下 watermark 策略：

- 从校正后的 `EventRecord.eventTimestamp` 提取事件时间；
- 允许 5 秒 bounded out-of-orderness；
- 输入连续 1 分钟没有消息时标记为空闲，避免空闲 Kafka split 阻塞其他活跃 split 的 watermark。

事件在到达 `KeyedProcessFunction` 时立即处理。watermark 只推进事件时间并触发定时器，不缓存或延迟业务输出。若所有 Kafka 分区都停止产生消息，watermark 不再推进，纯事件时间定时器不会仅因现实时间流逝而触发。

## Keyed State 与事件时间定时器

数据继续按 `<udid>_<start_id>` 分组。每个活跃 key 只保存：

- 最近一条完整 `common` 提供的六个公共字段；
- 最新的事件时间清理截止时间；
- Flink TimerService 管理的一个事件时间定时器。

每条任意类型消息都计算：

```text
候选截止时间 = 校正后的 event_timestamp + 30 秒
最新截止时间 = max(已保存截止时间, 候选截止时间)
```

只有候选截止时间更晚时，才删除旧事件时间定时器、注册新定时器并更新截止时间状态。因此，后到的乱序旧消息不会把清理时间提前。watermark 越过最新截止时间后，`onTimer` 比对回调时间戳与状态；只有最新定时器才能清除公共元数据和截止时间状态，陈旧回调不会误删续期后的状态。

`common` 和非 `common` 的补全、输出和错误路由规则保持不变。非 `common` 消息仍即时输出，不保存在状态中。

## Checkpoint 配置

作业继续启用周期性 checkpoint，默认间隔 60 秒，模式保持 `AT_LEAST_ONCE`。新增两个可覆盖的作业参数：

| 参数 | 默认值 |
| --- | --- |
| `--checkpoint-storage-uri` | `file:///home/zhangdi/workspace/flink_data/checkpoints` |
| `--savepoint-storage-uri` | `file:///home/zhangdi/workspace/flink_data/savepoints` |

代码将 checkpoint storage 配置为 filesystem，并设置 `ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION`。取消作业后 checkpoint 不自动删除，需要运维人员定期清理不再需要的旧作业目录。

一次成功 checkpoint 形成一致快照，包含：

- Kafka Source 消费 offset；
- `common` keyed state；
- 最新清理截止时间；
- 事件时间定时器；
- Kafka Sink 恢复所需状态。

异常后由原作业自动重启时，Flink 使用最新成功 checkpoint。作业取消、Flink 服务重启或重新提交 JAR 后，运维人员通过以下形式显式恢复：

```bash
/home/zhangdi/works/flink-2.3.0/bin/flink run --detached \
  -s file:///home/zhangdi/workspace/flink_data/checkpoints/<job-id>/chk-<id>/_metadata \
  flink-processer-1.0.0.jar
```

恢复时以 checkpoint 内的 Kafka Source offset 为准；只有未指定恢复快照的新作业才使用消费组已提交 offset，并在不存在已提交 offset 时从最早位置开始。计划内停机可使用 savepoint 目录生成 savepoint，再以同样的 `-s` 参数恢复。

输出 Sink 继续使用至少一次语义。恢复点之后、故障之前已经写出的记录可能被再次发送；下游继续使用 `event_id` 做幂等处理。

## 版本与打包

工程的 Flink 核心依赖从 2.2.0 升级到服务器一致的 2.3.0，并保持为 `provided`。截至 2026-10-14，Maven Central 发布的最新 Kafka connector 是 `5.0.0-2.2`，尚无 `-2.3` 构建，因此保留该 connector 并打入 fat JAR。构建测试之外，还必须在 Ubuntu 的 Flink 2.3.0 上进行作业提交和 checkpoint 冒烟验证；若 connector 在 2.3.0 运行时出现二进制不兼容，不通过验收，也不以未经验证的依赖版本替代。

所有现有算子 UID 保持稳定，确保恢复时能够映射 Kafka Source、状态补全算子和两个 Sink 的状态。

## 测试与验收

自动化测试覆盖：

1. 合法事件时间原样保留；
2. 超过当前时间 5 秒的时间戳同时校正内部字段和输出 JSON；
3. 缺失或非法 `event_timestamp` 被拒绝；
4. watermark 使用 5 秒乱序容忍和 1 分钟 idleness；
5. 每种消息都按事件时间续期，乱序旧消息不能提前截止时间；
6. watermark 未越过截止时间时状态保留，越过时状态和定时器被清除；
7. checkpoint 快照恢复后，公共元数据、截止时间和事件时间定时器仍可继续工作；
8. 默认 checkpoint/savepoint URI、覆盖参数与 URI 校验正确；
9. Maven `clean verify` 和 fat JAR 内容检查通过。

Ubuntu 冒烟验收覆盖：

1. 作业成功运行于 `/home/zhangdi/works/flink-2.3.0`；
2. 指定目录生成成功 checkpoint；
3. 取消作业后 checkpoint 仍存在；
4. 使用该 checkpoint 的 `_metadata` 重新提交后，从快照 offset 继续消费；
5. 恢复前保存的 `common` 状态仍能补全恢复后到达的非 `common` 消息。
