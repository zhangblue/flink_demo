# 手机 App 模拟数据发送器设计方案

## 1. 目标与范围

在 `sender/` 下实现一个基于 Rust、Tokio 和 Kafka 的持续运行数据模拟器。程序同时模拟多台 Android/iOS 手机，每台手机维持稳定的设备身份，周期性模拟 App 启动，并将 JSON 消息写入 Kafka。

本次只实现数据生成与 Kafka 发送，不实现 `processer/` 中的 Flink 消费逻辑。本文同时作为后续 Flink JSON 反序列化的数据契约。

## 2. 核心约束

- 同一台模拟手机在进程生命周期内始终使用相同 `udid`。
- 每次 App 启动使用启动时刻的 Unix 毫秒值作为 `start_id`；属于同一次启动的全部消息共享该值。
- 每次启动必须先发送 `start`，再发送 `devinfo`；`environment` 与 `app` 的先后顺序随机。
- 每种类型在一个启动周期内默认发送一次，共四条消息。
- `sequence` 从 1 开始严格递增：`start` 为 1，`devinfo` 为 2，其余为 3、4。
- Kafka record key 使用 `udid`，确保同一设备的消息进入同一分区并保持生产顺序。
- 所有时间均使用 Unix 毫秒时间戳，不包含 `event_time` 字符串字段。
- 消息类型字段按需求保留拼写 `protol_type`，不得自动更名为 `protocol_type`。

## 3. 总体架构

采用“每台设备一个异步任务 + 中央发送器”的结构：

```text
DeviceTask(1..N) -> bounded Tokio mpsc -> global rate limiter -> Kafka FutureProducer
```

- `DeviceTask` 保存设备档案、固定 `udid`、当前启动状态及安装列表。
- 每个设备任务生成完整启动周期，通过有界 `mpsc` channel 将消息交给发送器。
- 中央发送器对所有设备共享一个全局速率限制器，保证 `messages_per_second` 是总吞吐上限。
- channel 满时设备任务异步等待，形成背压，避免 Kafka 变慢时无限增长内存。
- 以 `udid` 为 Kafka key。单个设备任务按顺序入队，中央发送器按 channel 接收顺序提交，从而保留每台设备的启动消息顺序。

## 4. 公共消息结构

每条 Kafka record 的 value 是一个完整 JSON 对象：

```json
{
  "event_id": "00000000-0000-4001-8000-00000000002a",
  "protol_type": "start",
  "udid": "ad3eb36b-2145-4e70-8000-00000000002a",
  "start_id": 1791614142000,
  "event_timestamp": 1791614142381,
  "sequence": 1,
  "payload": {}
}
```

| 字段 | JSON 类型 | 约束与语义 |
| --- | --- | --- |
| `event_id` | string | 每条事件唯一的 UUID v4 |
| `protol_type` | string | 枚举：`start`、`devinfo`、`environment`、`app` |
| `udid` | string | 设备唯一 UUID v4；同一设备跨启动保持不变 |
| `start_id` | integer | 启动周期开始时的 Unix 毫秒值；每次启动更新，同一周期保持不变；Flink 使用 `BIGINT` |
| `event_timestamp` | integer | 单条事件生成时刻的 Unix 毫秒时间戳，与 `start_id` 独立 |
| `sequence` | integer | 当前启动周期内从 1 开始递增的无符号整数 |
| `payload` | object | 由 `protol_type` 决定的类型专属对象 |

未知字段允许下游忽略；上述字段和各 payload 必填字段不得静默删除或更名。需要演进时优先新增可选字段。

## 5. `start` 数据契约

用途：描述本次 App 启动时的状态。

```json
{
  "event_id": "00000000-0000-4001-8000-00000000002a",
  "protol_type": "start",
  "udid": "ad3eb36b-2145-4e70-8000-00000000002a",
  "start_id": 1791614142000,
  "event_timestamp": 1791614142381,
  "sequence": 1,
  "payload": {
    "app_package": "com.example.demo",
    "app_version": "3.8.1",
    "app_build": 30801,
    "launch_type": "cold",
    "foreground": true,
    "battery_level": 76,
    "network_type": "wifi"
  }
}
```

| payload 字段 | JSON 类型 | 约束与语义 |
| --- | --- | --- |
| `app_package` | string | Android package name 或 iOS bundle ID |
| `app_version` | string | 可读版本号，如 `3.8.1` |
| `app_build` | integer | 正整数构建号 |
| `launch_type` | string | `cold` 或 `warm` |
| `foreground` | boolean | 启动时 App 是否位于前台；本模拟器通常为 `true` |
| `battery_level` | integer | 0～100 的电量百分比 |
| `network_type` | string | `wifi`、`4g`、`5g` 或 `none` |

## 6. `devinfo` 数据契约

用途：描述相对稳定的设备硬件和操作系统信息。

```json
{
  "event_id": "00000000-0000-4002-8000-00000000002a",
  "protol_type": "devinfo",
  "udid": "ad3eb36b-2145-4e70-8000-00000000002a",
  "start_id": 1791614142000,
  "event_timestamp": 1791614142382,
  "sequence": 2,
  "payload": {
    "manufacturer": "Xiaomi",
    "brand": "Xiaomi",
    "model": "23127PN0CC",
    "device_name": "Xiaomi 14",
    "os": "Android",
    "os_version": "15",
    "cpu_arch": "arm64-v8a",
    "cpu_cores": 8,
    "memory_total_mb": 12288,
    "storage_total_mb": 262144,
    "screen_width": 1200,
    "screen_height": 2670,
    "screen_dpi": 460
  }
}
```

| payload 字段 | JSON 类型 | 约束与语义 |
| --- | --- | --- |
| `manufacturer` | string | 设备制造商，如 `Xiaomi`、`Apple` |
| `brand` | string | 对外品牌，如 `Xiaomi`、`iPhone` |
| `model` | string | 厂商型号标识 |
| `device_name` | string | 用户可读设备名称 |
| `os` | string | `Android` 或 `iOS` |
| `os_version` | string | 操作系统版本 |
| `cpu_arch` | string | Android 通常为 `arm64-v8a`，iOS 为 `arm64` |
| `cpu_cores` | integer | 正整数 CPU 逻辑核心数 |
| `memory_total_mb` | integer | 正整数，总内存 MiB |
| `storage_total_mb` | integer | 正整数，总存储 MiB |
| `screen_width` | integer | 正整数，屏幕像素宽度 |
| `screen_height` | integer | 正整数，屏幕像素高度 |
| `screen_dpi` | integer | 正整数，屏幕 DPI |

设备生成使用预定义的合理档案组合，Android/iOS 的品牌、型号、系统和 CPU 架构不会跨平台任意拼接。

## 7. `environment` 数据契约

用途：描述消息采集时可能变化的手机运行环境。

```json
{
  "event_id": "00000000-0000-4003-8000-00000000002a",
  "protol_type": "environment",
  "udid": "ad3eb36b-2145-4e70-8000-00000000002a",
  "start_id": 1791614142000,
  "event_timestamp": 1791614142383,
  "sequence": 3,
  "payload": {
    "network_type": "wifi",
    "carrier": "China Mobile",
    "ip_address": "10.18.26.37",
    "locale": "zh-CN",
    "timezone": "Asia/Shanghai",
    "battery_level": 76,
    "charging": false,
    "memory_available_mb": 6381,
    "storage_available_mb": 176420,
    "is_emulator": false,
    "is_rooted": false,
    "is_vpn": false
  }
}
```

| payload 字段 | JSON 类型 | 约束与语义 |
| --- | --- | --- |
| `network_type` | string | `wifi`、`4g`、`5g` 或 `none` |
| `carrier` | string | 运营商名称；无蜂窝网络时可为空字符串 |
| `ip_address` | string | 合法 IPv4 字符串；无网络时使用空字符串 |
| `locale` | string | BCP 47 风格区域标识，如 `zh-CN` |
| `timezone` | string | IANA 时区，如 `Asia/Shanghai` |
| `battery_level` | integer | 0～100 的电量百分比 |
| `charging` | boolean | 是否正在充电 |
| `memory_available_mb` | integer | 非负整数，且不超过设备总内存 |
| `storage_available_mb` | integer | 非负整数，且不超过设备总存储 |
| `is_emulator` | boolean | 是否模拟器环境 |
| `is_rooted` | boolean | Android 表示 Root，iOS 表示越狱，字段统一 |
| `is_vpn` | boolean | 是否使用 VPN |

## 8. `app` 数据契约

用途：在一条消息中携带设备当前完整安装列表。

以下精简示例假定配置为 `installed_apps_min = 1`、`installed_apps_max = 1`，因此 `apps` 中的一项就是该设备的完整列表；默认配置为 20～80 项。

```json
{
  "event_id": "00000000-0000-4004-8000-00000000002a",
  "protol_type": "app",
  "udid": "ad3eb36b-2145-4e70-8000-00000000002a",
  "start_id": 1791614142000,
  "event_timestamp": 1791614142384,
  "sequence": 4,
  "payload": {
    "collected_at": 1791614142384,
    "apps": [
      {
        "package_name": "com.tencent.mm",
        "app_name": "WeChat",
        "version_name": "3.0.2",
        "version_code": 2800,
        "is_system_app": false,
        "first_installed_at": 1782974142384,
        "last_updated_at": 1791441342384
      }
    ]
  }
}
```

| payload 字段 | JSON 类型 | 约束与语义 |
| --- | --- | --- |
| `collected_at` | integer | 安装列表采集时刻的 Unix 毫秒时间戳 |
| `apps` | array | 完整安装列表，长度处于配置范围内 |

每个 `apps` 元素：

| App 字段 | JSON 类型 | 约束与语义 |
| --- | --- | --- |
| `package_name` | string | Android package name 或 iOS bundle ID |
| `app_name` | string | 用户可读名称 |
| `version_name` | string | 可读版本号 |
| `version_code` | integer | 正整数构建/版本号 |
| `is_system_app` | boolean | 是否为系统 App |
| `first_installed_at` | integer | 首次安装的 Unix 毫秒时间戳 |
| `last_updated_at` | integer | 最近更新的 Unix 毫秒时间戳，不早于首次安装时间 |

## 9. 启动周期与数据流

每个设备任务持续执行以下状态机：

1. 采集启动时刻的 Unix 毫秒值作为新的 `start_id`，将周期内 `sequence` 重置为 1；同一设备发生同毫秒启动或时钟回拨时，将该值提升到上一周期加 1。
2. 生成并入队 `start`。
3. 生成并入队 `devinfo`。
4. 随机决定 `environment` 与 `app` 的入队顺序并依次生成。
5. 在 `startup_interval_min_secs` 与 `startup_interval_max_secs` 之间随机等待。
6. 返回步骤 1，开始新启动周期。

设备的硬件档案和 `udid` 在整个进程内稳定；电量、网络、可用内存/存储等环境字段随启动周期变化；安装列表以设备档案为基础保持合理稳定，可在不同周期模拟版本更新时间变化。

## 10. 配置

默认读取 `sender/config.toml`，命令行参数 `--config <PATH>` 可指定其他配置文件。

```toml
[kafka]
brokers = "192.168.250.106:9092"
topic = "flink_demo"
compression = "lz4"
message_timeout_ms = 5000

[simulation]
device_count = 100
messages_per_second = 500
startup_interval_min_secs = 30
startup_interval_max_secs = 120
installed_apps_min = 20
installed_apps_max = 80
channel_capacity = 10000
```

| 配置项 | 约束 |
| --- | --- |
| `kafka.brokers` | 非空 Kafka bootstrap server 列表 |
| `kafka.topic` | 非空 topic；默认 `flink_demo` |
| `kafka.compression` | Kafka producer 支持的压缩方式；默认 `lz4` |
| `kafka.message_timeout_ms` | 大于 0 |
| `simulation.device_count` | 大于 0 |
| `simulation.messages_per_second` | 大于 0，所有设备共享的全局速率 |
| `simulation.startup_interval_min_secs` | 大于 0，且不大于最大值 |
| `simulation.startup_interval_max_secs` | 不小于最小值 |
| `simulation.installed_apps_min` | 大于 0，且不大于最大值 |
| `simulation.installed_apps_max` | 不小于最小值，且不超过 4,000 |
| `simulation.channel_capacity` | 大于 0，且不超过 1,000,000 与 Tokio semaphore 许可上限 |

设备常驻安装列表满足 `device_count × installed_apps_max ≤ 1,000,000`；channel 中事件的安装列表副本满足 `channel_capacity × installed_apps_max ≤ 1,000,000`。两项乘法均须检查溢出；默认值分别占用 8,000 与 800,000 项预算。配置不合法时程序在连接 Kafka 和启动设备任务之前给出明确错误并退出。

## 11. Kafka 发送与故障处理

- 每条 JSON 作为独立 Kafka record value，record key 为 `udid`。
- 当前生成器将单个 `app` 事件 JSON value 控制在 900,000 字节预算内；4,000 项安装列表的构造序列化上界为 890,000 字节。producer 客户端 `message.max.bytes` 固定为 1,000,000；这不改变 broker/topic 的消息上限，部署环境如更严格须调低安装项数。
- 使用异步 Kafka producer；依赖 producer 的队列和重试机制处理暂时性错误。
- 中央发送器在每次提交前获取全局速率许可。
- 中央发送器在等待下一条 channel 事件期间同时回收已完成的 delivery ack，及时记录低流量场景下的交付失败。
- 最终发送失败时记录 `event_id`、`udid`、`start_id`、topic 和错误原因，然后继续处理后续消息，避免单条失败终止整个模拟器。
- JSON 序列化失败视为程序内部错误，记录上下文并继续；数据类型设计应使正常生成路径不可产生不可序列化值。
- 收到 Ctrl+C 后停止创建新的启动周期，关闭发送端，尽量排空 channel，并刷新 producer 后退出。

## 12. 测试策略

采用测试驱动开发，先验证失败再写最小实现。

- **数据契约测试：** 分别序列化四类消息，断言公共字段、`protol_type`、payload 全部字段名与 JSON 类型，保护 Flink 数据契约。
- **设备身份测试：** 同一设备跨多个周期保持 `udid`；不同周期产生严格递增的毫秒 `start_id`，并验证 `event_timestamp` 从独立的消息生成时间取得。
- **顺序测试：** 每周期第一条为 `start`、第二条为 `devinfo`，周期内 `start_id` 一致且 `sequence` 严格递增。
- **合理性测试：** 电量范围、可用资源不超过总量、App 数量范围、安装/更新时间关系、Android/iOS 档案组合正确。
- **配置测试：** 覆盖零设备、零速率、空 broker/topic、反向区间等非法配置。
- **限速测试：** 使用 Tokio 暂停时间验证所有设备共享一个限速器，不依赖真实等待。
- **发送器测试：** 通过 `EventSink` 抽象和内存实现验证 Kafka key 选择、消息顺序及单条发送失败后的继续处理。
- **Kafka 集成测试：** 显式启用后连接 `192.168.250.106:9092` 的 `flink_demo`，发送并消费一个完整启动周期，验证 key、JSON 和顺序。默认 `cargo test` 不访问真实 Kafka，避免意外写入共享环境。

完成实现后的验证命令：

```bash
cargo fmt --check --manifest-path sender/Cargo.toml
cargo clippy --manifest-path sender/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path sender/Cargo.toml --all-features
```

## 13. 交付内容

`sender/` 将包含：

- 独立 Rust crate 与可执行程序；
- 示例 `config.toml`，默认指向 `192.168.250.106:9092` 和 `flink_demo`；
- 配置解析、设备模型、四类数据生成、全局限速、Kafka producer 与优雅退出；
- 单元测试和显式启用的真实 Kafka 集成测试；
- README，记录运行方式、配置说明和完整 JSON 数据契约。

`processer/` 不在本次修改范围内。
