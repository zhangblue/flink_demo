# 手机事件模拟发送器

持续模拟多台 Android/iOS 手机的 App 启动，将每次启动的四条 JSON 事件写入 Kafka。每条 Kafka record 的 key 是设备 `udid`，value 是完整 JSON 对象。程序只负责生成与发送，不包含 Flink 消费逻辑。

## 构建与运行

从仓库根目录执行：

```bash
cargo build --manifest-path sender/Cargo.toml
cargo run --manifest-path sender/Cargo.toml -- --config sender/config.toml
RUST_LOG=debug cargo run --manifest-path sender/Cargo.toml -- --config sender/config.toml
```

默认 CLI 路径为当前工作目录下的 `config.toml`。因此从仓库根目录运行时请显式传 `--config sender/config.toml`；在 `sender/` 目录内运行可省略 `--config`。日志默认级别为 `info`，可用 `RUST_LOG=debug` 或 `RUST_LOG=mobile_event_sender=debug` 调整。按 Ctrl+C 后程序停止生成新周期，尽量排空待发送消息并退出。

## 配置

`sender/config.toml` 默认内容：

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

| 配置项 | 含义与约束 |
| --- | --- |
| `kafka.brokers` | 非空 Kafka bootstrap server 列表；默认 `192.168.250.106:9092` |
| `kafka.topic` | 非空目标 topic；默认 `flink_demo` |
| `kafka.compression` | Kafka producer 支持的压缩方式；默认 `lz4` |
| `kafka.message_timeout_ms` | 消息交付超时，正整数毫秒；默认 5000 |
| `simulation.device_count` | 模拟设备数，1～1,000,000；默认 100 |
| `simulation.messages_per_second` | 所有设备共享的总发送速率，正整数条/秒；必须能形成至少 1 纳秒的限速间隔；默认 500 |
| `simulation.startup_interval_min_secs` | 每台设备两次启动之间的最短间隔，正整数秒；不大于最大值；默认 30 |
| `simulation.startup_interval_max_secs` | 最长启动间隔，正整数秒；不小于最小值；默认 120 |
| `simulation.installed_apps_min` | 每台设备安装 App 数下限，正整数；不大于上限；默认 20 |
| `simulation.installed_apps_max` | 安装 App 数上限，不小于下限且不超过 4,000；默认 80 |
| `simulation.channel_capacity` | 发送队列容量，1～1,000,000，且不超过 Tokio semaphore 许可上限；默认 10,000 |

另有两项联合预算：设备常驻安装列表 `device_count × installed_apps_max ≤ 1,000,000`，channel 内事件复制的安装列表 `channel_capacity × installed_apps_max ≤ 1,000,000`。默认值分别为 8,000 与 800,000。程序在连接 Kafka、启动设备任务之前校验配置；超限、乘法溢出或区间无效会报错退出。

`app` 事件 JSON value 的生成预算为 900,000 字节，给默认约 1,000,000 字节的 Kafka 消息上限留出空间；producer 客户端的 `message.max.bytes` 固定为 1,000,000，这不会提高 broker 的接收上限。4,000 项边界在当前生成器中序列化的构造上界为 890,000 字节。若更改字段或生成规则，应重新验证此预算；broker 或 topic 配置若更严格，仍需相应调低上限。

## 公共数据契约

每条消息的顶层字段均必填。字段名 `protol_type` 是既定契约拼写，不要改成 `protocol_type`。所有时间值都是 Unix epoch 毫秒整数，没有 `event_time` 字符串字段。

| 字段 | JSON 类型 | 语义 |
| --- | --- | --- |
| `event_id` | string | 每条事件唯一的 UUID v4 |
| `protol_type` | string | `start`、`devinfo`、`environment` 或 `app` |
| `udid` | string | 设备 UUID v4；同一设备在进程生命周期内保持稳定 |
| `start_id` | integer | 本次启动开始时的 Unix 毫秒值；每个启动周期更新，该周期四条消息相同；Flink 使用 `BIGINT` |
| `event_timestamp` | integer | 单条消息生成时刻的 Unix 毫秒时间戳，与 `start_id` 独立 |
| `sequence` | integer | 当前启动周期内从 1 开始递增的无符号序号 |
| `payload` | object | 由 `protol_type` 决定的类型专属数据 |

每次启动依次发送 `start`（sequence 1）、`devinfo`（sequence 2），随后以随机顺序发送 `environment` 与 `app`（sequence 3、4）；默认每类各一条。一个周期中的 `udid` 和 `start_id` 相同，每条 `event_id` 唯一。Kafka key 使用 `udid`，使同一设备进入同一分区并保持发送顺序。不同设备可能具有相同的毫秒 `start_id`，消费端应使用 `(udid, start_id)` 标识启动周期并按 `sequence` 理解周期顺序。`event_timestamp` 在消息生成时独立采集，不从 `start_id` 派生；同一批消息可能具有相同值。下游可忽略未知字段，但不应静默删除或更名本文列出的必填字段；演进时优先新增可选字段。

### `start`

描述本次 App 启动状态：

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

| payload 字段 | JSON 类型 | 语义 |
| --- | --- | --- |
| `app_package` | string | Android package name 或 iOS bundle ID |
| `app_version` | string | 可读版本号，如 `3.8.1` |
| `app_build` | integer | 正整数构建号 |
| `launch_type` | string | `cold` 或 `warm` |
| `foreground` | boolean | 启动时 App 是否位于前台；模拟器通常为 `true` |
| `battery_level` | integer | 0～100 的电量百分比 |
| `network_type` | string | `wifi`、`4g`、`5g` 或 `none` |

### `devinfo`

描述相对稳定的设备硬件与操作系统信息：

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

| payload 字段 | JSON 类型 | 语义 |
| --- | --- | --- |
| `manufacturer` | string | 制造商，如 `Xiaomi`、`Apple` |
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

设备使用预定义的 Android/iOS 档案组合，品牌、型号、系统和 CPU 架构不会跨平台任意拼接。

### `environment`

描述消息采集时可能变化的手机运行环境：

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

| payload 字段 | JSON 类型 | 语义 |
| --- | --- | --- |
| `network_type` | string | `wifi`、`4g`、`5g` 或 `none` |
| `carrier` | string | 运营商名称；无蜂窝网络时可为空字符串 |
| `ip_address` | string | 合法 IPv4 字符串；无网络时为空字符串 |
| `locale` | string | BCP 47 风格区域标识，如 `zh-CN` |
| `timezone` | string | IANA 时区，如 `Asia/Shanghai` |
| `battery_level` | integer | 0～100 的电量百分比 |
| `charging` | boolean | 是否正在充电 |
| `memory_available_mb` | integer | 非负整数，且不超过设备总内存 |
| `storage_available_mb` | integer | 非负整数，且不超过设备总存储 |
| `is_emulator` | boolean | 是否为模拟器环境 |
| `is_rooted` | boolean | Android 表示 Root，iOS 表示越狱 |
| `is_vpn` | boolean | 是否使用 VPN |

### `app`

在一条消息中携带设备当前完整安装列表：

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

| payload 字段 | JSON 类型 | 语义 |
| --- | --- | --- |
| `collected_at` | integer | 安装列表采集时刻的 Unix 毫秒时间戳 |
| `apps` | array | 完整安装列表，长度在 `installed_apps_min`～`installed_apps_max` 内 |

每个 `apps` 元素：

| App 字段 | JSON 类型 | 语义 |
| --- | --- | --- |
| `package_name` | string | Android package name 或 iOS bundle ID |
| `app_name` | string | 用户可读名称 |
| `version_name` | string | 可读版本号 |
| `version_code` | integer | 正整数构建/版本号 |
| `is_system_app` | boolean | 是否为系统 App |
| `first_installed_at` | integer | 首次安装的 Unix 毫秒时间戳 |
| `last_updated_at` | integer | 最近更新的 Unix 毫秒时间戳，不早于首次安装时间 |

## Flink 消费建议

按 `protol_type` 将四种 payload 分流反序列化。将 `event_timestamp` 解释为 Unix epoch **milliseconds**，再作为事件时间及 watermark 的输入；不要按秒解析，也不要依赖不存在的 `event_time` 字段。Kafka 分区顺序只保证同一 `udid` 的 record 顺序，不代表不同设备之间有全局顺序。

## 测试

```bash
cargo test --manifest-path sender/Cargo.toml
cargo test --manifest-path sender/Cargo.toml --test kafka_integration -- --list
cargo test --manifest-path sender/Cargo.toml --test kafka_integration
```

普通测试无需 Kafka。集成测试带有 `#[ignore]`，默认显示 1 个 ignored，不连接也不写入真实集群。仅在明确允许向指定 topic 写入测试事件后手动运行：

```bash
KAFKA_BROKERS=192.168.250.106:9092 \
KAFKA_TOPIC=flink_demo \
cargo test --manifest-path sender/Cargo.toml \
  --test kafka_integration -- --ignored --nocapture
```

`KAFKA_BROKERS` 和 `KAFKA_TOPIC` 可覆盖默认地址与 topic。测试使用独立 consumer group 与唯一设备/事件 ID，写入一个四条消息的启动周期；整体超时为 45 秒。
