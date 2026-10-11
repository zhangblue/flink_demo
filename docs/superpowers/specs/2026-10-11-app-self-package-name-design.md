# App 消息 self_package_name 设计

## 目标

在 `protol_type = "app"` 的消息 payload 中增加必填字符串字段
`self_package_name`，用于表示本次启动所模拟的当前 App 包名。

## 数据契约

`app` 消息 payload 调整为：

```json
{
  "self_package_name": "com.tencent.mm",
  "collected_at": 1791721000865,
  "apps": [
    {
      "package_name": "com.tencent.mm",
      "app_name": "WeChat",
      "version_name": "8.0.62",
      "version_code": 2800,
      "is_system_app": false,
      "first_installed_at": 1735689600000,
      "last_updated_at": 1791000000000
    }
  ]
}
```

必须满足以下约束：

- `self_package_name` 必须等于同一条消息中某个
  `apps[].package_name`，不能生成列表之外的包名。
- `self_package_name` 与同一次启动的
  `start.payload.app_package` 完全相同。
- 每次启动只随机选择一次，选择范围是该设备完整安装列表中的全部 App，
  包括系统 App；每个列表元素采用等概率选择。
- 同一个设备在不同启动周期重新随机选择，因此包名可以相同，也可以不同。
- 设备安装列表非空。现有配置校验继续保证
  `installed_apps_min >= 1`，因此随机选择不存在空列表分支。

## 生成流程

`SimulatedDevice::next_cycle` 在构造四类消息前，从设备持有的
`installed_apps` 中随机选择一个元素。选中元素的 `package_name` 被复制两次：

1. 写入 `StartPayload.app_package`。
2. 写入 `AppPayload.self_package_name`。

安装列表本身不因选择操作而改变。`start`、`devinfo`、`environment`、`app`
消息的顺序规则、`udid`、`start_id`、`event_timestamp` 和 Kafka key 均不改变。

## 兼容性

这是对 `app` payload 的新增必填字段。Flink 消费端应在 `app` 类型对应的数据结构中
增加 `self_package_name STRING`，并可通过展开 `apps` 后校验包名存在性。其他三种
payload 的结构不变，但 `start.payload.app_package` 将不再固定为
`com.example.demo`，而是来自本次启动选择的已安装 App。

## 测试与文档

自动化测试覆盖：

- `AppPayload` 序列化后包含 `self_package_name`。
- 每个生成周期中，`app.payload.self_package_name` 存在于该消息的
  `apps[].package_name`。
- 每个生成周期中，`start.payload.app_package` 等于
  `app.payload.self_package_name`。
- Android 与 iOS 设备均只会选择各自安装列表中的包名。
- 现有消息顺序、时间戳和安装列表稳定性测试继续通过。

`sender/README.md` 和仓库根目录 `README.md` 中涉及消息结构的说明与示例同步更新；
运行目录中的配置格式不需要调整。
