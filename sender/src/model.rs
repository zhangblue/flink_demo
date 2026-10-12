use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct MobileEvent {
    pub event_id: uuid::Uuid,
    pub protol_type: ProtocolType,
    pub udid: uuid::Uuid,
    pub start_id: i64,
    pub event_timestamp: i64,
    pub sequence: u32,
    pub payload: EventPayload,
}

impl MobileEvent {
    pub fn kafka_key(&self) -> String {
        self.udid.to_string()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolType {
    Start,
    Devinfo,
    Environment,
    App,
    Common,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum EventPayload {
    Start(StartPayload),
    Devinfo(DevinfoPayload),
    Environment(EnvironmentPayload),
    App(AppPayload),
    Common(CommonPayload),
}

#[derive(Clone, Debug, Serialize)]
pub struct CommonPayload {
    pub self_app_package: String,
    pub self_app_name: String,
    pub model: String,
    pub brand: String,
    pub os: String,
    pub os_version: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StartPayload {
    pub app_package: String,
    pub app_version: String,
    pub app_build: u32,
    pub launch_type: String,
    pub foreground: bool,
    pub battery_level: u32,
    pub network_type: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DevinfoPayload {
    pub manufacturer: String,
    pub brand: String,
    pub model: String,
    pub device_name: String,
    pub os: String,
    pub os_version: String,
    pub cpu_arch: String,
    pub cpu_cores: u32,
    pub memory_total_mb: u64,
    pub storage_total_mb: u64,
    pub screen_width: u32,
    pub screen_height: u32,
    pub screen_dpi: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct EnvironmentPayload {
    pub network_type: String,
    pub carrier: String,
    pub ip_address: String,
    pub locale: String,
    pub timezone: String,
    pub battery_level: u32,
    pub charging: bool,
    pub memory_available_mb: u64,
    pub storage_available_mb: u64,
    pub is_emulator: bool,
    pub is_rooted: bool,
    pub is_vpn: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct AppPayload {
    pub self_package_name: String,
    pub collected_at: i64,
    pub apps: Vec<InstalledApp>,
}

#[derive(Clone, Debug, Serialize)]
pub struct InstalledApp {
    pub package_name: String,
    pub app_name: String,
    pub version_name: String,
    pub version_code: u32,
    pub is_system_app: bool,
    pub first_installed_at: i64,
    pub last_updated_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn fixture_event(
        protol_type: ProtocolType,
        sequence: u32,
        payload: EventPayload,
    ) -> MobileEvent {
        MobileEvent {
            event_id: uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            protol_type,
            udid: uuid::Uuid::parse_str("ad3eb36b-2145-4e70-a8cd-adc193ca865f").unwrap(),
            start_id: 1_791_614_142_000,
            event_timestamp: 1_791_614_142_381,
            sequence,
            payload,
        }
    }

    fn assert_envelope(value: &Value, kind: &str, sequence: u32) {
        let mut keys = value
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            [
                "event_id",
                "event_timestamp",
                "payload",
                "protol_type",
                "sequence",
                "start_id",
                "udid"
            ]
        );
        assert!(value["event_id"].is_string());
        assert_eq!(value["event_id"], "550e8400-e29b-41d4-a716-446655440000");
        assert!(value["udid"].is_string());
        assert_eq!(value["udid"], "ad3eb36b-2145-4e70-a8cd-adc193ca865f");
        assert!(value["start_id"].is_i64());
        assert_eq!(value["start_id"], 1_791_614_142_000_i64);
        assert_eq!(value["protol_type"], kind);
        assert_eq!(value["event_timestamp"], 1_791_614_142_381_i64);
        assert!(value.get("event_time").is_none());
        assert_eq!(value["sequence"], sequence);
    }

    #[test]
    fn start_event_serializes_with_stable_contract() {
        let event = fixture_event(
            ProtocolType::Start,
            1,
            EventPayload::Start(StartPayload {
                app_package: "com.example.demo".into(),
                app_version: "3.8.1".into(),
                app_build: 30_801,
                launch_type: "cold".into(),
                foreground: true,
                battery_level: 76,
                network_type: "wifi".into(),
            }),
        );
        let value = serde_json::to_value(event).unwrap();
        assert_envelope(&value, "start", 1);
        assert_eq!(
            value["payload"],
            json!({"app_package":"com.example.demo","app_version":"3.8.1","app_build":30801,"launch_type":"cold","foreground":true,"battery_level":76,"network_type":"wifi"})
        );
        assert!(value["payload"]["foreground"].is_boolean());
    }

    #[test]
    fn devinfo_event_serializes_with_stable_contract() {
        let event = fixture_event(
            ProtocolType::Devinfo,
            2,
            EventPayload::Devinfo(DevinfoPayload {
                manufacturer: "Xiaomi".into(),
                brand: "Xiaomi".into(),
                model: "23127PN0CC".into(),
                device_name: "Xiaomi 14".into(),
                os: "Android".into(),
                os_version: "15".into(),
                cpu_arch: "arm64-v8a".into(),
                cpu_cores: 8,
                memory_total_mb: 12_288,
                storage_total_mb: 262_144,
                screen_width: 1200,
                screen_height: 2670,
                screen_dpi: 460,
            }),
        );
        let value = serde_json::to_value(event).unwrap();
        assert_envelope(&value, "devinfo", 2);
        assert_eq!(
            value["payload"],
            json!({"manufacturer":"Xiaomi","brand":"Xiaomi","model":"23127PN0CC","device_name":"Xiaomi 14","os":"Android","os_version":"15","cpu_arch":"arm64-v8a","cpu_cores":8,"memory_total_mb":12288,"storage_total_mb":262144,"screen_width":1200,"screen_height":2670,"screen_dpi":460})
        );
    }

    #[test]
    fn environment_event_serializes_with_stable_contract() {
        let event = fixture_event(
            ProtocolType::Environment,
            3,
            EventPayload::Environment(EnvironmentPayload {
                network_type: "5g".into(),
                carrier: "China Mobile".into(),
                ip_address: "10.18.26.37".into(),
                locale: "zh-CN".into(),
                timezone: "Asia/Shanghai".into(),
                battery_level: 74,
                charging: false,
                memory_available_mb: 6381,
                storage_available_mb: 176420,
                is_emulator: false,
                is_rooted: false,
                is_vpn: false,
            }),
        );
        let value = serde_json::to_value(event).unwrap();
        assert_envelope(&value, "environment", 3);
        assert_eq!(
            value["payload"],
            json!({"network_type":"5g","carrier":"China Mobile","ip_address":"10.18.26.37","locale":"zh-CN","timezone":"Asia/Shanghai","battery_level":74,"charging":false,"memory_available_mb":6381,"storage_available_mb":176420,"is_emulator":false,"is_rooted":false,"is_vpn":false})
        );
    }

    #[test]
    fn app_event_serializes_with_stable_contract() {
        let event = fixture_event(
            ProtocolType::App,
            4,
            EventPayload::App(AppPayload {
                self_package_name: "com.tencent.mm".into(),
                collected_at: 1_791_614_142_417,
                apps: vec![InstalledApp {
                    package_name: "com.tencent.mm".into(),
                    app_name: "WeChat".into(),
                    version_name: "8.0.62".into(),
                    version_code: 2800,
                    is_system_app: false,
                    first_installed_at: 1_735_689_600_000,
                    last_updated_at: 1_789_000_000_000,
                }],
            }),
        );
        let value = serde_json::to_value(event).unwrap();
        assert_envelope(&value, "app", 4);
        assert_eq!(
            value["payload"],
            json!({
                "self_package_name": "com.tencent.mm",
                "collected_at": 1791614142417_i64,
                "apps": [{
                    "package_name": "com.tencent.mm",
                    "app_name": "WeChat",
                    "version_name": "8.0.62",
                    "version_code": 2800,
                    "is_system_app": false,
                    "first_installed_at": 1735689600000_i64,
                    "last_updated_at": 1789000000000_i64
                }]
            })
        );
    }

    #[test]
    fn common_event_serializes_with_stable_contract() {
        let event = fixture_event(
            ProtocolType::Common,
            1,
            EventPayload::Common(CommonPayload {
                self_app_package: "com.eg.android.AlipayGphone".into(),
                self_app_name: "支付宝".into(),
                model: "23127PN0CC".into(),
                brand: "Xiaomi".into(),
                os: "Android".into(),
                os_version: "15".into(),
            }),
        );

        let value = serde_json::to_value(event).unwrap();
        assert_envelope(&value, "common", 1);
        assert_eq!(
            value["payload"],
            json!({
                "self_app_package": "com.eg.android.AlipayGphone",
                "self_app_name": "支付宝",
                "model": "23127PN0CC",
                "brand": "Xiaomi",
                "os": "Android",
                "os_version": "15"
            })
        );
    }

    #[test]
    fn kafka_key_returns_udid_string() {
        let event = fixture_event(
            ProtocolType::Start,
            1,
            EventPayload::Start(StartPayload {
                app_package: "com.example.demo".into(),
                app_version: "3.8.1".into(),
                app_build: 30_801,
                launch_type: "cold".into(),
                foreground: true,
                battery_level: 76,
                network_type: "wifi".into(),
            }),
        );

        assert_eq!(event.kafka_key(), "ad3eb36b-2145-4e70-a8cd-adc193ca865f");
    }
}
