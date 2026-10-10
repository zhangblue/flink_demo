use crate::model::{
    AppPayload, DevinfoPayload, EnvironmentPayload, EventPayload, InstalledApp, MobileEvent,
    ProtocolType, StartPayload,
};
use rand::Rng;
use std::ops::RangeInclusive;
use uuid::Uuid;

pub const MAX_INSTALLED_APPS: usize = 4_000;
const UUID_COUNTER_LIMIT: u64 = 1 << 60;

#[derive(Clone, Copy)]
struct DeviceProfile {
    manufacturer: &'static str,
    brand: &'static str,
    model: &'static str,
    device_name: &'static str,
    os: &'static str,
    os_version: &'static str,
    cpu_arch: &'static str,
    cpu_cores: u32,
    memory_total_mb: u64,
    storage_total_mb: u64,
    screen_width: u32,
    screen_height: u32,
    screen_dpi: u32,
}

const PROFILES: [DeviceProfile; 4] = [
    DeviceProfile {
        manufacturer: "Xiaomi",
        brand: "Xiaomi",
        model: "23127PN0CC",
        device_name: "Xiaomi 14",
        os: "Android",
        os_version: "15",
        cpu_arch: "arm64-v8a",
        cpu_cores: 8,
        memory_total_mb: 12_288,
        storage_total_mb: 262_144,
        screen_width: 1200,
        screen_height: 2670,
        screen_dpi: 460,
    },
    DeviceProfile {
        manufacturer: "Samsung",
        brand: "Samsung",
        model: "SM-S928B",
        device_name: "Galaxy S24 Ultra",
        os: "Android",
        os_version: "14",
        cpu_arch: "arm64-v8a",
        cpu_cores: 8,
        memory_total_mb: 12_288,
        storage_total_mb: 524_288,
        screen_width: 1440,
        screen_height: 3120,
        screen_dpi: 505,
    },
    DeviceProfile {
        manufacturer: "Apple",
        brand: "Apple",
        model: "iPhone16,2",
        device_name: "iPhone 15 Pro Max",
        os: "iOS",
        os_version: "18.1",
        cpu_arch: "arm64",
        cpu_cores: 6,
        memory_total_mb: 8_192,
        storage_total_mb: 262_144,
        screen_width: 1290,
        screen_height: 2796,
        screen_dpi: 460,
    },
    DeviceProfile {
        manufacturer: "Apple",
        brand: "Apple",
        model: "iPhone17,3",
        device_name: "iPhone 16",
        os: "iOS",
        os_version: "18.1",
        cpu_arch: "arm64",
        cpu_cores: 6,
        memory_total_mb: 8_192,
        storage_total_mb: 131_072,
        screen_width: 1179,
        screen_height: 2556,
        screen_dpi: 460,
    },
];

const ANDROID_APPS: [(&str, &str); 5] = [
    ("com.tencent.mm", "WeChat"),
    ("com.taobao.taobao", "Taobao"),
    ("com.ss.android.ugc.aweme", "Douyin"),
    ("com.android.chrome", "Chrome"),
    ("com.google.android.youtube", "YouTube"),
];
const IOS_APPS: [(&str, &str); 5] = [
    ("com.tencent.xin", "WeChat"),
    ("com.taobao.taobao4iphone", "Taobao"),
    ("com.ss.iphone.ugc.Aweme", "Douyin"),
    ("com.apple.mobilesafari", "Safari"),
    ("com.google.ios.youtube", "YouTube"),
];

pub struct SimulatedDevice<R = rand::rngs::StdRng> {
    udid: Uuid,
    profile: DeviceProfile,
    installed_apps: Vec<InstalledApp>,
    apps_initialized: bool,
    last_cycle_end: Option<i64>,
    uuid_prefix: [u8; 8],
    next_uuid_counter: u64,
    rng: R,
}

impl<R: Rng> SimulatedDevice<R> {
    pub fn new(mut rng: R, app_count: RangeInclusive<usize>) -> Self {
        assert!(!app_count.is_empty(), "app_count must be nonempty");
        assert!(
            *app_count.end() <= MAX_INSTALLED_APPS,
            "app_count exceeds limit"
        );
        let udid = random_uuid(&mut rng);
        let mut uuid_prefix = [0_u8; 8];
        rng.fill_bytes(&mut uuid_prefix);
        let profile = PROFILES[rng.random_range(0..PROFILES.len())];
        let count = rng.random_range(app_count);
        let templates = if profile.os == "iOS" {
            &IOS_APPS
        } else {
            &ANDROID_APPS
        };
        let installed_apps = (0..count)
            .map(|index| {
                let (package_name, app_name) = if index < templates.len() {
                    (templates[index].0.to_owned(), templates[index].1.to_owned())
                } else {
                    let platform = if profile.os == "iOS" {
                        "ios"
                    } else {
                        "android"
                    };
                    (
                        format!("com.example.{platform}.app{index}"),
                        format!("Sample App {index}"),
                    )
                };
                InstalledApp {
                    package_name,
                    app_name,
                    version_name: format!(
                        "{}.{}.{}",
                        rng.random_range(1..=5),
                        rng.random_range(0..=9),
                        rng.random_range(0..=9)
                    ),
                    version_code: rng.random_range(100..=50_000),
                    is_system_app: index == 3,
                    first_installed_at: 0,
                    last_updated_at: 0,
                }
            })
            .collect();
        Self {
            udid,
            profile,
            installed_apps,
            apps_initialized: false,
            last_cycle_end: None,
            uuid_prefix,
            next_uuid_counter: 0,
            rng,
        }
    }

    pub fn udid(&self) -> Uuid {
        self.udid
    }

    pub fn with_namespace(rng: R, app_count: RangeInclusive<usize>, namespace: u64) -> Self {
        assert!(namespace < (1_u64 << 62), "device namespace exhausted");
        let mut device = Self::new(rng, app_count);
        let mut suffix = namespace.to_be_bytes();
        suffix[0] = (suffix[0] & 0x3f) | 0x80;
        device.uuid_prefix = suffix;
        let mut udid = *device.udid.as_bytes();
        udid[8..].copy_from_slice(&suffix);
        device.udid = Uuid::from_bytes(udid);
        device
    }

    pub fn next_cycle(&mut self, base_timestamp: i64) -> Vec<MobileEvent> {
        let max_base = i64::MAX - 3;
        let base_timestamp = base_timestamp.min(max_base);
        let base_timestamp = match self.last_cycle_end {
            Some(last_end) => {
                let next_start = last_end.checked_add(1).expect("timestamp range exhausted");
                assert!(next_start <= max_base, "timestamp range exhausted");
                base_timestamp.max(next_start)
            }
            None => base_timestamp,
        };
        let start_id = self.next_unique_uuid();
        let battery_level = self.rng.random_range(0..=100);
        let network_type = ["wifi", "4g", "5g"][self.rng.random_range(0..3)].to_owned();
        let profile = self.profile;
        let start = EventPayload::Start(StartPayload {
            app_package: "com.example.demo".into(),
            app_version: "3.8.1".into(),
            app_build: 30_801,
            launch_type: "cold".into(),
            foreground: true,
            battery_level,
            network_type: network_type.clone(),
        });
        let devinfo = EventPayload::Devinfo(DevinfoPayload {
            manufacturer: profile.manufacturer.into(),
            brand: profile.brand.into(),
            model: profile.model.into(),
            device_name: profile.device_name.into(),
            os: profile.os.into(),
            os_version: profile.os_version.into(),
            cpu_arch: profile.cpu_arch.into(),
            cpu_cores: profile.cpu_cores,
            memory_total_mb: profile.memory_total_mb,
            storage_total_mb: profile.storage_total_mb,
            screen_width: profile.screen_width,
            screen_height: profile.screen_height,
            screen_dpi: profile.screen_dpi,
        });
        let environment = EventPayload::Environment(EnvironmentPayload {
            network_type,
            carrier: ["China Mobile", "China Unicom", "China Telecom"][self.rng.random_range(0..3)]
                .into(),
            ip_address: format!(
                "10.{}.{}.{}",
                self.rng.random_range(0..=255),
                self.rng.random_range(0..=255),
                self.rng.random_range(1..=254)
            ),
            locale: "zh-CN".into(),
            timezone: "Asia/Shanghai".into(),
            battery_level,
            charging: self.rng.random_bool(0.35),
            memory_available_mb: self.rng.random_range(0..=profile.memory_total_mb),
            storage_available_mb: self.rng.random_range(0..=profile.storage_total_mb),
            is_emulator: false,
            is_rooted: false,
            is_vpn: self.rng.random_bool(0.1),
        });
        let app_first = self.rng.random_bool(0.5);
        let collected_at = base_timestamp.saturating_add(if app_first { 2 } else { 3 });
        if !self.apps_initialized {
            for installed_app in &mut self.installed_apps {
                let installed_ago = self.rng.random_range(30..=730) * 86_400_000_i64;
                let updated_ago = self.rng.random_range(0..=29) * 86_400_000_i64;
                installed_app.first_installed_at = collected_at.saturating_sub(installed_ago);
                installed_app.last_updated_at = collected_at.saturating_sub(updated_ago);
            }
            self.apps_initialized = true;
        }
        let apps = self.installed_apps.clone();
        let app = EventPayload::App(AppPayload { collected_at, apps });
        let mut later = [
            (ProtocolType::Environment, environment),
            (ProtocolType::App, app),
        ];
        if app_first {
            later.swap(0, 1);
        }
        let [third, fourth] = later;
        let payloads = [
            (ProtocolType::Start, start),
            (ProtocolType::Devinfo, devinfo),
            third,
            fourth,
        ];
        let events = payloads
            .into_iter()
            .enumerate()
            .map(|(index, (protol_type, payload))| MobileEvent {
                event_id: self.next_unique_uuid(),
                protol_type,
                udid: self.udid,
                start_id,
                event_timestamp: base_timestamp + index as i64,
                sequence: index as u32 + 1,
                payload,
            })
            .collect();
        self.last_cycle_end = Some(base_timestamp + 3);
        events
    }

    fn next_unique_uuid(&mut self) -> Uuid {
        // The counter occupies bytes 0..6, the low nibble of byte 6, and byte 7.
        // UUID version and variant bits never overwrite any counter bit.
        for _ in 0..2 {
            assert!(
                self.next_uuid_counter < UUID_COUNTER_LIMIT,
                "UUID counter exhausted"
            );
            let counter = self.next_uuid_counter;
            self.next_uuid_counter += 1;
            let mut bytes = [0_u8; 16];
            bytes[..6].copy_from_slice(&(counter >> 12).to_be_bytes()[2..]);
            bytes[6] = 0x40 | ((counter >> 8) as u8 & 0x0f);
            bytes[7] = counter as u8;
            bytes[8..].copy_from_slice(&self.uuid_prefix);
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            let candidate = Uuid::from_bytes(bytes);
            if candidate != self.udid {
                return candidate;
            }
        }
        unreachable!("only one counter value can equal the device UDID");
    }
}

fn random_uuid<R: Rng>(rng: &mut R) -> Uuid {
    let mut bytes = [0_u8; 16];
    rng.fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EventPayload, ProtocolType};
    use crate::sink::MAX_KAFKA_EVENT_BYTES;
    use rand::{SeedableRng, rngs::StdRng};
    use std::collections::HashSet;

    #[derive(Clone, Copy)]
    struct ConstantRng;

    impl rand::RngCore for ConstantRng {
        fn next_u32(&mut self) -> u32 {
            0
        }
        fn next_u64(&mut self) -> u64 {
            0
        }
        fn fill_bytes(&mut self, dest: &mut [u8]) {
            dest.fill(0);
        }
    }

    fn fixture_device(seed: u64) -> SimulatedDevice<StdRng> {
        SimulatedDevice::new(StdRng::seed_from_u64(seed), 3..=7)
    }

    #[test]
    fn largest_allowed_app_event_fits_kafka_payload_budget() {
        // Catches an app-count limit that permits a JSON value too large for Kafka.
        let mut largest = 0;
        for seed in [0, 1, 2, 3, 42, u64::MAX] {
            for base in [i64::MIN, 1_791_614_142_381, i64::MAX] {
                let mut device = SimulatedDevice::new(
                    StdRng::seed_from_u64(seed),
                    MAX_INSTALLED_APPS..=MAX_INSTALLED_APPS,
                );
                let event = device
                    .next_cycle(base)
                    .into_iter()
                    .find(|event| event.protol_type == ProtocolType::App)
                    .unwrap();
                let bytes = serde_json::to_vec(&event).unwrap().len();
                largest = largest.max(bytes);
                assert!(
                    bytes <= MAX_KAFKA_EVENT_BYTES,
                    "seed {seed}, base {base}: {bytes} JSON bytes"
                );
            }
        }
        // Make every variable-width field as long as this generator permits.
        // The first five package names take the longest template on either OS;
        // generated app names already use the longer Android package prefix.
        let mut worst = SimulatedDevice::new(ConstantRng, MAX_INSTALLED_APPS..=MAX_INSTALLED_APPS);
        for (index, app) in worst.installed_apps.iter_mut().enumerate() {
            if index < IOS_APPS.len() && IOS_APPS[index].0.len() > app.package_name.len() {
                app.package_name = IOS_APPS[index].0.to_owned();
            }
            app.version_name = "5.9.9".to_owned();
            app.version_code = 50_000;
            app.is_system_app = false;
            app.first_installed_at = i64::MIN;
            app.last_updated_at = i64::MIN;
        }
        worst.apps_initialized = true;
        let event = worst
            .next_cycle(i64::MIN)
            .into_iter()
            .find(|event| event.protol_type == ProtocolType::App)
            .unwrap();
        let worst_bytes = serde_json::to_vec(&event).unwrap().len();
        assert!(
            worst_bytes <= MAX_KAFKA_EVENT_BYTES,
            "worst-case app event: {worst_bytes} JSON bytes"
        );
        eprintln!(
            "largest sampled app event: {largest} JSON bytes; constructed upper bound: {worst_bytes} JSON bytes"
        );
    }

    #[test]
    fn one_cycle_starts_with_start_then_devinfo() {
        let mut device = fixture_device(7);
        let cycle = device.next_cycle(1_791_614_142_381);
        assert_eq!(cycle.len(), 4);
        assert_eq!(cycle[0].protol_type, ProtocolType::Start);
        assert_eq!(cycle[1].protol_type, ProtocolType::Devinfo);
        assert_eq!(
            cycle.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert!(
            cycle[2..]
                .iter()
                .any(|e| e.protol_type == ProtocolType::Environment)
        );
        assert!(
            cycle[2..]
                .iter()
                .any(|e| e.protol_type == ProtocolType::App)
        );
        for event in &cycle {
            assert!(matches!(
                (&event.protol_type, &event.payload),
                (ProtocolType::Start, EventPayload::Start(_))
                    | (ProtocolType::Devinfo, EventPayload::Devinfo(_))
                    | (ProtocolType::Environment, EventPayload::Environment(_))
                    | (ProtocolType::App, EventPayload::App(_))
            ));
        }
    }

    #[test]
    fn both_later_event_orders_occur_with_fixed_seeds() {
        let orders = (0..64)
            .map(|seed| fixture_device(seed).next_cycle(1000)[2].protol_type)
            .collect::<Vec<_>>();
        assert!(orders.contains(&ProtocolType::Environment));
        assert!(orders.contains(&ProtocolType::App));
    }

    #[test]
    fn high_base_is_clamped_to_four_distinct_timestamps() {
        let cycle = fixture_device(21).next_cycle(i64::MAX);
        assert_eq!(
            cycle.iter().map(|e| e.event_timestamp).collect::<Vec<_>>(),
            vec![i64::MAX - 3, i64::MAX - 2, i64::MAX - 1, i64::MAX]
        );
    }

    #[test]
    fn backward_base_is_promoted_past_previous_cycle() {
        let mut device = fixture_device(22);
        let first = device.next_cycle(1000);
        let second = device.next_cycle(1);
        assert_eq!(first[3].event_timestamp, 1003);
        assert_eq!(
            second.iter().map(|e| e.event_timestamp).collect::<Vec<_>>(),
            vec![1004, 1005, 1006, 1007]
        );
        let app = second
            .iter()
            .find_map(|e| match &e.payload {
                EventPayload::App(value) => Some(value),
                _ => None,
            })
            .unwrap();
        assert!(
            app.apps
                .iter()
                .all(|item| item.last_updated_at <= app.collected_at)
        );
    }

    #[test]
    fn timestamp_exhaustion_is_explicit() {
        let mut device = fixture_device(23);
        device.next_cycle(i64::MAX);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| device.next_cycle(0)));
        assert!(result.is_err());
    }

    #[test]
    fn repeated_random_bytes_still_produce_unique_ids() {
        let mut device = SimulatedDevice::new(ConstantRng, 1..=1);
        let events = (0..10)
            .flat_map(|n| device.next_cycle(n * 10))
            .collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|e| e.event_id)
                .collect::<HashSet<_>>()
                .len(),
            40
        );
        assert_eq!(
            events
                .iter()
                .map(|e| e.start_id)
                .collect::<HashSet<_>>()
                .len(),
            10
        );
    }

    #[test]
    fn uuid_generation_state_has_fixed_small_footprint() {
        let bytes = std::mem::size_of::<SimulatedDevice<ConstantRng>>();
        assert!(bytes <= 240, "device state uses {bytes} bytes");
    }

    #[test]
    fn counter_bits_survive_uuid_version_and_variant_fields() {
        let mut ids = HashSet::new();
        for counter in [
            1,
            2,
            255,
            256,
            4095,
            4096,
            (1 << 48) - 1,
            1 << 48,
            UUID_COUNTER_LIMIT - 1,
        ] {
            let mut device = SimulatedDevice::new(ConstantRng, 1..=1);
            device.next_uuid_counter = counter;
            let id = device.next_unique_uuid();
            assert_eq!(id.as_bytes()[6] & 0xf0, 0x40);
            assert_eq!(id.as_bytes()[8] & 0xc0, 0x80);
            assert!(ids.insert(id), "counter {counter} reused a UUID");
        }
    }

    #[test]
    fn uuid_counter_exhaustion_is_explicit() {
        let mut device = SimulatedDevice::new(ConstantRng, 1..=1);
        device.next_uuid_counter = UUID_COUNTER_LIMIT - 1;
        device.next_unique_uuid();
        let exhausted =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| device.next_unique_uuid()));
        assert!(exhausted.is_err());
    }

    #[test]
    fn platform_templates_match_explicit_id_allowlists() {
        let mut seen_android = false;
        let mut seen_ios = false;
        for seed in 0..64 {
            let mut device = SimulatedDevice::new(StdRng::seed_from_u64(seed), 7..=7);
            let cycle = device.next_cycle(1000);
            let EventPayload::Devinfo(info) = &cycle[1].payload else {
                panic!("devinfo")
            };
            let apps = cycle
                .iter()
                .find_map(|e| match &e.payload {
                    EventPayload::App(value) => Some(&value.apps),
                    _ => None,
                })
                .unwrap();
            let actual = apps
                .iter()
                .map(|app| app.package_name.as_str())
                .collect::<Vec<_>>();
            let expected = match info.os.as_str() {
                "Android" => {
                    seen_android = true;
                    vec![
                        "com.tencent.mm",
                        "com.taobao.taobao",
                        "com.ss.android.ugc.aweme",
                        "com.android.chrome",
                        "com.google.android.youtube",
                        "com.example.android.app5",
                        "com.example.android.app6",
                    ]
                }
                "iOS" => {
                    seen_ios = true;
                    vec![
                        "com.tencent.xin",
                        "com.taobao.taobao4iphone",
                        "com.ss.iphone.ugc.Aweme",
                        "com.apple.mobilesafari",
                        "com.google.ios.youtube",
                        "com.example.ios.app5",
                        "com.example.ios.app6",
                    ]
                }
                other => panic!("unexpected OS {other}"),
            };
            assert_eq!(actual, expected);
        }
        assert!(seen_android && seen_ios);
    }

    #[test]
    fn epoch_zero_install_time_is_not_reinitialized() {
        let mut device = SimulatedDevice::new(ConstantRng, 1..=1);
        let thirty_days_ms = 30 * 86_400_000_i64;
        let first = device.next_cycle(thirty_days_ms - 2);
        let second = device.next_cycle(thirty_days_ms + 100);
        let installed_at = |cycle: &[MobileEvent]| {
            cycle
                .iter()
                .find_map(|e| match &e.payload {
                    EventPayload::App(value) => Some(value.apps[0].first_installed_at),
                    _ => None,
                })
                .unwrap()
        };
        assert_eq!(installed_at(&first), 0);
        assert_eq!(installed_at(&second), 0);
    }

    #[test]
    #[should_panic(expected = "app_count exceeds limit")]
    fn constructor_rejects_oversized_app_range() {
        SimulatedDevice::new(StdRng::seed_from_u64(24), 1..=MAX_INSTALLED_APPS + 1);
    }

    #[test]
    fn app_count_range_endpoints_are_reachable() {
        let counts = (0..128)
            .map(|seed| {
                let mut device = SimulatedDevice::new(StdRng::seed_from_u64(seed), 2..=3);
                device
                    .next_cycle(1000)
                    .iter()
                    .find_map(|e| match &e.payload {
                        EventPayload::App(value) => Some(value.apps.len()),
                        _ => None,
                    })
                    .unwrap()
            })
            .collect::<HashSet<_>>();
        assert_eq!(counts, HashSet::from([2, 3]));
    }

    #[test]
    fn udid_is_stable_and_start_id_changes_each_cycle() {
        let mut device = fixture_device(9);
        let first = device.next_cycle(1000);
        let second = device.next_cycle(2000);
        assert!(first.iter().all(|e| e.udid == device.udid()));
        assert!(second.iter().all(|e| e.udid == device.udid()));
        assert_ne!(first[0].start_id, second[0].start_id);
        assert!(first.iter().all(|e| e.start_id == first[0].start_id));
        assert!(second.iter().all(|e| e.start_id == second[0].start_id));
        assert_eq!(
            first
                .iter()
                .chain(second.iter())
                .map(|e| e.event_id)
                .collect::<HashSet<_>>()
                .len(),
            8
        );
    }

    #[test]
    fn timestamps_increase_and_cycle_values_agree() {
        let mut device = fixture_device(11);
        let cycle = device.next_cycle(1_791_614_142_381);
        assert_eq!(cycle[0].event_timestamp, 1_791_614_142_381);
        assert!(
            cycle
                .windows(2)
                .all(|pair| pair[1].event_timestamp > pair[0].event_timestamp)
        );
        let EventPayload::Start(start) = &cycle[0].payload else {
            panic!("start payload")
        };
        let environment = cycle
            .iter()
            .find_map(|e| match &e.payload {
                EventPayload::Environment(value) => Some(value),
                _ => None,
            })
            .unwrap();
        assert_eq!(start.battery_level, environment.battery_level);
        assert_eq!(start.network_type, environment.network_type);
    }

    #[test]
    fn resource_values_and_app_times_are_bounded() {
        let mut device = SimulatedDevice::new(StdRng::seed_from_u64(13), 12..=12);
        let cycle = device.next_cycle(1_791_614_142_381);
        let EventPayload::Devinfo(devinfo) = &cycle[1].payload else {
            panic!("devinfo payload")
        };
        let environment = cycle
            .iter()
            .find_map(|e| match &e.payload {
                EventPayload::Environment(value) => Some(value),
                _ => None,
            })
            .unwrap();
        let app = cycle
            .iter()
            .find_map(|e| match &e.payload {
                EventPayload::App(value) => Some(value),
                _ => None,
            })
            .unwrap();
        assert!(environment.battery_level <= 100);
        assert!(environment.memory_available_mb <= devinfo.memory_total_mb);
        assert!(environment.storage_available_mb <= devinfo.storage_total_mb);
        assert_eq!(app.apps.len(), 12);
        assert!(
            app.apps
                .iter()
                .all(|a| a.first_installed_at <= a.last_updated_at)
        );
        assert!(
            app.apps
                .iter()
                .all(|a| a.last_updated_at <= app.collected_at)
        );
    }

    #[test]
    fn installed_app_history_stays_with_device() {
        let mut device = fixture_device(17);
        let first = device.next_cycle(1_791_614_142_381);
        let second = device.next_cycle(1_791_614_242_381);
        let first_event = first
            .iter()
            .find(|e| e.protol_type == ProtocolType::App)
            .unwrap();
        let second_event = second
            .iter()
            .find(|e| e.protol_type == ProtocolType::App)
            .unwrap();
        let EventPayload::App(first_apps) = &first_event.payload else {
            panic!("app payload")
        };
        let EventPayload::App(second_apps) = &second_event.payload else {
            panic!("app payload")
        };
        assert_eq!(first_apps.collected_at, first_event.event_timestamp);
        assert_eq!(second_apps.collected_at, second_event.event_timestamp);
        assert_eq!(first_apps.apps.len(), second_apps.apps.len());
        for (before, after) in first_apps.apps.iter().zip(&second_apps.apps) {
            assert_eq!(before.package_name, after.package_name);
            assert_eq!(before.first_installed_at, after.first_installed_at);
            assert_eq!(before.last_updated_at, after.last_updated_at);
        }
    }

    #[test]
    fn platform_profile_and_app_ids_agree() {
        for seed in 0..64 {
            let mut device = fixture_device(seed);
            let cycle = device.next_cycle(1_791_614_142_381);
            let EventPayload::Devinfo(info) = &cycle[1].payload else {
                panic!("devinfo payload")
            };
            let app = cycle
                .iter()
                .find_map(|e| match &e.payload {
                    EventPayload::App(value) => Some(value),
                    _ => None,
                })
                .unwrap();
            match info.os.as_str() {
                "Android" => {
                    assert_ne!(info.manufacturer, "Apple");
                    assert_eq!(info.cpu_arch, "arm64-v8a");
                    assert!(
                        app.apps
                            .iter()
                            .all(|a| !a.package_name.starts_with("com.apple."))
                    );
                    assert!(app.apps.iter().all(|a| !a.package_name.contains(".ios.")));
                }
                "iOS" => {
                    assert_eq!(info.manufacturer, "Apple");
                    assert_eq!(info.brand, "Apple");
                    assert_eq!(info.cpu_arch, "arm64");
                    assert!(
                        app.apps
                            .iter()
                            .all(|a| !a.package_name.starts_with("com.android."))
                    );
                    assert!(
                        app.apps
                            .iter()
                            .all(|a| !a.package_name.contains(".android."))
                    );
                }
                other => panic!("unexpected OS {other}"),
            }
        }
    }
}
