use std::path::{Path, PathBuf};

use crate::generator::MAX_INSTALLED_APPS;
use crate::rate::{GlobalRateLimiter, RateLimitError};

pub const MAX_DEVICE_COUNT: usize = 1_000_000;
pub const MAX_CHANNEL_CAPACITY: usize = 1_000_000;
pub const MAX_TOTAL_INSTALLED_APPS: usize = 1_000_000;
pub const MAX_QUEUED_APP_ITEMS: usize = 1_000_000;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub simulation: SimulationConfig,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub compression: String,
    pub message_timeout_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct SimulationConfig {
    pub device_count: usize,
    pub messages_per_second: u64,
    pub startup_interval_min_secs: u64,
    pub startup_interval_max_secs: u64,
    pub installed_apps_min: usize,
    pub installed_apps_max: usize,
    pub channel_capacity: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("kafka.brokers must not be empty")]
    EmptyBrokers,
    #[error("kafka.topic must not be empty")]
    EmptyTopic,
    #[error("kafka.message_timeout_ms must be greater than zero")]
    ZeroMessageTimeout,
    #[error("simulation.device_count must be greater than zero")]
    ZeroDeviceCount,
    #[error("simulation.device_count ({count}) must not exceed {limit}")]
    TooManyDevices { count: usize, limit: usize },
    #[error("simulation.messages_per_second must be greater than zero")]
    ZeroMessagesPerSecond,
    #[error("simulation.messages_per_second is too high for a positive nanosecond interval")]
    RateTooHigh,
    #[error(
        "simulation.startup_interval_min_secs and startup_interval_max_secs must be greater than zero"
    )]
    ZeroStartupInterval,
    #[error(
        "simulation.startup_interval_min_secs ({min}) must not exceed startup_interval_max_secs ({max})"
    )]
    InvalidStartupInterval { min: u64, max: u64 },
    #[error("simulation.installed_apps_min and installed_apps_max must be greater than zero")]
    ZeroInstalledApps,
    #[error("simulation.installed_apps_min ({min}) must not exceed installed_apps_max ({max})")]
    InvalidAppCountRange { min: usize, max: usize },
    #[error("simulation.installed_apps_max ({max}) must not exceed {limit}")]
    TooManyInstalledApps { max: usize, limit: usize },
    #[error(
        "simulation.device_count ({device_count}) * installed_apps_max ({apps_per_device}) must not exceed {limit}"
    )]
    TooManyTotalInstalledApps {
        device_count: usize,
        apps_per_device: usize,
        limit: usize,
    },
    #[error("simulation.channel_capacity must be greater than zero")]
    ZeroChannelCapacity,
    #[error("simulation.channel_capacity ({capacity}) must not exceed {limit}")]
    ChannelCapacityTooLarge { capacity: usize, limit: usize },
    #[error(
        "simulation.channel_capacity ({capacity}) * installed_apps_max ({apps_per_event}) must not exceed {limit}"
    )]
    TooManyQueuedAppItems {
        capacity: usize,
        apps_per_event: usize,
        limit: usize,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigLoadError {
    #[error("failed to read config at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse TOML config at {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid config at {path}: {source}")]
    Invalid {
        path: PathBuf,
        #[source]
        source: ConfigError,
    },
}

impl AppConfig {
    pub fn from_path(path: &Path) -> Result<Self, ConfigLoadError> {
        let contents = std::fs::read_to_string(path).map_err(|source| ConfigLoadError::Read {
            path: path.to_owned(),
            source,
        })?;
        let config: Self = toml::from_str(&contents).map_err(|source| ConfigLoadError::Parse {
            path: path.to_owned(),
            source,
        })?;
        config
            .validate()
            .map_err(|source| ConfigLoadError::Invalid {
                path: path.to_owned(),
                source,
            })?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.kafka.brokers.trim().is_empty() {
            return Err(ConfigError::EmptyBrokers);
        }
        if self.kafka.topic.trim().is_empty() {
            return Err(ConfigError::EmptyTopic);
        }
        if self.kafka.message_timeout_ms == 0 {
            return Err(ConfigError::ZeroMessageTimeout);
        }
        if self.simulation.device_count == 0 {
            return Err(ConfigError::ZeroDeviceCount);
        }
        let total_installed_apps = self
            .simulation
            .device_count
            .checked_mul(self.simulation.installed_apps_max)
            .ok_or(ConfigError::TooManyTotalInstalledApps {
                device_count: self.simulation.device_count,
                apps_per_device: self.simulation.installed_apps_max,
                limit: MAX_TOTAL_INSTALLED_APPS,
            })?;
        if self.simulation.device_count > MAX_DEVICE_COUNT {
            return Err(ConfigError::TooManyDevices {
                count: self.simulation.device_count,
                limit: MAX_DEVICE_COUNT,
            });
        }
        if self.simulation.messages_per_second == 0 {
            return Err(ConfigError::ZeroMessagesPerSecond);
        }
        if matches!(
            GlobalRateLimiter::new(self.simulation.messages_per_second),
            Err(RateLimitError::IntervalTooShort)
        ) {
            return Err(ConfigError::RateTooHigh);
        }
        if self.simulation.startup_interval_min_secs == 0
            || self.simulation.startup_interval_max_secs == 0
        {
            return Err(ConfigError::ZeroStartupInterval);
        }
        if self.simulation.startup_interval_min_secs > self.simulation.startup_interval_max_secs {
            return Err(ConfigError::InvalidStartupInterval {
                min: self.simulation.startup_interval_min_secs,
                max: self.simulation.startup_interval_max_secs,
            });
        }
        if self.simulation.installed_apps_min == 0 || self.simulation.installed_apps_max == 0 {
            return Err(ConfigError::ZeroInstalledApps);
        }
        if self.simulation.installed_apps_min > self.simulation.installed_apps_max {
            return Err(ConfigError::InvalidAppCountRange {
                min: self.simulation.installed_apps_min,
                max: self.simulation.installed_apps_max,
            });
        }
        if self.simulation.installed_apps_max > MAX_INSTALLED_APPS {
            return Err(ConfigError::TooManyInstalledApps {
                max: self.simulation.installed_apps_max,
                limit: MAX_INSTALLED_APPS,
            });
        }
        if total_installed_apps > MAX_TOTAL_INSTALLED_APPS {
            return Err(ConfigError::TooManyTotalInstalledApps {
                device_count: self.simulation.device_count,
                apps_per_device: self.simulation.installed_apps_max,
                limit: MAX_TOTAL_INSTALLED_APPS,
            });
        }
        if self.simulation.channel_capacity == 0 {
            return Err(ConfigError::ZeroChannelCapacity);
        }
        let queued_app_items = self
            .simulation
            .channel_capacity
            .checked_mul(self.simulation.installed_apps_max)
            .ok_or(ConfigError::TooManyQueuedAppItems {
                capacity: self.simulation.channel_capacity,
                apps_per_event: self.simulation.installed_apps_max,
                limit: MAX_QUEUED_APP_ITEMS,
            })?;
        if queued_app_items > MAX_QUEUED_APP_ITEMS {
            return Err(ConfigError::TooManyQueuedAppItems {
                capacity: self.simulation.channel_capacity,
                apps_per_event: self.simulation.installed_apps_max,
                limit: MAX_QUEUED_APP_ITEMS,
            });
        }
        let channel_limit = MAX_CHANNEL_CAPACITY.min(tokio::sync::Semaphore::MAX_PERMITS);
        if self.simulation.channel_capacity > channel_limit {
            return Err(ConfigError::ChannelCapacityTooLarge {
                capacity: self.simulation.channel_capacity,
                limit: channel_limit,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> AppConfig {
        toml::from_str(include_str!("../config.toml")).unwrap()
    }

    #[test]
    fn rejects_zero_device_count() {
        let mut config = valid_config();
        config.simulation.device_count = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroDeviceCount)
        ));
    }

    #[test]
    fn rejects_device_count_above_safe_limit() {
        let mut config = valid_config();
        config.simulation.device_count = MAX_DEVICE_COUNT + 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyDevices { .. })
        ));
    }

    #[test]
    fn accepts_total_installed_apps_at_budget() {
        let mut config = valid_config();
        config.simulation.device_count = 250;
        config.simulation.installed_apps_max = MAX_INSTALLED_APPS;
        config.simulation.channel_capacity = 1;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_total_installed_apps_above_budget() {
        let mut config = valid_config();
        config.simulation.device_count = 251;
        config.simulation.installed_apps_max = MAX_INSTALLED_APPS;
        config.simulation.channel_capacity = 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyTotalInstalledApps { .. })
        ));
    }

    #[test]
    fn overflowing_total_installed_apps_fails_validation() {
        let mut config = valid_config();
        config.simulation.device_count = usize::MAX;
        config.simulation.installed_apps_max = MAX_INSTALLED_APPS;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyTotalInstalledApps { .. })
        ));
    }

    #[test]
    fn rejects_channel_capacity_above_safe_limit() {
        let mut config = valid_config();
        config.simulation.channel_capacity = MAX_CHANNEL_CAPACITY + 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyQueuedAppItems { .. })
        ));
    }

    #[test]
    fn accepts_channel_app_copies_at_budget() {
        let mut config = valid_config();
        config.simulation.device_count = 1;
        config.simulation.installed_apps_max = 1_000;
        config.simulation.channel_capacity = 1_000;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_channel_app_copies_above_budget() {
        let mut config = valid_config();
        config.simulation.device_count = 1;
        config.simulation.installed_apps_max = 1_000;
        config.simulation.channel_capacity = 100_000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn overflowing_channel_app_copies_fails_validation() {
        let mut config = valid_config();
        config.simulation.device_count = 1;
        config.simulation.installed_apps_max = 1_000;
        config.simulation.channel_capacity = usize::MAX;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyQueuedAppItems { .. })
        ));
    }

    #[test]
    fn rejects_reversed_startup_interval() {
        let mut config = valid_config();
        config.simulation.startup_interval_min_secs = 121;
        config.simulation.startup_interval_max_secs = 120;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidStartupInterval { .. })
        ));
    }

    #[test]
    fn rejects_reversed_app_count_range() {
        let mut config = valid_config();
        config.simulation.installed_apps_min = 81;
        config.simulation.installed_apps_max = 80;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidAppCountRange { .. })
        ));
    }

    #[test]
    fn parses_complete_toml() {
        let config: AppConfig = toml::from_str(include_str!("../config.toml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.kafka.topic, "flink_demo");
    }

    #[test]
    fn rejects_empty_brokers() {
        let mut config = valid_config();
        config.kafka.brokers.clear();
        assert!(matches!(config.validate(), Err(ConfigError::EmptyBrokers)));
    }

    #[test]
    fn rejects_empty_topic() {
        let mut config = valid_config();
        config.kafka.topic.clear();
        assert!(matches!(config.validate(), Err(ConfigError::EmptyTopic)));
    }

    #[test]
    fn rejects_zero_message_timeout() {
        let mut config = valid_config();
        config.kafka.message_timeout_ms = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroMessageTimeout)
        ));
    }

    #[test]
    fn rejects_zero_message_rate() {
        let mut config = valid_config();
        config.simulation.messages_per_second = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroMessagesPerSecond)
        ));
    }

    #[test]
    fn rejects_rate_without_a_positive_nanosecond_period() {
        let mut config = valid_config();
        config.simulation.messages_per_second = u64::MAX;
        assert!(matches!(config.validate(), Err(ConfigError::RateTooHigh)));
    }

    #[test]
    fn rejects_zero_startup_interval() {
        let mut config = valid_config();
        config.simulation.startup_interval_min_secs = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroStartupInterval)
        ));
    }

    #[test]
    fn rejects_zero_startup_interval_max() {
        let mut config = valid_config();
        config.simulation.startup_interval_max_secs = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroStartupInterval)
        ));
    }

    #[test]
    fn rejects_zero_installed_app_count() {
        let mut config = valid_config();
        config.simulation.installed_apps_min = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroInstalledApps)
        ));
    }

    #[test]
    fn rejects_zero_installed_apps_max() {
        let mut config = valid_config();
        config.simulation.installed_apps_max = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroInstalledApps)
        ));
    }

    #[test]
    fn rejects_zero_channel_capacity() {
        let mut config = valid_config();
        config.simulation.channel_capacity = 0;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::ZeroChannelCapacity)
        ));
    }

    #[test]
    fn rejects_app_count_above_safety_limit() {
        let mut config = valid_config();
        config.simulation.device_count = 1;
        config.simulation.channel_capacity = 1;
        config.simulation.installed_apps_max = MAX_INSTALLED_APPS + 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TooManyInstalledApps {
                max,
                limit
            })
            if max == MAX_INSTALLED_APPS + 1 && limit == MAX_INSTALLED_APPS
        ));
        config.simulation.installed_apps_max = MAX_INSTALLED_APPS;
        assert!(config.validate().is_ok());
    }
}
