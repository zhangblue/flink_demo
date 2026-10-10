use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use mobile_event_sender::{config::AppConfig, runner};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Generate simulated mobile startup events for Kafka")]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let cli = Cli::parse();
    let config = AppConfig::from_path(&cli.config)
        .with_context(|| format!("failed to load {}", cli.config.display()))?;
    let report = runner::run(config).await?;
    tracing::info!(
        enqueued = report.enqueued,
        delivered = report.delivered,
        failed = report.failed,
        "sender stopped"
    );
    if report.failed != 0 {
        anyhow::bail!("{} event deliveries failed", report.failed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use std::path::Path;

    use super::Cli;

    #[test]
    fn cli_defaults_to_local_config_toml() {
        let cli = Cli::try_parse_from(["sender"]).unwrap();
        assert_eq!(cli.config, Path::new("config.toml"));
    }

    #[test]
    fn cli_accepts_explicit_config_path() {
        let cli = Cli::try_parse_from(["sender", "--config", "sender/config.toml"]).unwrap();
        assert_eq!(cli.config, Path::new("sender/config.toml"));
    }
}
