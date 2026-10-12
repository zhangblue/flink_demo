use std::future::Future;
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use rand::{Rng, SeedableRng, rngs::StdRng};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::config::{AppConfig, ConfigError};
use crate::generator::SimulatedDevice;
use crate::model::MobileEvent;
use crate::sink::{DispatchReport, EventSink, KafkaSink, SinkError, dispatch_events};

const NAMESPACE_MODULUS: u64 = 1_u64 << 62;

fn device_namespace(run_nonce: u64, device_index: usize) -> u64 {
    run_nonce.wrapping_add(device_index as u64) & (NAMESPACE_MODULUS - 1)
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error("invalid configuration: {0}")]
    Config(#[from] ConfigError),
    #[error("failed to construct Kafka producer: {0}")]
    Kafka(#[from] SinkError),
    #[error("shutdown signal failed: {0}")]
    Shutdown(#[from] std::io::Error),
    #[error("task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("device task exited before shutdown")]
    DeviceExited,
    #[error("dispatcher exited before shutdown")]
    DispatcherExited,
    #[error("dispatch rate failed: {0}")]
    Rate(String),
    #[error("producer flush failed: {0}")]
    Flush(String),
}

pub async fn run_device<R: Rng + Send + 'static>(
    mut device: SimulatedDevice<R>,
    sender: mpsc::Sender<MobileEvent>,
    mut shutdown: watch::Receiver<bool>,
    startup_interval: RangeInclusive<u64>,
) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }

        let start_timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let event_timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        for event in device.next_cycle(start_timestamp_ms, event_timestamp_ms) {
            if sender.send(event).await.is_err() {
                return;
            }
        }

        let seconds = {
            let mut interval_rng = rand::rng();
            interval_rng.random_range(startup_interval.clone())
        };
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(seconds)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow_and_update() {
                    return;
                }
            }
        }
    }
}

pub async fn run(config: AppConfig) -> Result<DispatchReport, RunError> {
    config.validate()?;
    let sink = KafkaSink::new(&config.kafka)?;
    run_until_sink(config, sink, tokio::signal::ctrl_c()).await
}

pub async fn run_until_sink<S, F>(
    config: AppConfig,
    sink: S,
    shutdown_future: F,
) -> Result<DispatchReport, RunError>
where
    S: EventSink,
    F: Future<Output = Result<(), std::io::Error>>,
{
    config.validate()?;
    let simulation = &config.simulation;
    let (sender, receiver) = mpsc::channel(simulation.channel_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let dispatcher = tokio::spawn(dispatch_events(
        receiver,
        sink,
        simulation.messages_per_second,
        simulation.channel_capacity,
    ));

    let mut devices = JoinSet::new();
    let run_nonce = rand::rng().random_range(1..NAMESPACE_MODULUS);
    for index in 0..simulation.device_count {
        let rng = StdRng::from_os_rng();
        let device = SimulatedDevice::with_namespace(
            rng,
            simulation.installed_apps_min..=simulation.installed_apps_max,
            device_namespace(run_nonce, index),
        );
        devices.spawn(run_device(
            device,
            sender.clone(),
            shutdown_rx.clone(),
            simulation.startup_interval_min_secs..=simulation.startup_interval_max_secs,
        ));
    }
    drop(sender);
    drop(shutdown_rx);

    coordinate(shutdown_future, shutdown_tx, devices, dispatcher).await
}

fn record_error(primary: &mut Option<RunError>, error: RunError) {
    if primary.is_none() {
        *primary = Some(error);
    } else {
        tracing::error!(%error, "additional shutdown error");
    }
}

fn record_dispatcher_error(primary: &mut Option<RunError>, error: RunError) {
    if matches!(primary, Some(RunError::DeviceExited)) {
        let generic = primary.replace(error).expect("device exit was recorded");
        tracing::error!(%generic, "additional shutdown error");
    } else {
        record_error(primary, error);
    }
}

async fn coordinate<F>(
    shutdown_future: F,
    shutdown_tx: watch::Sender<bool>,
    mut devices: JoinSet<()>,
    mut dispatcher: JoinHandle<DispatchReport>,
) -> Result<DispatchReport, RunError>
where
    F: Future<Output = Result<(), std::io::Error>>,
{
    tokio::pin!(shutdown_future);
    let mut primary = None;
    let mut early_report = None;
    let mut dispatcher_done = false;
    let mut signal_selected = false;
    tokio::select! {
        biased;
        device_result = devices.join_next(), if !devices.is_empty() => {
            match device_result {
                Some(Ok(())) => record_error(&mut primary, RunError::DeviceExited),
                Some(Err(error)) => record_error(&mut primary, RunError::Join(error)),
                None => record_error(&mut primary, RunError::DeviceExited),
            }
        }
        dispatch_result = &mut dispatcher => {
            dispatcher_done = true;
            match dispatch_result {
                Ok(report) => {
                    if let Some(error) = &report.rate_error {
                        record_error(&mut primary, RunError::Rate(error.clone()));
                    } else if let Some(error) = &report.flush_error {
                        record_error(&mut primary, RunError::Flush(error.clone()));
                    } else {
                        record_error(&mut primary, RunError::DispatcherExited);
                    }
                    early_report = Some(report);
                }
                Err(error) => record_error(&mut primary, RunError::Join(error)),
            }
        }
        signal_result = &mut shutdown_future => {
            signal_selected = true;
            if let Err(error) = signal_result {
                record_error(&mut primary, RunError::Shutdown(error));
            }
        }
    }

    let _ = shutdown_tx.send(true);
    while let Some(result) = devices.join_next().await {
        if let Err(error) = result {
            record_error(&mut primary, RunError::Join(error));
        }
    }
    drop(shutdown_tx);
    let report = if dispatcher_done {
        early_report
    } else {
        match dispatcher.await {
            Ok(report) => Some(report),
            Err(error) => {
                record_dispatcher_error(&mut primary, RunError::Join(error));
                None
            }
        }
    };
    if !signal_selected && let Some(Err(error)) = shutdown_future.as_mut().now_or_never() {
        record_error(&mut primary, RunError::Shutdown(error));
    }
    let Some(report) = report else {
        return Err(primary.expect("dispatcher failure must be recorded"));
    };
    if let Some(error) = &report.rate_error
        && !matches!(primary, Some(RunError::Rate(_)))
    {
        record_dispatcher_error(&mut primary, RunError::Rate(error.clone()));
    }
    if let Some(error) = &report.flush_error
        && !matches!(primary, Some(RunError::Flush(_)))
    {
        record_dispatcher_error(&mut primary, RunError::Flush(error.clone()));
    }
    match primary {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use rand::{SeedableRng, rngs::StdRng};
    use tokio::sync::{mpsc, watch};

    use crate::config::AppConfig;
    use crate::generator::SimulatedDevice;
    use crate::model::{MobileEvent, ProtocolType};
    use crate::sink::{DeliveryAck, EventSink, SinkError};

    use super::{RunError, coordinate, device_namespace, run_device, run_until_sink};

    #[test]
    fn run_nonce_and_device_index_produce_distinct_event_id_namespaces() {
        let namespace_a = device_namespace(123, 0);
        let namespace_b = device_namespace(124, 0);
        let namespace_c = device_namespace(123, 1);
        let first_id =
            SimulatedDevice::with_namespace(StdRng::seed_from_u64(99), 1..=1, namespace_a)
                .next_cycle(1_000, 2_000)[0]
                .event_id;
        let second_id =
            SimulatedDevice::with_namespace(StdRng::seed_from_u64(99), 1..=1, namespace_b)
                .next_cycle(1_000, 2_000)[0]
                .event_id;
        let third_id =
            SimulatedDevice::with_namespace(StdRng::seed_from_u64(99), 1..=1, namespace_c)
                .next_cycle(1_000, 2_000)[0]
                .event_id;
        assert_ne!(first_id, second_id);
        assert_ne!(first_id, third_id);
        let wrapping: HashSet<_> = (0..4)
            .map(|index| device_namespace((1_u64 << 62) - 2, index))
            .collect();
        assert_eq!(wrapping.len(), 4);
    }

    fn fixture_device(seed: u64) -> SimulatedDevice<StdRng> {
        SimulatedDevice::new(StdRng::seed_from_u64(seed), 1..=1)
    }

    async fn collect_n(
        receiver: &mut mpsc::Receiver<MobileEvent>,
        count: usize,
    ) -> Vec<MobileEvent> {
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(receiver.recv().await.expect("device channel closed early"));
        }
        events
    }

    #[tokio::test(start_paused = true)]
    async fn device_task_completes_started_cycle_after_shutdown() {
        let (sender, mut receiver) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_device(
            fixture_device(42),
            sender,
            shutdown_rx,
            30..=120,
        ));

        let first = receiver.recv().await.unwrap();
        assert_eq!(first.protol_type, ProtocolType::Common);
        shutdown_tx.send(true).unwrap();
        let mut events = vec![first];
        events.extend(collect_n(&mut receiver, 4).await);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            events
                .iter()
                .map(|event| event.protol_type)
                .collect::<Vec<_>>()[..3],
            [
                ProtocolType::Common,
                ProtocolType::Start,
                ProtocolType::Devinfo
            ]
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn device_task_exits_immediately_when_shutdown_arrives_during_sleep() {
        let (sender, mut receiver) = mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_device(
            fixture_device(43),
            sender,
            shutdown_rx,
            3600..=3600,
        ));
        collect_n(&mut receiver, 5).await;

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn device_task_exits_when_receiver_closes() {
        let (sender, mut receiver) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_device(fixture_device(44), sender, shutdown_rx, 1..=1));
        receiver.recv().await.unwrap();
        drop(receiver);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn multiple_devices_feed_one_receiver_with_distinct_ids() {
        let (sender, mut receiver) = mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let first = tokio::spawn(run_device(
            fixture_device(45),
            sender.clone(),
            shutdown_rx.clone(),
            3600..=3600,
        ));
        let second = tokio::spawn(run_device(
            fixture_device(46),
            sender,
            shutdown_rx,
            3600..=3600,
        ));
        let events = collect_n(&mut receiver, 10).await;
        shutdown_tx.send(true).unwrap();
        first.await.unwrap();
        second.await.unwrap();
        assert!(receiver.recv().await.is_none());
        let udids: HashSet<_> = events.iter().map(|event| event.udid).collect();
        assert_eq!(udids.len(), 2);
        let ids: HashSet<_> = events.iter().map(|event| event.event_id).collect();
        assert_eq!(ids.len(), 10);
    }

    #[derive(Clone)]
    struct MemorySink {
        events: Arc<Mutex<Vec<MobileEvent>>>,
        flushes: Arc<AtomicUsize>,
        acked: Arc<AtomicUsize>,
        ack_ready: watch::Receiver<bool>,
    }

    impl EventSink for MemorySink {
        fn enqueue(&self, event: &MobileEvent) -> Result<DeliveryAck, SinkError> {
            self.events.lock().unwrap().push(event.clone());
            let mut ack_ready = self.ack_ready.clone();
            let acked = self.acked.clone();
            Ok(Box::pin(async move {
                while !*ack_ready.borrow_and_update() {
                    ack_ready
                        .changed()
                        .await
                        .map_err(|error| SinkError::Delivery(error.to_string()))?;
                }
                acked.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
        }

        fn flush(&self, _timeout: Duration) -> Result<(), SinkError> {
            if self.acked.load(Ordering::SeqCst) != self.events.lock().unwrap().len() {
                return Err(SinkError::Delivery(
                    "flush preceded acknowledgements".into(),
                ));
            }
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_all_devices_and_acks_before_final_flush() {
        let mut config: AppConfig = toml::from_str(include_str!("../config.toml")).unwrap();
        config.simulation.device_count = 2;
        config.simulation.messages_per_second = 1_000;
        config.simulation.startup_interval_min_secs = 3600;
        config.simulation.startup_interval_max_secs = 3600;
        config.simulation.channel_capacity = 10;
        config.simulation.installed_apps_min = 1;
        config.simulation.installed_apps_max = 1;
        let events = Arc::new(Mutex::new(Vec::new()));
        let flushes = Arc::new(AtomicUsize::new(0));
        let acked = Arc::new(AtomicUsize::new(0));
        let (ack_tx, ack_rx) = watch::channel(false);
        let sink = MemorySink {
            events: events.clone(),
            flushes: flushes.clone(),
            acked: acked.clone(),
            ack_ready: ack_rx,
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(run_until_sink(config, sink, async move {
            shutdown_rx.await.map_err(std::io::Error::other)
        }));

        for _ in 0..100 {
            if events.lock().unwrap().len() == 10 {
                break;
            }
            tokio::time::advance(Duration::from_millis(1)).await;
        }
        assert_eq!(events.lock().unwrap().len(), 10);
        shutdown_tx.send(()).unwrap();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "must wait for delivery acknowledgements"
        );
        assert_eq!(flushes.load(Ordering::SeqCst), 0);
        ack_tx.send(true).unwrap();
        let report = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(report.enqueued, 10);
        assert_eq!(report.delivered, 10);
        assert_eq!(report.failed, 0);
        assert_eq!(acked.load(Ordering::SeqCst), 10);
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        let collected = events.lock().unwrap();
        let udids: HashSet<_> = collected.iter().map(|event| event.udid).collect();
        assert_eq!(udids.len(), 2);
        let namespaces: HashSet<_> = collected
            .iter()
            .map(|event| event.event_id.as_bytes()[8..].to_vec())
            .collect();
        assert_eq!(namespaces.len(), 2);
        assert_ne!(
            collected[0].event_id.as_bytes()[8..],
            [0x80, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    struct PanicSink;

    impl EventSink for PanicSink {
        fn enqueue(&self, _event: &MobileEvent) -> Result<DeliveryAck, SinkError> {
            panic!("dispatcher panic fixture");
        }

        fn flush(&self, _timeout: Duration) -> Result<(), SinkError> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dispatcher_panic_ends_run_without_shutdown_signal() {
        let mut config: AppConfig = toml::from_str(include_str!("../config.toml")).unwrap();
        config.simulation.device_count = 1;
        config.simulation.installed_apps_min = 1;
        config.simulation.installed_apps_max = 1;
        config.simulation.startup_interval_min_secs = 3600;
        config.simulation.startup_interval_max_secs = 3600;
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            run_until_sink(config, PanicSink, std::future::pending()),
        )
        .await
        .expect("dispatcher panic must stop orchestration promptly");
        assert!(matches!(result, Err(RunError::Join(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn device_panic_ends_coordinator_without_shutdown_signal() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut devices = tokio::task::JoinSet::new();
        devices.spawn(async { panic!("device panic fixture") });
        let dispatcher = tokio::spawn(async move {
            while !*shutdown_rx.borrow_and_update() {
                shutdown_rx.changed().await.unwrap();
            }
            crate::sink::DispatchReport::default()
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            coordinate(std::future::pending(), shutdown_tx, devices, dispatcher),
        )
        .await
        .expect("device panic must stop orchestration promptly");
        assert!(matches!(result, Err(RunError::Join(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn early_dispatch_preserves_specific_rate_and_flush_errors() {
        for (report, expected_rate) in [
            (
                crate::sink::DispatchReport {
                    rate_error: Some("bad rate".into()),
                    ..Default::default()
                },
                true,
            ),
            (
                crate::sink::DispatchReport {
                    flush_error: Some("bad flush".into()),
                    ..Default::default()
                },
                false,
            ),
        ] {
            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let mut devices = tokio::task::JoinSet::new();
            devices.spawn(async move {
                while !*shutdown_rx.borrow_and_update() {
                    shutdown_rx.changed().await.unwrap();
                }
            });
            let dispatcher = tokio::spawn(async move { report });
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                coordinate(std::future::pending(), shutdown_tx, devices, dispatcher),
            )
            .await
            .unwrap();
            if expected_rate {
                assert!(matches!(result, Err(RunError::Rate(_))));
            } else {
                assert!(matches!(result, Err(RunError::Flush(_))));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn completed_device_cannot_mask_completed_dispatch_errors() {
        for (report, expected_rate) in [
            (
                crate::sink::DispatchReport {
                    rate_error: Some("bad rate".into()),
                    ..Default::default()
                },
                true,
            ),
            (
                crate::sink::DispatchReport {
                    flush_error: Some("bad flush".into()),
                    ..Default::default()
                },
                false,
            ),
        ] {
            let (shutdown_tx, _shutdown_rx) = watch::channel(false);
            let mut devices = tokio::task::JoinSet::new();
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            devices.spawn(async move {
                done_tx.send(()).unwrap();
            });
            done_rx.await.unwrap();
            let dispatcher = tokio::spawn(async move { report });
            while !dispatcher.is_finished() {
                tokio::task::yield_now().await;
            }
            tokio::task::yield_now().await;
            let result = coordinate(std::future::pending(), shutdown_tx, devices, dispatcher).await;
            if expected_rate {
                assert!(matches!(result, Err(RunError::Rate(_))));
            } else {
                assert!(matches!(result, Err(RunError::Flush(_))));
            }
        }

        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut devices = tokio::task::JoinSet::new();
        devices.spawn(async {});
        let dispatcher = tokio::spawn(async { panic!("dispatcher panic fixture") });
        while !dispatcher.is_finished() {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        let result = coordinate(std::future::pending(), shutdown_tx, devices, dispatcher).await;
        assert!(matches!(result, Err(RunError::Join(_))));
    }

    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ready_signal_error_is_logged_after_background_failure() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || CaptureWriter(captured.clone()))
            .finish();
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);

        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut devices = tokio::task::JoinSet::new();
        devices.spawn(async {});
        let dispatcher = tokio::spawn(async {
            crate::sink::DispatchReport {
                rate_error: Some("bad rate".into()),
                ..Default::default()
            }
        });
        while !dispatcher.is_finished() {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        let result = coordinate(
            async { Err(std::io::Error::other("signal fixture")) },
            shutdown_tx,
            devices,
            dispatcher,
        )
        .await;
        assert!(matches!(result, Err(RunError::Rate(_))));
        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("shutdown signal failed: signal fixture"),
            "{logs}"
        );
    }
}
