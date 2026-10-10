use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::config::KafkaConfig;
use crate::model::MobileEvent;
use crate::rate::GlobalRateLimiter;

pub const MAX_KAFKA_EVENT_BYTES: usize = 900_000;

pub type DeliveryAck = Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'static>>;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("producer queue is full")]
    QueueFull,
    #[error("failed to serialize event: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("delivery failed: {0}")]
    Delivery(String),
}

pub trait EventSink: Send + Sync + 'static {
    fn enqueue(&self, event: &MobileEvent) -> Result<DeliveryAck, SinkError>;
    fn flush(&self, timeout: Duration) -> Result<(), SinkError>;

    fn topic(&self) -> &str {
        "unknown"
    }

    fn message_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

struct EncodedEvent {
    key: String,
    value: String,
}

fn encode_event(event: &MobileEvent) -> Result<EncodedEvent, SinkError> {
    Ok(EncodedEvent {
        key: event.kafka_key(),
        value: serde_json::to_string(event)?,
    })
}

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    message_timeout: Duration,
}

impl KafkaSink {
    pub fn new(config: &KafkaConfig) -> Result<Self, SinkError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", config.message_timeout_ms.to_string())
            .set("compression.type", &config.compression)
            .set("message.max.bytes", "1000000")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()?;
        Ok(Self {
            producer,
            topic: config.topic.clone(),
            message_timeout: Duration::from_millis(config.message_timeout_ms),
        })
    }
}

impl EventSink for KafkaSink {
    fn enqueue(&self, event: &MobileEvent) -> Result<DeliveryAck, SinkError> {
        let encoded = encode_event(event)?;
        let record = FutureRecord::to(&self.topic)
            .key(&encoded.key)
            .payload(&encoded.value);
        let delivery = self
            .producer
            .send_result(record)
            .map_err(|(error, _record)| {
                if matches!(
                    error,
                    KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                ) {
                    SinkError::QueueFull
                } else {
                    SinkError::Kafka(error)
                }
            })?;
        Ok(Box::pin(async move {
            match delivery.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err((error, _message))) => Err(SinkError::Delivery(error.to_string())),
                Err(cancelled) => Err(SinkError::Delivery(cancelled.to_string())),
            }
        }))
    }

    fn flush(&self, timeout: Duration) -> Result<(), SinkError> {
        self.producer.flush(timeout).map_err(SinkError::Kafka)
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn message_timeout(&self) -> Duration {
        self.message_timeout
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DispatchReport {
    pub enqueued: u64,
    pub delivered: u64,
    pub failed: u64,
    pub rate_error: Option<String>,
    pub flush_error: Option<String>,
}

fn record_delivery(report: &mut DispatchReport, result: Result<(), SinkError>) {
    if result.is_ok() {
        report.delivered += 1;
    } else {
        report.failed += 1;
    }
}

pub async fn dispatch_events<S: EventSink>(
    mut receiver: mpsc::Receiver<MobileEvent>,
    sink: S,
    messages_per_second: u64,
    max_in_flight: usize,
) -> DispatchReport {
    let mut report = DispatchReport::default();
    let mut in_flight = FuturesUnordered::<DeliveryAck>::new();
    let mut limiter = match GlobalRateLimiter::new(messages_per_second) {
        Ok(limiter) => limiter,
        Err(error) => {
            tracing::error!(%error, "invalid dispatch rate");
            report.rate_error = Some(error.to_string());
            return report;
        }
    };
    let max_in_flight = max_in_flight.max(1);

    loop {
        let next_event = if in_flight.is_empty() {
            receiver.recv().await
        } else {
            tokio::select! {
                biased;
                result = in_flight.next() => {
                    if let Some(result) = result {
                        record_delivery(&mut report, result);
                    }
                    continue;
                }
                event = receiver.recv() => event,
            }
        };
        let Some(event) = next_event else {
            break;
        };
        while in_flight.len() >= max_in_flight {
            if let Some(result) = in_flight.next().await {
                record_delivery(&mut report, result);
            }
        }

        let mut retry_deadline = None;
        let mut queue_full_timed_out = false;
        loop {
            if let Some(deadline) = retry_deadline {
                if Instant::now() >= deadline
                    || timeout_at(deadline, limiter.acquire()).await.is_err()
                    || Instant::now() >= deadline
                {
                    queue_full_timed_out = true;
                    break;
                }
            } else {
                limiter.acquire().await;
            }
            match sink.enqueue(&event) {
                Ok(ack) => {
                    report.enqueued += 1;
                    let event_id = event.event_id;
                    let udid = event.udid;
                    let start_id = event.start_id;
                    let topic = sink.topic().to_owned();
                    tracing::debug!(%event_id, %udid, %start_id, %topic, "event enqueued");
                    in_flight.push(Box::pin(async move {
                        let result = ack.await;
                        if let Err(ref error) = result {
                            tracing::error!(%event_id, %udid, %start_id, %topic, %error, "event delivery failed");
                        }
                        result
                    }));
                    break;
                }
                Err(SinkError::QueueFull) => {
                    let deadline = *retry_deadline
                        .get_or_insert_with(|| Instant::now() + sink.message_timeout());
                    if Instant::now() >= deadline {
                        queue_full_timed_out = true;
                        break;
                    }
                    if in_flight.is_empty() {
                        let next_retry = (Instant::now() + Duration::from_millis(10)).min(deadline);
                        sleep_until(next_retry).await;
                    } else {
                        match timeout_at(deadline, in_flight.next()).await {
                            Ok(Some(result)) => record_delivery(&mut report, result),
                            Ok(None) => continue,
                            Err(_) => {
                                queue_full_timed_out = true;
                                break;
                            }
                        }
                    }
                    if Instant::now() >= deadline {
                        queue_full_timed_out = true;
                        break;
                    }
                }
                Err(error) => {
                    report.failed += 1;
                    tracing::error!(event_id = %event.event_id, udid = %event.udid, start_id = %event.start_id, topic = sink.topic(), %error, "event enqueue failed");
                    break;
                }
            }
        }
        if queue_full_timed_out {
            report.failed += 1;
            tracing::error!(event_id = %event.event_id, udid = %event.udid, start_id = %event.start_id, topic = sink.topic(), "producer queue remained full until message timeout");
        }
    }

    while let Some(result) = in_flight.next().await {
        record_delivery(&mut report, result);
    }
    if let Err(error) = sink.flush(sink.message_timeout()) {
        tracing::error!(topic = sink.topic(), %error, "producer flush failed");
        report.flush_error = Some(error.to_string());
    }
    report
}

#[cfg(test)]
mod tests {
    use super::{DeliveryAck, DispatchReport, EventSink, SinkError, dispatch_events, encode_event};
    use crate::model::{EventPayload, MobileEvent, ProtocolType, StartPayload};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use uuid::Uuid;

    #[derive(Clone, Debug)]
    struct Recorded {
        event_id: Uuid,
        key: String,
        value: String,
        accepted_at: tokio::time::Instant,
    }

    #[derive(Default)]
    struct State {
        attempted: Vec<Uuid>,
        accepted: Vec<Recorded>,
        pending: VecDeque<oneshot::Sender<Result<(), SinkError>>>,
        queue_full_once_at: Option<usize>,
        always_queue_full: bool,
        delivery_failure_at: Option<usize>,
        failure_polled: Option<oneshot::Sender<()>>,
        manual_ack: bool,
        flush_calls: usize,
        flush_fails: bool,
        message_timeout: Option<Duration>,
    }

    #[derive(Clone, Default)]
    struct RecordingSink(Arc<Mutex<State>>);

    impl RecordingSink {
        fn with_manual_ack() -> Self {
            let sink = Self::default();
            sink.0.lock().unwrap().manual_ack = true;
            sink
        }

        fn ack_next(&self) {
            self.0
                .lock()
                .unwrap()
                .pending
                .pop_front()
                .expect("pending delivery")
                .send(Ok(()))
                .unwrap();
        }

        fn accepted(&self) -> Vec<Recorded> {
            self.0.lock().unwrap().accepted.clone()
        }

        fn attempts(&self) -> Vec<Uuid> {
            self.0.lock().unwrap().attempted.clone()
        }
    }

    impl EventSink for RecordingSink {
        fn enqueue(&self, event: &MobileEvent) -> Result<DeliveryAck, SinkError> {
            let mut state = self.0.lock().unwrap();
            state.attempted.push(event.event_id);
            let index = state.accepted.len();
            if state.always_queue_full || state.queue_full_once_at == Some(index) {
                state.queue_full_once_at = None;
                return Err(SinkError::QueueFull);
            }

            let encoded = encode_event(event)?;
            state.accepted.push(Recorded {
                event_id: event.event_id,
                key: encoded.key,
                value: encoded.value,
                accepted_at: tokio::time::Instant::now(),
            });
            if state.manual_ack {
                let (tx, rx) = oneshot::channel();
                state.pending.push_back(tx);
                Ok(Box::pin(
                    async move { rx.await.expect("ack sender retained") },
                ))
            } else if state.delivery_failure_at == Some(index) {
                let failure_polled = state.failure_polled.take();
                Ok(Box::pin(async move {
                    if let Some(signal) = failure_polled {
                        let _ = signal.send(());
                    }
                    Err(SinkError::Delivery("injected".into()))
                }))
            } else {
                Ok(Box::pin(async { Ok(()) }))
            }
        }

        fn flush(&self, _timeout: Duration) -> Result<(), SinkError> {
            let mut state = self.0.lock().unwrap();
            state.flush_calls += 1;
            if state.flush_fails {
                Err(SinkError::Delivery("injected flush failure".into()))
            } else {
                Ok(())
            }
        }

        fn message_timeout(&self) -> Duration {
            self.0
                .lock()
                .unwrap()
                .message_timeout
                .unwrap_or(Duration::from_secs(5))
        }
    }

    fn fixture_events(count: usize) -> Vec<MobileEvent> {
        (0..count)
            .map(|index| MobileEvent {
                event_id: Uuid::from_u128(index as u128 + 1),
                protol_type: ProtocolType::Start,
                udid: Uuid::from_u128(100 + index as u128),
                start_id: Uuid::from_u128(200 + index as u128),
                event_timestamp: 1_700_000_000_000,
                sequence: 1,
                payload: EventPayload::Start(StartPayload {
                    app_package: "com.example".into(),
                    app_version: "1".into(),
                    app_build: 1,
                    launch_type: "cold".into(),
                    foreground: true,
                    battery_level: 50,
                    network_type: "wifi".into(),
                }),
            })
            .collect()
    }

    fn receiver_with(events: Vec<MobileEvent>) -> mpsc::Receiver<MobileEvent> {
        let (tx, rx) = mpsc::channel(events.len().max(1));
        for event in events {
            tx.try_send(event).unwrap();
        }
        rx
    }

    #[tokio::test(start_paused = true)]
    async fn dispatcher_preserves_order_and_continues_after_delivery_failure() {
        // Catches reordering, early termination, or counting a failed delivery as success.
        let events = fixture_events(3);
        let sink = RecordingSink::default();
        sink.0.lock().unwrap().delivery_failure_at = Some(1);
        let report = dispatch_events(receiver_with(events.clone()), sink.clone(), 100, 16).await;

        let accepted = sink.accepted();
        assert_eq!(
            accepted.iter().map(|r| r.event_id).collect::<Vec<_>>(),
            events.iter().map(|e| e.event_id).collect::<Vec<_>>()
        );
        assert_eq!(report.enqueued, 3);
        assert_eq!(report.delivered, 2);
        assert_eq!(report.failed, 1);
        assert_eq!(sink.0.lock().unwrap().flush_calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_failure_is_polled_while_receiver_remains_open() {
        // Catches ack starvation under low traffic and an open input channel.
        let (sender, receiver) = mpsc::channel(1);
        let (polled_tx, mut polled_rx) = oneshot::channel();
        let sink = RecordingSink::default();
        {
            let mut state = sink.0.lock().unwrap();
            state.delivery_failure_at = Some(0);
            state.failure_polled = Some(polled_tx);
        }
        let task = tokio::spawn(dispatch_events(receiver, sink, 100, 16));
        sender.send(fixture_events(1).pop().unwrap()).await.unwrap();
        tokio::task::yield_now().await;
        assert!(matches!(polled_rx.try_recv(), Ok(())));
        drop(sender);
        let report = task.await.unwrap();
        assert_eq!(
            (report.enqueued, report.delivered, report.failed),
            (1, 0, 1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dispatcher_passes_udid_key_and_serializable_json_value() {
        // Catches wrong key or payload handed to the sink boundary.
        let event = fixture_events(1).pop().unwrap();
        let sink = RecordingSink::default();
        dispatch_events(receiver_with(vec![event]), sink.clone(), 100, 1).await;
        let accepted = sink.accepted();
        assert_eq!(accepted[0].key, "00000000-0000-0000-0000-000000000064");
        let value: serde_json::Value = serde_json::from_str(&accepted[0].value).unwrap();
        assert_eq!(value["event_id"], "00000000-0000-0000-0000-000000000001");
        assert_eq!(value["udid"], accepted[0].key);
    }

    #[tokio::test(start_paused = true)]
    async fn dispatcher_drains_deliveries_after_receiver_closes() {
        // Catches returning or flushing before an outstanding ack resolves.
        let sink = RecordingSink::with_manual_ack();
        let task = tokio::spawn(dispatch_events(
            receiver_with(fixture_events(1)),
            sink.clone(),
            100,
            2,
        ));
        tokio::task::yield_now().await;
        assert_eq!(sink.accepted().len(), 1);
        assert!(!task.is_finished());
        assert_eq!(sink.0.lock().unwrap().flush_calls, 0);
        sink.ack_next();
        let report = task.await.unwrap();
        assert_eq!(report.delivered, 1);
        assert_eq!(sink.0.lock().unwrap().flush_calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dispatcher_bounds_in_flight_deliveries() {
        // Catches enqueueing a third event before one of two pending acks completes.
        let sink = RecordingSink::with_manual_ack();
        let task = tokio::spawn(dispatch_events(
            receiver_with(fixture_events(3)),
            sink.clone(),
            100,
            2,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.accepted().len(), 2);
        sink.ack_next();
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.accepted().len(), 3);
        sink.ack_next();
        sink.ack_next();
        let report = task.await.unwrap();
        assert_eq!(report.delivered, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn dispatcher_retries_current_event_after_queue_full_without_reordering() {
        // Catches dropping a queue-full event or advancing to a later event first.
        let events = fixture_events(3);
        let sink = RecordingSink::with_manual_ack();
        sink.0.lock().unwrap().queue_full_once_at = Some(1);
        let task = tokio::spawn(dispatch_events(
            receiver_with(events.clone()),
            sink.clone(),
            100,
            2,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            sink.attempts(),
            vec![events[0].event_id, events[1].event_id]
        );
        sink.ack_next();
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            sink.attempts(),
            vec![
                events[0].event_id,
                events[1].event_id,
                events[1].event_id,
                events[2].event_id
            ]
        );
        assert_eq!(
            sink.accepted()
                .iter()
                .map(|r| r.event_id)
                .collect::<Vec<_>>(),
            events.iter().map(|e| e.event_id).collect::<Vec<_>>()
        );
        sink.ack_next();
        sink.ack_next();
        let report: DispatchReport = task.await.unwrap();
        assert_eq!(
            (report.enqueued, report.delivered, report.failed),
            (3, 3, 0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn persistent_queue_full_has_bounded_retry_delay() {
        // Catches a tight retry loop or an unbounded wait with no pending delivery.
        let sink = RecordingSink::default();
        {
            let mut state = sink.0.lock().unwrap();
            state.always_queue_full = true;
            state.message_timeout = Some(Duration::from_millis(30));
        }
        let task = tokio::spawn(dispatch_events(
            receiver_with(fixture_events(1)),
            sink.clone(),
            100,
            1,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(31)).await;
        let report = task.await.unwrap();
        assert_eq!(
            (report.enqueued, report.delivered, report.failed),
            (0, 0, 1)
        );
        assert!(sink.attempts().len() <= 4, "retry must back off");
    }

    #[tokio::test(start_paused = true)]
    async fn successful_enqueues_remain_rate_limited_after_queue_full() {
        // Catches using an expired permit for the next event after a delayed retry.
        let sink = RecordingSink::default();
        sink.0.lock().unwrap().queue_full_once_at = Some(0);
        let report = dispatch_events(receiver_with(fixture_events(2)), sink.clone(), 100, 2).await;
        let accepted = sink.accepted();
        assert_eq!(report.delivered, 2);
        assert!(
            accepted[1]
                .accepted_at
                .duration_since(accepted[0].accepted_at)
                >= Duration::from_millis(10)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn queue_full_timeout_advances_without_consuming_pending_ack() {
        // Catches waiting past the deadline and retrying the timed-out event after a late ack.
        let events = fixture_events(3);
        let sink = RecordingSink::with_manual_ack();
        {
            let mut state = sink.0.lock().unwrap();
            state.queue_full_once_at = Some(1);
            state.message_timeout = Some(Duration::from_millis(30));
        }
        let task = tokio::spawn(dispatch_events(
            receiver_with(events.clone()),
            sink.clone(),
            100,
            2,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            sink.attempts(),
            vec![events[0].event_id, events[1].event_id]
        );
        assert_eq!(sink.accepted().len(), 1);
        tokio::time::advance(Duration::from_millis(31)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            sink.attempts(),
            vec![events[0].event_id, events[1].event_id, events[2].event_id]
        );
        assert_eq!(
            sink.accepted()
                .iter()
                .map(|r| r.event_id)
                .collect::<Vec<_>>(),
            vec![events[0].event_id, events[2].event_id]
        );
        assert!(!task.is_finished(), "pending acks remain for final drain");
        sink.ack_next();
        sink.ack_next();
        let report = task.await.unwrap();
        assert_eq!(
            (report.enqueued, report.delivered, report.failed),
            (2, 2, 1)
        );
    }

    #[tokio::test]
    async fn invalid_rate_is_visible_in_report() {
        // Catches reporting a successful empty run when the limiter rejects the configured rate.
        let sink = RecordingSink::default();
        let report = dispatch_events(receiver_with(fixture_events(1)), sink, u64::MAX, 1).await;
        assert!(report.rate_error.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn flush_failure_is_visible_after_deliveries_complete() {
        // Catches silently ignoring a failed flush after the receiver closes.
        let sink = RecordingSink::default();
        sink.0.lock().unwrap().flush_fails = true;
        let report = dispatch_events(receiver_with(fixture_events(1)), sink, 100, 1).await;
        assert_eq!(report.delivered, 1);
        assert!(
            report
                .flush_error
                .as_deref()
                .unwrap()
                .contains("injected flush failure")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn queue_full_timeout_covers_next_rate_permit() {
        // Catches waiting for a later permit and enqueueing after the retry deadline.
        let events = fixture_events(2);
        let started_at = tokio::time::Instant::now();
        let sink = RecordingSink::default();
        {
            let mut state = sink.0.lock().unwrap();
            state.queue_full_once_at = Some(1);
            state.message_timeout = Some(Duration::from_millis(30));
        }
        let report = dispatch_events(receiver_with(events.clone()), sink.clone(), 1, 1).await;
        assert_eq!(
            sink.attempts(),
            vec![events[0].event_id, events[1].event_id]
        );
        assert_eq!(sink.accepted().len(), 1);
        assert_eq!(
            (report.enqueued, report.delivered, report.failed),
            (1, 1, 1)
        );
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            Duration::from_millis(1_030),
            "retry must stop at its deadline instead of waiting for another rate permit"
        );
    }
}
