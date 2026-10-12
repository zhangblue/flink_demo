use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mobile_event_sender::config::KafkaConfig;
use mobile_event_sender::generator::SimulatedDevice;
use mobile_event_sender::sink::{EventSink, KafkaSink};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::Offset;
use serde_json::Value;
use tokio::time::{Instant, timeout};
use uuid::Uuid;

fn position_at_current_end(consumer: &StreamConsumer, deadline: Instant) {
    let assignment = consumer.assignment().expect("read consumer assignment");
    assert!(
        assignment.count() > 0,
        "consumer has no assigned partitions"
    );
    for partition in assignment.elements() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "deadline expired while fetching watermarks"
        );
        let (_, high) = consumer
            .fetch_watermarks(partition.topic(), partition.partition(), remaining)
            .expect("fetch assigned partition watermarks");
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "deadline expired while seeking partitions"
        );
        consumer
            .seek(
                partition.topic(),
                partition.partition(),
                Offset::Offset(high),
                remaining,
            )
            .expect("seek assigned partition to high watermark");
    }
}

#[tokio::test]
#[ignore = "writes to the configured real Kafka cluster"]
async fn sends_and_consumes_one_ordered_startup_cycle() {
    let deadline = Instant::now() + Duration::from_secs(45);
    tokio::time::timeout_at(deadline, async {
        let brokers =
            std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "192.168.250.106:9092".to_owned());
        let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "flink_demo".to_owned());
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set(
                "group.id",
                format!("mobile-event-sender-test-{}", Uuid::new_v4()),
            )
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("create test consumer");
        consumer.subscribe(&[&topic]).expect("subscribe test topic");

        // Drive consumer polling until group assignment exists. Explicitly seek
        // every assigned partition to its current high watermark before sending.
        let assignment_deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if consumer.assignment().expect("read assignment").count() > 0 {
                break;
            }
            assert!(
                Instant::now() < assignment_deadline,
                "consumer was not assigned a partition"
            );
            let _ = timeout(Duration::from_millis(200), consumer.recv()).await;
        }
        position_at_current_end(&consumer, deadline);

        let seed = rand::random::<u64>();
        let mut device = SimulatedDevice::new(StdRng::seed_from_u64(seed), 2..=2);
        let start_timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let event_timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let events = device.next_cycle(start_timestamp_ms, event_timestamp_ms);
        let udid = device.udid().to_string();
        let start_id = events[0].start_id;
        let event_ids: HashSet<String> = events
            .iter()
            .map(|event| event.event_id.to_string())
            .collect();
        assert_eq!(event_ids.len(), 5);

        let sink = KafkaSink::new(&KafkaConfig {
            brokers,
            topic,
            compression: "lz4".to_owned(),
            message_timeout_ms: 5000,
        })
        .expect("create test producer");
        for event in &events {
            sink.enqueue(event)
                .expect("enqueue event")
                .await
                .expect("deliver event");
        }

        let mut received = Vec::new();
        while received.len() < 5 {
            let message = consumer.recv().await.expect("consume Kafka record");
            let Some(payload) = message.payload() else {
                continue;
            };
            let Ok(value) = serde_json::from_slice::<Value>(payload) else {
                continue;
            };
            let Some(id) = value["event_id"].as_str() else {
                continue;
            };
            if value["udid"] != udid || !event_ids.contains(id) {
                continue;
            }
            assert_eq!(
                message.key(),
                Some(udid.as_bytes()),
                "Kafka key must equal udid"
            );
            assert_eq!(value["start_id"].as_i64(), Some(start_id));
            received.push((message.partition(), message.offset(), value));
        }

        assert_eq!(
            received
                .iter()
                .map(|(_, _, value)| value["sequence"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(received[0].2["protol_type"], "common");
        assert_eq!(received[1].2["protol_type"], "start");
        assert_eq!(received[2].2["protol_type"], "devinfo");
        let later = [&received[3].2["protol_type"], &received[4].2["protol_type"]];
        assert!(later.contains(&&Value::String("environment".to_owned())));
        assert!(later.contains(&&Value::String("app".to_owned())));
        let start = &received[1].2["payload"];
        let app = received
            .iter()
            .find_map(|(_, _, value)| (value["protol_type"] == "app").then_some(&value["payload"]))
            .expect("app payload");
        let selected = app["self_package_name"]
            .as_str()
            .expect("self_package_name string");

        assert_eq!(start["app_package"], selected);
        assert!(
            app["apps"]
                .as_array()
                .expect("apps array")
                .iter()
                .any(|installed| installed["package_name"] == selected)
        );
        assert!(
            received
                .iter()
                .all(|(partition, _, _)| *partition == received[0].0)
        );
        assert!(received.windows(2).all(|pair| pair[0].1 < pair[1].1));
        for (index, (_, _, value)) in received.iter().enumerate() {
            assert_eq!(*value, serde_json::to_value(&events[index]).unwrap());
        }
    })
    .await
    .expect("Kafka integration test exceeded its 45-second deadline");
}
