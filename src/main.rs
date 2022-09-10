use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{BorrowedHeaders, FromBytes, Headers, Message};
use serde::{Deserialize, Serialize};
use serde_json::{json, Result, Value};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct ClientMessage {
    to: Uuid,
    from: Uuid,
    content: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            // example log str to get trace logs for kafka client:
            // RUST_LOG="librdkafka=trace,rdkafka::client=debug"
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:29092")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("group.id", "rust-rdkafka-roundtrip-example")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&vec!["messages"])
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => tracing::error!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        tracing::warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                tracing::info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    let cnt = headers.count();
                    for i in 0..cnt {
                        if let Some((key, val)) = headers.get_as::<str>(i) {
                            tracing::info!("  Header {:#?}: {:?}", key, val);
                        }
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
