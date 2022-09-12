use futures::future::join_all;
use futures_util::StreamExt as _;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use tokio::spawn;
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::{uuid, Uuid};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let (tx, mut rx) = mpsc::channel(32);
    let sub_handle = spawn(async move {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        match client.get_async_connection().await {
            Err(e) => {
                tracing::error!("pubsub connection failed: {:?}", e);
                // system exit via https://stackoverflow.com/a/54526047/9360856
            }
            Ok(c) => {
                let mut conn = c.into_pubsub();
                let uuid = uuid::uuid!("14412d14-c21d-4f00-a74e-f02146e16f48");
                let _a = conn.subscribe(uuid.as_bytes()).await;
                while let Some(msg) = conn.on_message().next().await {
                    tracing::debug!("GOT MESSAGE FROM REDIS: {:?}", msg);
                }
            }
        }
    });
    let pub_handle = spawn(async move {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        match client.get_async_connection().await {
            Err(e) => {
                tracing::error!("Redis connection failed: {:?}", e);
                // system exit via https://stackoverflow.com/a/54526047/9360856
            }
            Ok(mut conn) => loop {
                let (to, payload) = rx.recv().await.unwrap();
                tracing::debug!("Publishing message to: ${:?}", to);
                tracing::debug!("Publishing message payload: ${:?}", payload);
                let pub_result: redis::RedisResult<()> = conn.publish(&to, &payload).await;
                match pub_result {
                    Err(e) => tracing::error!("Pub failed: ${:?}", e),
                    Ok(r) => tracing::debug!("Pub succeeded: ${:?}", r),
                }
            },
        }
    });

    let pub_tx = tx.clone();
    let consumer_handle = spawn(async move {
        // The topic must exist here otherwise it never consumes
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
                    if let Some(to) = m.key() {
                        if let Some(payload) = m.payload() {
                            tracing::info!("key: '{:?}', payload: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                                m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());

                            // possibly bad to await here (if send is blocked,
                            // then the consumer stops). if we were to spawn
                            // this, we'd need to ack the message in the spawned
                            // task. awaiting is at least nice here since acking
                            // the messages happens in order.
                            let _pub_result =
                                pub_tx.send((to.to_owned(), payload.to_owned())).await;
                        };
                    };
                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                }
            };
        }
    });

    let handles = vec![consumer_handle, pub_handle, sub_handle];
    let _results = join_all(handles).await;
}
