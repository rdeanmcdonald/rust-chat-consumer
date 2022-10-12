use config::Config;
use futures::future::join_all;
use futures_util::StreamExt as _;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use tokio::spawn;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::{uuid, Uuid};

mod app_config;

async fn pubber(mut rx: Receiver<(Vec<u8>, Vec<u8>)>, config: Config) {
    let url = format!("redis://{}/", config.get_string("redis.host").unwrap());
    let client = redis::Client::open(url).unwrap();
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
}

async fn subber(config: Config) {
    let url = format!("redis://{}/", config.get_string("redis.host").unwrap());
    let client = redis::Client::open(url).unwrap();
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
}

async fn consumer(tx: Sender<(Vec<u8>, Vec<u8>)>, config: Config) {
    // The topic must exist here otherwise it never consumes
    let consumer: StreamConsumer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            &format!(
                "{}:{}",
                config.get_string("kafka.host").unwrap(),
                config.get_string("kafka.port").unwrap()
            ),
        )
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
                        let _pub_result = tx.send((to.to_owned(), payload.to_owned())).await;
                    };
                };
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // cloning config for now, probably a way to send refs, but the tokio spawn
    // requires static lifetime of refs?
    let config = app_config::get_config();
    let (tx, rx) = mpsc::channel(32);
    let pubber_handle = spawn(pubber(rx, config.clone()));
    let subber_handle = spawn(subber(config.clone()));
    let consumer_handle = spawn(consumer(tx.clone(), config.clone()));

    let handles = vec![consumer_handle, pubber_handle, subber_handle];
    let _results = join_all(handles).await;
}
