use actix_web::{
    self,
    error::{self, Error},
    main,
    web::{get, Data},
};
use dotenv::dotenv;
use rdkafka::{
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use std::time::Duration;

#[main]
async fn main() -> std::io::Result<()> {
    dotenv().unwrap();
    actix_web::HttpServer::new(move || {
        actix_web::App::new().route("/nearby_locations", get().to(nearby_locations))
    })
    .bind("localhost:8001")?
    .run()
    .await
}

async fn nearby_locations() -> Result<String, Error> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:12092")
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();
    let status = producer
        .send(
            FutureRecord::to("nearby_locations")
                .key("key")
                .payload("hello"),
            Duration::from_secs(0),
        )
        .await
        .unwrap();
    Ok(format!("partition: {} offset: {}", status.0, status.1))
}
