use actix_web::{
    self,
    error::{self, Error},
    main,
    web::{get, Data},
};
use dotenv::dotenv;
use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
use r2d2::{ManageConnection, Pool};
use std::time::Duration;

struct KafkaClientManager {
    address: String,
}

impl KafkaClientManager {
    fn new(address: String) -> Self {
        Self { address }
    }
}

impl ManageConnection for KafkaClientManager {
    type Error = Error;
    type Connection = KafkaClient;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut client = KafkaClient::new(vec![self.address.clone()]);
        client
            .load_metadata_all()
            .map_err(|e| error::ErrorInternalServerError(e))?;
        Ok(client)
    }
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.load_metadata_all().map_or(false, |_| true)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.load_metadata_all()
            .map_err(|e| error::ErrorInternalServerError(e))
    }
}

#[main]
async fn main() -> std::io::Result<()> {
    dotenv().unwrap();
    actix_web::HttpServer::new(move || {
        let kafka_server = dotenv::var("KAFKA_SERVER").unwrap();
        let mgr = KafkaClientManager::new(kafka_server);
        let pool = Pool::new(mgr).unwrap();
        actix_web::App::new()
            .app_data(Data::new(pool))
            .route("/nearby_locations", get().to(nearby_locations))
    })
    .bind("localhost:8001")?
    .run()
    .await
}

async fn nearby_locations(client: Data<Pool<KafkaClientManager>>) -> Result<String, Error> {
    let msgs = vec![ProduceMessage::new(
        "nearby_locations",
        0,
        None,
        Some(b"hello"),
    )];
    client
        .get()
        .map_err(|e| error::ErrorInternalServerError(e))?
        .produce_messages(RequiredAcks::One, Duration::new(5, 0), msgs)
        .map_err(|e| error::ErrorInternalServerError(e))?;
    Ok("success".into())
}
