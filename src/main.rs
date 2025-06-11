mod controllers;
mod sqlite;

mod data_source;
mod response;
mod server;
mod sql;
mod utils;
mod request;

use actix_web::{middleware, App, HttpServer};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    sqlite::init_db();
    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .configure(controllers::init)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
