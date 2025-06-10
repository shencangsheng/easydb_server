mod controllers;
mod database;
mod sqlite;

mod utils;
mod response;
mod sql;
mod data_source;
mod server;

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