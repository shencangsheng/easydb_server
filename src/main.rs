mod controllers;
mod database;
mod sqlite;

mod utils;

use actix_web::{App, HttpServer, Responder};
use crate::utils::OperatingSystem;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    sqlite::init_db();
    HttpServer::new(|| {
        App::new()
            .configure(controllers::init)
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}