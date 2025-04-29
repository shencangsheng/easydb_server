mod handlers;
mod controllers;
mod database;
mod sqlite;

use actix_web::{App, HttpServer, Responder};
use handlers::product_controller;

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