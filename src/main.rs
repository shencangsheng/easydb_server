mod controllers;
mod sqlite;

mod context;
mod data_source;
mod request;
mod response;
mod server;
mod sql;
mod utils;

use crate::context::session::ConcurrentSessionContext;
use actix_web::{middleware, web, App, HttpServer};
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

struct AppState {
    session: Cache<String, Arc<ConcurrentSessionContext>>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let app_state = web::Data::new(AppState {
        session: Cache::builder()
            .time_to_idle(Duration::from_secs(2 * 60 * 60))
            .build(),
    });

    sqlite::init_db();
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(middleware::Logger::default())
            .configure(controllers::init)
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
