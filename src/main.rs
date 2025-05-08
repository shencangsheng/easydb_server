mod controllers;
mod database;
mod sqlite;

mod utils;
mod error;

use actix_web::dev;
use std::error::Error;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder, ResponseError, Result};
use actix_web::http::{header, StatusCode};
use actix_web::middleware::{ErrorHandlerResponse, ErrorHandlers};

fn add_error_header<B>(mut res: dev::ServiceResponse<B>) -> Result<ErrorHandlerResponse<B>> {
    res.response_mut().headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("Error"),
    );

    Ok(ErrorHandlerResponse::Response(res.map_into_left_body()))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    sqlite::init_db();
    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .configure(controllers::init)
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}