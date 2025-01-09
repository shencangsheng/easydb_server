mod controller;

use actix_web::{App, HttpServer, Responder};
use controller::product_controller;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .configure(product_controller::init) // 配置 product 控制器
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}