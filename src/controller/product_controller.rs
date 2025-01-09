use actix_web::{web, HttpResponse, Responder};

// 定义处理函数
async fn get_product() -> impl Responder {
    HttpResponse::Ok().body("Get product")
}

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/product")
            .route("", web::get().to(get_product))
    );
}