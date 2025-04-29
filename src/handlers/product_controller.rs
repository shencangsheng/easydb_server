// use std::collections::HashMap;
// use std::ptr::null;
// use actix_web::{web, HttpResponse, Responder};
// use arrow::error::ArrowError;
// use arrow::util::display::{ArrayFormatter, FormatOptions};
// use arrow_array::RecordBatch;
// use serde::{Deserialize, Serialize};
// use serde_json::Value;
//
// use crate::models::common::HttpResponseResult;
//
//
// #[derive(Deserialize)]
// struct MyRequest {
//     sql: String,
// }
//
// #[derive(Serialize)]
// struct MyResponse<T> {
//     message: String,
//     data: Option<T>,
// }
//
// #[derive(Serialize)]
// struct Table<V> {
//     header: Vec<String>,
//     rows: Vec<HashMap<String, V>>,
// }
//
// // 定义处理函数
// async fn get_product(req_body: web::Json<MyRequest>) -> impl Responder {
//     let sql = &req_body.sql;
//     let mut db: HashMap<String, Value> = HashMap::new();
//     let results: datafusion::common::Result<Vec<RecordBatch>> = select(sql).await;
//     let options = FormatOptions::default().with_null("null");
//     match results {
//         Ok(value) => {
//             let schema = value[0].schema();
//             let mut table = Vec::new();
//             let mut header = Vec::new();
//             for field in schema.fields() {
//                 header.push(field.name().to_string());
//             }
//             for batch in value {
//                 let formatters = match batch
//                     .columns()
//                     .iter()
//                     .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
//                     .collect::<Result<Vec<_>, ArrowError>>() {
//                     Ok(f) => f,
//                     Err(err) => {
//                         return web::Json(HttpResponseResult {
//                             resp_msg: format!("Error formatting data: {:?}", err),
//                             data: None,
//                             resp_code: 1
//                         });
//                     }
//                 };
//
//                 for row in 0..batch.num_rows() {
//                     let mut cells = HashMap::new();
//                     for (index, formatter) in formatters.iter().enumerate() {
//                         cells.insert(header.get(index).unwrap().clone(), formatter.value(row).to_string());
//                     }
//                     table.push(cells);
//                 }
//             }
//             web::Json(MyResponse {
//                 message: "".to_string(),
//                 data: Some(Table {
//                     header,
//                     rows: table,
//                 }),
//             })
//         }
//         Err(err) => {
//             web::Json(MyResponse {
//                 message: format!("Error: {:?}", err),
//                 data: None,
//             })
//         }
//     }
// }
//
// pub fn init(cfg: &mut web::ServiceConfig) {
//     cfg.service(
//         web::scope("/product")
//             .route("", web::post().to(get_product))
//     );
// }