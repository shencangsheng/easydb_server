use crate::response::http_error::Exception;
use crate::server::schema::TableFieldSchema;
use crate::sql::schema::SQLType;
use actix_web::HttpResponse;
use serde::Serialize;

#[derive(Serialize)]
pub struct HttpResponseResult<T> {
    pub resp_msg: String,
    pub data: Option<T>,
    pub resp_code: i32,
}

impl<T: serde::Serialize> HttpResponseResult<T> {
    pub fn success(data: Option<T>, resp_msg: &str) -> actix_web::Result<HttpResponse, Exception> {
        Self::response_json(HttpResponseResult {
            resp_msg: resp_msg.to_string(),
            data,
            resp_code: 0,
        })
    }

    #[allow(dead_code)]
    pub fn fail(
        resp_msg: String,
        resp_code: Option<i32>,
    ) -> actix_web::Result<HttpResponse, Exception> {
        Self::response_json(HttpResponseResult {
            resp_msg: resp_msg.to_string(),
            data: None,
            resp_code: resp_code.unwrap_or(1),
        })
    }

    pub fn response_json(
        result: HttpResponseResult<T>,
    ) -> actix_web::Result<HttpResponse, Exception> {
        Ok(HttpResponse::Ok().json(result))
    }
}

#[derive(Serialize)]
pub struct HttpResponseError {
    pub resp_msg: String,
    pub resp_code: i32,
}

#[derive(Serialize)]
pub struct FetchResult<V> {
    pub header: Option<Vec<String>>,
    pub rows: Option<Vec<Vec<V>>>,
    pub sql_type: Option<SQLType>,
    pub query_time: String,
}

#[derive(Serialize)]
pub struct TableCatalog {
    pub id: i32,
    pub table_ref: String,
    pub table_path: String,
    pub table_schema: Vec<TableFieldSchema>,
}

#[derive(Serialize)]
pub struct FetchHistory {
    pub sql: String,
    pub status: String,
    pub created_at: String,
}
