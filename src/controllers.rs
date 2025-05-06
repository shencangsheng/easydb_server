use std::collections::HashMap;
use actix_web::{web, web::Json, HttpResponse};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use crate::{sqlite};
use rusqlite::params;
use crate::database;

#[derive(Deserialize)]
struct Query {
    sql: String
}

#[derive(Serialize)]
struct HttpResponseResult<T> {
    resp_msg: String,
    data: Option<T>,
    resp_code: i32,
}

#[derive(Serialize)]
struct SelectResult<V> {
    header: Vec<String>,
    rows: Vec<HashMap<String, V>>,
}

#[derive(Deserialize)]
#[derive(Serialize)]
struct TableSchema {
    field: String,
    field_type: String,
    comment: String,
}

#[derive(Deserialize)]
struct DDL {
    db: Option<String>,
    table_name: String,
    table_path: String,
    table_schemas: Vec<TableSchema>,
    auto_schema: bool,
}

pub fn error_response<E: std::fmt::Debug>(err: E) -> HttpResponse {
    HttpResponse::BadRequest().json(HttpResponseResult::<String> {
        resp_msg: format!("Error: {:?}", err),
        data: None,
        resp_code: 1,
    })
}

async fn dml(body: Json<Query>) -> HttpResponse {
    let sql = &body.sql;
    let ctx = database::register_listing_table(&sql).await;
    let cols: Vec<RecordBatch> = database::execute(ctx, sql).await;
    let options = FormatOptions::default().with_null("null");
    let schema = cols[0].schema();
    let mut rows = Vec::new();
    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(field.name().to_string());
    }
    for col in cols {
        let formatters = match col
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, ArrowError>>() {
            Ok(f) => f,
            Err(err) => {
                return error_response(err);
            }
        };
        for row in 0..col.num_rows() {
            let mut cells = HashMap::new();
            for (index, formatter) in formatters.iter().enumerate() {
                cells.insert(header.get(index).unwrap().clone(), formatter.value(row).to_string());
            }
            rows.push(cells);
        }
    }
    HttpResponse::Ok().json(HttpResponseResult {
        resp_msg: "".to_string(),
        data: Some(SelectResult {
            header,
            rows,
        }),
        resp_code: 0,
    })
}

async fn ddl(body: Json<DDL>) -> HttpResponse {
    let conn = sqlite::conn();
    let table_ref = &body.table_name;
    conn.execute(
        r#"
        insert into table_schema ( table_ref, table_path, schema )
        values
        (?1, ?2, ?3)
        "#,
        params![table_ref, &body.table_path, serde_json::to_string(&body.table_schemas).unwrap()],
    ).expect("TODO: panic message");

    HttpResponse::Ok().json(HttpResponseResult::<String> {
        resp_msg: "".to_string(),
        resp_code: 0,
        data: None,
    })
}

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/db")
            .route("dml", web::post().to(dml))
            .route("ddl", web::post().to(ddl))
    );
}