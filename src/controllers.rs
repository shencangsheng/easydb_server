use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fmt::Display;
use std::io::Bytes;
use std::pin::Pin;
use std::task::{Context, Poll};
use actix_web::{get, post, web, web::Json, HttpResponse, Responder, Result};
use actix_web::body::{BodySize, MessageBody};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::logical_expr::sqlparser::ast::Statement;
use datafusion::logical_expr::sqlparser::dialect::AnsiDialect;
use datafusion::logical_expr::sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};
use crate::{sqlite, utils};
use rusqlite::params;
use crate::database;
use crate::database::DBError;

#[derive(Deserialize)]
struct Query {
    sql: String,
}

#[derive(Serialize)]
pub struct HttpResponseResult<T> {
    pub(crate) resp_msg: String,
    pub(crate) data: Option<T>,
    pub(crate) resp_code: i32,
}

impl<T: Serialize + std::fmt::Debug> Display for HttpResponseResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("data: {:?}, resp_msg: {}, resp_code: {}", self.data, self.resp_msg, self.resp_code))
    }
}

#[derive(Serialize)]
struct QueryResult<V> {
    header: Vec<String>,
    rows: Vec<HashMap<String, V>>,
}

#[derive(Deserialize)]
#[derive(Serialize)]
pub struct TableFieldSchema {
    pub(crate) field: String,
    pub(crate) field_type: String,
    pub(crate) comment: Option<String>,
}

#[derive(Deserialize)]
struct DDL {
    db: Option<String>,
    table_name: String,
    table_path: String,
    table_schemas: Vec<TableFieldSchema>,
    auto_schema: bool,
}

#[derive(Serialize)]
struct TableCatalog {
    id: i32,
    table_ref: String,
    table_path: String,
    table_schema: Vec<TableFieldSchema>,
}

pub fn error_response<E: std::fmt::Debug>(err: E) -> HttpResponse {
    HttpResponse::BadRequest().json(HttpResponseResult::<String> {
        resp_msg: format!("Error: {:?}", err),
        data: None,
        resp_code: 1,
    })
}

pub fn error_message_response<E: std::fmt::Debug>(err_message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(HttpResponseResult::<String> {
        resp_msg: format!("Error: {:?}", err_message),
        data: None,
        resp_code: 1,
    })
}

#[post("/dml")]
async fn dml(body: Json<Query>) -> Result<impl Responder> {
    let sql = &body.sql;
    let ctx = database::register_listing_table(&sql).await?;
    let cols = database::execute(ctx, sql).await?;

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
                return Ok(error_response(err));
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
    Ok(HttpResponse::Ok().json(HttpResponseResult {
        resp_msg: "".to_string(),
        data: Some(QueryResult {
            header,
            rows,
        }),
        resp_code: 0,
    }))
}

#[post("/ddl")]
async fn ddl(body: Json<Query>) -> Result<impl Responder, DBError> {
    let statements = database::parse_sql(&body.sql)?;
    for statement in statements {
        match statement {
            Statement::CreateTable(query) => {
                let location = query.hive_formats.unwrap().location.unwrap();
                if !utils::is_relative_path(location.as_ref()) {
                    return Err(DBError::SQLError { message: "The location must be present.".to_string() });
                }
                let table_ref = query.name.to_string();
                let table_schemas: Vec<TableFieldSchema> = query.columns.iter().map(|column| {
                    TableFieldSchema {
                        field: column.name.to_string(),
                        field_type: column.data_type.to_string(),
                        comment: None,
                    }
                }).collect();
                let table_comment = query.comment.map(|x| x.to_string());
                let conn = sqlite::conn();
                conn.execute(
                    r#"
                        insert into catalog ( table_ref, table_path, table_schema, table_comment )
                        values
                        (?1, ?2, ?3, ?4)
                        "#,
                    params![table_ref, location, serde_json::to_string(&table_schemas).unwrap(), table_comment],
                ).unwrap();
            }
            _ => {}
        }
    }

    Ok(HttpResponse::Ok().json(HttpResponseResult::<String> {
        resp_msg: "".to_string(),
        resp_code: 0,
        data: None,
    }))
}

#[get("/catalog")]
async fn catalog() -> Result<impl Responder, DBError> {
    let conn = sqlite::conn();
    let mut stmt = conn.prepare("select id, table_ref, table_path, table_schema from catalog").unwrap();
    let catalog_iter = stmt.query_map([], |row| {
        Ok(TableCatalog {
            id: row.get_unwrap(0),
            table_ref: row.get_unwrap(1),
            table_path: row.get_unwrap(2),
            table_schema: serde_json::from_str(&row.get_unwrap::<usize, String>(3)).unwrap(),
        })
    }).unwrap();

    let mut tables = Vec::new();
    for catalog in catalog_iter {
        tables.push(catalog.unwrap());
    }

    Ok(HttpResponse::Ok().json(HttpResponseResult::<Vec<TableCatalog>> {
        resp_msg: "".to_string(),
        resp_code: 0,
        data: Option::from(tables),
    }))
}

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/db")
            .service(dml)
            .service(catalog)
            .service(ddl)
    );
}