use std::collections::HashMap;
use std::fmt::Display;
use actix_web::{get, post, web, web::Json, HttpResponse, Responder, Result};
use actix_web::body::{MessageBody};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::cse::FoundCommonNodes::No;
use datafusion::logical_expr::sqlparser::ast::Statement;
use serde::{Deserialize, Serialize};
use crate::{sqlite, utils};
use rusqlite::params;
use crate::database;
use crate::database::{DBError, SqlType};
use crate::database::SqlType::{DML, DDL};

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
        write!(f, "{}", format!("{{data: {:?}, resp_msg: {}, resp_code: {}}}", self.data, self.resp_msg, self.resp_code))
    }
}

#[derive(Serialize)]
struct QueryResult<V> {
    header: Option<Vec<String>>,
    rows: Option<Vec<V>>,
    sql_type: Option<SqlType>,
}

#[derive(Deserialize)]
#[derive(Serialize)]
pub struct TableFieldSchema {
    pub(crate) field: String,
    pub(crate) field_type: String,
    pub(crate) comment: Option<String>,
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

#[post("/query")]
async fn query(body: Json<Query>) -> Result<impl Responder> {
    let sql = &body.sql;
    let (statements, sql_type) = database::determine_sql_type(sql)?;
    let ctx = database::register_listing_table(&sql).await?;
    return match sql_type {
        DML => {
            let results = match database::execute(ctx, sql).await {
                Ok(c) => c,
                Err(err) => return Ok(error_response(err)),
            };
            if results.is_empty() {
                return Ok(HttpResponse::Ok().json(HttpResponseResult {
                    resp_msg: "".to_string(),
                    data: Some(QueryResult::<String> {
                        header: Some(Vec::new()),
                        rows: Some(Vec::new()),
                        sql_type: Some(DML),
                    }),
                    resp_code: 0,
                }));
            }
            let options = FormatOptions::default().with_null("null");
            let schema = results[0].schema();
            let mut rows = Vec::new();
            let mut header = Vec::new();
            for field in schema.fields() {
                header.push(field.name().to_string());
            }
            for batch in results {
                let formatters = match batch
                    .columns()
                    .iter()
                    .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
                    .collect::<std::result::Result<Vec<_>, ArrowError>>() {
                    Ok(f) => f,
                    Err(err) => {
                        return Ok(error_response(err));
                    }
                };
                for row in 0..batch.num_rows() {
                    let mut cells = Vec::new();
                    for (index, formatter) in formatters.iter().enumerate() {
                        cells.push(formatter.value(row).to_string());
                    }
                    rows.push(cells);
                }
            }
            Ok(HttpResponse::Ok().json(HttpResponseResult {
                resp_msg: "".to_string(),
                data: Some(QueryResult {
                    header: Some(header),
                    rows: Some(rows),
                    sql_type: Some(DML),
                }),
                resp_code: 0,
            }))
        }
        DDL => {
            for statement in statements {
                match statement {
                    Statement::CreateTable(query) => {
                        let location = match query.hive_formats.and_then(|hf| hf.location) {
                            Some(loc) => loc,
                            None => return Ok(error_response("The location must be present.".to_string())),
                        };
                        if !utils::is_relative_path(location.as_ref()) {
                            return Ok(error_response("The location must be a relative path.".to_string()));
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
                        if let Err(err) = conn.execute(
                            r#"
                        insert into catalog ( table_ref, table_path, table_schema, table_comment )
                        values
                        (?1, ?2, ?3, ?4)
                        "#,
                            params![table_ref, location, serde_json::to_string(&table_schemas).unwrap(), table_comment],
                        ) {
                            return Ok(error_response(err.to_string()));
                        }
                    }
                    _ => {
                        return Ok(error_response("Unsupported statement.".to_string()));
                    }
                }
            }
            Ok(HttpResponse::Ok().json(HttpResponseResult::<QueryResult<String>> {
                resp_msg: "".to_string(),
                data: Some(QueryResult {
                    rows: None,
                    header: None,
                    sql_type: Some(DML),
                }),
                resp_code: 0,
            }))
        }
    };
}

#[get("/catalog")]
async fn catalog() -> Result<impl Responder> {
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
            .service(query)
            .service(catalog)
    );
}