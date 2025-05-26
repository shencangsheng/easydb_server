use crate::database;
use crate::database::SqlType::{DDL, DML};
use crate::database::{DBError, SqlType};
use crate::sqlite::insert_query_history;
use crate::utils::{get_encoded_file_name, FileType, HttpError};
use crate::{sqlite, utils};
use actix_web::{get, post, web, web::Json, Error, HttpResponse, Result};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use chrono::{Local, Utc};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::sqlparser::ast::Statement;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Deserialize)]
struct Query {
    sql: String,
}

#[derive(Deserialize)]
struct ExportFile {
    sql: String,
    file_type: FileType,
}

#[derive(Serialize)]
pub struct HttpResponseResult<T> {
    pub(crate) resp_msg: String,
    pub(crate) data: Option<T>,
    pub(crate) resp_code: i32,
}

#[derive(Serialize)]
struct QueryResult<V> {
    header: Option<Vec<String>>,
    rows: Option<Vec<Vec<V>>>,
    sql_type: Option<SqlType>,
    query_time: String,
}

#[derive(Deserialize, Serialize)]
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

#[derive(Serialize)]
struct QueryHistory {
    sql: String,
    status: String,
    created_at: String,
}

pub fn http_response_succeed<V: serde::Serialize>(
    data: Option<V>,
    resp_msg: &str,
) -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(HttpResponseResult {
        resp_msg: resp_msg.to_string(),
        data,
        resp_code: 0,
    }))
}

#[post("/fetch")]
async fn fetch(body: Json<Query>) -> Result<HttpResponse, Error> {
    let sql = &body.sql;
    let (statements, sql_type) = database::determine_sql_type(sql)?;
    let start = Utc::now();
    match sql_type {
        DML => {
            let sql = format!("select * from ({}) limit 200", sql.trim_end_matches(";"));
            let (ctx, execute_sql) = database::register_listing_table(&sql).await?;
            let results = database::execute(&ctx, &execute_sql).await?;
            if results.is_empty() {
                insert_query_history(&body.sql, "successful");
                return http_response_succeed(
                    Some(QueryResult::<String> {
                        header: Some(Vec::new()),
                        rows: Some(Vec::new()),
                        sql_type: Some(DML),
                        query_time: utils::time_difference_from_now(start),
                    }),
                    "",
                );
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
                    .collect::<std::result::Result<Vec<_>, ArrowError>>()
                {
                    Ok(f) => f,
                    Err(err) => {
                        insert_query_history(&body.sql, "fail");
                        return Err(DBError::SQLError {
                            message: err.to_string(),
                        }
                        .into());
                    }
                };
                for row in 0..batch.num_rows() {
                    let mut cells = Vec::new();
                    for (_, formatter) in formatters.iter().enumerate() {
                        cells.push(formatter.value(row).to_string());
                    }
                    rows.push(cells);
                }
            }
            insert_query_history(&body.sql, "successful");
            http_response_succeed(
                Some(QueryResult {
                    header: Some(header),
                    rows: Some(rows),
                    sql_type: Some(DML),
                    query_time: utils::time_difference_from_now(start),
                }),
                "",
            )
        }
        DDL => {
            for statement in statements {
                match statement {
                    Statement::CreateTable(query) => {
                        let location = match query.hive_formats.and_then(|hf| hf.location) {
                            Some(loc) => loc,
                            None => {
                                return Err(DBError::SQLError {
                                    message: "The location must be present.".to_string(),
                                }
                                .into())
                            }
                        };
                        let table_ref = query.name.to_string();
                        let table_schemas: Vec<TableFieldSchema> = query
                            .columns
                            .iter()
                            .map(|column| TableFieldSchema {
                                field: column.name.to_string(),
                                field_type: column.data_type.to_string(),
                                comment: None,
                            })
                            .collect();
                        let table_comment = query.comment.map(|x| x.to_string());
                        let conn = sqlite::conn();
                        if let Err(err) = conn.execute(
                            r#"
                        insert into catalog ( table_ref, table_path, table_schema, table_comment )
                        values
                        (?1, ?2, ?3, ?4)
                        "#,
                            params![
                                table_ref,
                                location,
                                serde_json::to_string(&table_schemas).map_err(|_| {
                                    DBError::SQLError {
                                        message: sql.to_string(),
                                    }
                                })?,
                                table_comment
                            ],
                        ) {
                            return Err(DBError::SQLError {
                                message: err.to_string(),
                            }
                            .into());
                        }
                    }
                    _ => {
                        return Err(DBError::SQLError {
                            message: "Unsupported statement.".to_string(),
                        }
                        .into());
                    }
                }
            }
            http_response_succeed(
                Some(QueryResult::<String> {
                    rows: Some(vec![vec!["successful".to_string()]]),
                    header: Some(vec!["summary".to_string()]),
                    sql_type: Some(DDL),
                    query_time: utils::time_difference_from_now(start),
                }),
                "",
            )
        }
    }
}

#[get("/catalog")]
async fn catalog() -> Result<HttpResponse, Error> {
    let conn = sqlite::conn();
    let mut stmt = conn
        .prepare("select id, table_ref, table_path, table_schema from catalog where type != 'TEMP'")
        .unwrap();
    let catalog_iter = stmt
        .query_map([], |row| {
            let id = row.get::<usize, i32>(0);
            let table_ref = row.get::<usize, String>(1);
            let table_path = row.get::<usize, String>(2);
            let table_schema = row.get::<usize, String>(3);

            if id.is_err() || table_ref.is_err() || table_path.is_err() || table_schema.is_err() {
                Err(rusqlite::Error::InvalidQuery)
            } else {
                let table_schema = match serde_json::from_str(&table_schema?) {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(rusqlite::Error::InvalidQuery);
                    }
                };

                Ok(TableCatalog {
                    id: id?,
                    table_ref: table_ref?,
                    table_path: table_path?,
                    table_schema,
                })
            }
        })
        .map_err(|e| HttpError::InternalServerError {
            error: e.to_string(),
        })?;

    let mut tables = Vec::new();
    for catalog in catalog_iter {
        tables.push(catalog.map_err(|e| HttpError::InternalServerError {
            error: e.to_string(),
        })?);
    }

    http_response_succeed(Some(tables), "")
}

#[post("/query/export")]
async fn fetch_export(body: Json<ExportFile>) -> Result<HttpResponse, Error> {
    let sql = &body.sql;
    let (_, sql_type) = database::determine_sql_type(sql)?;
    match sql_type {
        DML => {
            let (ctx, execute_sql) = database::register_listing_table(&sql).await?;
            let data_frame = database::data_frame(&ctx, &execute_sql).await?;
            let now = Local::now();
            let mut file_path = format!(
                "{}query-{}{}",
                utils::get_os().tmp_dir(),
                now.format("%Y%m%d%H%M%S"),
                now.timestamp_subsec_millis()
            );
            match &body.file_type {
                FileType::JSON | FileType::LOG => {
                    file_path.push_str(".json");
                    data_frame
                        .write_json(&file_path, DataFrameWriteOptions::new(), None)
                        .await
                        .map_err(|e| DBError::SQLError {
                            message: e.to_string(),
                        })?;
                }
                FileType::CSV => {
                    file_path.push_str(".csv");
                    data_frame
                        .write_csv(&file_path, DataFrameWriteOptions::new(), None)
                        .await
                        .map_err(|e| DBError::SQLError {
                            message: e.to_string(),
                        })?;
                }
            }
            let path = Path::new(&file_path);
            match File::open(&path) {
                Ok(mut file) => {
                    let mut contents = Vec::new();
                    let name = get_encoded_file_name(&path).map_err(|e| HttpError::NotFound {
                        file_name: e.to_string(),
                    })?;

                    match file.read_to_end(&mut contents) {
                        Ok(_) => Ok(HttpResponse::Ok()
                            .content_type("application/octet-stream")
                            .append_header(("attachment", format!("filename={}", name)))
                            .body(contents)),
                        Err(_) => Err(Error::from(HttpError::InternalServerError {
                            error: "Could not read file".to_string(),
                        })),
                    }
                }
                Err(_) => Err(HttpError::NotFound {
                    file_name: file_path.to_string(),
                }
                .into()),
            }
        }
        _ => Err(DBError::SQLError {
            message: "Only supports Select SQL".to_string(),
        }
        .into()),
    }
}

#[get("/query/history")]
async fn query_history() -> Result<HttpResponse, Error> {
    let conn = sqlite::conn();
    let mut stmt = conn
        .prepare("select sql, status, created_at from query_history order by id desc limit 30")
        .unwrap();

    let query_history_iter = stmt
        .query_map([], |row| {
            Ok(QueryHistory {
                sql: row.get_unwrap(0),
                status: row.get_unwrap(1),
                created_at: row.get_unwrap(2),
            })
        })
        .unwrap();

    let mut results = Vec::new();
    for query_history in query_history_iter {
        results.push(query_history.unwrap());
    }

    http_response_succeed(Some(results), "")
}

#[get("/health")]
async fn health() -> Result<HttpResponse, Error> {
    http_response_succeed(Some(""), "")
}

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .service(fetch)
            .service(catalog)
            .service(fetch_export)
            .service(query_history)
            .service(health),
    );
}
