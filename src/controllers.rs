use crate::data_source::context::{execute, get_data_frame, register_listing_table};
use crate::request::body::{ExportFile, Fetch};
use crate::response::http_error::Exception;
use crate::response::schema::{FetchHistory, FetchResult, HttpResponseResult, TableCatalog};
use crate::response::utils::get_encoded_file_name;
use crate::server::schema::TableFieldSchema;
use crate::sql::parse::get_sql_type;
use crate::sql::schema::SQLType;
use crate::sqlite::insert_query_history;
use crate::utils::FileType;
use crate::{sqlite, utils};
use actix_web::{get, post, web, web::Json, HttpResponse, Result};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use chrono::{Local, Utc};
use datafusion::config::CsvOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::sqlparser::ast::Statement;
use rusqlite::params;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[post("/fetch")]
async fn fetch(body: Json<Fetch>) -> Result<HttpResponse, Exception> {
    let sql = body.sql.trim();
    let (statements, sql_type) = get_sql_type(sql)?;
    let start = Utc::now();
    match sql_type {
        SQLType::DML => {
            let sql = format!("select * from ({}) limit 200", sql.trim_end_matches(";"));
            let (ctx, execute_sql) = register_listing_table(&sql).await?;

            let results = execute(&ctx, &execute_sql).await?;
            if results.is_empty() {
                insert_query_history(&body.sql, "successful");

                return HttpResponseResult::success(
                    Some(FetchResult::<String> {
                        header: Some(Vec::new()),
                        rows: Some(Vec::new()),
                        sql_type: Some(SQLType::DML),
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
                let formatters = batch
                    .columns()
                    .iter()
                    .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
                    .collect::<std::result::Result<Vec<_>, ArrowError>>()
                    .map_err(|err| {
                        insert_query_history(&body.sql, "fail");
                        err
                    })?;

                for row in 0..batch.num_rows() {
                    let mut cells = Vec::new();
                    for (_, formatter) in formatters.iter().enumerate() {
                        cells.push(formatter.value(row).to_string());
                    }
                    rows.push(cells);
                }
            }

            insert_query_history(&body.sql, "successful");

            HttpResponseResult::success(
                Some(FetchResult {
                    header: Some(header),
                    rows: Some(rows),
                    sql_type: Some(SQLType::DML),
                    query_time: utils::time_difference_from_now(start),
                }),
                "",
            )
        }
        SQLType::DDL => {
            for statement in statements {
                match statement {
                    Statement::CreateTable(query) => {
                        let location = match query.hive_formats.and_then(|hf| hf.location) {
                            Some(loc) => loc,
                            None => {
                                return Err(Exception::unprocessable_entity_error(
                                    "The location must be present.",
                                ));
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
                        conn.execute(
                            r#"
                        insert into catalog ( table_ref, table_path, table_schema, table_comment )
                        values
                        (?1, ?2, ?3, ?4)
                        "#,
                            params![
                                table_ref,
                                location,
                                serde_json::to_string(&table_schemas)?,
                                table_comment
                            ],
                        )?;
                    }
                    _ => {
                        return Err(Exception::unprocessable_entity_error(
                            "unprocessable_entity_error",
                        ));
                    }
                }
            }

            HttpResponseResult::success(
                Some(FetchResult::<String> {
                    rows: Some(vec![vec!["successful".to_string()]]),
                    header: Some(vec!["summary".to_string()]),
                    sql_type: Some(SQLType::DDL),
                    query_time: utils::time_difference_from_now(start),
                }),
                "",
            )
        }
    }
}

#[get("/catalog")]
async fn catalog() -> Result<HttpResponse, Exception> {
    let conn = sqlite::conn();
    let mut stmt = conn.prepare(
        "select id, table_ref, table_path, table_schema from catalog where type != 'TEMP'",
    )?;

    let catalog_iter = stmt.query_map([], |row| {
        let id = row.get::<usize, i32>(0)?;
        let table_ref = row.get::<usize, String>(1)?;
        let table_path = row.get::<usize, String>(2)?;
        let table_schema = row.get::<usize, String>(3)?;
        let table_schema = serde_json::from_str(&table_schema).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Blob, Box::new(e))
        })?;

        Ok(TableCatalog {
            id,
            table_ref,
            table_path,
            table_schema,
        })
    })?;

    let mut tables = Vec::new();
    for catalog in catalog_iter {
        tables.push(catalog?);
    }

    HttpResponseResult::success(Some(tables), "")
}

#[post("/query/export")]
async fn fetch_export(body: Json<ExportFile>) -> Result<HttpResponse, Exception> {
    let sql = &body.sql;
    let (_, sql_type) = get_sql_type(sql)?;
    match sql_type {
        SQLType::DML => {
            let (ctx, execute_sql) = register_listing_table(&sql).await?;
            let data_frame = get_data_frame(&ctx, &execute_sql).await?;
            let now = Local::now();
            let mut file_path = format!(
                "{}query-{}{}",
                utils::get_os().tmp_dir(),
                now.format("%Y%m%d%H%M%S"),
                now.timestamp_subsec_millis()
            );

            match &body.file_type {
                FileType::JSON | FileType::DnJson => {
                    file_path.push_str(".json");
                    data_frame
                        .write_json(&file_path, DataFrameWriteOptions::new(), None)
                        .await?;
                }
                FileType::CSV => {
                    file_path.push_str(".csv");
                    data_frame
                        .write_csv(&file_path, DataFrameWriteOptions::new(), None)
                        .await?;
                }
                FileType::TSV => {
                    file_path.push_str(".tsv");
                    let mut options = CsvOptions::default();
                    options.delimiter = b'\t';
                    data_frame
                        .write_csv(&file_path, DataFrameWriteOptions::new(), None)
                        .await?;
                }
            }
            let path = Path::new(&file_path);
            match File::open(&path) {
                Ok(mut file) => {
                    let mut contents = Vec::new();
                    let name = get_encoded_file_name(&path)
                        .map_err(|e| Exception::unprocessable_entity_error(e))?;

                    match file.read_to_end(&mut contents) {
                        Ok(_) => Ok(HttpResponse::Ok()
                            .content_type("application/octet-stream")
                            .append_header(("attachment", format!("filename={}", name)))
                            .body(contents)),
                        Err(_) => Err(Exception::internal_server_error("Could not read file")),
                    }
                }
                Err(_) => Err(Exception::file_not_found_error(file_path.to_string())),
            }
        }
        _ => Err(Exception::unprocessable_entity_error(
            "Only supports Select SQL",
        )),
    }
}

#[get("/query/history")]
async fn query_history() -> Result<HttpResponse, Exception> {
    let conn = sqlite::conn();
    let mut stmt = conn
        .prepare("select sql, status, created_at from query_history order by id desc limit 30")?;

    let query_history_iter = stmt.query_map([], |row| {
        Ok(FetchHistory {
            sql: row.get(0)?,
            status: row.get(1)?,
            created_at: row.get(2)?,
        })
    })?;

    let mut results = Vec::new();
    for query_history in query_history_iter {
        results.push(query_history?);
    }

    HttpResponseResult::success(Some(results), "")
}

#[get("/health")]
async fn health() -> Result<HttpResponse, Exception> {
    HttpResponseResult::success(Some(""), "")
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
