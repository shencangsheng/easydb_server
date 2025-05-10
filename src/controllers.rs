use actix_web::{get, post, web, web::Json, Error, HttpResponse, Responder, Result};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
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

#[derive(Serialize)]
struct QueryResult<V> {
    header: Option<Vec<String>>,
    rows: Option<Vec<Vec<V>>>,
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

pub fn http_response_succeed<V: serde::Serialize>(data: Option<V>, resp_msg: &str) -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(HttpResponseResult {
        resp_msg: resp_msg.to_string(),
        data,
        resp_code: 0,
    }))
}

#[post("/query")]
async fn query(body: Json<Query>) -> Result<HttpResponse, Error> {
    let sql = &body.sql;
    let (statements, sql_type) = database::determine_sql_type(sql)?;
    return match sql_type {
        DML => {
            let ctx = database::register_listing_table(&sql).await?;
            let results = database::execute(ctx, sql).await?;
            if results.is_empty() {
                return http_response_succeed(
                    Some(QueryResult::<String> {
                        header: Some(Vec::new()),
                        rows: Some(Vec::new()),
                        sql_type: Some(DML),
                    }), "",
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
                    .collect::<std::result::Result<Vec<_>, ArrowError>>() {
                    Ok(f) => f,
                    Err(err) => {
                        return Err(DBError::SQLError {
                            message: err.to_string()
                        }.into())
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
            http_response_succeed(
                Some(QueryResult {
                    header: Some(header),
                    rows: Some(rows),
                    sql_type: Some(DML),
                }), "",
            )
        }
        DDL => {
            for statement in statements {
                match statement {
                    Statement::CreateTable(query) => {
                        let location = match query.hive_formats.and_then(|hf| hf.location) {
                            Some(loc) => loc,
                            None => return Err(DBError::SQLError { message: "The location must be present.".to_string() }.into()),
                        };
                        if !utils::is_relative_path(location.as_ref()) {
                            return Err(DBError::SQLError { message: "The location must be a relative path.".to_string() }.into());
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
                            params![table_ref, location, serde_json::to_string(&table_schemas)
                                .map_err(|_| DBError::SQLError { message: sql.to_string()})?, table_comment],
                        ) {
                            return Err(DBError::SQLError { message: err.to_string() }.into());
                        }
                    }
                    _ => {
                        return Err(DBError::SQLError { message: "Unsupported statement.".to_string() }.into());
                    }
                }
            }
            http_response_succeed(
                Some(QueryResult::<String> {
                    rows: None,
                    header: None,
                    sql_type: Some(DDL),
                }), "",
            )
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

    http_response_succeed(
        Some(tables), "",
    )
}

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/db")
            .service(query)
            .service(catalog)
    );
}