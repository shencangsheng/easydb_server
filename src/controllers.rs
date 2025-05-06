use std::collections::HashMap;
use actix_web::{web, web::Json, HttpResponse};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::RecordBatch;
use datafusion::logical_expr::sqlparser::ast::Statement;
use datafusion::logical_expr::sqlparser::dialect::AnsiDialect;
use datafusion::logical_expr::sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};
use crate::{sqlite, utils};
use rusqlite::params;
use crate::database;

#[derive(Deserialize)]
struct Query {
    sql: String,
}

#[derive(Serialize)]
struct HttpResponseResult<T> {
    resp_msg: String,
    data: Option<T>,
    resp_code: i32,
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
        data: Some(QueryResult {
            header,
            rows,
        }),
        resp_code: 0,
    })
}

async fn ddl(body: Json<Query>) -> HttpResponse {
    let dialect = AnsiDialect {};
    let statements = Parser::parse_sql(&dialect, &body.sql).expect("SQL parsing failed");
    for statement in statements {
        match statement {
            Statement::CreateTable(query) => {
                let location = query.hive_formats.unwrap().location.expect("The location must be present.");
                if !utils::is_relative_path(location.as_str()) {
                    panic!("Path '{}' is not a relative path", location);
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
                ).expect("TODO: panic message");
            }
            _ => {}
        }
    }

    HttpResponse::Ok().json(HttpResponseResult::<String> {
        resp_msg: "".to_string(),
        resp_code: 0,
        data: None,
    })
}

async fn catalog() -> HttpResponse {
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

    HttpResponse::Ok().json(HttpResponseResult::<Vec<TableCatalog>> {
        resp_msg: "".to_string(),
        resp_code: 0,
        data: Option::from(tables),
    })
}

pub fn init(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/db")
            .route("dml", web::post().to(dml))
            .route("ddl", web::post().to(ddl))
            .route("catalog", web::get().to(catalog))
    );
}