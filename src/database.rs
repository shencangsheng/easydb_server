use arrow_array::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use rusqlite::{params, params_from_iter};
use crate::sqlite;

pub fn session() -> SessionContext {
    SessionContext::new()
}

struct TableSchema {
    table_name: String,
    table_path: String,
}

pub async fn register_listing_table(tables: &Vec<String>) -> SessionContext {
    let conn = sqlite::conn();
    let placeholders = tables.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let sql = format!("SELECT table_name, table_path FROM table_schema WHERE table_name IN ({})", placeholders);
    let mut stmt = conn.prepare(&sql).unwrap();
    let results = stmt.query_map(params_from_iter(tables.iter().map(|s| s.as_str())), |row| {
        Ok(TableSchema {
            table_name: row.get(0)?,
            table_path: row.get(1)?,
        })
    }).unwrap();

    let ctx = session();
    for item in results {
        match item {
            Ok(v) => {
                register(&v.table_name, &v.table_path, &ctx, CsvReadOptions::new()).await;
            }
            _ => {}
        }
    }
    ctx
}

pub async fn register(table_ref: &String, table_path: &String, ctx: &SessionContext, options: CsvReadOptions<'_>) {
    println!("{}: {}", table_ref, table_path);
    ctx.register_csv(table_ref, table_path, options).await.expect("TODO: panic message");
}

pub async fn execute(ctx: SessionContext, sql: &String) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await;
    df.unwrap().collect().await.unwrap()
}