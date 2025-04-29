use arrow_array::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionContext};

pub fn session() -> SessionContext {
    SessionContext::new()
}

pub async fn register(table_ref: String, table_path: String, ctx: Option<SessionContext>, options: Option<CsvReadOptions<'_>>,) -> SessionContext {
    let ctx = ctx.unwrap_or(session());
    ctx.register_csv(table_ref, table_path, options.unwrap_or(CsvReadOptions::new())).await.expect("TODO: panic message");
    ctx
}

pub async fn execute(ctx: SessionContext, sql: &String) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await;
    df.unwrap().collect().await.unwrap()
}