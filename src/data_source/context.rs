use crate::data_source::excel::from_file_to_record_batch;
use crate::data_source::schema::DataSourceFormat;
use crate::data_source::utils::get_format;
use crate::response::http_error::Exception;
use crate::server::schema::TableCatalog;
use crate::sql::parse::get_table_names;
use crate::sql::sql_error::SQLError;
use crate::utils::get_os;
use crate::{sqlite, utils};
use arrow_array::RecordBatch;
use chrono::Utc;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{CsvReadOptions, NdJsonReadOptions, SessionContext};
use rusqlite::{params, params_from_iter};
use std::env;

pub fn session() -> SessionContext {
    let ctx = SessionContext::new();
    // ctx.copied_config()
    //     .options_mut()
    //     .execution
    //     .listing_table_ignore_subdirectory = false;
    ctx
}

pub async fn get_data_frame(ctx: &SessionContext, sql: &String) -> Result<DataFrame, Exception> {
    ctx.sql(sql).await.map_err(Exception::from)
}

pub fn get_data_dir() -> String {
    env::var("DATA_DIR").unwrap_or(get_os().default_data_dir().to_string())
}

pub async fn register_table(
    table_ref: &String,
    table_path: &String,
    ctx: &SessionContext,
) -> Result<(), Exception> {
    let data_source_format = get_format(table_path);
    let table_path = if utils::is_relative_path(table_path) {
        format!("{}/{}", get_data_dir(), table_path)
    } else {
        table_path.to_string()
    };

    match data_source_format {
        Some(format) => match format {
            DataSourceFormat::CSV => {
                ctx.register_csv(table_ref, &table_path, CsvReadOptions::default())
                    .await?;
            }
            DataSourceFormat::TSV => {
                let mut options = CsvReadOptions::default();
                options.delimiter = b'\t';
                options.file_extension = ".tsv";
                ctx.register_csv(table_ref, &table_path, options).await?;
            }
            DataSourceFormat::JSON => {
                return Err(Exception::bad_request_error(
                    "JSON files are currently not supported.",
                ))
            }
            DataSourceFormat::NdJson { file_extension } => {
                let mut options = NdJsonReadOptions::default();
                options.file_extension = &file_extension;
                ctx.register_json(table_ref, &table_path, options).await?;
            }
            DataSourceFormat::XLSX => {
                ctx.register_batch(table_ref, from_file_to_record_batch(&table_path)?)?;
            }
        },
        None => {
            return Err(Exception::unprocessable_entity_error(format!(
                "{:?}",
                data_source_format
            )));
        }
    }

    Ok(())
}

pub async fn register_listing_table(sql: &String) -> Result<(SessionContext, String), Exception> {
    let mut sql = sql.clone();
    let table_names = get_table_names(&sql)?;
    if table_names.is_empty() {
        return Err(SQLError::sql_syntax_error("Table name is empty", &sql))?;
    }

    let ctx = session();
    let conn = sqlite::conn();
    let mut tables: Vec<String> = Vec::new();

    let temp_tables = table_names
        .iter()
        .filter(|name| match { get_format(name) } {
            Some(_) => true,
            None => {
                tables.push(name.to_string());
                false
            }
        })
        .map(|name| TableCatalog {
            table_name: format!(
                "temp_{}_{}",
                Utc::now().timestamp(),
                utils::generate_random_string(4)
            ),
            table_path: name.to_string(),
        })
        .collect::<Vec<TableCatalog>>();

    if !temp_tables.is_empty() {
        for table in temp_tables {
            conn.execute(
                r#"
                        insert into catalog ( table_ref, table_path, type )
                        values
                        (?1, ?2, ?3)
                        "#,
                params![
                    &table.table_name,
                    &table.table_path.replace("'", ""),
                    "TEMP"
                ],
            )?;

            sql = sql.replace(&table.table_path, &table.table_name);
            tables.push(table.table_name);
        }
    }

    let placeholders = table_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let catalog_sql = format!(
        "SELECT table_ref, table_path FROM catalog WHERE table_ref IN ({})",
        placeholders
    );

    let mut stmt = conn.prepare(&catalog_sql)?;
    let results = stmt.query_map(params_from_iter(tables.iter().map(|s| s.as_str())), |row| {
        Ok(TableCatalog {
            table_name: row.get(0)?,
            table_path: row.get(1)?,
        })
    })?;

    for item in results {
        let item = item?;
        register_table(&item.table_name, &item.table_path, &ctx).await?;
    }
    Ok((ctx, sql))
}

pub async fn execute(ctx: &SessionContext, sql: &String) -> Result<Vec<RecordBatch>, Exception> {
    let data_frame = get_data_frame(&ctx, sql).await?;
    data_frame.collect().await.map_err(Exception::from)
}
