use std::env;
use arrow_array::RecordBatch;
use datafusion::logical_expr::sqlparser::ast::{Expr, Statement, TableFactor, TableWithJoins};
use datafusion::logical_expr::sqlparser::dialect::AnsiDialect;
use datafusion::logical_expr::sqlparser::parser::Parser;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion::sql::sqlparser::ast::{Query, SetExpr};
use rusqlite::{params, params_from_iter};
use crate::{sqlite, utils};
use crate::controllers::TableFieldSchema;
use crate::utils::get_os;

pub fn session() -> SessionContext {
    SessionContext::new()
}

struct TableSchema {
    table_name: String,
    table_path: String,
}

pub async fn register_listing_table(sql: &String) -> SessionContext {
    let table_names = sql_to_table_names(sql);
    let conn = sqlite::conn();
    let placeholders = table_names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let sql = format!("SELECT table_ref, table_path FROM catalog WHERE table_ref IN ({})", placeholders);
    let mut stmt = conn.prepare(&sql).unwrap();
    let results = stmt.query_map(params_from_iter(table_names.iter().map(|s| s.as_str())), |row| {
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

pub fn sql_to_table_names(sql: &String) -> Vec<String> {
    let dialect = AnsiDialect {};

    // 解析 SQL 语句
    let statements = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");

    // 存储表名的集合
    let mut table_names = Vec::new();

    for statement in statements {
        match statement {
            Statement::Query(query) => {
                extract_table_names_from_query(&query, &mut table_names);
            }
            _ => {}
        }
    }

    table_names
}

/// 从查询中提取表名
/// Extract table names from the query
fn extract_table_names_from_query(query: &Query, table_names: &mut Vec<String>) {
    // 处理 SELECT 语句
    // Handle SELECT statements
    if let SetExpr::Select(select) = &*query.body {
        for table_with_joins in &select.from {
            extract_table_names_from_table_with_joins(table_with_joins, table_names);
        }

        // 处理 WHERE 子句中的子查询
        // Handle subqueries in the WHERE clause
        if let Some(selection) = &select.selection {
            extract_table_names_from_expr(selection, table_names);
        }
    }

    // 递归处理子查询
    // Recursively handle subqueries
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_table_names_from_query(&cte.query, table_names);
        }
    }

    // 处理 UNION 或其他组合查询
    // Handle UNION or other combined queries
    if let SetExpr::Query(query) = &*query.body {
        extract_table_names_from_query(query, table_names);
    }
}

/// 从带有连接的表中提取表名
/// Extract table names from tables with joins
fn extract_table_names_from_table_with_joins(table_with_joins: &TableWithJoins, table_names: &mut Vec<String>) {
    extract_table_names_from_table_factor(&table_with_joins.relation, table_names);

    for join in &table_with_joins.joins {
        extract_table_names_from_table_factor(&join.relation, table_names);
    }
}

/// 从表因子中提取表名
/// Extract table names from table factors
fn extract_table_names_from_table_factor(table_factor: &TableFactor, table_names: &mut Vec<String>) {
    match table_factor {
        // 处理普通表
        // Handle regular tables
        TableFactor::Table { name, .. } => {
            table_names.push(name.to_string());
        }
        // 处理派生表（子查询）
        // Handle derived tables (subqueries)
        TableFactor::Derived { subquery, .. } => {
            extract_table_names_from_query(subquery, table_names);
        }
        _ => {}
    }
}

/// 从表达式中提取表名
/// Extract table names from expressions
fn extract_table_names_from_expr(expr: &Expr, table_names: &mut Vec<String>) {
    match expr {
        // 处理子查询
        // Handle subqueries
        Expr::Subquery(query) => {
            extract_table_names_from_query(query, table_names);
        }
        // 处理二元操作符
        // Handle binary operations
        Expr::BinaryOp { left, right, .. } => {
            extract_table_names_from_expr(left, table_names);
            extract_table_names_from_expr(right, table_names);
        }
        // 处理 EXISTS 子查询
        // Handle EXISTS subqueries
        Expr::Exists { subquery, .. } => {
            extract_table_names_from_query(subquery, table_names);
        }
        // 处理 IN 子查询
        // Handle IN subqueries
        Expr::InSubquery { subquery, .. } => {
            extract_table_names_from_query(subquery, table_names);
        }
        _ => {}
    }
}

pub fn test() {
    let dialect = AnsiDialect {};

    let statements = Parser::parse_sql(&dialect, "CREATE TABLE user6 ( id int ) LOCATION 'xxx' COMMENT 'asdsa'").expect("SQL parsing failed");

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
}

pub fn get_data_dir() -> String {
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|e| get_os().default_data_dir().to_string());

    data_dir
}