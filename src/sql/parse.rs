use crate::response::http_error::Exception;
use crate::sql::sql_error::SQLError;
use datafusion::logical_expr::sqlparser::ast::{
    Expr, Query, SetExpr, Statement, TableFactor, TableWithJoins,
};
use datafusion::logical_expr::sqlparser::dialect::AnsiDialect;
use datafusion::logical_expr::sqlparser::parser::Parser;
use crate::sql::schema::SQLType;
use crate::sql::schema::SQLType::{DDL, DML};

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, Exception> {
    let dialect = AnsiDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    Ok(statements)
}

pub fn get_table_names(sql: &str) -> Result<Vec<String>, Exception> {
    let statements = parse_sql(sql)?;
    let mut table_names = Vec::new();

    for statement in statements {
        match statement {
            Statement::Query(query) => {
                extract_table_names_from_query(&query, &mut table_names);
            }
            _ => {
                return Err(SQLError::sql_syntax_error(
                    "Only supports Select syntax.",
                    sql,
                ))?
            }
        }
    }

    Ok(table_names)
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
fn extract_table_names_from_table_with_joins(
    table_with_joins: &TableWithJoins,
    table_names: &mut Vec<String>,
) {
    extract_table_names_from_table_factor(&table_with_joins.relation, table_names);

    for join in &table_with_joins.joins {
        extract_table_names_from_table_factor(&join.relation, table_names);
    }
}

/// 从表因子中提取表名
/// Extract table names from table factors
fn extract_table_names_from_table_factor(
    table_factor: &TableFactor,
    table_names: &mut Vec<String>,
) {
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

pub fn get_sql_type(sql: &str) -> Result<(Vec<Statement>, SQLType), Exception> {
    let statements = parse_sql(sql)?;
    if statements.is_empty() {
        return Err(SQLError::sql_syntax_error(
            "SQL parsing error: statements are empty",
            &sql,
        ))?;
    }

    for statement in &statements {
        return match statement {
            Statement::Query(_) => Ok((statements, DML)),
            Statement::CreateTable(_) => Ok((statements, DDL)),
            _ => Err(Exception::unprocessable_entity_error(
                "Currently, only the Select and Create statements are supported.",
            )),
        };
    }

    unreachable!()
}
