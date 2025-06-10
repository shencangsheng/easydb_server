use serde::Serialize;

#[derive(Serialize)]
pub enum SQLType {
    DDL,
    DML,
}