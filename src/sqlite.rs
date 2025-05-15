use chrono::Local;
use rusqlite::{params, Connection};

pub fn conn() -> Connection {
    Connection::open("sqlite/easydb.db").unwrap()
}

pub fn init_db() {
    let conn = conn();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS catalog (
                  id              INTEGER PRIMARY KEY,
                  db_ref TEXT DEFAULT 'default',
                  table_ref            TEXT NOT NULL UNIQUE,
                  table_path           TEXT NOT NULL,
                  table_comment           TEXT,
                  table_schema TEXT
                  )",
        [],
    )
    .expect("Failed to create catalog");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS query_history (
                  id              INTEGER PRIMARY KEY,
                  user_id INTEGER,
                  sql text,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  status text
                  )",
        [],
    )
    .expect("Failed to create catalog");
}

pub fn insert_query_history(sql: &str, status: &str) {
    if let Err(_) = conn().execute(
        r#"
                        insert into query_history ( sql, status, created_at )
                        values
                        (?1, ?2, ?3)
                        "#,
        params![sql, status, Local::now().format("%Y-%m-%d %H:%M:%S").to_string()],
    ) {}
}
