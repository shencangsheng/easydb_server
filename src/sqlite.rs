use rusqlite::Connection;

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
                  table_schema TEXT
                  )",
        [],
    ).expect("Failed to create catalog");
}