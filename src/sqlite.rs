use rusqlite::Connection;

pub fn conn() -> Connection {
    Connection::open("sqlite/easydb.db").unwrap()
}

pub fn init_db() {
    let conn = conn();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS table_scheme (
                  id              INTEGER PRIMARY KEY,
                  db_ref TEXT NOT NULL DEFAULT 'default',
                  table_ref            TEXT NOT NULL UNIQUE,
                  table_path             TEXT NOT NULL
                  )",
        [],
    ).expect("Failed to create table_scheme");
}