use crate::server::schema::TableCatalog;
use chrono::{DateTime, Utc};
use datafusion::execution::context;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct SessionContext {
    df_ctx: context::SessionContext,
    ttl: DateTime<Utc>,
    table_map: HashMap<String, TableCatalog>,
}

pub type ConcurrentSessionContext = RwLock<SessionContext>;
