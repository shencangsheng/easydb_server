use crate::server::schema::TableCatalog;
use async_trait::async_trait;
use datafusion::execution::context;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SessionContext {
    df_ctx: context::SessionContext,
    table_map: HashMap<String, TableCatalog>,
}

pub type ConcurrentSessionContext = RwLock<SessionContext>;

#[async_trait]
pub trait Session: Send + Sync + 'static {
    async fn id(&self) -> String;
}

#[async_trait]
impl Session for ConcurrentSessionContext {
    async fn id(&self) -> String {
        self.read().await.df_ctx.session_id()
    }
}
