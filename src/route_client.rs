use crate::errors::Result;
use async_trait::async_trait;

/// Route metric to endpoint
#[async_trait]
pub trait RouteClient {
    async fn route(metric: String) -> Result<String>;
}
