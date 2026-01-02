use sqlx::{PgPool, postgres::PgPoolOptions};
use anyhow::Result;

pub type DbPool = PgPool;

/// Create a database connection pool from a connection string
pub async fn connect(database_url: &str) -> Result<DbPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    
    Ok(pool)
}