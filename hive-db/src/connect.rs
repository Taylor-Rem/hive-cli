use sqlx::{PgPool, postgres::PgPoolOptions};

pub type DbPool = PgPool;

pub async fn connect(database_url: &str) -> Result<DbPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;
    
    Ok(pool)
}