use sqlx::{PgPool, postgres::PgPoolOptions};
use anyhow::{Result};

pub type DbPool = PgPool;

pub async fn connect(database_url: &str) -> Result<DbPool> {
    // Encode the password if the URL contains special characters
    let encoded_url = encode_password_in_url(database_url)?;
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&encoded_url)
        .await?;
    
    Ok(pool)
}

/// Parse a connection URL and re-encode the password portion
fn encode_password_in_url(url: &str) -> Result<String> {
    // Parse format: postgresql://username:password@host:port/database
    
    if !url.starts_with("postgresql://") {
        return Ok(url.to_string());
    }
    
    let after_protocol = &url[13..]; // Skip "postgresql://"
    
    // Split into credentials@host/db
    let parts: Vec<&str> = after_protocol.split('@').collect();
    if parts.len() != 2 {
        return Ok(url.to_string()); // Return as-is if format is unexpected
    }
    
    let credentials = parts[0];
    let host_and_db = parts[1];
    
    // Split credentials into username:password
    let cred_parts: Vec<&str> = credentials.splitn(2, ':').collect();
    if cred_parts.len() != 2 {
        return Ok(url.to_string());
    }
    
    let username = cred_parts[0];
    let password = cred_parts[1];
    
    // Rebuild with encoded password
    Ok(format!(
        "postgresql://{}:{}@{}",
        urlencoding::encode(username),
        urlencoding::encode(password),
        host_and_db
    ))
}