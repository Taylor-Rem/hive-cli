use anyhow::Result;
use hive_capabilities::{connect, read_schema_toml, write_schema_to_db, retrieve_from_env};

pub async fn run(url: Option<&str>, schema_path: &str) -> Result<()> {
    let schema = read_schema_toml(&schema_path)?;

    let database_url = match url {
        Some(u) => u.to_string(),
        None => retrieve_from_env("DATABASE_URL")?,
    };

    println!("Connecting to database...");
    let pool = connect(&database_url).await?;

    println!("Applying schema migrations...");
    let migrations = write_schema_to_db(&pool, schema).await?;

    if migrations.is_empty() {
        println!("No migrations needed.");
    } else {
        println!("\nMigration complete! {} statement(s) executed.", migrations.len());
    }

    Ok(())
}
