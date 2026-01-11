use anyhow::Result;
use hive_capabilities::{connect, read_db_schema, write_schema_toml, retrieve_from_env};

pub async fn run(url: Option<&str>, output_path: String) -> Result<()> {
    let database_url = match url {
        Some(u) => u.to_string(),
        None => retrieve_from_env("DATABASE_URL")?,
    };

    // Connect to db
    println!("Connecting to database...");
    let pool = connect(&database_url).await?;


    // Read db schema
    println!("Reading database schema...");
    let schema = read_db_schema(&pool).await?;

    // Write schema to toml
    write_schema_toml(schema, &output_path)?;

    Ok(())
}
