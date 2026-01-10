use anyhow::Result;
use hive_capabilities::{connect, read_db_schema, write_schema_toml};

pub async fn run(database_url: &str, output_path: &str) -> Result<()> {
    println!("Connecting to database...");

    // Step 1: Connect to the database
    let pool = connect(database_url).await?;

    println!("Reading database schema...");

    // Step 2: Read the database schema
    let schema = read_db_schema(&pool).await?;

    println!("Writing schema to {}...", output_path);

    // Step 3: Write the schema to a TOML file
    write_schema_toml(schema, output_path)?;

    println!("Schema file generated successfully!");

    Ok(())
}
