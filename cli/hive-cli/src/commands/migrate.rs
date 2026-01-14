use anyhow::Result;
use hive_capabilities::{connect, Schema};

pub async fn run(url: Option<&str>, schema_path: &str) -> Result<()> {
    let schema = Schema::from_toml_file(schema_path)?;

    println!("Connecting to database...");
    let pool = connect(url).await?;

    println!("Applying schema migrations...");
    let migrations = schema.apply_to_db(&pool).await?;

    if migrations.is_empty() {
        println!("No migrations needed.");
    } else {
        println!("\nMigration complete! {} statement(s) executed.", migrations.len());
    }

    Ok(())
}
