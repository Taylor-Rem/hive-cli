use anyhow::Result;
use hive_capabilities::{connect, Schema};

pub async fn run(url: Option<&str>, output_path: String) -> Result<()> {

    println!("Connecting to database...");
    let pool = connect(url).await?;

    println!("Reading database schema...");
    let schema = Schema::from_db(&pool).await?;

    schema.write_toml(&output_path)?;

    Ok(())
}
