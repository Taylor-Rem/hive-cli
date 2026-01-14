use anyhow::Result;
use hive_capabilities::Schema;

pub async fn run(schema_path: &str, output: &str) -> Result<()> {
    let schema = Schema::from_toml_file(schema_path)?;

    schema.write_models(output)?;

    Ok(())
}
