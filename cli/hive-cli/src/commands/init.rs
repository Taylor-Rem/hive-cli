use std::path::Path;
use std::fs;

pub fn run(path: &str) -> anyhow::Result<()> {
    let base = Path::new(&path);
    // Create schema directory
    let schema_dir = base.join("schema");
    fs::create_dir_all(&schema_dir)?;

    // Create schema file
    fs::write(schema_dir.join("schema.toml"), "")?;

    // Create .env file
    fs::write(base.join(".env"), "DATABASE_URL=\n")?;

    // Create models directory
    fs::create_dir_all(base.join("models"))?;

    Ok(())
}