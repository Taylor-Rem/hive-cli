use hive_capabilities::{create_directory, create_file};
use std::path::Path;

pub fn run(path: Option<&str>) -> anyhow::Result<()> {
    let base_path = path.unwrap_or(".");
    let base = Path::new(base_path);

    // Create schema directory
    let schema_dir = base.join("schema");
    create_directory(&schema_dir)?;

    // Create schema file
    let schema_file = schema_dir.join("schema.toml");
    create_file(&schema_file)?;

    // Create .env file
    let env_file = base.join(".env");
    create_file(&env_file)?;
    if !env_file.exists() {
        fs::write(&env_file, "DATABASE_URL=\n")
            .with_context(|| format!("Failed to create {:?}", env_file))?;
        println!("Created .env");
    } else {
        println!(".env already exists, skipping");
    }

    // Create models directory
    let models_dir = base.join("models");
    create_directory(&models_dir)?;

    Ok(())
}