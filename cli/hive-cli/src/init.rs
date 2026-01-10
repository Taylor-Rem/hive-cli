use std::fs;
use std::path::Path;
use anyhow::Context;

pub fn run(path: Option<&str>) -> anyhow::Result<()> {
    let base_path = path.unwrap_or(".");
    let base = Path::new(base_path);

    // Create schema directory
    let schema_dir = base.join("schema");
    fs::create_dir_all(&schema_dir)
        .with_context(|| format!("Failed to create schema directory at {:?}", schema_dir))?;
    println!("Created schema/");

    // Create empty schema.toml
    let schema_file = schema_dir.join("schema.toml");
    if !schema_file.exists() {
        fs::write(&schema_file, "# Hive schema file\n# Run `hive introspect` to populate from database\n")
            .with_context(|| format!("Failed to create {:?}", schema_file))?;
        println!("Created schema/schema.toml");
    } else {
        println!("schema/schema.toml already exists, skipping");
    }

    // Create .env file
    let env_file = base.join(".env");
    if !env_file.exists() {
        fs::write(&env_file, "DATABASE_URL=\n")
            .with_context(|| format!("Failed to create {:?}", env_file))?;
        println!("Created .env");
    } else {
        println!(".env already exists, skipping");
    }

    // Create models directory
    let models_dir = base.join("models");
    fs::create_dir_all(&models_dir)
        .with_context(|| format!("Failed to create models directory at {:?}", models_dir))?;
    println!("Created models/");

    println!("\nHive project initialized successfully!");
    println!("Next steps:");
    println!("  1. Update .env with your database connection string");
    println!("  2. Run `hive introspect` to read your database schema");

    Ok(())
}