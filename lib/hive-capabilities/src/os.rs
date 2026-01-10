use std::fs;
use std::path::Path;
use anyhow::{Context, Result};

pub fn create_directory(path: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(&path).with_context(|| format!("Failed to create schema directory at {:?}", path))?;
    Ok(())
}

pub fn create_file(path: &Path, content: Option<&str>) -> Result<()> {
    if path.exists() { anyhow::bail!("File already exists: {:?}", path); }
    let data = content.unwrap_or("");
    fs::write(path, data)?;
    Ok(())
}