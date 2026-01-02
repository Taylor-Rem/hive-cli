use anyhow::Result;
use std::fs;
use std::collections::HashMap;

use crate::jobs::capabilities::toml_capabilities::structs::{
    DbSchema,
    DbTable,
    TomlSchema,
};

pub fn read_schema_toml(path: &str) -> Result<DbSchema> {
    let toml_str = fs::read_to_string(path)?;
    let toml_schema: TomlSchema = toml::from_str(&toml_str)?;
    
    let mut tables = HashMap::new();
    
    for toml_table in toml_schema.table {
        tables.insert(
            toml_table.name,
            DbTable {
                columns: toml_table.column,
                foreign_keys: toml_table.foreign_key,
                indexes: toml_table.index,
            }
        );
    }
    
    Ok(DbSchema { tables })
}