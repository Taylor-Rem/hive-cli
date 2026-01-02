use anyhow::Result;
use std::fs;

use crate::structs::{
    DbSchema,
    DbTable,
    TomlSchema,
    TomlTable
};

pub fn write_schema_toml(schema: DbSchema, path: &str) -> Result<()> {
    let mut tables: Vec<TomlTable> = schema
        .tables
        .into_iter()
        .map(|(name, table)| to_toml_table(name, table))
        .collect();

    tables.sort_by(|a, b| a.name.cmp(&b.name));

    let toml_schema = TomlSchema { table: tables };

    let toml_string = toml::to_string_pretty(&toml_schema)?;
    fs::write(path, toml_string)?;

    Ok(())
}

fn to_toml_table(name: String, table: DbTable) -> TomlTable {
    TomlTable {
        name,
        column: table.columns,
        foreign_key: table.foreign_keys,
        index: table.indexes,
    }
}