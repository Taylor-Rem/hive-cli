use anyhow::Result;
use std::collections::HashMap;
use std::fs;

use crate::structs::{DbSchema, DbTable, TomlSchema, TomlTable};

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