use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;

use crate::schema::{Column, ForeignKey, Index, Schema};

// ============ Type Definitions ============

#[derive(Serialize, Deserialize)]
pub struct TomlSchema {
    pub table: Vec<TomlTable>,
}

#[derive(Serialize, Deserialize)]
pub struct TomlTable {
    pub name: String,
    pub column: Vec<Column>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_key: Vec<ForeignKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub index: Vec<Index>,
}

// ============ TomlSchema Methods ============

impl TomlSchema {
    /// Read a TomlSchema from a TOML file
    pub fn from_file(path: &str) -> Result<Self> {
        let toml_str = fs::read_to_string(path)?;
        let toml_schema: TomlSchema = toml::from_str(&toml_str)?;
        Ok(toml_schema)
    }

    /// Write this TomlSchema to a file
    pub fn write_file(&self, path: &str) -> Result<()> {
        let toml_string = toml::to_string_pretty(self)?;
        fs::write(path, toml_string)?;
        Ok(())
    }
    pub fn from_schema(schema: Schema) -> Self {
        let mut tables: Vec<TomlTable> = schema
            .tables
            .into_iter()
            .map(|(name, table)| TomlTable {
                name,
                column: table.columns,
                foreign_key: table.foreign_keys,
                index: table.indexes,
            })
            .collect();

        tables.sort_by(|a, b| a.name.cmp(&b.name));

        TomlSchema { table: tables }
    }
}