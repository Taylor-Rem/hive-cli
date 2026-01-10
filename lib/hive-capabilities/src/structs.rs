use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct DbSchema {
    pub tables: HashMap<String, DbTable>,
}

#[derive(Debug, Deserialize)]
pub struct DbTable {
    pub columns: Vec<DbColumn>,
    pub foreign_keys: Vec<DbForeignKey>,
    pub indexes: Vec<DbIndex>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbColumn {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub default: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbForeignKey {
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub index_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct TomlSchema {
    pub table: Vec<TomlTable>,
}

#[derive(Serialize, Deserialize)]
pub struct TomlTable {
    pub name: String,
    pub column: Vec<DbColumn>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub foreign_key: Vec<DbForeignKey>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub index: Vec<DbIndex>
}