use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use crate::toml_schema::{TomlSchema, TomlTable};

// ============ Type Definitions ============

#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    pub tables: HashMap<String, Table>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Table {
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>,
    pub indexes: Vec<Index>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub index_type: String,
}

// ============ Schema Methods ============

impl Schema {
    /// Read schema from a database
    pub async fn from_db(pool: &PgPool) -> Result<Self> {
        let mut tables: HashMap<String, Table> = HashMap::new();

        // Step 1: Get all columns
        let column_rows = sqlx::query(
            r#"
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            "#,
        )
        .fetch_all(pool)
        .await?;

        for row in column_rows {
            let table_name: String = row.get("table_name");

            let table = tables.entry(table_name).or_insert_with(|| Table {
                columns: Vec::new(),
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
            });

            table.columns.push(Column {
                name: row.get("column_name"),
                data_type: row.get("data_type"),
                is_nullable: row.get::<String, _>("is_nullable") == "YES",
                default: row.get("column_default"),
            });
        }

        // Step 2: Get foreign keys
        let fk_rows = sqlx::query(
            r#"
            SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
            ORDER BY tc.table_name, kcu.column_name
            "#,
        )
        .fetch_all(pool)
        .await?;

        for row in fk_rows {
            let table_name: String = row.get("table_name");

            if let Some(table) = tables.get_mut(&table_name) {
                table.foreign_keys.push(ForeignKey {
                    column: row.get("column_name"),
                    referenced_table: row.get("referenced_table"),
                    referenced_column: row.get("referenced_column"),
                });
            }
        }

        // Step 3: Get indexes
        let index_rows = sqlx::query(
            r#"
            SELECT
                t.relname AS table_name,
                i.relname AS index_name,
                a.attname AS column_name,
                ix.indisunique AS is_unique,
                am.amname AS index_type
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON i.relam = am.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'public'
                AND t.relkind = 'r'
            ORDER BY t.relname, i.relname, a.attnum
            "#,
        )
        .fetch_all(pool)
        .await?;

        // Group index columns by index name
        let mut index_map: HashMap<(String, String), (Vec<String>, bool, String)> = HashMap::new();

        for row in index_rows {
            let table_name: String = row.get("table_name");
            let index_name: String = row.get("index_name");
            let column_name: String = row.get("column_name");
            let is_unique: bool = row.get("is_unique");
            let index_type: String = row.get("index_type");

            let entry = index_map
                .entry((table_name.clone(), index_name.clone()))
                .or_insert_with(|| (Vec::new(), is_unique, index_type.clone()));

            entry.0.push(column_name);
        }

        // Add indexes to tables
        for ((table_name, index_name), (columns, is_unique, index_type)) in index_map {
            if let Some(table) = tables.get_mut(&table_name) {
                table.indexes.push(Index {
                    name: index_name,
                    columns,
                    is_unique,
                    index_type,
                });
            }
        }

        Ok(Schema { tables })
    }

    /// Apply this schema to a database, generating and executing migrations
    pub async fn apply_to_db(&self, pool: &PgPool) -> Result<Vec<String>> {
        let current = Schema::from_db(pool).await?;
        let migrations = generate_migrations(&current, self);

        if migrations.is_empty() {
            println!("Database is already in sync with schema.");
            return Ok(migrations);
        }

        let mut tx = pool.begin().await?;

        for sql in &migrations {
            println!("Executing: {}", sql);
            sqlx::query(sql)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("Failed to execute: {}", sql))?;
        }

        tx.commit().await?;

        println!("\nApplied {} migration(s) successfully!", migrations.len());
        Ok(migrations)
    }

    pub fn from_toml_schema(toml_schema: TomlSchema) -> Self {
        let mut tables = HashMap::new();

        for toml_table in toml_schema.table {
            tables.insert(
                toml_table.name,
                Table {
                    columns: toml_table.column,
                    foreign_keys: toml_table.foreign_key,
                    indexes: toml_table.index,
                },
            );
        }

        Schema { tables }
    }

    /// Read schema from a TOML file
    pub fn from_toml_file(path: &str) -> Result<Self> {
        let toml_schema = TomlSchema::from_file(path)?;
        Ok(Self::from_toml_schema(toml_schema))
    }

    /// Write this schema to a TOML file
    pub fn write_toml(&self, path: &str) -> Result<()> {
        let toml_schema = TomlSchema::from_schema(self.clone());
        toml_schema.write_file(path)
    }

    /// Generate model files from this schema
    pub fn write_models(&self, output_path: &str) -> Result<()> {
        // Ensure output directory exists
        fs::create_dir_all(output_path)?;

        // Convert to TomlSchema for easier iteration (preserves table structure)
        let toml_schema = TomlSchema::from_schema(self.clone());

        // Build relation maps
        let (belongs_to, has_many) = build_relation_maps(&toml_schema);

        // Generate each table file
        let mut table_names: Vec<&str> = toml_schema.table.iter().map(|t| t.name.as_str()).collect();
        table_names.sort();

        for table in &toml_schema.table {
            let file_content = generate_table_file(table, &belongs_to, &has_many, &table_names);
            let file_path = Path::new(output_path).join(format!("{}.rs", table.name));
            fs::write(&file_path, file_content)?;
        }

        // Generate mod.rs
        let mod_content = generate_mod_file(&table_names);
        let mod_path = Path::new(output_path).join("mod.rs");
        fs::write(&mod_path, mod_content)?;

        println!("Generated {} model files in {}", toml_schema.table.len(), output_path);

        Ok(())
    }
}

// ============ Migration Logic (private helpers) ============

fn generate_migrations(current: &Schema, target: &Schema) -> Vec<String> {
    let mut migrations = Vec::new();

    // Phase 1: Drop foreign keys that no longer exist
    for (table_name, current_table) in &current.tables {
        if let Some(target_table) = target.tables.get(table_name) {
            let dropped_fks = find_dropped_foreign_keys(current_table, target_table);
            for fk in dropped_fks {
                migrations.push(generate_drop_fk(table_name, &fk));
            }
        }
    }

    // Phase 2: Create new tables (order by dependencies)
    let new_tables = find_new_tables(current, target);
    let ordered_tables = order_tables_by_dependency(&new_tables, target);
    for table_name in ordered_tables {
        if let Some(table) = target.tables.get(&table_name) {
            migrations.push(generate_create_table(&table_name, table));
        }
    }

    // Phase 3: Alter existing tables (add/modify columns)
    for (table_name, target_table) in &target.tables {
        if let Some(current_table) = current.tables.get(table_name) {
            let new_columns = find_new_columns(current_table, target_table);
            for col in new_columns {
                migrations.push(generate_add_column(table_name, &col));
            }

            let changed_columns = find_changed_columns(current_table, target_table);
            for (old, new) in changed_columns {
                migrations.extend(generate_alter_column(table_name, &old, &new));
            }
        }
    }

    // Phase 4: Create new indexes
    for (table_name, target_table) in &target.tables {
        let current_table = current.tables.get(table_name);
        let new_indexes = find_new_indexes(current_table, target_table);
        for idx in new_indexes {
            if !idx.name.ends_with("_pkey") {
                migrations.push(generate_create_index(table_name, &idx));
            }
        }
    }

    // Phase 5: Create new foreign keys
    for (table_name, target_table) in &target.tables {
        let current_table = current.tables.get(table_name);
        let new_fks = find_new_foreign_keys(current_table, target_table);
        for fk in new_fks {
            migrations.push(generate_add_fk(table_name, &fk));
        }
    }

    // Phase 6: Drop removed indexes
    for (table_name, current_table) in &current.tables {
        if let Some(target_table) = target.tables.get(table_name) {
            let dropped_indexes = find_dropped_indexes(current_table, target_table);
            for idx in dropped_indexes {
                if !idx.name.ends_with("_pkey") {
                    migrations.push(generate_drop_index(&idx.name));
                }
            }
        }
    }

    migrations
}

fn find_new_tables(current: &Schema, target: &Schema) -> Vec<String> {
    target
        .tables
        .keys()
        .filter(|name| !current.tables.contains_key(*name))
        .cloned()
        .collect()
}

fn find_new_columns(current: &Table, target: &Table) -> Vec<Column> {
    let current_cols: HashSet<_> = current.columns.iter().map(|c| &c.name).collect();
    target
        .columns
        .iter()
        .filter(|c| !current_cols.contains(&c.name))
        .cloned()
        .collect()
}

fn find_changed_columns(current: &Table, target: &Table) -> Vec<(Column, Column)> {
    let current_map: HashMap<_, _> = current.columns.iter().map(|c| (&c.name, c)).collect();

    target
        .columns
        .iter()
        .filter_map(|target_col| {
            current_map.get(&target_col.name).and_then(|current_col| {
                if columns_differ(current_col, target_col) {
                    Some(((*current_col).clone(), target_col.clone()))
                } else {
                    None
                }
            })
        })
        .collect()
}

fn columns_differ(a: &Column, b: &Column) -> bool {
    a.data_type != b.data_type || a.is_nullable != b.is_nullable || a.default != b.default
}

fn find_new_indexes(current: Option<&Table>, target: &Table) -> Vec<Index> {
    let current_idx_names: HashSet<_> = current
        .map(|t| t.indexes.iter().map(|i| &i.name).collect())
        .unwrap_or_default();

    target
        .indexes
        .iter()
        .filter(|i| !current_idx_names.contains(&i.name))
        .cloned()
        .collect()
}

fn find_dropped_indexes(current: &Table, target: &Table) -> Vec<Index> {
    let target_idx_names: HashSet<_> = target.indexes.iter().map(|i| &i.name).collect();

    current
        .indexes
        .iter()
        .filter(|i| !target_idx_names.contains(&i.name))
        .cloned()
        .collect()
}

fn find_new_foreign_keys(current: Option<&Table>, target: &Table) -> Vec<ForeignKey> {
    let current_fks: HashSet<_> = current
        .map(|t| {
            t.foreign_keys
                .iter()
                .map(|f| (&f.column, &f.referenced_table))
                .collect()
        })
        .unwrap_or_default();

    target
        .foreign_keys
        .iter()
        .filter(|f| !current_fks.contains(&(&f.column, &f.referenced_table)))
        .cloned()
        .collect()
}

fn find_dropped_foreign_keys(current: &Table, target: &Table) -> Vec<ForeignKey> {
    let target_fks: HashSet<_> = target
        .foreign_keys
        .iter()
        .map(|f| (&f.column, &f.referenced_table))
        .collect();

    current
        .foreign_keys
        .iter()
        .filter(|f| !target_fks.contains(&(&f.column, &f.referenced_table)))
        .cloned()
        .collect()
}

fn order_tables_by_dependency(tables: &[String], schema: &Schema) -> Vec<String> {
    let mut ordered = Vec::new();
    let mut remaining: HashSet<_> = tables.iter().cloned().collect();

    while !remaining.is_empty() {
        let mut added_this_round = Vec::new();

        for table_name in &remaining {
            if let Some(table) = schema.tables.get(table_name) {
                let deps_satisfied = table.foreign_keys.iter().all(|fk| {
                    ordered.contains(&fk.referenced_table)
                        || !remaining.contains(&fk.referenced_table)
                        || &fk.referenced_table == table_name
                });

                if deps_satisfied {
                    added_this_round.push(table_name.clone());
                }
            }
        }

        if added_this_round.is_empty() && !remaining.is_empty() {
            ordered.extend(remaining.drain());
            break;
        }

        for name in added_this_round {
            remaining.remove(&name);
            ordered.push(name);
        }
    }

    ordered
}

// ============ SQL Generation ============

fn generate_create_table(name: &str, table: &Table) -> String {
    let columns: Vec<String> = table.columns.iter().map(|c| format_column_def(c)).collect();

    let pk = table.indexes.iter().find(|i| i.name.ends_with("_pkey"));

    let mut parts = columns;

    if let Some(pk_idx) = pk {
        parts.push(format!("PRIMARY KEY ({})", pk_idx.columns.join(", ")));
    }

    format!("CREATE TABLE \"{}\" (\n  {}\n)", name, parts.join(",\n  "))
}

fn format_column_def(col: &Column) -> String {
    let mut def = format!("\"{}\" {}", col.name, map_data_type(&col.data_type));

    if !col.is_nullable {
        def.push_str(" NOT NULL");
    }

    if let Some(default) = &col.default {
        if !default.contains("nextval") {
            def.push_str(&format!(" DEFAULT {}", default));
        }
    }

    def
}

fn map_data_type(pg_type: &str) -> &str {
    match pg_type {
        "character varying" => "VARCHAR(255)",
        "timestamp without time zone" => "TIMESTAMP",
        "timestamp with time zone" => "TIMESTAMPTZ",
        _ => pg_type,
    }
}

fn generate_add_column(table: &str, col: &Column) -> String {
    let mut sql = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
        table,
        col.name,
        map_data_type(&col.data_type)
    );

    if !col.is_nullable {
        sql.push_str(" NOT NULL");
    }

    if let Some(default) = &col.default {
        sql.push_str(&format!(" DEFAULT {}", default));
    }

    sql
}

fn generate_alter_column(table: &str, _old: &Column, new: &Column) -> Vec<String> {
    let mut migrations = Vec::new();

    migrations.push(format!(
        "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" TYPE {} USING \"{}\"::{}",
        table,
        new.name,
        map_data_type(&new.data_type),
        new.name,
        map_data_type(&new.data_type)
    ));

    if new.is_nullable {
        migrations.push(format!(
            "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" DROP NOT NULL",
            table, new.name
        ));
    } else {
        migrations.push(format!(
            "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" SET NOT NULL",
            table, new.name
        ));
    }

    match &new.default {
        Some(default) => migrations.push(format!(
            "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" SET DEFAULT {}",
            table, new.name, default
        )),
        None => migrations.push(format!(
            "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" DROP DEFAULT",
            table, new.name
        )),
    }

    migrations
}

fn generate_create_index(table: &str, idx: &Index) -> String {
    let unique = if idx.is_unique { "UNIQUE " } else { "" };
    let columns: Vec<String> = idx.columns.iter().map(|c| format!("\"{}\"", c)).collect();

    format!(
        "CREATE {}INDEX \"{}\" ON \"{}\" USING {} ({})",
        unique,
        idx.name,
        table,
        idx.index_type,
        columns.join(", ")
    )
}

fn generate_drop_index(name: &str) -> String {
    format!("DROP INDEX IF EXISTS \"{}\"", name)
}

fn generate_add_fk(table: &str, fk: &ForeignKey) -> String {
    let constraint_name = format!("{}_{}_fkey", table, fk.column);
    format!(
        "ALTER TABLE \"{}\" ADD CONSTRAINT \"{}\" FOREIGN KEY (\"{}\") REFERENCES \"{}\"(\"{}\")",
        table, constraint_name, fk.column, fk.referenced_table, fk.referenced_column
    )
}

fn generate_drop_fk(table: &str, fk: &ForeignKey) -> String {
    let constraint_name = format!("{}_{}_fkey", table, fk.column);
    format!(
        "ALTER TABLE \"{}\" DROP CONSTRAINT IF EXISTS \"{}\"",
        table, constraint_name
    )
}

// ============ Codegen Helpers ============

/// Returns (belongs_to, has_many) maps
fn build_relation_maps(schema: &TomlSchema) -> (
    HashMap<String, Vec<(String, String, String)>>,
    HashMap<String, Vec<String>>,
) {
    let mut belongs_to: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    let mut has_many: HashMap<String, Vec<String>> = HashMap::new();

    for table in &schema.table {
        for fk in &table.foreign_key {
            belongs_to
                .entry(table.name.clone())
                .or_default()
                .push((
                    fk.column.clone(),
                    fk.referenced_table.clone(),
                    fk.referenced_column.clone(),
                ));

            has_many
                .entry(fk.referenced_table.clone())
                .or_default()
                .push(table.name.clone());
        }
    }

    (belongs_to, has_many)
}

fn generate_table_file(
    table: &TomlTable,
    belongs_to: &HashMap<String, Vec<(String, String, String)>>,
    has_many: &HashMap<String, Vec<String>>,
    all_tables: &[&str],
) -> String {
    let struct_name = to_struct_name(&table.name);
    let mut lines = Vec::new();

    let mut needs_chrono = false;
    let mut needs_decimal = false;
    let mut needs_uuid = false;
    let mut needs_json = false;

    for col in &table.column {
        let rust_type = pg_type_to_rust(&col.data_type, col.is_nullable);
        if rust_type.contains("chrono::") {
            needs_chrono = true;
        }
        if rust_type.contains("Decimal") {
            needs_decimal = true;
        }
        if rust_type.contains("Uuid") {
            needs_uuid = true;
        }
        if rust_type.contains("serde_json::") {
            needs_json = true;
        }
    }

    // Collect relation imports
    let mut relation_imports = HashSet::new();

    if let Some(bt_relations) = belongs_to.get(&table.name) {
        for (_, parent_table, _) in bt_relations {
            if all_tables.contains(&parent_table.as_str()) && parent_table != &table.name {
                relation_imports.insert(parent_table.clone());
            }
        }
    }

    if let Some(hm_relations) = has_many.get(&table.name) {
        for child_table in hm_relations {
            if all_tables.contains(&child_table.as_str()) && child_table != &table.name {
                relation_imports.insert(child_table.clone());
            }
        }
    }

    // Build imports section
    lines.push("use sqlx::FromRow;".to_string());

    if needs_chrono {
        lines.push("use chrono;".to_string());
    }
    if needs_decimal {
        lines.push("use rust_decimal::Decimal;".to_string());
    }
    if needs_uuid {
        lines.push("use uuid::Uuid;".to_string());
    }
    if needs_json {
        lines.push("use serde_json;".to_string());
    }

    let mut sorted_relation_imports: Vec<_> = relation_imports.into_iter().collect();
    sorted_relation_imports.sort();
    for rel_table in &sorted_relation_imports {
        lines.push(format!(
            "use super::{}::{};",
            rel_table,
            to_struct_name(rel_table)
        ));
    }

    lines.push(String::new());

    // Struct definition
    lines.push("#[derive(Debug, Clone, FromRow)]".to_string());
    lines.push(format!("pub struct {} {{", struct_name));

    // Column fields
    for col in &table.column {
        let rust_type = pg_type_to_rust(&col.data_type, col.is_nullable);
        lines.push(format!("    pub {}: {},", col.name, rust_type));
    }

    // belongs_to relation fields
    if let Some(bt_relations) = belongs_to.get(&table.name) {
        if !bt_relations.is_empty() {
            lines.push(String::new());
            lines.push("    // belongs_to relations".to_string());
            for (fk_column, parent_table, _) in bt_relations {
                if all_tables.contains(&parent_table.as_str()) {
                    let field_name = fk_column.trim_end_matches("_id");
                    let parent_struct = to_struct_name(parent_table);
                    lines.push("    #[sqlx(skip)]".to_string());
                    lines.push(format!(
                        "    pub {}: Option<{}>,",
                        field_name, parent_struct
                    ));
                }
            }
        }
    }

    // has_many relation fields
    if let Some(hm_relations) = has_many.get(&table.name) {
        if !hm_relations.is_empty() {
            lines.push(String::new());
            lines.push("    // has_many relations".to_string());
            for child_table in hm_relations {
                if all_tables.contains(&child_table.as_str()) {
                    let child_struct = to_struct_name(child_table);
                    let field_name = to_plural(child_table);
                    lines.push("    #[sqlx(skip)]".to_string());
                    lines.push(format!(
                        "    pub {}: Option<Vec<{}>>,",
                        field_name, child_struct
                    ));
                }
            }
        }
    }

    lines.push("}".to_string());
    lines.push(String::new());

    lines.join("\n")
}

fn generate_mod_file(table_names: &[&str]) -> String {
    let mut lines = Vec::new();

    for name in table_names {
        lines.push(format!("mod {};", name));
    }

    lines.push(String::new());

    for name in table_names {
        lines.push(format!("pub use {}::{};", name, to_struct_name(name)));
    }

    lines.push(String::new());

    lines.join("\n")
}

fn pg_type_to_rust(data_type: &str, is_nullable: bool) -> String {
    let base_type = match data_type {
        "integer" | "int" | "int4" => "i32",
        "bigint" | "int8" => "i64",
        "smallint" | "int2" => "i16",
        "text" | "character varying" | "varchar" | "char" | "character" => "String",
        "boolean" | "bool" => "bool",
        "real" | "float4" => "f32",
        "double precision" | "float8" => "f64",
        "timestamp without time zone" | "timestamp" => "chrono::NaiveDateTime",
        "timestamp with time zone" | "timestamptz" => "chrono::DateTime<chrono::Utc>",
        "date" => "chrono::NaiveDate",
        "time" | "time without time zone" => "chrono::NaiveTime",
        "numeric" | "decimal" => "Decimal",
        "uuid" => "Uuid",
        "json" | "jsonb" => "serde_json::Value",
        "bytea" => "Vec<u8>",
        _ => "String",
    };

    if is_nullable {
        format!("Option<{}>", base_type)
    } else {
        base_type.to_string()
    }
}

fn to_struct_name(name: &str) -> String {
    name.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

fn to_plural(name: &str) -> String {
    if name.ends_with('s') {
        format!("{}es", name)
    } else if name.ends_with('y') {
        format!("{}ies", name.trim_end_matches('y'))
    } else {
        format!("{}s", name)
    }
}
