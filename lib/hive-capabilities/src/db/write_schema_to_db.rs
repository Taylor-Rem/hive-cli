use anyhow::{Result, Context};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use crate::structs::{DbSchema, DbTable, DbColumn, DbForeignKey, DbIndex};
use crate::read_db_schema;

/// Applies the target schema to the database, generating and executing migrations
pub async fn write_schema_to_db(pool: &PgPool, target: DbSchema) -> Result<Vec<String>> {
    // Step 1: Read current database state
    let current = read_db_schema(pool).await?;

    // Step 2: Generate migration SQL
    let migrations = generate_migrations(&current, &target);

    if migrations.is_empty() {
        println!("Database is already in sync with schema.");
        return Ok(migrations);
    }

    // Step 3: Execute migrations in a transaction
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

/// Generates SQL statements to migrate from current to target schema
fn generate_migrations(current: &DbSchema, target: &DbSchema) -> Vec<String> {
    let mut migrations = Vec::new();

    // Phase 1: Drop foreign keys that no longer exist (must happen before dropping tables/columns)
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
            // Add new columns
            let new_columns = find_new_columns(current_table, target_table);
            for col in new_columns {
                migrations.push(generate_add_column(table_name, &col));
            }

            // Modify changed columns
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
            // Skip primary keys - they're created with the table
            if !idx.name.ends_with("_pkey") {
                migrations.push(generate_create_index(table_name, &idx));
            }
        }
    }

    // Phase 5: Create new foreign keys (must happen after all tables exist)
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

// ============ Diff Functions ============

fn find_new_tables(current: &DbSchema, target: &DbSchema) -> Vec<String> {
    target.tables.keys()
        .filter(|name| !current.tables.contains_key(*name))
        .cloned()
        .collect()
}

fn find_new_columns(current: &DbTable, target: &DbTable) -> Vec<DbColumn> {
    let current_cols: HashSet<_> = current.columns.iter().map(|c| &c.name).collect();
    target.columns.iter()
        .filter(|c| !current_cols.contains(&c.name))
        .cloned()
        .collect()
}

fn find_changed_columns(current: &DbTable, target: &DbTable) -> Vec<(DbColumn, DbColumn)> {
    let current_map: HashMap<_, _> = current.columns.iter()
        .map(|c| (&c.name, c))
        .collect();

    target.columns.iter()
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

fn columns_differ(a: &DbColumn, b: &DbColumn) -> bool {
    a.data_type != b.data_type ||
    a.is_nullable != b.is_nullable ||
    a.default != b.default
}

fn find_new_indexes(current: Option<&DbTable>, target: &DbTable) -> Vec<DbIndex> {
    let current_idx_names: HashSet<_> = current
        .map(|t| t.indexes.iter().map(|i| &i.name).collect())
        .unwrap_or_default();

    target.indexes.iter()
        .filter(|i| !current_idx_names.contains(&i.name))
        .cloned()
        .collect()
}

fn find_dropped_indexes(current: &DbTable, target: &DbTable) -> Vec<DbIndex> {
    let target_idx_names: HashSet<_> = target.indexes.iter().map(|i| &i.name).collect();

    current.indexes.iter()
        .filter(|i| !target_idx_names.contains(&i.name))
        .cloned()
        .collect()
}

fn find_new_foreign_keys(current: Option<&DbTable>, target: &DbTable) -> Vec<DbForeignKey> {
    let current_fks: HashSet<_> = current
        .map(|t| t.foreign_keys.iter().map(|f| (&f.column, &f.referenced_table)).collect())
        .unwrap_or_default();

    target.foreign_keys.iter()
        .filter(|f| !current_fks.contains(&(&f.column, &f.referenced_table)))
        .cloned()
        .collect()
}

fn find_dropped_foreign_keys(current: &DbTable, target: &DbTable) -> Vec<DbForeignKey> {
    let target_fks: HashSet<_> = target.foreign_keys.iter()
        .map(|f| (&f.column, &f.referenced_table))
        .collect();

    current.foreign_keys.iter()
        .filter(|f| !target_fks.contains(&(&f.column, &f.referenced_table)))
        .cloned()
        .collect()
}

// ============ Table Ordering ============

fn order_tables_by_dependency(tables: &[String], schema: &DbSchema) -> Vec<String> {
    let mut ordered = Vec::new();
    let mut remaining: HashSet<_> = tables.iter().cloned().collect();

    // Keep iterating until all tables are ordered
    while !remaining.is_empty() {
        let mut added_this_round = Vec::new();

        for table_name in &remaining {
            if let Some(table) = schema.tables.get(table_name) {
                // Check if all FK dependencies are satisfied
                let deps_satisfied = table.foreign_keys.iter().all(|fk| {
                    // Dependency is satisfied if:
                    // 1. Referenced table is already ordered, OR
                    // 2. Referenced table is not in our new tables list (already exists), OR
                    // 3. Self-referencing
                    ordered.contains(&fk.referenced_table) ||
                    !remaining.contains(&fk.referenced_table) ||
                    &fk.referenced_table == table_name
                });

                if deps_satisfied {
                    added_this_round.push(table_name.clone());
                }
            }
        }

        // Prevent infinite loop
        if added_this_round.is_empty() && !remaining.is_empty() {
            // Force add remaining (circular dependency or missing table)
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

fn generate_create_table(name: &str, table: &DbTable) -> String {
    let columns: Vec<String> = table.columns.iter()
        .map(|c| format_column_def(c))
        .collect();

    // Find primary key
    let pk = table.indexes.iter()
        .find(|i| i.name.ends_with("_pkey"));

    let mut parts = columns;

    if let Some(pk_idx) = pk {
        parts.push(format!("PRIMARY KEY ({})", pk_idx.columns.join(", ")));
    }

    format!("CREATE TABLE \"{}\" (\n  {}\n)", name, parts.join(",\n  "))
}

fn format_column_def(col: &DbColumn) -> String {
    let mut def = format!("\"{}\" {}", col.name, map_data_type(&col.data_type));

    if !col.is_nullable {
        def.push_str(" NOT NULL");
    }

    if let Some(default) = &col.default {
        // Skip sequence defaults for new tables (SERIAL handles this)
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

fn generate_add_column(table: &str, col: &DbColumn) -> String {
    let mut sql = format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
        table, col.name, map_data_type(&col.data_type)
    );

    if !col.is_nullable {
        sql.push_str(" NOT NULL");
    }

    if let Some(default) = &col.default {
        sql.push_str(&format!(" DEFAULT {}", default));
    }

    sql
}

fn generate_alter_column(table: &str, _old: &DbColumn, new: &DbColumn) -> Vec<String> {
    let mut migrations = Vec::new();

    // Change data type
    migrations.push(format!(
        "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" TYPE {} USING \"{}\"::{}",
        table, new.name, map_data_type(&new.data_type), new.name, map_data_type(&new.data_type)
    ));

    // Change nullability
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

    // Change default
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

fn generate_create_index(table: &str, idx: &DbIndex) -> String {
    let unique = if idx.is_unique { "UNIQUE " } else { "" };
    let columns: Vec<String> = idx.columns.iter()
        .map(|c| format!("\"{}\"", c))
        .collect();

    format!(
        "CREATE {}INDEX \"{}\" ON \"{}\" USING {} ({})",
        unique, idx.name, table, idx.index_type, columns.join(", ")
    )
}

fn generate_drop_index(name: &str) -> String {
    format!("DROP INDEX IF EXISTS \"{}\"", name)
}

fn generate_add_fk(table: &str, fk: &DbForeignKey) -> String {
    let constraint_name = format!("{}_{}_fkey", table, fk.column);
    format!(
        "ALTER TABLE \"{}\" ADD CONSTRAINT \"{}\" FOREIGN KEY (\"{}\") REFERENCES \"{}\"(\"{}\")",
        table, constraint_name, fk.column, fk.referenced_table, fk.referenced_column
    )
}

fn generate_drop_fk(table: &str, fk: &DbForeignKey) -> String {
    let constraint_name = format!("{}_{}_fkey", table, fk.column);
    format!(
        "ALTER TABLE \"{}\" DROP CONSTRAINT IF EXISTS \"{}\"",
        table, constraint_name
    )
}
