use crate::db::connect::DbState;
use std::collections::HashMap;
use anyhow::Result;
use sqlx::Row;

use hive_schema::{
    DbSchema,
    DbTable,
    DbColumn,
    DbForeignKey,
    DbIndex
};

pub async fn read_db_schema(db: &DbState) -> Result<DbSchema> {
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
        "#
    )
    .fetch_all(&db.pool)
    .await?;

    let mut tables: HashMap<String, DbTable> = HashMap::new();

    for row in column_rows {
        let table_name: String = row.get("table_name");

        let table = tables
            .entry(table_name)
            .or_insert_with(|| DbTable {
                columns: Vec::new(),
                foreign_keys: Vec::new(),
                indexes: Vec::new()
            });

        table.columns.push(DbColumn {
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
        "#
    )
    .fetch_all(&db.pool)
    .await?;

    for row in fk_rows {
        let table_name: String = row.get("table_name");
        
        if let Some(table) = tables.get_mut(&table_name) {
            table.foreign_keys.push(DbForeignKey {
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
        "#
    )
    .fetch_all(&db.pool)
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
            table.indexes.push(DbIndex {
                name: index_name,
                columns,
                is_unique,
                index_type,
            });
        }
    }

    Ok(DbSchema { tables })
}