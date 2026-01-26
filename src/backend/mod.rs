mod mysql;
mod postgres;
mod sqlite;

pub use mysql::MySql;
pub use postgres::Postgres;
pub use sqlite::Sqlite;

use sea_query::{
    Alias, ColumnDef, ForeignKey as SeaForeignKey, ForeignKeyAction, Index as SeaIndex,
    IndexCreateStatement, IndexDropStatement, Table, TableAlterStatement, TableCreateStatement,
    TableDropStatement, TableRenameStatement,
};

use crate::field::{Field, FieldType, ReferentialAction};
use crate::operation::{Constraint, Index, IndexOrder};

pub trait Backend: Send + Sync {
    fn name(&self) -> &'static str;
    fn supports_if_not_exists(&self) -> bool;
    fn supports_alter_column(&self) -> bool;
    fn supports_drop_column(&self) -> bool;
    fn supports_transactional_ddl(&self) -> bool;

    fn build_table_create(&self, stmt: TableCreateStatement) -> String;
    fn build_table_drop(&self, stmt: TableDropStatement) -> String;
    fn build_table_rename(&self, stmt: TableRenameStatement) -> String;
    fn build_table_alter(&self, stmt: TableAlterStatement) -> String;
    fn build_index_create(&self, stmt: IndexCreateStatement) -> String;
    fn build_index_drop(&self, stmt: IndexDropStatement) -> String;

    fn create_table_sql(&self, name: &str, fields: &[Field]) -> Vec<String> {
        let mut stmt = Table::create();
        stmt.table(Alias::new(name));

        for field in fields {
            stmt.col(field_to_column_def(field));
        }

        // Add foreign key constraints separately for fields that have them
        for field in fields {
            if let Some(ref fk) = field.references {
                stmt.foreign_key(
                    SeaForeignKey::create()
                        .from_col(Alias::new(&field.name))
                        .to_tbl(Alias::new(&fk.table))
                        .to_col(Alias::new(&fk.column))
                        .on_delete(referential_action_to_sea(&fk.on_delete))
                        .on_update(referential_action_to_sea(&fk.on_update)),
                );
            }
        }

        vec![self.build_table_create(stmt)]
    }

    fn drop_table_sql(&self, name: &str) -> String {
        let stmt = Table::drop().table(Alias::new(name)).to_owned();
        self.build_table_drop(stmt)
    }

    fn rename_table_sql(&self, old_name: &str, new_name: &str) -> String {
        let stmt = Table::rename()
            .table(Alias::new(old_name), Alias::new(new_name))
            .to_owned();
        self.build_table_rename(stmt)
    }

    fn add_field_sql(&self, table: &str, field: &Field) -> Vec<String> {
        let stmt = Table::alter()
            .table(Alias::new(table))
            .add_column(field_to_column_def(field))
            .to_owned();
        vec![self.build_table_alter(stmt)]
    }

    fn drop_field_sql(&self, table: &str, field_name: &str) -> Vec<String> {
        let stmt = Table::alter()
            .table(Alias::new(table))
            .drop_column(Alias::new(field_name))
            .to_owned();
        vec![self.build_table_alter(stmt)]
    }

    fn rename_field_sql(&self, table: &str, old_name: &str, new_name: &str) -> Vec<String> {
        let stmt = Table::alter()
            .table(Alias::new(table))
            .rename_column(Alias::new(old_name), Alias::new(new_name))
            .to_owned();
        vec![self.build_table_alter(stmt)]
    }

    fn alter_field_sql(
        &self,
        table: &str,
        field_name: &str,
        changes: &FieldChanges,
    ) -> Vec<String> {
        let mut col = ColumnDef::new(Alias::new(field_name));

        if let Some(ref field_type) = changes.field_type {
            apply_column_type(&mut col, field_type);
        }

        if let Some(nullable) = changes.nullable {
            if nullable {
                col.null();
            } else {
                col.not_null();
            }
        }

        if let Some(Some(ref default_val)) = changes.default {
            col.default(sea_query::Expr::cust(default_val));
        }

        let stmt = Table::alter()
            .table(Alias::new(table))
            .modify_column(col)
            .to_owned();

        vec![self.build_table_alter(stmt)]
    }

    fn add_index_sql(&self, table: &str, index: &Index) -> String {
        let mut stmt = SeaIndex::create();
        stmt.name(&index.name).table(Alias::new(table));

        if index.unique {
            stmt.unique();
        }

        for (col_name, order) in &index.columns {
            match order {
                IndexOrder::Asc => stmt.col(Alias::new(col_name)),
                IndexOrder::Desc => stmt.col((Alias::new(col_name), sea_query::IndexOrder::Desc)),
            };
        }

        self.build_index_create(stmt.to_owned())
    }

    fn drop_index_sql(&self, table: &str, index_name: &str) -> String {
        let stmt = SeaIndex::drop()
            .name(index_name)
            .table(Alias::new(table))
            .to_owned();
        self.build_index_drop(stmt)
    }

    fn add_constraint_sql(&self, table: &str, constraint: &Constraint) -> String {
        match constraint {
            Constraint::Unique { name, columns } => {
                let mut stmt = SeaIndex::create();
                stmt.name(name).table(Alias::new(table)).unique();
                for col in columns {
                    stmt.col(Alias::new(col));
                }
                self.build_index_create(stmt.to_owned())
            }
            Constraint::Check { name, expression } => {
                // sea-query doesn't have great check constraint support
                // Fall back to raw SQL construction
                format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({})",
                    self.quote_identifier(table),
                    self.quote_identifier(name),
                    expression
                )
            }
            Constraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
            } => {
                // sea-query FK support is limited for ALTER TABLE ADD CONSTRAINT
                let cols: Vec<String> = columns.iter().map(|c| self.quote_identifier(c)).collect();
                let ref_cols: Vec<String> = ref_columns
                    .iter()
                    .map(|c| self.quote_identifier(c))
                    .collect();
                format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
                    self.quote_identifier(table),
                    self.quote_identifier(name),
                    cols.join(", "),
                    self.quote_identifier(ref_table),
                    ref_cols.join(", "),
                    on_delete.as_sql(),
                    on_update.as_sql()
                )
            }
        }
    }

    fn drop_constraint_sql(&self, table: &str, constraint_name: &str) -> String;

    fn quote_identifier(&self, name: &str) -> String;
}

#[derive(Debug, Clone, Default)]
pub struct FieldChanges {
    pub field_type: Option<FieldType>,
    pub nullable: Option<bool>,
    pub default: Option<Option<String>>,
}

impl FieldChanges {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_type(mut self, field_type: FieldType) -> Self {
        self.field_type = Some(field_type);
        self
    }

    pub fn set_nullable(mut self, nullable: bool) -> Self {
        self.nullable = Some(nullable);
        self
    }

    pub fn set_default(mut self, default: Option<String>) -> Self {
        self.default = Some(default);
        self
    }
}

fn field_to_column_def(field: &Field) -> ColumnDef {
    let mut col = ColumnDef::new(Alias::new(&field.name));

    apply_column_type(&mut col, &field.field_type);

    if field.primary_key {
        col.primary_key();
        if matches!(field.field_type, FieldType::Serial | FieldType::BigSerial) {
            col.auto_increment();
        }
    }

    if !field.nullable && !field.primary_key {
        col.not_null();
    }

    if field.unique && !field.primary_key {
        col.unique_key();
    }

    if let Some(ref default) = field.default {
        col.default(sea_query::Expr::cust(default));
    }

    col
}

fn apply_column_type(col: &mut ColumnDef, field_type: &FieldType) {
    match field_type {
        FieldType::Serial => {
            col.integer();
        }
        FieldType::BigSerial => {
            col.big_integer();
        }
        FieldType::Integer => {
            col.integer();
        }
        FieldType::BigInt => {
            col.big_integer();
        }
        FieldType::SmallInt => {
            col.small_integer();
        }
        FieldType::Text => {
            col.text();
        }
        FieldType::VarChar(len) => {
            col.string_len(*len as u32);
        }
        FieldType::Boolean => {
            col.boolean();
        }
        FieldType::Timestamp => {
            col.timestamp();
        }
        FieldType::TimestampTz => {
            col.timestamp_with_time_zone();
        }
        FieldType::Date => {
            col.date();
        }
        FieldType::Time => {
            col.time();
        }
        FieldType::Uuid => {
            col.uuid();
        }
        FieldType::Json => {
            col.json();
        }
        FieldType::JsonB => {
            col.json_binary();
        }
        FieldType::Binary => {
            col.binary();
        }
        FieldType::Real => {
            col.float();
        }
        FieldType::DoublePrecision => {
            col.double();
        }
        FieldType::Decimal { precision, scale } => {
            col.decimal_len(*precision as u32, *scale as u32);
        }
    }
}

fn referential_action_to_sea(action: &ReferentialAction) -> ForeignKeyAction {
    match action {
        ReferentialAction::NoAction => ForeignKeyAction::NoAction,
        ReferentialAction::Restrict => ForeignKeyAction::Restrict,
        ReferentialAction::Cascade => ForeignKeyAction::Cascade,
        ReferentialAction::SetNull => ForeignKeyAction::SetNull,
        ReferentialAction::SetDefault => ForeignKeyAction::SetDefault,
    }
}
