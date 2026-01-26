use crate::backend::{Backend, FieldChanges};
use crate::field::{Field, FieldType};
use crate::operation::Operation;

#[derive(Debug, Clone)]
pub struct AddField {
    pub table: String,
    pub field: Field,
}

impl AddField {
    pub fn new(table: impl Into<String>, field: Field) -> Self {
        Self {
            table: table.into(),
            field,
        }
    }
}

impl Operation for AddField {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        backend.add_field_sql(&self.table, &self.field)
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        if backend.supports_drop_column() {
            Some(backend.drop_field_sql(&self.table, &self.field.name))
        } else {
            None
        }
    }

    fn describe(&self) -> String {
        format!("Add field {} to {}", self.field.name, self.table)
    }

    fn is_reversible(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct RemoveField {
    pub table: String,
    pub field_name: String,
    pub field: Option<Field>,
}

impl RemoveField {
    pub fn new(table: impl Into<String>, field_name: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            field_name: field_name.into(),
            field: None,
        }
    }

    pub fn with_definition(mut self, field: Field) -> Self {
        self.field = Some(field);
        self
    }
}

impl Operation for RemoveField {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        backend.drop_field_sql(&self.table, &self.field_name)
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        self.field
            .as_ref()
            .map(|f| backend.add_field_sql(&self.table, f))
    }

    fn describe(&self) -> String {
        format!("Remove field {} from {}", self.field_name, self.table)
    }

    fn is_reversible(&self) -> bool {
        self.field.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct RenameField {
    pub table: String,
    pub old_name: String,
    pub new_name: String,
}

impl RenameField {
    pub fn new(
        table: impl Into<String>,
        old_name: impl Into<String>,
        new_name: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            old_name: old_name.into(),
            new_name: new_name.into(),
        }
    }
}

impl Operation for RenameField {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        backend.rename_field_sql(&self.table, &self.old_name, &self.new_name)
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        Some(backend.rename_field_sql(&self.table, &self.new_name, &self.old_name))
    }

    fn describe(&self) -> String {
        format!(
            "Rename field {} to {} on {}",
            self.old_name, self.new_name, self.table
        )
    }
}

#[derive(Debug, Clone)]
pub struct AlterField {
    pub table: String,
    pub field_name: String,
    pub changes: FieldChanges,
    pub reverse_changes: Option<FieldChanges>,
}

impl AlterField {
    pub fn new(table: impl Into<String>, field_name: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            field_name: field_name.into(),
            changes: FieldChanges::new(),
            reverse_changes: None,
        }
    }

    pub fn set_type(mut self, field_type: FieldType) -> Self {
        self.changes.field_type = Some(field_type);
        self
    }

    pub fn set_nullable(mut self, nullable: bool) -> Self {
        self.changes.nullable = Some(nullable);
        self
    }

    pub fn set_default(mut self, default: Option<String>) -> Self {
        self.changes.default = Some(default);
        self
    }

    pub fn with_reverse(mut self, reverse_changes: FieldChanges) -> Self {
        self.reverse_changes = Some(reverse_changes);
        self
    }
}

impl Operation for AlterField {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        backend.alter_field_sql(&self.table, &self.field_name, &self.changes)
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        self.reverse_changes
            .as_ref()
            .map(|changes| backend.alter_field_sql(&self.table, &self.field_name, changes))
    }

    fn describe(&self) -> String {
        format!("Alter field {} on {}", self.field_name, self.table)
    }

    fn is_reversible(&self) -> bool {
        self.reverse_changes.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Postgres, Sqlite};

    #[test]
    fn add_field_generates_sql() {
        let field = Field::new("email", FieldType::Text).not_null().unique();
        let op = AddField::new("users", field);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("\"users\""));
        assert!(sql[0].contains("ADD COLUMN"));
        assert!(sql[0].contains("\"email\""));
        assert!(sql[0].contains("NOT NULL"));
        assert!(sql[0].contains("UNIQUE"));
    }

    #[test]
    fn add_field_is_reversible() {
        let field = Field::new("email", FieldType::Text);
        let op = AddField::new("users", field);

        let reverse = op.backward(&Sqlite).unwrap();
        assert_eq!(reverse[0], "ALTER TABLE \"users\" DROP COLUMN \"email\"");
    }

    #[test]
    fn remove_field_generates_sql() {
        let op = RemoveField::new("users", "email");

        let sql = op.forward(&Sqlite);
        assert_eq!(sql[0], "ALTER TABLE \"users\" DROP COLUMN \"email\"");
    }

    #[test]
    fn remove_field_without_definition_not_reversible() {
        let op = RemoveField::new("users", "email");
        assert!(!op.is_reversible());
    }

    #[test]
    fn remove_field_with_definition_is_reversible() {
        let field = Field::new("email", FieldType::Text).not_null();
        let op = RemoveField::new("users", "email").with_definition(field);

        assert!(op.is_reversible());
        let reverse = op.backward(&Sqlite).unwrap();
        assert!(reverse[0].contains("ADD COLUMN"));
    }

    #[test]
    fn rename_field_is_reversible() {
        let op = RenameField::new("users", "email", "email_address");

        let forward = op.forward(&Sqlite);
        assert_eq!(
            forward[0],
            "ALTER TABLE \"users\" RENAME COLUMN \"email\" TO \"email_address\""
        );

        let backward = op.backward(&Sqlite).unwrap();
        assert_eq!(
            backward[0],
            "ALTER TABLE \"users\" RENAME COLUMN \"email_address\" TO \"email\""
        );
    }

    #[test]
    fn add_field_describe() {
        let field = Field::new("email", FieldType::Text);
        let op = AddField::new("users", field);
        assert_eq!(op.describe(), "Add field email to users");
    }

    #[test]
    fn alter_field_change_type() {
        // Use Postgres - SQLite doesn't support MODIFY COLUMN
        let op = AlterField::new("users", "age").set_type(FieldType::BigInt);

        let sql = op.forward(&Postgres);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("\"users\""));
        assert!(sql[0].contains("\"age\""));
    }

    #[test]
    fn alter_field_set_not_null() {
        let op = AlterField::new("users", "email").set_nullable(false);

        let sql = op.forward(&Postgres);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("\"email\""));
        assert!(sql[0].contains("NOT NULL"));
    }

    #[test]
    fn alter_field_set_nullable() {
        let op = AlterField::new("users", "bio").set_nullable(true);

        let sql = op.forward(&Postgres);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("\"bio\""));
        assert!(sql[0].contains("NULL"));
    }

    #[test]
    fn alter_field_set_default() {
        let op = AlterField::new("users", "status").set_default(Some("'active'".to_string()));

        let sql = op.forward(&Postgres);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("\"status\""));
        assert!(sql[0].contains("DEFAULT"));
        assert!(sql[0].contains("'active'"));
    }

    #[test]
    fn alter_field_without_reverse_not_reversible() {
        let op = AlterField::new("users", "email").set_nullable(false);
        assert!(!op.is_reversible());
        assert!(op.backward(&Postgres).is_none());
    }

    #[test]
    fn alter_field_with_reverse_is_reversible() {
        let reverse = FieldChanges::new().set_nullable(true);
        let op = AlterField::new("users", "email")
            .set_nullable(false)
            .with_reverse(reverse);

        assert!(op.is_reversible());
        let backward = op.backward(&Postgres).unwrap();
        assert!(backward[0].contains("NULL"));
    }

    #[test]
    fn alter_field_describe() {
        let op = AlterField::new("users", "email").set_nullable(false);
        assert_eq!(op.describe(), "Alter field email on users");
    }

    #[test]
    fn alter_field_multiple_changes() {
        let op = AlterField::new("users", "score")
            .set_type(FieldType::BigInt)
            .set_nullable(false)
            .set_default(Some("0".to_string()));

        let sql = op.forward(&Postgres);
        assert!(sql[0].contains("\"score\""));
        assert!(sql[0].contains("NOT NULL"));
        assert!(sql[0].contains("DEFAULT"));
    }

    #[test]
    fn alter_field_not_supported_on_sqlite() {
        // SQLite doesn't support ALTER COLUMN, verify the backend reports this
        assert!(!Sqlite.supports_alter_column());
        assert!(Postgres.supports_alter_column());
    }
}
