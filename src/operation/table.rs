use crate::backend::Backend;
use crate::field::{Field, FieldType};
use crate::operation::Operation;

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: String,
    pub fields: Vec<Field>,
}

impl CreateTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: Vec::new(),
        }
    }

    pub fn field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.push(Field::new(name, field_type));
        self
    }

    pub fn add_field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }
}

impl Operation for CreateTable {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        backend.create_table_sql(&self.name, &self.fields)
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        Some(vec![backend.drop_table_sql(&self.name)])
    }

    fn describe(&self) -> String {
        format!("Create table {}", self.name)
    }
}

#[derive(Debug, Clone)]
pub struct DropTable {
    pub name: String,
    pub fields: Option<Vec<Field>>,
}

impl DropTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            fields: None,
        }
    }

    pub fn with_fields(mut self, fields: Vec<Field>) -> Self {
        self.fields = Some(fields);
        self
    }
}

impl Operation for DropTable {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        vec![backend.drop_table_sql(&self.name)]
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        self.fields
            .as_ref()
            .map(|fields| backend.create_table_sql(&self.name, fields))
    }

    fn describe(&self) -> String {
        format!("Drop table {}", self.name)
    }

    fn is_reversible(&self) -> bool {
        self.fields.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct RenameTable {
    pub old_name: String,
    pub new_name: String,
}

impl RenameTable {
    pub fn new(old_name: impl Into<String>, new_name: impl Into<String>) -> Self {
        Self {
            old_name: old_name.into(),
            new_name: new_name.into(),
        }
    }
}

impl Operation for RenameTable {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        vec![backend.rename_table_sql(&self.old_name, &self.new_name)]
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        Some(vec![
            backend.rename_table_sql(&self.new_name, &self.old_name)
        ])
    }

    fn describe(&self) -> String {
        format!("Rename table {} to {}", self.old_name, self.new_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Sqlite;

    #[test]
    fn create_table_generates_valid_sql() {
        let op = CreateTable::new("users")
            .add_field(Field::new("id", FieldType::Serial).primary_key())
            .add_field(Field::new("email", FieldType::Text).not_null());

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("CREATE TABLE"));
        assert!(sql[0].contains("\"users\""));
        assert!(sql[0].contains("\"id\""));
        assert!(sql[0].contains("PRIMARY KEY"));
        assert!(sql[0].contains("AUTOINCREMENT"));
        assert!(sql[0].contains("\"email\""));
        assert!(sql[0].contains("NOT NULL"));
    }

    #[test]
    fn create_table_is_reversible() {
        let op = CreateTable::new("users");
        let reverse = op.backward(&Sqlite);
        assert_eq!(reverse, Some(vec!["DROP TABLE \"users\"".to_string()]));
    }

    #[test]
    fn drop_table_without_fields_is_not_reversible() {
        let op = DropTable::new("users");
        assert!(!op.is_reversible());
        assert!(op.backward(&Sqlite).is_none());
    }

    #[test]
    fn drop_table_with_fields_is_reversible() {
        let fields = vec![
            Field::new("id", FieldType::Serial).primary_key(),
            Field::new("name", FieldType::Text),
        ];
        let op = DropTable::new("users").with_fields(fields);

        assert!(op.is_reversible());
        let reverse = op.backward(&Sqlite).unwrap();
        assert!(reverse[0].contains("CREATE TABLE"));
    }

    #[test]
    fn rename_table_is_reversible() {
        let op = RenameTable::new("old_users", "users");

        let forward = op.forward(&Sqlite);
        assert_eq!(forward[0], "ALTER TABLE \"old_users\" RENAME TO \"users\"");

        let backward = op.backward(&Sqlite).unwrap();
        assert_eq!(backward[0], "ALTER TABLE \"users\" RENAME TO \"old_users\"");
    }

    #[test]
    fn create_table_describe() {
        let op = CreateTable::new("users");
        assert_eq!(op.describe(), "Create table users");
    }

    #[test]
    fn create_table_field_builder() {
        let op = CreateTable::new("users")
            .field("id", FieldType::Serial)
            .field("email", FieldType::Text);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("\"id\""));
        assert!(sql[0].contains("\"email\""));
    }

    #[test]
    fn drop_table_describe() {
        let op = DropTable::new("users");
        assert_eq!(op.describe(), "Drop table users");
    }

    #[test]
    fn rename_table_describe() {
        let op = RenameTable::new("old", "new");
        assert_eq!(op.describe(), "Rename table old to new");
    }

    #[test]
    fn drop_table_forward() {
        let op = DropTable::new("users");
        let sql = op.forward(&Sqlite);
        assert_eq!(sql[0], "DROP TABLE \"users\"");
    }
}
