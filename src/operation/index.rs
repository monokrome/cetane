use crate::backend::Backend;
use crate::operation::Operation;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum IndexOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub columns: Vec<(String, IndexOrder)>,
    pub unique: bool,
    pub where_clause: Option<String>,
}

impl Index {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            unique: false,
            where_clause: None,
        }
    }

    pub fn column(mut self, name: impl Into<String>) -> Self {
        self.columns.push((name.into(), IndexOrder::Asc));
        self
    }

    pub fn column_desc(mut self, name: impl Into<String>) -> Self {
        self.columns.push((name.into(), IndexOrder::Desc));
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Add a WHERE clause to create a partial index.
    /// Example: `.filter("status = 'active'")`
    pub fn filter(mut self, condition: impl Into<String>) -> Self {
        self.where_clause = Some(condition.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct AddIndex {
    pub table: String,
    pub index: Index,
}

impl AddIndex {
    pub fn new(table: impl Into<String>, index: Index) -> Self {
        Self {
            table: table.into(),
            index,
        }
    }
}

impl Operation for AddIndex {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        vec![backend.add_index_sql(&self.table, &self.index)]
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        Some(vec![backend.drop_index_sql(&self.table, &self.index.name)])
    }

    fn describe(&self) -> String {
        format!("Add index {} on {}", self.index.name, self.table)
    }
}

#[derive(Debug, Clone)]
pub struct RemoveIndex {
    pub table: String,
    pub name: String,
    pub index: Option<Index>,
}

impl RemoveIndex {
    pub fn new(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            name: name.into(),
            index: None,
        }
    }

    pub fn with_definition(mut self, index: Index) -> Self {
        self.index = Some(index);
        self
    }
}

impl Operation for RemoveIndex {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        vec![backend.drop_index_sql(&self.table, &self.name)]
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        self.index
            .as_ref()
            .map(|index| vec![backend.add_index_sql(&self.table, index)])
    }

    fn describe(&self) -> String {
        format!("Remove index {} from {}", self.name, self.table)
    }

    fn is_reversible(&self) -> bool {
        self.index.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Sqlite;

    #[test]
    fn add_index_generates_sql() {
        let index = Index::new("idx_users_email").column("email");
        let op = AddIndex::new("users", index);

        let sql = op.forward(&Sqlite);
        assert_eq!(
            sql[0],
            "CREATE INDEX \"idx_users_email\" ON \"users\" (\"email\")"
        );
    }

    #[test]
    fn add_unique_index_generates_sql() {
        let index = Index::new("idx_users_email").column("email").unique();
        let op = AddIndex::new("users", index);

        let sql = op.forward(&Sqlite);
        assert_eq!(
            sql[0],
            "CREATE UNIQUE INDEX \"idx_users_email\" ON \"users\" (\"email\")"
        );
    }

    #[test]
    fn add_composite_index_generates_sql() {
        let index = Index::new("idx_users_name")
            .column("first_name")
            .column("last_name");
        let op = AddIndex::new("users", index);

        let sql = op.forward(&Sqlite);
        assert_eq!(
            sql[0],
            "CREATE INDEX \"idx_users_name\" ON \"users\" (\"first_name\", \"last_name\")"
        );
    }

    #[test]
    fn add_index_is_reversible() {
        let index = Index::new("idx_users_email").column("email");
        let op = AddIndex::new("users", index);

        let reverse = op.backward(&Sqlite).unwrap();
        assert!(reverse[0].contains("DROP INDEX"));
        assert!(reverse[0].contains("\"idx_users_email\""));
    }

    #[test]
    fn remove_index_generates_sql() {
        let op = RemoveIndex::new("users", "idx_users_email");

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("DROP INDEX"));
        assert!(sql[0].contains("\"idx_users_email\""));
    }

    #[test]
    fn remove_index_without_definition_not_reversible() {
        let op = RemoveIndex::new("users", "idx_users_email");
        assert!(!op.is_reversible());
    }

    #[test]
    fn remove_index_with_definition_is_reversible() {
        let index = Index::new("idx_users_email").column("email");
        let op = RemoveIndex::new("users", "idx_users_email").with_definition(index);

        assert!(op.is_reversible());
        let reverse = op.backward(&Sqlite).unwrap();
        assert!(reverse[0].contains("CREATE INDEX"));
    }

    #[test]
    fn index_with_descending_column() {
        let index = Index::new("idx_created_at").column_desc("created_at");
        let op = AddIndex::new("posts", index);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("\"created_at\" DESC"));
    }

    #[test]
    fn index_mixed_order() {
        let index = Index::new("idx_mixed")
            .column("user_id")
            .column_desc("created_at");
        let op = AddIndex::new("posts", index);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("\"user_id\""));
        assert!(sql[0].contains("\"created_at\" DESC"));
    }

    #[test]
    fn add_index_describe() {
        let index = Index::new("idx_email").column("email");
        let op = AddIndex::new("users", index);
        assert_eq!(op.describe(), "Add index idx_email on users");
    }

    #[test]
    fn remove_index_describe() {
        let op = RemoveIndex::new("users", "idx_email");
        assert_eq!(op.describe(), "Remove index idx_email from users");
    }

    #[test]
    fn partial_index_with_filter() {
        let index = Index::new("idx_active_users")
            .column("email")
            .filter("status = 'active'");
        let op = AddIndex::new("users", index);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("CREATE INDEX"));
        assert!(sql[0].contains("WHERE status = 'active'"));
    }

    #[test]
    fn partial_unique_index() {
        let index = Index::new("idx_unique_active_email")
            .column("email")
            .unique()
            .filter("deleted_at IS NULL");
        let op = AddIndex::new("users", index);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("CREATE UNIQUE INDEX"));
        assert!(sql[0].contains("WHERE deleted_at IS NULL"));
    }
}
