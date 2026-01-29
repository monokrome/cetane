use sea_query::{
    IndexCreateStatement, IndexDropStatement, SqliteQueryBuilder, TableAlterStatement,
    TableCreateStatement, TableDropStatement, TableRenameStatement,
};

use crate::backend::Backend;

#[derive(Debug, Clone, Copy, Default)]
pub struct Sqlite;

impl Backend for Sqlite {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn supports_if_not_exists(&self) -> bool {
        true
    }

    fn supports_alter_column(&self) -> bool {
        false
    }

    fn supports_drop_column(&self) -> bool {
        true
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }

    fn build_table_create(&self, stmt: TableCreateStatement) -> String {
        stmt.to_string(SqliteQueryBuilder)
    }

    fn build_table_drop(&self, stmt: TableDropStatement) -> String {
        stmt.to_string(SqliteQueryBuilder)
    }

    fn build_table_rename(&self, stmt: TableRenameStatement) -> String {
        stmt.to_string(SqliteQueryBuilder)
    }

    fn build_table_alter(&self, stmt: TableAlterStatement) -> String {
        stmt.to_string(SqliteQueryBuilder)
    }

    fn build_index_create(&self, stmt: IndexCreateStatement) -> String {
        stmt.to_string(SqliteQueryBuilder)
    }

    fn build_index_drop(&self, stmt: IndexDropStatement) -> String {
        stmt.to_string(SqliteQueryBuilder)
    }

    fn drop_constraint_sql(&self, _table: &str, constraint_name: &str) -> String {
        // SQLite doesn't support DROP CONSTRAINT, but indexes can be dropped
        format!(
            "DROP INDEX IF EXISTS \"{}\"",
            constraint_name.replace('"', "\"\"")
        )
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::{Field, FieldType, ReferentialAction};
    use crate::operation::{Index, IndexOrder};

    #[test]
    fn sqlite_backend_name() {
        assert_eq!(Sqlite.name(), "sqlite");
    }

    #[test]
    fn sqlite_supports_if_not_exists() {
        assert!(Sqlite.supports_if_not_exists());
    }

    #[test]
    fn sqlite_does_not_support_alter_column() {
        assert!(!Sqlite.supports_alter_column());
    }

    #[test]
    fn sqlite_supports_drop_column() {
        assert!(Sqlite.supports_drop_column());
    }

    #[test]
    fn sqlite_supports_transactional_ddl() {
        assert!(Sqlite.supports_transactional_ddl());
    }

    #[test]
    fn sqlite_creates_simple_table() {
        let backend = Sqlite;
        let fields = vec![
            Field::new("id", FieldType::Serial).primary_key(),
            Field::new("name", FieldType::Text).not_null(),
        ];

        let sql = backend.create_table_sql("users", &fields);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("CREATE TABLE"));
        assert!(sql[0].contains("\"users\""));
        assert!(sql[0].contains("\"id\""));
        assert!(sql[0].contains("\"name\""));
        assert!(sql[0].contains("PRIMARY KEY"));
        assert!(sql[0].contains("NOT NULL"));
    }

    #[test]
    fn sqlite_creates_table_with_foreign_key() {
        let backend = Sqlite;
        let fields = vec![
            Field::new("id", FieldType::Serial).primary_key(),
            Field::new("user_id", FieldType::Integer)
                .not_null()
                .references("users", "id")
                .on_delete(ReferentialAction::Cascade),
        ];

        let sql = backend.create_table_sql("posts", &fields);
        assert!(sql[0].contains("FOREIGN KEY"));
        assert!(sql[0].contains("REFERENCES"));
        assert!(sql[0].contains("ON DELETE CASCADE"));
    }

    #[test]
    fn sqlite_drop_table() {
        let backend = Sqlite;
        let sql = backend.drop_table_sql("users");
        assert!(sql.contains("DROP TABLE"));
        assert!(sql.contains("\"users\""));
    }

    #[test]
    fn sqlite_rename_table() {
        let backend = Sqlite;
        let sql = backend.rename_table_sql("old_users", "users");
        assert!(sql.contains("RENAME"));
        assert!(sql.contains("\"old_users\""));
        assert!(sql.contains("\"users\""));
    }

    #[test]
    fn sqlite_add_field() {
        let backend = Sqlite;
        let field = Field::new("email", FieldType::Text).not_null().unique();

        let sql = backend.add_field_sql("users", &field);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("ADD COLUMN"));
        assert!(sql[0].contains("\"email\""));
    }

    #[test]
    fn sqlite_drop_field() {
        let backend = Sqlite;
        let sql = backend.drop_field_sql("users", "email");
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("DROP COLUMN"));
        assert!(sql[0].contains("\"email\""));
    }

    #[test]
    fn sqlite_rename_field() {
        let backend = Sqlite;
        let sql = backend.rename_field_sql("users", "email", "email_address");
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("RENAME COLUMN"));
        assert!(sql[0].contains("\"email\""));
        assert!(sql[0].contains("\"email_address\""));
    }

    #[test]
    fn sqlite_create_index() {
        let backend = Sqlite;
        let index = Index {
            name: "idx_users_email".to_string(),
            columns: vec![("email".to_string(), IndexOrder::Asc)],
            unique: false,
            where_clause: None,
        };

        let sql = backend.add_index_sql("users", &index);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("\"idx_users_email\""));
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("\"email\""));
    }

    #[test]
    fn sqlite_create_unique_index() {
        let backend = Sqlite;
        let index = Index {
            name: "idx_users_email".to_string(),
            columns: vec![("email".to_string(), IndexOrder::Asc)],
            unique: true,
            where_clause: None,
        };

        let sql = backend.add_index_sql("users", &index);
        assert!(sql.contains("CREATE UNIQUE INDEX"));
    }

    #[test]
    fn sqlite_create_partial_index() {
        let backend = Sqlite;
        let index = Index {
            name: "idx_active_users".to_string(),
            columns: vec![("email".to_string(), IndexOrder::Asc)],
            unique: false,
            where_clause: Some("status = 'active'".to_string()),
        };

        let sql = backend.add_index_sql("users", &index);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("WHERE status = 'active'"));
    }

    #[test]
    fn sqlite_drop_index() {
        let backend = Sqlite;
        let sql = backend.drop_index_sql("users", "idx_users_email");
        assert!(sql.contains("DROP INDEX"));
        assert!(sql.contains("\"idx_users_email\""));
    }

    #[test]
    fn sqlite_quote_identifier() {
        let backend = Sqlite;
        assert_eq!(backend.quote_identifier("users"), "\"users\"");
        assert_eq!(backend.quote_identifier("user\"name"), "\"user\"\"name\"");
    }

    #[test]
    fn sqlite_drop_constraint() {
        let backend = Sqlite;
        let sql = backend.drop_constraint_sql("users", "uq_email");
        assert!(sql.contains("DROP INDEX"));
        assert!(sql.contains("\"uq_email\""));
    }
}
