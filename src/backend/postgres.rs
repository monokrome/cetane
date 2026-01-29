use sea_query::{
    IndexCreateStatement, IndexDropStatement, PostgresQueryBuilder, TableAlterStatement,
    TableCreateStatement, TableDropStatement, TableRenameStatement,
};

use crate::backend::Backend;

#[derive(Debug, Clone, Copy, Default)]
pub struct Postgres;

impl Backend for Postgres {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn supports_if_not_exists(&self) -> bool {
        true
    }

    fn supports_alter_column(&self) -> bool {
        true
    }

    fn supports_drop_column(&self) -> bool {
        true
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }

    fn build_table_create(&self, stmt: TableCreateStatement) -> String {
        stmt.to_string(PostgresQueryBuilder)
    }

    fn build_table_drop(&self, stmt: TableDropStatement) -> String {
        stmt.to_string(PostgresQueryBuilder)
    }

    fn build_table_rename(&self, stmt: TableRenameStatement) -> String {
        stmt.to_string(PostgresQueryBuilder)
    }

    fn build_table_alter(&self, stmt: TableAlterStatement) -> String {
        stmt.to_string(PostgresQueryBuilder)
    }

    fn build_index_create(&self, stmt: IndexCreateStatement) -> String {
        stmt.to_string(PostgresQueryBuilder)
    }

    fn build_index_drop(&self, stmt: IndexDropStatement) -> String {
        stmt.to_string(PostgresQueryBuilder)
    }

    fn drop_constraint_sql(&self, table: &str, constraint_name: &str) -> String {
        format!(
            "ALTER TABLE \"{}\" DROP CONSTRAINT \"{}\"",
            table.replace('"', "\"\""),
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
    fn postgres_backend_name() {
        assert_eq!(Postgres.name(), "postgres");
    }

    #[test]
    fn postgres_supports_if_not_exists() {
        assert!(Postgres.supports_if_not_exists());
    }

    #[test]
    fn postgres_supports_alter_column() {
        assert!(Postgres.supports_alter_column());
    }

    #[test]
    fn postgres_supports_drop_column() {
        assert!(Postgres.supports_drop_column());
    }

    #[test]
    fn postgres_supports_transactional_ddl() {
        assert!(Postgres.supports_transactional_ddl());
    }

    #[test]
    fn postgres_creates_simple_table() {
        let backend = Postgres;
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
    fn postgres_creates_table_with_foreign_key() {
        let backend = Postgres;
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
    fn postgres_drop_table() {
        let backend = Postgres;
        let sql = backend.drop_table_sql("users");
        assert!(sql.contains("DROP TABLE"));
        assert!(sql.contains("\"users\""));
    }

    #[test]
    fn postgres_add_field() {
        let backend = Postgres;
        let field = Field::new("email", FieldType::Text).not_null().unique();

        let sql = backend.add_field_sql("users", &field);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("ADD COLUMN"));
        assert!(sql[0].contains("\"email\""));
    }

    #[test]
    fn postgres_create_index() {
        let backend = Postgres;
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
    }

    #[test]
    fn postgres_create_partial_index() {
        let backend = Postgres;
        let index = Index {
            name: "idx_active_users".to_string(),
            columns: vec![("email".to_string(), IndexOrder::Asc)],
            unique: false,
            where_clause: Some("deleted_at IS NULL".to_string()),
        };

        let sql = backend.add_index_sql("users", &index);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("WHERE deleted_at IS NULL"));
    }

    #[test]
    fn postgres_drop_constraint() {
        let backend = Postgres;
        let sql = backend.drop_constraint_sql("users", "uq_email");
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("DROP CONSTRAINT"));
        assert!(sql.contains("\"uq_email\""));
    }

    #[test]
    fn postgres_quote_identifier() {
        let backend = Postgres;
        assert_eq!(backend.quote_identifier("users"), "\"users\"");
        assert_eq!(backend.quote_identifier("user\"name"), "\"user\"\"name\"");
    }
}
