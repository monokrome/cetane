use sea_query::{
    IndexCreateStatement, IndexDropStatement, MysqlQueryBuilder, TableAlterStatement,
    TableCreateStatement, TableDropStatement, TableRenameStatement,
};

use crate::backend::Backend;

#[derive(Debug, Clone, Copy, Default)]
pub struct MySql;

impl Backend for MySql {
    fn name(&self) -> &'static str {
        "mysql"
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
        // MySQL issues implicit commits for DDL statements
        false
    }

    fn build_table_create(&self, stmt: TableCreateStatement) -> String {
        stmt.to_string(MysqlQueryBuilder)
    }

    fn build_table_drop(&self, stmt: TableDropStatement) -> String {
        stmt.to_string(MysqlQueryBuilder)
    }

    fn build_table_rename(&self, stmt: TableRenameStatement) -> String {
        stmt.to_string(MysqlQueryBuilder)
    }

    fn build_table_alter(&self, stmt: TableAlterStatement) -> String {
        stmt.to_string(MysqlQueryBuilder)
    }

    fn build_index_create(&self, stmt: IndexCreateStatement) -> String {
        stmt.to_string(MysqlQueryBuilder)
    }

    fn build_index_drop(&self, stmt: IndexDropStatement) -> String {
        stmt.to_string(MysqlQueryBuilder)
    }

    fn drop_constraint_sql(&self, table: &str, constraint_name: &str) -> String {
        // MySQL uses DROP INDEX for most constraints, DROP FOREIGN KEY for FKs
        // This is a simplified version - in practice you'd need to know the constraint type
        format!(
            "ALTER TABLE `{}` DROP INDEX `{}`",
            table.replace('`', "``"),
            constraint_name.replace('`', "``")
        )
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::{Field, FieldType, ReferentialAction};
    use crate::operation::{Index, IndexOrder};

    #[test]
    fn mysql_backend_name() {
        assert_eq!(MySql.name(), "mysql");
    }

    #[test]
    fn mysql_supports_if_not_exists() {
        assert!(MySql.supports_if_not_exists());
    }

    #[test]
    fn mysql_supports_alter_column() {
        assert!(MySql.supports_alter_column());
    }

    #[test]
    fn mysql_supports_drop_column() {
        assert!(MySql.supports_drop_column());
    }

    #[test]
    fn mysql_does_not_support_transactional_ddl() {
        assert!(!MySql.supports_transactional_ddl());
    }

    #[test]
    fn mysql_creates_simple_table() {
        let backend = MySql;
        let fields = vec![
            Field::new("id", FieldType::Serial).primary_key(),
            Field::new("name", FieldType::Text).not_null(),
        ];

        let sql = backend.create_table_sql("users", &fields);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("CREATE TABLE"));
        // MySQL uses backticks
        assert!(sql[0].contains("`users`"));
        assert!(sql[0].contains("`id`"));
        assert!(sql[0].contains("`name`"));
        assert!(sql[0].contains("PRIMARY KEY"));
        assert!(sql[0].contains("NOT NULL"));
    }

    #[test]
    fn mysql_creates_table_with_auto_increment() {
        let backend = MySql;
        let fields = vec![Field::new("id", FieldType::Serial).primary_key()];

        let sql = backend.create_table_sql("users", &fields);
        assert!(sql[0].contains("AUTO_INCREMENT"));
    }

    #[test]
    fn mysql_creates_table_with_foreign_key() {
        let backend = MySql;
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
    fn mysql_drop_table() {
        let backend = MySql;
        let sql = backend.drop_table_sql("users");
        assert!(sql.contains("DROP TABLE"));
        assert!(sql.contains("`users`"));
    }

    #[test]
    fn mysql_add_field() {
        let backend = MySql;
        let field = Field::new("email", FieldType::VarChar(255)).not_null().unique();

        let sql = backend.add_field_sql("users", &field);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER TABLE"));
        assert!(sql[0].contains("ADD COLUMN"));
        assert!(sql[0].contains("`email`"));
    }

    #[test]
    fn mysql_create_index() {
        let backend = MySql;
        let index = Index {
            name: "idx_users_email".to_string(),
            columns: vec![("email".to_string(), IndexOrder::Asc)],
            unique: false,
        };

        let sql = backend.add_index_sql("users", &index);
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("`idx_users_email`"));
        assert!(sql.contains("`users`"));
    }

    #[test]
    fn mysql_drop_constraint() {
        let backend = MySql;
        let sql = backend.drop_constraint_sql("users", "uq_email");
        assert!(sql.contains("ALTER TABLE"));
        assert!(sql.contains("DROP INDEX"));
        assert!(sql.contains("`uq_email`"));
    }

    #[test]
    fn mysql_quote_identifier() {
        let backend = MySql;
        assert_eq!(backend.quote_identifier("users"), "`users`");
        assert_eq!(backend.quote_identifier("user`name"), "`user``name`");
    }
}
