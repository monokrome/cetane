pub mod backend;
pub mod field;
pub mod migration;
pub mod migrator;
pub mod operation;
pub mod state;

pub mod prelude {
    pub use crate::backend::{Backend, FieldChanges, MySql, Postgres, Sqlite};
    pub use crate::field::{Field, FieldType, ForeignKey, ReferentialAction};
    pub use crate::migration::{Migration, MigrationError, MigrationRegistry};
    pub use crate::migrator::{InMemoryState, MigrationStateStore, Migrator};
    pub use crate::operation::{
        AddConstraint, AddField, AddIndex, AlterField, Constraint, CreateTable, DropTable, Index,
        IndexOrder, Operation, RemoveConstraint, RemoveField, RemoveIndex, RenameField,
        RenameTable, RunSql,
    };

    #[cfg(feature = "sqlite")]
    pub use crate::state::SqliteMigrationState;

    #[cfg(feature = "postgres")]
    pub use crate::state::PostgresMigrationState;

    #[cfg(feature = "mysql")]
    pub use crate::state::MySqlMigrationState;
}

#[cfg(test)]
mod tests {
    use super::prelude::*;

    #[test]
    fn full_migration_workflow() {
        let mut registry = MigrationRegistry::new();

        registry.register(
            Migration::new("0001_create_users")
                .operation(
                    CreateTable::new("users")
                        .add_field(Field::new("id", FieldType::Serial).primary_key())
                        .add_field(Field::new("email", FieldType::Text).not_null().unique())
                        .add_field(Field::new("created_at", FieldType::Timestamp).not_null()),
                )
                .operation(AddIndex::new(
                    "users",
                    Index::new("idx_users_email").column("email").unique(),
                )),
        );

        registry.register(
            Migration::new("0002_create_posts")
                .depends_on(&["0001_create_users"])
                .operation(
                    CreateTable::new("posts")
                        .add_field(Field::new("id", FieldType::Serial).primary_key())
                        .add_field(
                            Field::new("user_id", FieldType::Integer)
                                .not_null()
                                .references("users", "id")
                                .on_delete(ReferentialAction::Cascade),
                        )
                        .add_field(Field::new("title", FieldType::Text).not_null())
                        .add_field(Field::new("body", FieldType::Text)),
                ),
        );

        registry.register(
            Migration::new("0003_add_user_name")
                .depends_on(&["0001_create_users"])
                .operation(AddField::new(
                    "users",
                    Field::new("name", FieldType::VarChar(255)),
                )),
        );

        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let forward_sql = migrator.generate_forward_sql().unwrap();
        assert_eq!(forward_sql.len(), 3);

        let mut executed_sqls = Vec::new();
        let applied = migrator
            .migrate_forward(|sql| {
                executed_sqls.push(sql.to_string());
                Ok(())
            })
            .unwrap();

        assert_eq!(applied.len(), 3);
        assert!(executed_sqls
            .iter()
            .any(|s| s.contains("CREATE TABLE \"users\"")));
        assert!(executed_sqls
            .iter()
            .any(|s| s.contains("CREATE TABLE \"posts\"")));
        assert!(executed_sqls
            .iter()
            .any(|s| s.contains("ADD COLUMN \"name\"")));
    }

    #[test]
    fn migration_rollback() {
        let mut registry = MigrationRegistry::new();

        let users_fields = [
            Field::new("id", FieldType::Serial).primary_key(),
            Field::new("email", FieldType::Text).not_null(),
        ];

        registry.register(
            Migration::new("0001_create_users").operation(
                CreateTable::new("users")
                    .add_field(users_fields[0].clone())
                    .add_field(users_fields[1].clone()),
            ),
        );

        registry.register(
            Migration::new("0002_add_name")
                .depends_on(&["0001_create_users"])
                .operation(AddField::new("users", Field::new("name", FieldType::Text))),
        );

        let state = InMemoryState::with_applied(vec![
            "0001_create_users".to_string(),
            "0002_add_name".to_string(),
        ]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let backward_sql = migrator.generate_backward_sql(None).unwrap();
        assert_eq!(backward_sql.len(), 2);
        assert_eq!(backward_sql[0].0, "0002_add_name");
        assert_eq!(backward_sql[1].0, "0001_create_users");

        let mut executed_sqls = Vec::new();
        let unapplied = migrator
            .migrate_backward(Some("0002_add_name"), |sql| {
                executed_sqls.push(sql.to_string());
                Ok(())
            })
            .unwrap();

        assert_eq!(unapplied.len(), 1);
        assert!(executed_sqls[0].contains("DROP COLUMN"));
    }
}
