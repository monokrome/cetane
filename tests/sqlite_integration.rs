use std::cell::RefCell;
use std::rc::Rc;

use cetane::prelude::*;
use rusqlite::Connection;

struct SqliteState {
    conn: Rc<RefCell<Connection>>,
}

impl SqliteState {
    fn new(conn: Connection) -> Self {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TEXT DEFAULT CURRENT_TIMESTAMP)",
            [],
        )
        .unwrap();
        Self {
            conn: Rc::new(RefCell::new(conn)),
        }
    }

    fn conn(&self) -> Rc<RefCell<Connection>> {
        Rc::clone(&self.conn)
    }
}

impl MigrationStateStore for SqliteState {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare("SELECT name FROM schema_migrations ORDER BY applied_at")
            .map_err(|e| e.to_string())?;

        let names = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<String>, _>>()
            .map_err(|e| e.to_string())?;

        Ok(names)
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        self.conn
            .borrow()
            .execute("INSERT INTO schema_migrations (name) VALUES (?1)", [name])
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        self.conn
            .borrow()
            .execute("DELETE FROM schema_migrations WHERE name = ?1", [name])
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

fn setup_registry() -> MigrationRegistry {
    let mut registry = MigrationRegistry::new();

    registry.register(
        Migration::new("0001_create_users").operation(
            CreateTable::new("users")
                .add_field(Field::new("id", FieldType::Serial).primary_key())
                .add_field(Field::new("email", FieldType::Text).not_null().unique())
                .add_field(
                    Field::new("created_at", FieldType::Timestamp)
                        .not_null()
                        .default("CURRENT_TIMESTAMP"),
                ),
        ),
    );

    registry.register(
        Migration::new("0002_add_user_name")
            .depends_on(&["0001_create_users"])
            .operation(AddField::new(
                "users",
                Field::new("name", FieldType::VarChar(255)),
            )),
    );

    registry.register(
        Migration::new("0003_create_posts")
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
        Migration::new("0004_add_post_index")
            .depends_on(&["0003_create_posts"])
            .operation(AddIndex::new(
                "posts",
                Index::new("idx_posts_user_id").column("user_id"),
            )),
    );

    registry
}

#[test]
fn migrate_forward_creates_tables() {
    let conn = Connection::open_in_memory().unwrap();
    let state = SqliteState::new(conn);
    let conn_ref = state.conn();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Sqlite, state);

    let applied = migrator
        .migrate_forward(|sql| {
            conn_ref
                .borrow()
                .execute(sql, [])
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(applied.len(), 4);

    let conn = conn_ref.borrow();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap();
    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(tables.contains(&"users".to_string()));
    assert!(tables.contains(&"posts".to_string()));
    assert!(tables.contains(&"schema_migrations".to_string()));
}

#[test]
fn migrate_forward_is_idempotent() {
    let conn = Connection::open_in_memory().unwrap();
    let state = SqliteState::new(conn);
    let conn_ref = state.conn();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Sqlite, state);

    let executor = |sql: &str| {
        conn_ref
            .borrow()
            .execute(sql, [])
            .map_err(|e| e.to_string())?;
        Ok(())
    };

    let first_applied = migrator.migrate_forward(executor).unwrap();
    let second_applied = migrator.migrate_forward(executor).unwrap();

    assert_eq!(first_applied.len(), 4);
    assert_eq!(second_applied.len(), 0);
}

#[test]
fn migrate_backward_drops_tables() {
    let conn = Connection::open_in_memory().unwrap();
    let state = SqliteState::new(conn);
    let conn_ref = state.conn();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Sqlite, state);

    let executor = |sql: &str| {
        conn_ref
            .borrow()
            .execute(sql, [])
            .map_err(|e| e.to_string())?;
        Ok(())
    };

    migrator.migrate_forward(executor).unwrap();

    let unapplied = migrator
        .migrate_backward(Some("0003_create_posts"), executor)
        .unwrap();

    assert_eq!(unapplied.len(), 2);
    assert!(unapplied.contains(&"0004_add_post_index".to_string()));
    assert!(unapplied.contains(&"0003_create_posts".to_string()));

    let conn = conn_ref.borrow();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
        .unwrap();
    let count: i64 = stmt.query_row([], |_| Ok(1)).unwrap_or(0);
    assert_eq!(count, 0);
}

#[test]
fn can_insert_data_after_migration() {
    let conn = Connection::open_in_memory().unwrap();
    let state = SqliteState::new(conn);
    let conn_ref = state.conn();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Sqlite, state);

    migrator
        .migrate_forward(|sql| {
            conn_ref
                .borrow()
                .execute(sql, [])
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    let conn = conn_ref.borrow();
    conn.execute(
        "INSERT INTO users (email, name) VALUES ('test@example.com', 'Test User')",
        [],
    )
    .unwrap();

    let user_id: i64 = conn
        .query_row(
            "SELECT id FROM users WHERE email = 'test@example.com'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    conn.execute(
        "INSERT INTO posts (user_id, title, body) VALUES (?1, 'Test Post', 'Hello World')",
        [user_id],
    )
    .unwrap();

    let post_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM posts WHERE user_id = ?1",
            [user_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(post_count, 1);
}

#[test]
fn foreign_key_cascade_deletes() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();
    let state = SqliteState::new(conn);
    let conn_ref = state.conn();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Sqlite, state);

    migrator
        .migrate_forward(|sql| {
            conn_ref
                .borrow()
                .execute(sql, [])
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    let conn = conn_ref.borrow();
    conn.execute("INSERT INTO users (email) VALUES ('test@example.com')", [])
        .unwrap();

    let user_id: i64 = conn
        .query_row("SELECT id FROM users", [], |row| row.get(0))
        .unwrap();

    conn.execute(
        "INSERT INTO posts (user_id, title) VALUES (?1, 'Post 1')",
        [user_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO posts (user_id, title) VALUES (?1, 'Post 2')",
        [user_id],
    )
    .unwrap();

    conn.execute("DELETE FROM users WHERE id = ?1", [user_id])
        .unwrap();

    let post_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0))
        .unwrap();

    assert_eq!(post_count, 0);
}

#[test]
fn generate_sql_preview() {
    let conn = Connection::open_in_memory().unwrap();
    let state = SqliteState::new(conn);

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Sqlite, state);

    let forward_sql = migrator.generate_forward_sql().unwrap();

    assert_eq!(forward_sql.len(), 4);

    let (name, sqls) = &forward_sql[0];
    assert_eq!(name, "0001_create_users");
    assert!(sqls[0].contains("CREATE TABLE"));
    assert!(sqls[0].contains("\"users\""));

    let (name, sqls) = &forward_sql[3];
    assert_eq!(name, "0004_add_post_index");
    assert!(sqls[0].contains("CREATE INDEX"));
}

#[test]
fn transaction_rollback_on_failure() {
    use cetane::migration::MigrationError;
    use cetane::operation::RunSql;

    let conn = Connection::open_in_memory().unwrap();
    let state = SqliteState::new(conn);
    let conn_ref = state.conn();

    // Create a registry with a migration that will fail partway through
    let mut registry = MigrationRegistry::new();

    registry.register(
        Migration::new("0001_create_test").operation(
            CreateTable::new("test_table")
                .add_field(Field::new("id", FieldType::Serial).primary_key())
                .add_field(Field::new("name", FieldType::Text).not_null()),
        ),
    );

    registry.register(
        Migration::new("0002_will_fail")
            .depends_on(&["0001_create_test"])
            .forward_ops(vec![
                Box::new(RunSql::new(
                    "INSERT INTO test_table (name) VALUES ('before_fail')",
                )),
                Box::new(RunSql::new("THIS IS INVALID SQL THAT WILL FAIL")),
            ])
            .backward_ops(vec![Box::new(RunSql::new(
                "DELETE FROM test_table WHERE name = 'before_fail'",
            ))]),
    );

    let mut migrator = Migrator::new(&registry, &Sqlite, state);

    // Use real transaction callbacks
    let result = migrator.migrate_forward_with_transactions(
        &mut |sql| {
            conn_ref
                .borrow()
                .execute(sql, [])
                .map_err(|e| e.to_string())?;
            Ok(())
        },
        &mut || {
            conn_ref
                .borrow()
                .execute("BEGIN TRANSACTION", [])
                .map_err(|e| e.to_string())?;
            Ok(())
        },
        &mut || {
            conn_ref
                .borrow()
                .execute("COMMIT", [])
                .map_err(|e| e.to_string())?;
            Ok(())
        },
        &mut || {
            conn_ref
                .borrow()
                .execute("ROLLBACK", [])
                .map_err(|e| e.to_string())?;
            Ok(())
        },
    );

    // Should have failed on the second migration
    assert!(result.is_err());
    match result {
        Err(MigrationError::ExecutionFailed {
            migration,
            completed,
            ..
        }) => {
            assert_eq!(migration, "0002_will_fail");
            assert_eq!(completed, vec!["0001_create_test"]);
        }
        _ => panic!("Expected ExecutionFailed error"),
    }

    // The first migration should have been committed
    let conn = conn_ref.borrow();
    let table_exists: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='test_table'",
            [],
            |_| Ok(true),
        )
        .unwrap_or(false);
    assert!(table_exists, "First migration's table should exist");

    // The failed second migration's partial work should have been rolled back
    let row_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM test_table", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        row_count, 0,
        "Failed migration's insert should have been rolled back"
    );
}
