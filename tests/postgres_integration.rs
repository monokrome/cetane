//! PostgreSQL integration tests
//!
//! These tests require a running PostgreSQL instance. They are ignored by default.
//! To run them:
//!
//! ```sh
//! # Set environment variables (optional, defaults shown)
//! export POSTGRES_HOST=localhost
//! export POSTGRES_USER=postgres
//! export POSTGRES_PASSWORD=postgres
//! export POSTGRES_DB=cetane_test
//!
//! # Run the ignored tests
//! cargo test --features postgres --test postgres_integration -- --ignored
//! ```

use std::cell::RefCell;
use std::rc::Rc;

use cetane::prelude::*;
use postgres::{Client, NoTls};
use std::env;

fn get_test_client() -> Option<Client> {
    let host = env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
    let user = env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_string());
    let dbname = env::var("POSTGRES_DB").unwrap_or_else(|_| "cetane_test".to_string());

    let config = format!(
        "host={} user={} password={} dbname={}",
        host, user, password, dbname
    );

    Client::connect(&config, NoTls).ok()
}

fn cleanup_tables(client: &mut Client) {
    let _ = client.execute("DROP TABLE IF EXISTS posts CASCADE", &[]);
    let _ = client.execute("DROP TABLE IF EXISTS users CASCADE", &[]);
    let _ = client.execute("DROP TABLE IF EXISTS test_table CASCADE", &[]);
    let _ = client.execute("DROP TABLE IF EXISTS schema_migrations CASCADE", &[]);
}

struct PostgresState {
    client: Rc<RefCell<Client>>,
}

impl PostgresState {
    fn new(client: Client) -> Self {
        let client = Rc::new(RefCell::new(client));
        client
            .borrow_mut()
            .execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    name TEXT PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )",
                &[],
            )
            .unwrap();
        Self { client }
    }

    fn client(&self) -> Rc<RefCell<Client>> {
        Rc::clone(&self.client)
    }
}

impl MigrationStateStore for PostgresState {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        let mut client = self.client.borrow_mut();
        let rows = client
            .query(
                "SELECT name FROM schema_migrations ORDER BY applied_at",
                &[],
            )
            .map_err(|e| e.to_string())?;

        let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        Ok(names)
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        self.client
            .borrow_mut()
            .execute(
                "INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT DO NOTHING",
                &[&name],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        self.client
            .borrow_mut()
            .execute("DELETE FROM schema_migrations WHERE name = $1", &[&name])
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
#[ignore = "requires postgres connection"]
fn migrate_forward_creates_tables() {
    let Some(mut client) = get_test_client() else {
        eprintln!("Skipping test: no postgres connection");
        return;
    };
    cleanup_tables(&mut client);

    let state = PostgresState::new(client);
    let client_ref = state.client();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Postgres, state);

    let applied = migrator
        .migrate_forward(|sql| {
            client_ref
                .borrow_mut()
                .execute(sql, &[])
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(applied.len(), 4);

    // Verify tables exist
    let mut client = client_ref.borrow_mut();
    let table_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name IN ('users', 'posts')",
            &[],
        )
        .map(|row| row.get(0))
        .unwrap();

    assert_eq!(table_count, 2);

    cleanup_tables(&mut client);
}

#[test]
#[ignore = "requires postgres connection"]
fn migrate_forward_is_idempotent() {
    let Some(mut client) = get_test_client() else {
        return;
    };
    cleanup_tables(&mut client);

    let state = PostgresState::new(client);
    let client_ref = state.client();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Postgres, state);

    let executor = |sql: &str| {
        client_ref
            .borrow_mut()
            .execute(sql, &[])
            .map_err(|e| e.to_string())?;
        Ok(())
    };

    let first_applied = migrator.migrate_forward(executor).unwrap();
    let second_applied = migrator.migrate_forward(executor).unwrap();

    assert_eq!(first_applied.len(), 4);
    assert_eq!(second_applied.len(), 0);

    cleanup_tables(&mut client_ref.borrow_mut());
}

#[test]
#[ignore = "requires postgres connection"]
fn migrate_backward_drops_tables() {
    let Some(mut client) = get_test_client() else {
        return;
    };
    cleanup_tables(&mut client);

    let state = PostgresState::new(client);
    let client_ref = state.client();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Postgres, state);

    let executor = |sql: &str| {
        client_ref
            .borrow_mut()
            .execute(sql, &[])
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

    // Verify posts table no longer exists
    let mut client = client_ref.borrow_mut();
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'posts')",
            &[],
        )
        .map(|row| row.get(0))
        .unwrap_or(false);

    assert!(!exists);
    cleanup_tables(&mut client);
}

#[test]
#[ignore = "requires postgres connection"]
fn can_insert_data_after_migration() {
    let Some(mut client) = get_test_client() else {
        return;
    };
    cleanup_tables(&mut client);

    let state = PostgresState::new(client);
    let client_ref = state.client();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Postgres, state);

    migrator
        .migrate_forward(|sql| {
            client_ref
                .borrow_mut()
                .execute(sql, &[])
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    let mut client = client_ref.borrow_mut();
    client
        .execute(
            "INSERT INTO users (email, name) VALUES ('test@example.com', 'Test User')",
            &[],
        )
        .unwrap();

    let user_id: i32 = client
        .query_one(
            "SELECT id FROM users WHERE email = 'test@example.com'",
            &[],
        )
        .map(|row| row.get(0))
        .unwrap();

    client
        .execute(
            "INSERT INTO posts (user_id, title, body) VALUES ($1, 'Test Post', 'Hello World')",
            &[&user_id],
        )
        .unwrap();

    let post_count: i64 = client
        .query_one("SELECT COUNT(*) FROM posts WHERE user_id = $1", &[&user_id])
        .map(|row| row.get(0))
        .unwrap();

    assert_eq!(post_count, 1);

    drop(client);
    cleanup_tables(&mut client_ref.borrow_mut());
}

#[test]
#[ignore = "requires postgres connection"]
fn foreign_key_cascade_deletes() {
    let Some(mut client) = get_test_client() else {
        return;
    };
    cleanup_tables(&mut client);

    let state = PostgresState::new(client);
    let client_ref = state.client();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Postgres, state);

    migrator
        .migrate_forward(|sql| {
            client_ref
                .borrow_mut()
                .execute(sql, &[])
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    let mut client = client_ref.borrow_mut();
    client
        .execute("INSERT INTO users (email) VALUES ('test@example.com')", &[])
        .unwrap();

    let user_id: i32 = client
        .query_one("SELECT id FROM users", &[])
        .map(|row| row.get(0))
        .unwrap();

    client
        .execute(
            "INSERT INTO posts (user_id, title) VALUES ($1, 'Post 1')",
            &[&user_id],
        )
        .unwrap();
    client
        .execute(
            "INSERT INTO posts (user_id, title) VALUES ($1, 'Post 2')",
            &[&user_id],
        )
        .unwrap();

    client
        .execute("DELETE FROM users WHERE id = $1", &[&user_id])
        .unwrap();

    let post_count: i64 = client
        .query_one("SELECT COUNT(*) FROM posts", &[])
        .map(|row| row.get(0))
        .unwrap();

    assert_eq!(post_count, 0);

    drop(client);
    cleanup_tables(&mut client_ref.borrow_mut());
}

#[test]
#[ignore = "requires postgres connection"]
fn generate_sql_preview() {
    let Some(mut client) = get_test_client() else {
        return;
    };
    cleanup_tables(&mut client);

    let state = PostgresState::new(client);
    let client_ref = state.client();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Postgres, state);

    let forward_sql = migrator.generate_forward_sql().unwrap();

    assert_eq!(forward_sql.len(), 4);

    let (name, sqls) = &forward_sql[0];
    assert_eq!(name, "0001_create_users");
    assert!(sqls[0].contains("CREATE TABLE"));
    assert!(sqls[0].contains("\"users\""));

    let (name, sqls) = &forward_sql[3];
    assert_eq!(name, "0004_add_post_index");
    assert!(sqls[0].contains("CREATE INDEX"));

    cleanup_tables(&mut client_ref.borrow_mut());
}

#[test]
#[ignore = "requires postgres connection"]
fn transaction_support() {
    let Some(mut client) = get_test_client() else {
        return;
    };
    cleanup_tables(&mut client);

    // PostgreSQL supports transactional DDL
    assert!(Postgres.supports_transactional_ddl());

    let state = PostgresState::new(client);
    let client_ref = state.client();

    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &Postgres, state);

    let mut begins = 0;
    let mut commits = 0;

    let applied = migrator
        .migrate_forward_with_transactions(
            &mut |sql| {
                client_ref
                    .borrow_mut()
                    .execute(sql, &[])
                    .map_err(|e| e.to_string())?;
                Ok(())
            },
            &mut || {
                begins += 1;
                client_ref
                    .borrow_mut()
                    .execute("BEGIN", &[])
                    .map_err(|e| e.to_string())?;
                Ok(())
            },
            &mut || {
                commits += 1;
                client_ref
                    .borrow_mut()
                    .execute("COMMIT", &[])
                    .map_err(|e| e.to_string())?;
                Ok(())
            },
            &mut || {
                client_ref
                    .borrow_mut()
                    .execute("ROLLBACK", &[])
                    .map_err(|e| e.to_string())?;
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(applied.len(), 4);
    assert_eq!(begins, 4);
    assert_eq!(commits, 4);

    cleanup_tables(&mut client_ref.borrow_mut());
}
