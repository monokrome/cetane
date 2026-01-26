//! MySQL integration tests
//!
//! These tests require a running MySQL instance. They are ignored by default.
//! To run them:
//!
//! ```sh
//! # Set environment variables (optional, defaults shown)
//! export MYSQL_HOST=localhost
//! export MYSQL_USER=root
//! export MYSQL_PASSWORD=root
//! export MYSQL_DB=cetane_test
//!
//! # Run the ignored tests
//! cargo test --features mysql --test mysql_integration -- --ignored
//! ```

use cetane::prelude::*;
use mysql::prelude::*;
use mysql::{Opts, Pool};
use std::env;

fn get_test_pool() -> Option<Pool> {
    let host = env::var("MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string());
    let user = env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string());
    let password = env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "root".to_string());
    let dbname = env::var("MYSQL_DB").unwrap_or_else(|_| "cetane_test".to_string());

    let url = format!("mysql://{}:{}@{}/{}", user, password, host, dbname);
    let opts = Opts::from_url(&url).ok()?;
    Pool::new(opts).ok()
}

fn cleanup_tables(pool: &Pool) {
    let mut conn = pool.get_conn().unwrap();
    let _ = conn.query_drop("SET FOREIGN_KEY_CHECKS = 0");
    let _ = conn.query_drop("DROP TABLE IF EXISTS posts");
    let _ = conn.query_drop("DROP TABLE IF EXISTS users");
    let _ = conn.query_drop("DROP TABLE IF EXISTS test_table");
    let _ = conn.query_drop("DROP TABLE IF EXISTS schema_migrations");
    let _ = conn.query_drop("SET FOREIGN_KEY_CHECKS = 1");
}

struct MySqlState {
    pool: Pool,
}

impl MySqlState {
    fn new(pool: Pool) -> Self {
        let mut conn = pool.get_conn().unwrap();
        conn.query_drop(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                name VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .unwrap();
        Self { pool }
    }
}

impl MigrationStateStore for MySqlState {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        let names: Vec<String> = conn
            .query("SELECT name FROM schema_migrations ORDER BY applied_at")
            .map_err(|e| e.to_string())?;
        Ok(names)
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        conn.exec_drop(
            "INSERT IGNORE INTO schema_migrations (name) VALUES (?)",
            (name,),
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        let mut conn = self.pool.get_conn().map_err(|e| e.to_string())?;
        conn.exec_drop("DELETE FROM schema_migrations WHERE name = ?", (name,))
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
                .add_field(
                    Field::new("email", FieldType::VarChar(255))
                        .not_null()
                        .unique(),
                )
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
                    .add_field(Field::new("title", FieldType::VarChar(255)).not_null())
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
#[ignore = "requires mysql connection"]
fn migrate_forward_creates_tables() {
    let Some(pool) = get_test_pool() else {
        eprintln!("Skipping test: no mysql connection");
        return;
    };
    cleanup_tables(&pool);

    let state = MySqlState::new(pool.clone());
    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &MySql, state);

    let applied = migrator
        .migrate_forward(|sql| {
            let mut conn = pool.get_conn().map_err(|e| e.to_string())?;
            conn.query_drop(sql).map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(applied.len(), 4);

    // Verify tables exist
    let mut conn = pool.get_conn().unwrap();
    let tables: Vec<String> = conn.query("SHOW TABLES").unwrap();
    assert!(tables.iter().any(|t| t == "users"));
    assert!(tables.iter().any(|t| t == "posts"));

    cleanup_tables(&pool);
}

#[test]
#[ignore = "requires mysql connection"]
fn migrate_forward_is_idempotent() {
    let Some(pool) = get_test_pool() else {
        return;
    };
    cleanup_tables(&pool);

    let state = MySqlState::new(pool.clone());
    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &MySql, state);

    let executor = |sql: &str| {
        let mut conn = pool.get_conn().map_err(|e| e.to_string())?;
        conn.query_drop(sql).map_err(|e| e.to_string())?;
        Ok(())
    };

    let first_applied = migrator.migrate_forward(executor).unwrap();
    let second_applied = migrator.migrate_forward(executor).unwrap();

    assert_eq!(first_applied.len(), 4);
    assert_eq!(second_applied.len(), 0);

    cleanup_tables(&pool);
}

#[test]
#[ignore = "requires mysql connection"]
fn migrate_backward_drops_tables() {
    let Some(pool) = get_test_pool() else {
        return;
    };
    cleanup_tables(&pool);

    let state = MySqlState::new(pool.clone());
    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &MySql, state);

    let executor = |sql: &str| {
        let mut conn = pool.get_conn().map_err(|e| e.to_string())?;
        conn.query_drop(sql).map_err(|e| e.to_string())?;
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
    let mut conn = pool.get_conn().unwrap();
    let tables: Vec<String> = conn.query("SHOW TABLES LIKE 'posts'").unwrap();
    assert!(tables.is_empty());

    cleanup_tables(&pool);
}

#[test]
#[ignore = "requires mysql connection"]
fn can_insert_data_after_migration() {
    let Some(pool) = get_test_pool() else {
        return;
    };
    cleanup_tables(&pool);

    let state = MySqlState::new(pool.clone());
    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &MySql, state);

    migrator
        .migrate_forward(|sql| {
            let mut conn = pool.get_conn().map_err(|e| e.to_string())?;
            conn.query_drop(sql).map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    let mut conn = pool.get_conn().unwrap();
    conn.exec_drop(
        "INSERT INTO users (email, name) VALUES (?, ?)",
        ("test@example.com", "Test User"),
    )
    .unwrap();

    let user_id: u64 = conn
        .query_first("SELECT id FROM users WHERE email = 'test@example.com'")
        .unwrap()
        .unwrap();

    conn.exec_drop(
        "INSERT INTO posts (user_id, title, body) VALUES (?, ?, ?)",
        (user_id, "Test Post", "Hello World"),
    )
    .unwrap();

    let post_count: i64 = conn
        .exec_first("SELECT COUNT(*) FROM posts WHERE user_id = ?", (user_id,))
        .unwrap()
        .unwrap();

    assert_eq!(post_count, 1);
    cleanup_tables(&pool);
}

#[test]
#[ignore = "requires mysql connection"]
fn foreign_key_cascade_deletes() {
    let Some(pool) = get_test_pool() else {
        return;
    };
    cleanup_tables(&pool);

    let state = MySqlState::new(pool.clone());
    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &MySql, state);

    migrator
        .migrate_forward(|sql| {
            let mut conn = pool.get_conn().map_err(|e| e.to_string())?;
            conn.query_drop(sql).map_err(|e| e.to_string())?;
            Ok(())
        })
        .unwrap();

    let mut conn = pool.get_conn().unwrap();
    conn.exec_drop(
        "INSERT INTO users (email) VALUES (?)",
        ("test@example.com",),
    )
    .unwrap();

    let user_id: u64 = conn.query_first("SELECT id FROM users").unwrap().unwrap();

    conn.exec_drop(
        "INSERT INTO posts (user_id, title) VALUES (?, ?)",
        (user_id, "Post 1"),
    )
    .unwrap();
    conn.exec_drop(
        "INSERT INTO posts (user_id, title) VALUES (?, ?)",
        (user_id, "Post 2"),
    )
    .unwrap();

    conn.exec_drop("DELETE FROM users WHERE id = ?", (user_id,))
        .unwrap();

    let post_count: i64 = conn
        .query_first("SELECT COUNT(*) FROM posts")
        .unwrap()
        .unwrap();

    assert_eq!(post_count, 0);
    cleanup_tables(&pool);
}

#[test]
#[ignore = "requires mysql connection"]
fn generate_sql_preview() {
    let Some(pool) = get_test_pool() else {
        return;
    };
    cleanup_tables(&pool);

    let state = MySqlState::new(pool.clone());
    let registry = setup_registry();
    let mut migrator = Migrator::new(&registry, &MySql, state);

    let forward_sql = migrator.generate_forward_sql().unwrap();

    assert_eq!(forward_sql.len(), 4);

    let (name, sqls) = &forward_sql[0];
    assert_eq!(name, "0001_create_users");
    assert!(sqls[0].contains("CREATE TABLE"));
    // MySQL uses backticks
    assert!(sqls[0].contains("`users`"));

    let (name, sqls) = &forward_sql[3];
    assert_eq!(name, "0004_add_post_index");
    assert!(sqls[0].contains("CREATE INDEX"));

    cleanup_tables(&pool);
}

#[test]
#[ignore = "requires mysql connection"]
fn mysql_does_not_support_transactional_ddl() {
    // MySQL issues implicit commits for DDL statements
    assert!(!MySql.supports_transactional_ddl());
}
