# Cetane

A schema migration **framework** for Rust, inspired by Django's migration system.

Cetane is not a migration runner. Migration runners like [refinery](https://github.com/rust-db/refinery) execute SQL scripts in order and track which ones have been applied. They treat migrations as opaque strings — you write the SQL, they run it.

Cetane works at a higher level. You describe schema changes as typed Rust values — tables, fields, indexes, constraints — and cetane handles the rest:

- **Generates SQL** for your target database using [sea-query](https://github.com/SeaQL/sea-query)
- **Derives rollback SQL** automatically from your operations (no need to write `down` migrations by hand)
- **Resolves dependencies** between migrations via topological sort, not filename ordering
- **Adapts to backends** — one set of migration definitions works across SQLite, PostgreSQL, and MySQL

## Quick example

```rust
use cetane::prelude::*;

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

// Apply migrations forward
let state = SqliteMigrationState::new(&connection);
let mut migrator = Migrator::new(&registry, &Sqlite, state);

migrator.migrate_forward(|sql| {
    connection.execute(sql, [])?;
    Ok(())
})?;
```

Rollback requires no additional code. Cetane derives the reverse SQL from the operations you already defined:

```rust
// Roll back the last migration
migrator.migrate_backward(Some("0002_create_posts"), |sql| {
    connection.execute(sql, [])?;
    Ok(())
})?;
```

## How it differs from migration runners

| | Migration runners (refinery, etc.) | Cetane |
|---|---|---|
| **Migration format** | Raw SQL files or strings | Typed Rust operations |
| **Rollback** | Write a separate down migration | Derived automatically |
| **Ordering** | Sequential version numbers | Declared dependency graph |
| **Cross-backend** | Write different SQL per database | One definition, backend-specific SQL generated |
| **Schema knowledge** | None — SQL is opaque | Full — fields, types, constraints are modeled |

Cetane and migration runners solve different problems. You might use both: cetane to define and generate your schema changes, and a runner to execute them in production.

## Operations

### Tables

```rust
// Create a table
CreateTable::new("users")
    .add_field(Field::new("id", FieldType::Serial).primary_key())
    .add_field(Field::new("email", FieldType::Text).not_null().unique())

// Drop a table (provide fields to make it reversible)
DropTable::new("users")
    .with_fields(vec![
        Field::new("id", FieldType::Serial).primary_key(),
        Field::new("email", FieldType::Text).not_null().unique(),
    ])

// Rename a table
RenameTable::new("users", "accounts")
```

### Fields

```rust
// Add a column
AddField::new("users", Field::new("name", FieldType::VarChar(255)).not_null())

// Drop a column (provide field definition to make it reversible)
RemoveField::new("users", "name")
    .with_definition(Field::new("name", FieldType::VarChar(255)).not_null())

// Rename a column
RenameField::new("users", "name", "display_name")

// Alter a column's type or constraints
AlterField::new("users", "name")
    .set_type(FieldType::Text)
    .set_nullable(true)
    .with_reverse(
        FieldChanges::new()
            .set_type(FieldType::VarChar(255))
            .set_nullable(false),
    )
```

### Indexes

```rust
// Standard index
AddIndex::new("users", Index::new("idx_users_email").column("email"))

// Unique index
AddIndex::new("users", Index::new("idx_users_email").column("email").unique())

// Composite index with ordering
AddIndex::new("posts",
    Index::new("idx_posts_user_date")
        .column("user_id")
        .column_desc("created_at")
)

// Partial index (PostgreSQL)
AddIndex::new("users",
    Index::new("idx_active_users")
        .column("email")
        .filter("active = true")
)

// Drop an index (provide definition to make it reversible)
RemoveIndex::new("users", "idx_users_email")
    .with_definition(Index::new("idx_users_email").column("email").unique())
```

### Constraints

```rust
// Unique constraint
AddConstraint::new("users",
    Constraint::unique("uq_users_email", vec!["email".into()])
)

// Check constraint
AddConstraint::new("users",
    Constraint::check("ck_users_age", "age >= 0")
)

// Foreign key constraint
AddConstraint::new("posts",
    Constraint::foreign_key(
        "fk_posts_user",
        vec!["user_id".into()],
        "users",
        vec!["id".into()],
    )
    .on_delete(ReferentialAction::Cascade)
)
```

### Raw SQL

For anything the operation types don't cover:

```rust
// One-way raw SQL
RunSql::new("UPDATE users SET active = true")

// Reversible raw SQL
RunSql::reversible(
    "UPDATE users SET active = true",
    "UPDATE users SET active = false",
)

// Backend-specific SQL
RunSql::portable()
    .for_backend("postgres", "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
    .for_backend("sqlite", "SELECT 1") // no-op on SQLite
```

## Field types

| FieldType | PostgreSQL | SQLite | MySQL |
|---|---|---|---|
| `Serial` | `serial` | `integer autoincrement` | `int auto_increment` |
| `BigSerial` | `bigserial` | `integer autoincrement` | `bigint auto_increment` |
| `Integer` | `integer` | `integer` | `int` |
| `BigInt` | `bigint` | `integer` | `bigint` |
| `SmallInt` | `smallint` | `integer` | `smallint` |
| `Text` | `text` | `text` | `text` |
| `VarChar(n)` | `varchar(n)` | `text` | `varchar(n)` |
| `Boolean` | `boolean` | `integer` | `bool` |
| `Timestamp` | `timestamp` | `text` | `timestamp` |
| `TimestampTz` | `timestamptz` | `text` | `timestamp` |
| `Date` | `date` | `text` | `date` |
| `Time` | `time` | `text` | `time` |
| `Uuid` | `uuid` | `text` | `char(36)` |
| `Json` | `json` | `text` | `json` |
| `JsonB` | `jsonb` | `text` | `json` |
| `Binary` | `bytea` | `blob` | `blob` |
| `Real` | `real` | `real` | `float` |
| `DoublePrecision` | `double precision` | `real` | `double` |
| `Decimal { p, s }` | `decimal(p,s)` | `real` | `decimal(p,s)` |

## Dependencies between migrations

Migrations declare their dependencies explicitly. Cetane resolves execution order using topological sort and detects cycles:

```rust
registry.register(Migration::new("0001_users"));
registry.register(Migration::new("0002_posts").depends_on(&["0001_users"]));
registry.register(Migration::new("0003_comments").depends_on(&["0001_users", "0002_posts"]));

// Diamond dependencies, multiple roots, long chains — all handled
let order = registry.resolve_order()?; // returns migrations in valid execution order
```

## Transactions

Migrations are atomic by default on backends that support transactional DDL (PostgreSQL, SQLite). Use the transaction-aware API to wrap each migration in a transaction:

```rust
migrator.migrate_forward_with_transactions(
    |sql| { connection.execute(sql, [])?; Ok(()) },
    || { connection.execute("BEGIN", [])?; Ok(()) },
    || { connection.execute("COMMIT", [])?; Ok(()) },
    || { connection.execute("ROLLBACK", [])?; Ok(()) },
)?;
```

Disable transactions for individual migrations that require it (e.g., `CREATE INDEX CONCURRENTLY` in PostgreSQL):

```rust
Migration::new("0004_add_index")
    .atomic(false)
    .operation(/* ... */)
```

## State tracking

Cetane tracks which migrations have been applied using the `MigrationStateStore` trait. Built-in implementations store state in a `_cetane_migrations` table:

| Store | Feature flag |
|---|---|
| `SqliteMigrationState` | `sqlite` |
| `PostgresMigrationState` | `postgres` |
| `MySqlMigrationState` | `mysql` |
| `InMemoryState` | (always available, for testing) |

You can implement `MigrationStateStore` yourself if you need custom storage:

```rust
pub trait MigrationStateStore {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String>;
    fn mark_applied(&mut self, name: &str) -> Result<(), String>;
    fn mark_unapplied(&mut self, name: &str) -> Result<(), String>;
}
```

## Feature flags

| Flag | Adds | Dependencies |
|---|---|---|
| `sqlite` | `SqliteMigrationState` | `rusqlite` |
| `postgres` | `PostgresMigrationState` | `postgres` |
| `mysql` | `MySqlMigrationState` | `mysql` |

The core library (operations, registry, migrator) works without any feature flags. Feature flags add database-specific state stores and integration support.

## Roadmap

Cetane is designed to integrate with [Diesel](https://diesel.rs). Upcoming features include:

- **Automatic schema diffing** — detect changes between your Rust types and the database, generate migrations automatically
- **Diesel table integration** — define migrations directly from Diesel table definitions

The name "cetane" is a reference to [cetane number](https://en.wikipedia.org/wiki/Cetane_number), a measure of diesel fuel quality.

## License

BSD 2-Clause. See [LICENSE](LICENSE).
