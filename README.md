# Cetane

Django-inspired database migrations for Rust with automatic reversibility.

## Features

- **Backend-agnostic**: SQLite, PostgreSQL, and MySQL support
- **Type-safe SQL generation**: Uses [sea-query](https://github.com/SeaQL/sea-query) for compile-time safety
- **Automatic reversibility**: Most operations generate their own reverse migrations
- **Dependency ordering**: Migrations declare dependencies, cetane resolves the order
- **Transaction support**: Atomic migrations where backends support it (PostgreSQL, SQLite)

## Usage

```rust
use cetane::prelude::*;

let mut registry = MigrationRegistry::new();

registry.register(
    Migration::new("0001_create_users")
        .operation(
            CreateTable::new("users")
                .add_field(Field::new("id", FieldType::Serial).primary_key())
                .add_field(Field::new("email", FieldType::Text).not_null().unique())
        )
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
                        .on_delete(ReferentialAction::Cascade)
                )
                .add_field(Field::new("title", FieldType::Text).not_null())
        )
);

// Run migrations
let state = InMemoryState::new();
let mut migrator = Migrator::new(&registry, &Sqlite, state);

migrator.migrate_forward(|sql| {
    // Execute SQL against your database
    db.execute(sql)?;
    Ok(())
})?;
```

## Operations

| Operation | Description | Auto-reversible |
|-----------|-------------|-----------------|
| `CreateTable` | Create a new table | Yes |
| `DropTable` | Drop a table | With field definitions |
| `RenameTable` | Rename a table | Yes |
| `AddField` | Add a column | Yes |
| `RemoveField` | Remove a column | With field definition |
| `RenameField` | Rename a column | Yes |
| `AlterField` | Modify column type/constraints | With reverse changes |
| `AddIndex` | Create an index | Yes |
| `RemoveIndex` | Drop an index | With index definition |
| `AddConstraint` | Add a constraint | Yes |
| `RemoveConstraint` | Remove a constraint | With constraint definition |
| `RunSql` | Execute raw SQL | With reverse SQL |

## Feature Flags

- `sqlite` - SQLite support (includes `rusqlite`)
- `postgres` - PostgreSQL support (includes `postgres`)
- `mysql` - MySQL support (includes `mysql`)

## License

BSD 2-Clause License. See [LICENSE](LICENSE) for details.
