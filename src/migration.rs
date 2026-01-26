use std::collections::HashMap;

use crate::backend::Backend;
use crate::operation::Operation;

pub struct Migration {
    pub name: &'static str,
    pub dependencies: &'static [&'static str],
    forward: Vec<Box<dyn Operation>>,
    backward: Option<Vec<Box<dyn Operation>>>,
    atomic: bool,
}

impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Migration")
            .field("name", &self.name)
            .field("dependencies", &self.dependencies)
            .field("forward", &format!("[{} operations]", self.forward.len()))
            .field(
                "backward",
                &self
                    .backward
                    .as_ref()
                    .map(|b| format!("[{} operations]", b.len())),
            )
            .field("atomic", &self.atomic)
            .finish()
    }
}

impl Migration {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            dependencies: &[],
            forward: Vec::new(),
            backward: None,
            atomic: true,
        }
    }

    pub fn depends_on(mut self, dependencies: &'static [&'static str]) -> Self {
        self.dependencies = dependencies;
        self
    }

    /// Set whether this migration should run inside a transaction.
    /// Defaults to `true`. Set to `false` for operations that cannot
    /// run in transactions (e.g., `CREATE INDEX CONCURRENTLY` in PostgreSQL).
    pub fn atomic(mut self, atomic: bool) -> Self {
        self.atomic = atomic;
        self
    }

    /// Check if this migration should run atomically (in a transaction).
    pub fn is_atomic(&self) -> bool {
        self.atomic
    }

    /// Add an operation with automatic reverse derivation.
    /// The backward migration will be derived from each operation's `backward()` method.
    pub fn operation(mut self, op: impl Operation + 'static) -> Self {
        self.forward.push(Box::new(op));
        self
    }

    /// Set forward operations (replaces any existing).
    /// Use with `backward_ops()` for explicit control over both directions.
    pub fn forward_ops(mut self, ops: Vec<Box<dyn Operation>>) -> Self {
        self.forward = ops;
        self
    }

    /// Set explicit backward operations (replaces automatic derivation).
    /// When set, these operations run in order (not reversed) during rollback.
    pub fn backward_ops(mut self, ops: Vec<Box<dyn Operation>>) -> Self {
        self.backward = Some(ops);
        self
    }

    /// Check if this migration can be reversed.
    pub fn is_reversible(&self) -> bool {
        if self.backward.is_some() {
            return true;
        }
        self.forward.iter().all(|op| op.is_reversible())
    }

    /// Generate forward SQL statements.
    pub fn forward_sql(&self, backend: &dyn Backend) -> Vec<String> {
        self.forward
            .iter()
            .flat_map(|op| op.forward(backend))
            .collect()
    }

    /// Generate backward SQL statements.
    /// Returns None if not reversible.
    pub fn backward_sql(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        if let Some(ref backward) = self.backward {
            // Explicit backward ops: run in order (not reversed)
            Some(backward.iter().flat_map(|op| op.forward(backend)).collect())
        } else {
            // Derive from forward ops: run in reverse order
            if !self.is_reversible() {
                return None;
            }
            Some(
                self.forward
                    .iter()
                    .rev()
                    .filter_map(|op| op.backward(backend))
                    .flatten()
                    .collect(),
            )
        }
    }

    /// Access forward operations (for inspection).
    pub fn forward_operations(&self) -> &[Box<dyn Operation>] {
        &self.forward
    }

    /// Access backward operations if explicitly set.
    pub fn backward_operations(&self) -> Option<&[Box<dyn Operation>]> {
        self.backward.as_deref()
    }
}

#[derive(Default)]
pub struct MigrationRegistry {
    migrations: HashMap<&'static str, Migration>,
    order: Vec<&'static str>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, migration: Migration) {
        let name = migration.name;
        self.migrations.insert(name, migration);
        self.order.push(name);
    }

    pub fn get(&self, name: &str) -> Option<&Migration> {
        self.migrations.get(name)
    }

    pub fn all(&self) -> impl Iterator<Item = &Migration> {
        self.order
            .iter()
            .filter_map(|name| self.migrations.get(name))
    }

    pub fn resolve_order(&self) -> Result<Vec<&'static str>, MigrationError> {
        let mut resolved: Vec<&'static str> = Vec::new();
        let mut seen: HashMap<&'static str, bool> = HashMap::new();

        for name in &self.order {
            self.resolve_deps(name, &mut resolved, &mut seen)?;
        }

        Ok(resolved)
    }

    fn resolve_deps(
        &self,
        name: &'static str,
        resolved: &mut Vec<&'static str>,
        seen: &mut HashMap<&'static str, bool>,
    ) -> Result<(), MigrationError> {
        if let Some(&in_progress) = seen.get(name) {
            if in_progress {
                return Err(MigrationError::CircularDependency(name.to_string()));
            }
            return Ok(());
        }

        seen.insert(name, true);

        let migration = self
            .migrations
            .get(name)
            .ok_or_else(|| MigrationError::NotFound(name.to_string()))?;

        for dep in migration.dependencies {
            self.resolve_deps(dep, resolved, seen)?;
        }

        seen.insert(name, false);

        if !resolved.contains(&name) {
            resolved.push(name);
        }

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.migrations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MigrationError {
    NotFound(String),
    CircularDependency(String),
    NotReversible(String),
    ExecutionFailed {
        migration: String,
        error: String,
        /// Migrations that were successfully applied before the failure.
        completed: Vec<String>,
    },
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::NotFound(name) => write!(f, "Migration not found: {}", name),
            MigrationError::CircularDependency(name) => {
                write!(f, "Circular dependency detected at: {}", name)
            }
            MigrationError::NotReversible(name) => {
                write!(f, "Migration is not reversible: {}", name)
            }
            MigrationError::ExecutionFailed {
                migration,
                error,
                completed,
            } => {
                if completed.is_empty() {
                    write!(f, "Migration {} failed: {}", migration, error)
                } else {
                    write!(
                        f,
                        "Migration {} failed: {} (completed: {})",
                        migration,
                        error,
                        completed.join(", ")
                    )
                }
            }
        }
    }
}

impl std::error::Error for MigrationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Sqlite;
    use crate::field::{Field, FieldType};
    use crate::operation::{CreateTable, DropTable, RunSql};

    #[test]
    fn migration_builder() {
        let migration = Migration::new("0001_initial")
            .depends_on(&[])
            .operation(CreateTable::new("users").field("id", FieldType::Serial));

        assert_eq!(migration.name, "0001_initial");
        assert!(migration.dependencies.is_empty());
        assert_eq!(migration.forward_operations().len(), 1);
    }

    #[test]
    fn migration_auto_reverse() {
        let migration = Migration::new("0001_create_users").operation(
            CreateTable::new("users").add_field(Field::new("id", FieldType::Serial).primary_key()),
        );

        assert!(migration.is_reversible());

        let forward = migration.forward_sql(&Sqlite);
        assert!(forward[0].contains("CREATE TABLE"));

        let backward = migration.backward_sql(&Sqlite).unwrap();
        assert!(backward[0].contains("DROP TABLE"));
    }

    #[test]
    fn migration_explicit_backward_ops() {
        let migration = Migration::new("0001_migrate_data")
            .forward_ops(vec![
                Box::new(
                    CreateTable::new("new_users")
                        .add_field(Field::new("id", FieldType::Serial).primary_key()),
                ),
                Box::new(RunSql::new("INSERT INTO new_users SELECT * FROM users")),
                Box::new(DropTable::new("users")),
            ])
            .backward_ops(vec![
                Box::new(
                    CreateTable::new("users")
                        .add_field(Field::new("id", FieldType::Serial).primary_key()),
                ),
                Box::new(RunSql::new("INSERT INTO users SELECT * FROM new_users")),
                // intentionally not dropping new_users
            ]);

        assert!(migration.is_reversible());

        let forward = migration.forward_sql(&Sqlite);
        assert_eq!(forward.len(), 3);
        assert!(forward[0].contains("CREATE TABLE \"new_users\""));
        assert!(forward[1].contains("INSERT INTO new_users"));
        assert!(forward[2].contains("DROP TABLE \"users\""));

        let backward = migration.backward_sql(&Sqlite).unwrap();
        assert_eq!(backward.len(), 2);
        assert!(backward[0].contains("CREATE TABLE \"users\""));
        assert!(backward[1].contains("INSERT INTO users"));
    }

    #[test]
    fn migration_not_reversible_without_backward() {
        let migration =
            Migration::new("0001_destructive").operation(DropTable::new("legacy_table"));

        assert!(!migration.is_reversible());
        assert!(migration.backward_sql(&Sqlite).is_none());
    }

    #[test]
    fn registry_register_and_get() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("0001_initial"));

        assert!(registry.get("0001_initial").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn registry_resolve_order_no_deps() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("0001_first"));
        registry.register(Migration::new("0002_second"));

        let order = registry.resolve_order().unwrap();
        assert_eq!(order, vec!["0001_first", "0002_second"]);
    }

    #[test]
    fn registry_resolve_order_with_deps() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("0002_second").depends_on(&["0001_first"]));
        registry.register(Migration::new("0001_first"));

        let order = registry.resolve_order().unwrap();
        assert_eq!(order[0], "0001_first");
        assert_eq!(order[1], "0002_second");
    }

    #[test]
    fn registry_detects_circular_dependency() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("a").depends_on(&["b"]));
        registry.register(Migration::new("b").depends_on(&["a"]));

        let result = registry.resolve_order();
        assert!(matches!(result, Err(MigrationError::CircularDependency(_))));
    }

    #[test]
    fn registry_detects_missing_dependency() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("a").depends_on(&["missing"]));

        let result = registry.resolve_order();
        assert!(matches!(result, Err(MigrationError::NotFound(_))));
    }

    #[test]
    fn migration_error_display() {
        assert_eq!(
            MigrationError::NotFound("test".to_string()).to_string(),
            "Migration not found: test"
        );
        assert_eq!(
            MigrationError::CircularDependency("a".to_string()).to_string(),
            "Circular dependency detected at: a"
        );
        assert_eq!(
            MigrationError::NotReversible("b".to_string()).to_string(),
            "Migration is not reversible: b"
        );
        assert_eq!(
            MigrationError::ExecutionFailed {
                migration: "c".to_string(),
                error: "sql error".to_string(),
                completed: vec![],
            }
            .to_string(),
            "Migration c failed: sql error"
        );
        assert_eq!(
            MigrationError::ExecutionFailed {
                migration: "c".to_string(),
                error: "sql error".to_string(),
                completed: vec!["a".to_string(), "b".to_string()],
            }
            .to_string(),
            "Migration c failed: sql error (completed: a, b)"
        );
    }

    #[test]
    fn migration_debug() {
        let migration = Migration::new("0001_test")
            .depends_on(&["0000_base"])
            .operation(CreateTable::new("users"));

        let debug = format!("{:?}", migration);
        assert!(debug.contains("0001_test"));
        assert!(debug.contains("0000_base"));
        assert!(debug.contains("1 operations"));
    }

    #[test]
    fn migration_debug_with_backward() {
        let migration = Migration::new("0001_test")
            .forward_ops(vec![Box::new(CreateTable::new("a"))])
            .backward_ops(vec![Box::new(DropTable::new("a"))]);

        let debug = format!("{:?}", migration);
        assert!(debug.contains("1 operations"));
    }

    #[test]
    fn registry_len_and_is_empty() {
        let mut registry = MigrationRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);

        registry.register(Migration::new("0001"));
        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registry_all_iterator() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("0001_a"));
        registry.register(Migration::new("0002_b"));

        let names: Vec<_> = registry.all().map(|m| m.name).collect();
        assert_eq!(names, vec!["0001_a", "0002_b"]);
    }

    #[test]
    fn migration_backward_operations_accessor() {
        let migration = Migration::new("test")
            .forward_ops(vec![Box::new(CreateTable::new("a"))])
            .backward_ops(vec![Box::new(DropTable::new("a"))]);

        assert!(migration.backward_operations().is_some());
        assert_eq!(migration.backward_operations().unwrap().len(), 1);
    }

    #[test]
    fn migration_backward_operations_none() {
        let migration = Migration::new("test").operation(CreateTable::new("a"));

        assert!(migration.backward_operations().is_none());
    }

    #[test]
    fn migration_atomic_default_true() {
        let migration = Migration::new("test");
        assert!(migration.is_atomic());
    }

    #[test]
    fn migration_atomic_can_be_disabled() {
        let migration = Migration::new("test").atomic(false);
        assert!(!migration.is_atomic());
    }

    // Complex dependency graph tests

    #[test]
    fn registry_diamond_dependency() {
        // Diamond pattern:
        //     A
        //    / \
        //   B   C
        //    \ /
        //     D
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("A"));
        registry.register(Migration::new("B").depends_on(&["A"]));
        registry.register(Migration::new("C").depends_on(&["A"]));
        registry.register(Migration::new("D").depends_on(&["B", "C"]));

        let order = registry.resolve_order().unwrap();

        // A must come first
        assert_eq!(order[0], "A");
        // B and C can be in any order, but both before D
        let b_pos = order.iter().position(|x| *x == "B").unwrap();
        let c_pos = order.iter().position(|x| *x == "C").unwrap();
        let d_pos = order.iter().position(|x| *x == "D").unwrap();
        assert!(b_pos < d_pos);
        assert!(c_pos < d_pos);
    }

    #[test]
    fn registry_long_chain() {
        // Linear chain: A -> B -> C -> D -> E
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("E").depends_on(&["D"]));
        registry.register(Migration::new("C").depends_on(&["B"]));
        registry.register(Migration::new("A"));
        registry.register(Migration::new("D").depends_on(&["C"]));
        registry.register(Migration::new("B").depends_on(&["A"]));

        let order = registry.resolve_order().unwrap();
        assert_eq!(order, vec!["A", "B", "C", "D", "E"]);
    }

    #[test]
    fn registry_multiple_roots() {
        // Multiple independent starting points:
        //   A      X
        //   |      |
        //   B      Y
        //    \    /
        //      C
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("A"));
        registry.register(Migration::new("X"));
        registry.register(Migration::new("B").depends_on(&["A"]));
        registry.register(Migration::new("Y").depends_on(&["X"]));
        registry.register(Migration::new("C").depends_on(&["B", "Y"]));

        let order = registry.resolve_order().unwrap();

        // A before B, X before Y, B and Y before C
        let a_pos = order.iter().position(|x| *x == "A").unwrap();
        let b_pos = order.iter().position(|x| *x == "B").unwrap();
        let x_pos = order.iter().position(|x| *x == "X").unwrap();
        let y_pos = order.iter().position(|x| *x == "Y").unwrap();
        let c_pos = order.iter().position(|x| *x == "C").unwrap();

        assert!(a_pos < b_pos);
        assert!(x_pos < y_pos);
        assert!(b_pos < c_pos);
        assert!(y_pos < c_pos);
    }

    #[test]
    fn registry_complex_dag() {
        // More complex DAG:
        //     A
        //    /|\
        //   B C D
        //   |\ /|
        //   E  F
        //    \/
        //     G
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("A"));
        registry.register(Migration::new("B").depends_on(&["A"]));
        registry.register(Migration::new("C").depends_on(&["A"]));
        registry.register(Migration::new("D").depends_on(&["A"]));
        registry.register(Migration::new("E").depends_on(&["B"]));
        registry.register(Migration::new("F").depends_on(&["C", "D"]));
        registry.register(Migration::new("G").depends_on(&["E", "F"]));

        let order = registry.resolve_order().unwrap();

        // Verify all dependency constraints
        let pos = |name: &str| order.iter().position(|x| *x == name).unwrap();

        assert!(pos("A") < pos("B"));
        assert!(pos("A") < pos("C"));
        assert!(pos("A") < pos("D"));
        assert!(pos("B") < pos("E"));
        assert!(pos("C") < pos("F"));
        assert!(pos("D") < pos("F"));
        assert!(pos("E") < pos("G"));
        assert!(pos("F") < pos("G"));
    }

    #[test]
    fn registry_three_node_cycle() {
        // Cycle: A -> B -> C -> A
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("A").depends_on(&["C"]));
        registry.register(Migration::new("B").depends_on(&["A"]));
        registry.register(Migration::new("C").depends_on(&["B"]));

        let result = registry.resolve_order();
        assert!(matches!(result, Err(MigrationError::CircularDependency(_))));
    }

    #[test]
    fn registry_self_dependency() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("A").depends_on(&["A"]));

        let result = registry.resolve_order();
        assert!(matches!(result, Err(MigrationError::CircularDependency(_))));
    }

    #[test]
    fn registry_independent_migrations() {
        // Completely independent migrations
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("users"));
        registry.register(Migration::new("posts"));
        registry.register(Migration::new("comments"));

        let order = registry.resolve_order().unwrap();
        assert_eq!(order.len(), 3);
        // All should be present (order may vary)
        assert!(order.contains(&"users"));
        assert!(order.contains(&"posts"));
        assert!(order.contains(&"comments"));
    }
}
