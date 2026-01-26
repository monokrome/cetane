use crate::backend::Backend;
use crate::migration::{MigrationError, MigrationRegistry};

pub trait MigrationStateStore {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String>;
    fn mark_applied(&mut self, name: &str) -> Result<(), String>;
    fn mark_unapplied(&mut self, name: &str) -> Result<(), String>;
}

pub struct MigrationPlan<'a> {
    pub to_apply: Vec<&'a str>,
    pub to_unapply: Vec<&'a str>,
}

pub struct Migrator<'a, S: MigrationStateStore> {
    registry: &'a MigrationRegistry,
    backend: &'a dyn Backend,
    state: S,
}

impl<'a, S: MigrationStateStore> Migrator<'a, S> {
    pub fn new(registry: &'a MigrationRegistry, backend: &'a dyn Backend, state: S) -> Self {
        Self {
            registry,
            backend,
            state,
        }
    }

    pub fn state(&self) -> &S {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    pub fn into_state(self) -> S {
        self.state
    }

    pub fn plan_forward(&mut self) -> Result<Vec<&'static str>, MigrationError> {
        let order = self.registry.resolve_order()?;
        let applied =
            self.state
                .applied_migrations()
                .map_err(|e| MigrationError::ExecutionFailed {
                    migration: "state".to_string(),
                    error: e,
                    completed: vec![],
                })?;

        Ok(order
            .into_iter()
            .filter(|name| !applied.contains(&name.to_string()))
            .collect())
    }

    pub fn plan_backward(
        &mut self,
        target: Option<&str>,
    ) -> Result<Vec<&'static str>, MigrationError> {
        let order = self.registry.resolve_order()?;
        let applied =
            self.state
                .applied_migrations()
                .map_err(|e| MigrationError::ExecutionFailed {
                    migration: "state".to_string(),
                    error: e,
                    completed: vec![],
                })?;

        let mut to_unapply: Vec<&'static str> = order
            .iter()
            .rev()
            .filter(|name| applied.contains(&name.to_string()))
            .copied()
            .collect();

        if let Some(target) = target {
            let target_idx = to_unapply.iter().position(|&n| n == target);
            if let Some(idx) = target_idx {
                to_unapply.truncate(idx + 1);
            }
        }

        for name in &to_unapply {
            let migration = self
                .registry
                .get(name)
                .ok_or_else(|| MigrationError::NotFound(name.to_string()))?;

            if !migration.is_reversible() {
                return Err(MigrationError::NotReversible(name.to_string()));
            }
        }

        Ok(to_unapply)
    }

    pub fn generate_forward_sql(&mut self) -> Result<Vec<(String, Vec<String>)>, MigrationError> {
        let to_apply = self.plan_forward()?;
        let mut result = Vec::new();

        for name in to_apply {
            let migration = self
                .registry
                .get(name)
                .ok_or_else(|| MigrationError::NotFound(name.to_string()))?;

            result.push((name.to_string(), migration.forward_sql(self.backend)));
        }

        Ok(result)
    }

    pub fn generate_backward_sql(
        &mut self,
        target: Option<&str>,
    ) -> Result<Vec<(String, Vec<String>)>, MigrationError> {
        let to_unapply = self.plan_backward(target)?;
        let mut result = Vec::new();

        for name in to_unapply {
            let migration = self
                .registry
                .get(name)
                .ok_or_else(|| MigrationError::NotFound(name.to_string()))?;

            let sqls = migration
                .backward_sql(self.backend)
                .ok_or_else(|| MigrationError::NotReversible(name.to_string()))?;

            result.push((name.to_string(), sqls));
        }

        Ok(result)
    }

    /// Migrate forward without transaction support.
    /// Each migration runs its SQL statements in order.
    /// On failure, returns an error containing which migrations completed successfully.
    pub fn migrate_forward<F>(&mut self, mut executor: F) -> Result<Vec<String>, MigrationError>
    where
        F: FnMut(&str) -> Result<(), String>,
    {
        self.migrate_forward_with_transactions(
            &mut executor,
            &mut || Ok(()),
            &mut || Ok(()),
            &mut || Ok(()),
        )
    }

    /// Migrate forward with transaction support.
    ///
    /// For each migration:
    /// - If the backend supports transactional DDL AND the migration is atomic,
    ///   wraps the migration in begin/commit (or rollback on failure)
    /// - Otherwise, runs without transaction wrapping
    ///
    /// On failure within a transaction, rollback is called before returning the error.
    pub fn migrate_forward_with_transactions<E, B, C, R>(
        &mut self,
        executor: &mut E,
        begin: &mut B,
        commit: &mut C,
        rollback: &mut R,
    ) -> Result<Vec<String>, MigrationError>
    where
        E: FnMut(&str) -> Result<(), String>,
        B: FnMut() -> Result<(), String>,
        C: FnMut() -> Result<(), String>,
        R: FnMut() -> Result<(), String>,
    {
        let to_apply = self.plan_forward()?;
        let mut applied = Vec::new();
        let use_transactions = self.backend.supports_transactional_ddl();

        for name in to_apply {
            let migration = self
                .registry
                .get(name)
                .ok_or_else(|| MigrationError::NotFound(name.to_string()))?;

            let should_wrap = use_transactions && migration.is_atomic();

            if should_wrap {
                begin().map_err(|e| MigrationError::ExecutionFailed {
                    migration: name.to_string(),
                    error: format!("begin transaction: {}", e),
                    completed: applied.clone(),
                })?;
            }

            let result = (|| {
                for sql in migration.forward_sql(self.backend) {
                    executor(&sql)?;
                }
                Ok(())
            })();

            if let Err(e) = result {
                if should_wrap {
                    let _ = rollback(); // Best effort rollback
                }
                return Err(MigrationError::ExecutionFailed {
                    migration: name.to_string(),
                    error: e,
                    completed: applied,
                });
            }

            if should_wrap {
                if let Err(e) = commit() {
                    let _ = rollback(); // Best effort rollback
                    return Err(MigrationError::ExecutionFailed {
                        migration: name.to_string(),
                        error: format!("commit transaction: {}", e),
                        completed: applied,
                    });
                }
            }

            self.state
                .mark_applied(name)
                .map_err(|e| MigrationError::ExecutionFailed {
                    migration: name.to_string(),
                    error: e,
                    completed: applied.clone(),
                })?;

            applied.push(name.to_string());
        }

        Ok(applied)
    }

    /// Migrate backward without transaction support.
    pub fn migrate_backward<F>(
        &mut self,
        target: Option<&str>,
        mut executor: F,
    ) -> Result<Vec<String>, MigrationError>
    where
        F: FnMut(&str) -> Result<(), String>,
    {
        self.migrate_backward_with_transactions(
            target,
            &mut executor,
            &mut || Ok(()),
            &mut || Ok(()),
            &mut || Ok(()),
        )
    }

    /// Migrate backward with transaction support.
    pub fn migrate_backward_with_transactions<E, B, C, R>(
        &mut self,
        target: Option<&str>,
        executor: &mut E,
        begin: &mut B,
        commit: &mut C,
        rollback: &mut R,
    ) -> Result<Vec<String>, MigrationError>
    where
        E: FnMut(&str) -> Result<(), String>,
        B: FnMut() -> Result<(), String>,
        C: FnMut() -> Result<(), String>,
        R: FnMut() -> Result<(), String>,
    {
        let to_unapply = self.plan_backward(target)?;
        let mut unapplied = Vec::new();
        let use_transactions = self.backend.supports_transactional_ddl();

        for name in to_unapply {
            let migration = self
                .registry
                .get(name)
                .ok_or_else(|| MigrationError::NotFound(name.to_string()))?;

            let sqls = migration
                .backward_sql(self.backend)
                .ok_or_else(|| MigrationError::NotReversible(name.to_string()))?;

            let should_wrap = use_transactions && migration.is_atomic();

            if should_wrap {
                begin().map_err(|e| MigrationError::ExecutionFailed {
                    migration: name.to_string(),
                    error: format!("begin transaction: {}", e),
                    completed: unapplied.clone(),
                })?;
            }

            let result = (|| {
                for sql in sqls {
                    executor(&sql)?;
                }
                Ok(())
            })();

            if let Err(e) = result {
                if should_wrap {
                    let _ = rollback();
                }
                return Err(MigrationError::ExecutionFailed {
                    migration: name.to_string(),
                    error: e,
                    completed: unapplied,
                });
            }

            if should_wrap {
                if let Err(e) = commit() {
                    let _ = rollback();
                    return Err(MigrationError::ExecutionFailed {
                        migration: name.to_string(),
                        error: format!("commit transaction: {}", e),
                        completed: unapplied,
                    });
                }
            }

            self.state
                .mark_unapplied(name)
                .map_err(|e| MigrationError::ExecutionFailed {
                    migration: name.to_string(),
                    error: e,
                    completed: unapplied.clone(),
                })?;

            unapplied.push(name.to_string());
        }

        Ok(unapplied)
    }
}

#[derive(Default)]
pub struct InMemoryState {
    applied: Vec<String>,
}

impl InMemoryState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_applied(applied: Vec<String>) -> Self {
        Self { applied }
    }
}

impl MigrationStateStore for InMemoryState {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        Ok(self.applied.clone())
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        if !self.applied.contains(&name.to_string()) {
            self.applied.push(name.to_string());
        }
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        self.applied.retain(|n| n != name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Sqlite;
    use crate::field::{Field, FieldType};
    use crate::migration::Migration;
    use crate::operation::{AddField, CreateTable, DropTable};

    fn setup_registry() -> MigrationRegistry {
        let mut registry = MigrationRegistry::new();

        registry.register(
            Migration::new("0001_create_users").operation(
                CreateTable::new("users")
                    .add_field(Field::new("id", FieldType::Serial).primary_key())
                    .add_field(Field::new("email", FieldType::Text).not_null()),
            ),
        );

        registry.register(
            Migration::new("0002_add_name")
                .depends_on(&["0001_create_users"])
                .operation(AddField::new("users", Field::new("name", FieldType::Text))),
        );

        registry
    }

    #[test]
    fn plan_forward_empty_state() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let plan = migrator.plan_forward().unwrap();
        assert_eq!(plan, vec!["0001_create_users", "0002_add_name"]);
    }

    #[test]
    fn plan_forward_partial_state() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec!["0001_create_users".to_string()]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let plan = migrator.plan_forward().unwrap();
        assert_eq!(plan, vec!["0002_add_name"]);
    }

    #[test]
    fn plan_backward_all() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec![
            "0001_create_users".to_string(),
            "0002_add_name".to_string(),
        ]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let plan = migrator.plan_backward(None).unwrap();
        assert_eq!(plan, vec!["0002_add_name", "0001_create_users"]);
    }

    #[test]
    fn plan_backward_to_target() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec![
            "0001_create_users".to_string(),
            "0002_add_name".to_string(),
        ]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let plan = migrator.plan_backward(Some("0002_add_name")).unwrap();
        assert_eq!(plan, vec!["0002_add_name"]);
    }

    #[test]
    fn generate_forward_sql() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let sqls = migrator.generate_forward_sql().unwrap();
        assert_eq!(sqls.len(), 2);
        assert!(sqls[0].1[0].contains("CREATE TABLE"));
        assert!(sqls[1].1[0].contains("ADD COLUMN"));
    }

    #[test]
    fn generate_backward_sql() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec![
            "0001_create_users".to_string(),
            "0002_add_name".to_string(),
        ]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let sqls = migrator.generate_backward_sql(None).unwrap();
        assert_eq!(sqls.len(), 2);
        assert!(sqls[0].1[0].contains("DROP COLUMN"));
        assert!(sqls[1].1[0].contains("DROP TABLE"));
    }

    #[test]
    fn migrate_forward_executes_and_tracks() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let mut executed = Vec::new();
        let applied = migrator
            .migrate_forward(|sql| {
                executed.push(sql.to_string());
                Ok(())
            })
            .unwrap();

        assert_eq!(applied.len(), 2);
        assert!(executed.iter().any(|s| s.contains("CREATE TABLE")));
        assert!(executed.iter().any(|s| s.contains("ADD COLUMN")));
    }

    #[test]
    fn migrate_backward_executes_and_tracks() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec![
            "0001_create_users".to_string(),
            "0002_add_name".to_string(),
        ]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let mut executed = Vec::new();
        let unapplied = migrator
            .migrate_backward(None, |sql| {
                executed.push(sql.to_string());
                Ok(())
            })
            .unwrap();

        assert_eq!(unapplied.len(), 2);
        assert!(executed.iter().any(|s| s.contains("DROP COLUMN")));
        assert!(executed.iter().any(|s| s.contains("DROP TABLE")));
    }

    #[test]
    fn non_reversible_migration_fails_backward_plan() {
        let mut registry = MigrationRegistry::new();
        registry.register(
            Migration::new("0001_irreversible").operation(DropTable::new("legacy_table")),
        );

        let state = InMemoryState::with_applied(vec!["0001_irreversible".to_string()]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let result = migrator.plan_backward(None);
        assert!(matches!(result, Err(MigrationError::NotReversible(_))));
    }

    #[test]
    fn state_accessor() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec!["0001_create_users".to_string()]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let applied = migrator.state_mut().applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_create_users"]);
    }

    #[test]
    fn state_mut_accessor() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        migrator.state_mut().mark_applied("manual").unwrap();
        let applied = migrator.state_mut().applied_migrations().unwrap();
        assert!(applied.contains(&"manual".to_string()));
    }

    #[test]
    fn into_state_consumes_migrator() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec!["test".to_string()]);
        let migrator = Migrator::new(&registry, &Sqlite, state);

        let mut recovered_state = migrator.into_state();
        assert_eq!(
            recovered_state.applied_migrations().unwrap(),
            vec!["test".to_string()]
        );
    }

    #[test]
    fn migrate_forward_with_transactions_calls_begin_commit() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let mut begins = 0;
        let mut commits = 0;
        let mut rollbacks = 0;

        let applied = migrator
            .migrate_forward_with_transactions(
                &mut |_sql| Ok(()),
                &mut || {
                    begins += 1;
                    Ok(())
                },
                &mut || {
                    commits += 1;
                    Ok(())
                },
                &mut || {
                    rollbacks += 1;
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(applied.len(), 2);
        assert_eq!(begins, 2);
        assert_eq!(commits, 2);
        assert_eq!(rollbacks, 0);
    }

    #[test]
    fn migrate_forward_failure_calls_rollback() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let mut rollbacks = 0;

        let result = migrator.migrate_forward_with_transactions(
            &mut |sql| {
                if sql.contains("ADD COLUMN") {
                    Err("simulated failure".to_string())
                } else {
                    Ok(())
                }
            },
            &mut || Ok(()),
            &mut || Ok(()),
            &mut || {
                rollbacks += 1;
                Ok(())
            },
        );

        assert!(result.is_err());
        assert_eq!(rollbacks, 1);

        if let Err(MigrationError::ExecutionFailed {
            migration,
            completed,
            ..
        }) = result
        {
            assert_eq!(migration, "0002_add_name");
            assert_eq!(completed, vec!["0001_create_users"]);
        } else {
            panic!("Expected ExecutionFailed error");
        }
    }

    #[test]
    fn migrate_forward_failure_reports_completed() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let result = migrator.migrate_forward(|sql| {
            if sql.contains("ADD COLUMN") {
                Err("simulated failure".to_string())
            } else {
                Ok(())
            }
        });

        match result {
            Err(MigrationError::ExecutionFailed {
                migration,
                completed,
                ..
            }) => {
                assert_eq!(migration, "0002_add_name");
                assert_eq!(completed, vec!["0001_create_users"]);
            }
            _ => panic!("Expected ExecutionFailed error"),
        }
    }

    #[test]
    fn non_atomic_migration_skips_transaction() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("0001_create_users").atomic(false).operation(
            CreateTable::new("users").add_field(Field::new("id", FieldType::Serial).primary_key()),
        ));

        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let mut begins = 0;

        let applied = migrator
            .migrate_forward_with_transactions(
                &mut |_sql| Ok(()),
                &mut || {
                    begins += 1;
                    Ok(())
                },
                &mut || Ok(()),
                &mut || Ok(()),
            )
            .unwrap();

        assert_eq!(applied.len(), 1);
        assert_eq!(begins, 0); // No transaction for non-atomic migration
    }

    // Additional error path tests

    #[test]
    fn migrate_forward_failure_on_first_migration() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let result = migrator.migrate_forward(|sql| {
            if sql.contains("CREATE TABLE") {
                Err("first migration failed".to_string())
            } else {
                Ok(())
            }
        });

        match result {
            Err(MigrationError::ExecutionFailed {
                migration,
                completed,
                error,
            }) => {
                assert_eq!(migration, "0001_create_users");
                assert!(completed.is_empty()); // No migrations completed
                assert!(error.contains("first migration failed"));
            }
            _ => panic!("Expected ExecutionFailed error"),
        }
    }

    #[test]
    fn empty_migration_executes_successfully() {
        let mut registry = MigrationRegistry::new();
        // Migration with no operations
        registry.register(Migration::new("0001_placeholder"));

        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let mut executed_count = 0;
        let applied = migrator
            .migrate_forward(|_sql| {
                executed_count += 1;
                Ok(())
            })
            .unwrap();

        assert_eq!(applied, vec!["0001_placeholder"]);
        assert_eq!(executed_count, 0); // No SQL executed
    }

    #[test]
    fn backward_migration_failure_mid_way() {
        let mut registry = MigrationRegistry::new();
        registry.register(Migration::new("0001_create_users").operation(
            CreateTable::new("users").add_field(Field::new("id", FieldType::Serial).primary_key()),
        ));
        registry.register(
            Migration::new("0002_create_posts")
                .depends_on(&["0001_create_users"])
                .operation(
                    CreateTable::new("posts")
                        .add_field(Field::new("id", FieldType::Serial).primary_key()),
                ),
        );

        let state = InMemoryState::with_applied(vec![
            "0001_create_users".to_string(),
            "0002_create_posts".to_string(),
        ]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        // First rollback succeeds, second fails
        let mut call_count = 0;
        let result = migrator.migrate_backward(None, |_sql| {
            call_count += 1;
            if call_count > 1 {
                Err("rollback failed".to_string())
            } else {
                Ok(())
            }
        });

        match result {
            Err(MigrationError::ExecutionFailed {
                migration,
                completed,
                ..
            }) => {
                assert_eq!(migration, "0001_create_users");
                assert_eq!(completed, vec!["0002_create_posts"]);
            }
            _ => panic!("Expected ExecutionFailed error"),
        }
    }

    #[test]
    fn already_applied_migrations_skipped() {
        let registry = setup_registry();
        // All migrations already applied
        let state = InMemoryState::with_applied(vec![
            "0001_create_users".to_string(),
            "0002_add_name".to_string(),
        ]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let applied = migrator.migrate_forward(|_| Ok(())).unwrap();

        assert!(applied.is_empty()); // Nothing to apply
    }

    #[test]
    fn no_applied_migrations_nothing_to_rollback() {
        let registry = setup_registry();
        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let unapplied = migrator.migrate_backward(None, |_| Ok(())).unwrap();

        assert!(unapplied.is_empty());
    }

    #[test]
    fn backward_target_not_applied_rolls_back_all() {
        let registry = setup_registry();
        let state = InMemoryState::with_applied(vec!["0001_create_users".to_string()]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        // Target 0002 isn't applied, so all applied migrations are rolled back
        // (target acts as a filter, not a stopping point)
        let unapplied = migrator
            .migrate_backward(Some("0002_add_name"), |_| Ok(()))
            .unwrap();

        assert_eq!(unapplied, vec!["0001_create_users"]);
    }

    #[test]
    fn generate_backward_sql_for_non_reversible_fails() {
        let mut registry = MigrationRegistry::new();
        // DropTable without field definitions is not reversible
        registry.register(Migration::new("0001_drop").operation(DropTable::new("legacy")));

        let state = InMemoryState::with_applied(vec!["0001_drop".to_string()]);
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let result = migrator.generate_backward_sql(None);
        assert!(matches!(result, Err(MigrationError::NotReversible(_))));
    }

    #[test]
    fn multiple_sql_statements_per_migration() {
        let mut registry = MigrationRegistry::new();
        registry.register(
            Migration::new("0001_complex").operation(
                CreateTable::new("users")
                    .add_field(Field::new("id", FieldType::Serial).primary_key())
                    .add_field(
                        Field::new("org_id", FieldType::Integer)
                            .not_null()
                            .references("orgs", "id"),
                    ),
            ),
        );

        let state = InMemoryState::new();
        let mut migrator = Migrator::new(&registry, &Sqlite, state);

        let sql = migrator.generate_forward_sql().unwrap();
        assert_eq!(sql.len(), 1);
        // The table creation has FK, but it's all in one statement
        assert!(sql[0].1[0].contains("CREATE TABLE"));
    }
}
