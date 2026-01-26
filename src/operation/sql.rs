use std::collections::HashMap;

use crate::backend::Backend;
use crate::operation::Operation;

#[derive(Debug, Clone)]
enum SqlSource {
    Static {
        sql: Vec<String>,
        only_backends: Option<Vec<String>>,
    },
    ByBackend(HashMap<String, Vec<String>>),
}

impl SqlSource {
    fn resolve(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        match self {
            SqlSource::Static { sql, only_backends } => {
                if let Some(ref only) = only_backends {
                    if !only.iter().any(|b| b == backend.name()) {
                        return Some(vec![]);
                    }
                }
                Some(sql.clone())
            }
            SqlSource::ByBackend(map) => map.get(backend.name()).cloned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunSql {
    forward: SqlSource,
    backward: Option<SqlSource>,
    description: String,
}

impl RunSql {
    /// Create a RunSql operation with SQL that runs on all backends.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            forward: SqlSource::Static {
                sql: vec![sql.into()],
                only_backends: None,
            },
            backward: None,
            description: "Run custom SQL".to_string(),
        }
    }

    /// Create a RunSql operation with multiple SQL statements.
    pub fn multiple(sqls: Vec<String>) -> Self {
        Self {
            forward: SqlSource::Static {
                sql: sqls,
                only_backends: None,
            },
            backward: None,
            description: "Run custom SQL".to_string(),
        }
    }

    /// Create a reversible RunSql operation.
    pub fn reversible(forward: impl Into<String>, backward: impl Into<String>) -> Self {
        Self {
            forward: SqlSource::Static {
                sql: vec![forward.into()],
                only_backends: None,
            },
            backward: Some(SqlSource::Static {
                sql: vec![backward.into()],
                only_backends: None,
            }),
            description: "Run custom SQL".to_string(),
        }
    }

    /// Create a portable RunSql that requires backend-specific SQL.
    /// Use `for_backend()` to add SQL for each supported backend.
    /// Will panic at runtime if executed on an unconfigured backend.
    pub fn portable() -> Self {
        Self {
            forward: SqlSource::ByBackend(HashMap::new()),
            backward: None,
            description: "Run portable SQL".to_string(),
        }
    }

    /// Add SQL for a specific backend (use with `portable()`).
    pub fn for_backend(mut self, backend: &str, sql: impl Into<String>) -> Self {
        if let SqlSource::ByBackend(ref mut map) = self.forward {
            map.insert(backend.to_string(), vec![sql.into()]);
        }
        self
    }

    /// Add SQL with reverse for a specific backend (use with `portable()`).
    pub fn for_backend_reversible(
        mut self,
        backend: &str,
        forward: impl Into<String>,
        backward: impl Into<String>,
    ) -> Self {
        if let SqlSource::ByBackend(ref mut map) = self.forward {
            map.insert(backend.to_string(), vec![forward.into()]);
        }
        let backward_map = match self.backward {
            Some(SqlSource::ByBackend(map)) => map,
            _ => HashMap::new(),
        };
        let mut backward_map = backward_map;
        backward_map.insert(backend.to_string(), vec![backward.into()]);
        self.backward = Some(SqlSource::ByBackend(backward_map));
        self
    }

    /// Restrict this operation to only run on specific backends.
    /// On other backends, the operation is silently skipped.
    pub fn only_for(mut self, backends: &[&str]) -> Self {
        if let SqlSource::Static {
            ref mut only_backends,
            ..
        } = self.forward
        {
            *only_backends = Some(backends.iter().map(|s| s.to_string()).collect());
        }
        if let Some(SqlSource::Static {
            ref mut only_backends,
            ..
        }) = self.backward
        {
            *only_backends = Some(backends.iter().map(|s| s.to_string()).collect());
        }
        self
    }

    /// Add a reverse SQL statement.
    pub fn with_reverse(mut self, sql: impl Into<String>) -> Self {
        let only_backends = if let SqlSource::Static {
            ref only_backends, ..
        } = self.forward
        {
            only_backends.clone()
        } else {
            None
        };
        self.backward = Some(SqlSource::Static {
            sql: vec![sql.into()],
            only_backends,
        });
        self
    }

    /// Add multiple reverse SQL statements.
    pub fn with_reverse_multiple(mut self, sqls: Vec<String>) -> Self {
        let only_backends = if let SqlSource::Static {
            ref only_backends, ..
        } = self.forward
        {
            only_backends.clone()
        } else {
            None
        };
        self.backward = Some(SqlSource::Static {
            sql: sqls,
            only_backends,
        });
        self
    }

    /// Set a description for this operation.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }
}

impl Operation for RunSql {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        self.forward.resolve(backend).unwrap_or_else(|| {
            panic!(
                "RunSql: no SQL configured for backend '{}'. Use for_backend() to add it.",
                backend.name()
            )
        })
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        self.backward.as_ref().map(|b| {
            b.resolve(backend).unwrap_or_else(|| {
                panic!(
                    "RunSql: no reverse SQL configured for backend '{}'. Use for_backend_reversible() to add it.",
                    backend.name()
                )
            })
        })
    }

    fn describe(&self) -> String {
        self.description.clone()
    }

    fn is_reversible(&self) -> bool {
        self.backward.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Sqlite;

    #[test]
    fn run_sql_forward() {
        let op = RunSql::new("INSERT INTO config (key, value) VALUES ('version', '1')");

        let sql = op.forward(&Sqlite);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("INSERT INTO config"));
    }

    #[test]
    fn run_sql_without_reverse_not_reversible() {
        let op = RunSql::new("DELETE FROM users");
        assert!(!op.is_reversible());
        assert!(op.backward(&Sqlite).is_none());
    }

    #[test]
    fn run_sql_with_reverse_is_reversible() {
        let op = RunSql::reversible(
            "INSERT INTO config (key, value) VALUES ('version', '1')",
            "DELETE FROM config WHERE key = 'version'",
        );

        assert!(op.is_reversible());
        let reverse = op.backward(&Sqlite).unwrap();
        assert!(reverse[0].contains("DELETE FROM config"));
    }

    #[test]
    fn run_sql_multiple_statements() {
        let op = RunSql::multiple(vec![
            "INSERT INTO a VALUES (1)".to_string(),
            "INSERT INTO b VALUES (2)".to_string(),
        ]);

        let sql = op.forward(&Sqlite);
        assert_eq!(sql.len(), 2);
    }

    #[test]
    fn run_sql_custom_description() {
        let op = RunSql::new("SELECT 1").with_description("Test query");
        assert_eq!(Operation::describe(&op), "Test query");
    }

    #[test]
    fn run_sql_only_for_matching_backend() {
        let op = RunSql::new("VACUUM").only_for(&["sqlite"]);

        let sql = op.forward(&Sqlite);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "VACUUM");
    }

    #[test]
    fn run_sql_only_for_non_matching_backend_skipped() {
        let op = RunSql::new("CLUSTER users").only_for(&["postgres"]);

        let sql = op.forward(&Sqlite);
        assert!(sql.is_empty());
    }

    #[test]
    fn run_sql_only_for_with_reverse() {
        let op = RunSql::new("PRAGMA optimize")
            .only_for(&["sqlite"])
            .with_reverse("SELECT 1");

        assert!(op.is_reversible());
        let forward = op.forward(&Sqlite);
        assert_eq!(forward.len(), 1);

        let backward = op.backward(&Sqlite).unwrap();
        assert_eq!(backward.len(), 1);
    }

    #[test]
    fn run_sql_portable_matching_backend() {
        let op = RunSql::portable()
            .for_backend("sqlite", "CREATE INDEX idx ON t(c)")
            .for_backend("postgres", "CREATE INDEX CONCURRENTLY idx ON t(c)");

        let sql = op.forward(&Sqlite);
        assert_eq!(sql[0], "CREATE INDEX idx ON t(c)");
    }

    #[test]
    #[should_panic(expected = "no SQL configured for backend")]
    fn run_sql_portable_missing_backend_panics() {
        let op = RunSql::portable().for_backend("postgres", "ANALYZE");

        op.forward(&Sqlite);
    }

    #[test]
    fn run_sql_portable_reversible() {
        let op = RunSql::portable()
            .for_backend_reversible("sqlite", "CREATE INDEX idx ON t(c)", "DROP INDEX idx")
            .for_backend_reversible(
                "postgres",
                "CREATE INDEX CONCURRENTLY idx ON t(c)",
                "DROP INDEX CONCURRENTLY idx",
            );

        assert!(op.is_reversible());

        let forward = op.forward(&Sqlite);
        assert!(forward[0].contains("CREATE INDEX"));

        let backward = op.backward(&Sqlite).unwrap();
        assert!(backward[0].contains("DROP INDEX"));
    }

    #[test]
    fn run_sql_with_reverse_multiple() {
        let op = RunSql::new("DROP TABLE a; DROP TABLE b").with_reverse_multiple(vec![
            "CREATE TABLE a (id INT)".to_string(),
            "CREATE TABLE b (id INT)".to_string(),
        ]);

        let backward = op.backward(&Sqlite).unwrap();
        assert_eq!(backward.len(), 2);
    }
}
