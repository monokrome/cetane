use mysql::prelude::*;
use mysql::PooledConn;

use crate::migrator::MigrationStateStore;

const DEFAULT_TABLE_NAME: &str = "schema_migrations";

pub struct MySqlMigrationState<'a> {
    conn: &'a mut PooledConn,
    table_name: String,
}

impl<'a> MySqlMigrationState<'a> {
    pub fn new(conn: &'a mut PooledConn) -> Result<Self, String> {
        Self::with_table_name(conn, DEFAULT_TABLE_NAME)
    }

    pub fn with_table_name(conn: &'a mut PooledConn, table_name: &str) -> Result<Self, String> {
        let mut state = Self {
            conn,
            table_name: table_name.to_string(),
        };
        state.ensure_table()?;
        Ok(state)
    }

    fn ensure_table(&mut self) -> Result<(), String> {
        self.conn
            .query_drop(format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    migration_name VARCHAR(255) PRIMARY KEY,
                    applied BOOLEAN NOT NULL DEFAULT TRUE
                )",
                self.table_name
            ))
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl MigrationStateStore for MySqlMigrationState<'_> {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        let names: Vec<String> = self
            .conn
            .query(format!(
                "SELECT migration_name FROM {} WHERE applied = TRUE ORDER BY migration_name",
                self.table_name
            ))
            .map_err(|e| e.to_string())?;

        Ok(names)
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        self.conn
            .exec_drop(
                format!(
                    "INSERT INTO {} (migration_name, applied) VALUES (?, TRUE)
                     ON DUPLICATE KEY UPDATE applied = TRUE",
                    self.table_name
                ),
                (name,),
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        self.conn
            .exec_drop(
                format!(
                    "UPDATE {} SET applied = FALSE WHERE migration_name = ?",
                    self.table_name
                ),
                (name,),
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql::{Pool, PooledConn};
    use std::env;

    fn get_test_conn() -> Option<PooledConn> {
        let host = env::var("MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string());
        let user = env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string());
        let password = env::var("MYSQL_PASSWORD").ok();
        let dbname = env::var("MYSQL_DB").unwrap_or_else(|_| "cetane_test".to_string());

        let url = if let Some(pw) = password {
            format!("mysql://{}:{}@{}/{}", user, pw, host, dbname)
        } else {
            format!("mysql://{}@{}/{}", user, host, dbname)
        };

        Pool::new(url.as_str()).ok()?.get_conn().ok()
    }

    fn cleanup_table(conn: &mut PooledConn, table_name: &str) {
        let _ = conn.query_drop(format!("DROP TABLE IF EXISTS {}", table_name));
    }

    #[test]
    #[ignore = "requires mysql connection"]
    fn creates_table_on_init() {
        let Some(mut conn) = get_test_conn() else {
            return;
        };
        let table_name = "test_creates_table_migrations";
        cleanup_table(&mut conn, table_name);

        let _state = MySqlMigrationState::with_table_name(&mut conn, table_name).unwrap();

        let exists: Option<String> = conn
            .query_first(format!(
                "SELECT table_name FROM information_schema.tables WHERE table_name = '{}'",
                table_name
            ))
            .unwrap_or(None);

        assert!(exists.is_some());
        cleanup_table(&mut conn, table_name);
    }

    #[test]
    #[ignore = "requires mysql connection"]
    fn mark_applied_and_query() {
        let Some(mut conn) = get_test_conn() else {
            return;
        };
        let table_name = "test_mark_applied_migrations";
        cleanup_table(&mut conn, table_name);

        let mut state = MySqlMigrationState::with_table_name(&mut conn, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0002_add_users").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert!(applied.contains(&"0001_initial".to_string()));
        assert!(applied.contains(&"0002_add_users".to_string()));

        cleanup_table(&mut conn, table_name);
    }

    #[test]
    #[ignore = "requires mysql connection"]
    fn mark_unapplied() {
        let Some(mut conn) = get_test_conn() else {
            return;
        };
        let table_name = "test_mark_unapplied_migrations";
        cleanup_table(&mut conn, table_name);

        let mut state = MySqlMigrationState::with_table_name(&mut conn, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0002_add_users").unwrap();
        state.mark_unapplied("0002_add_users").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);

        cleanup_table(&mut conn, table_name);
    }

    #[test]
    #[ignore = "requires mysql connection"]
    fn mark_applied_is_idempotent() {
        let Some(mut conn) = get_test_conn() else {
            return;
        };
        let table_name = "test_idempotent_migrations";
        cleanup_table(&mut conn, table_name);

        let mut state = MySqlMigrationState::with_table_name(&mut conn, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0001_initial").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);

        cleanup_table(&mut conn, table_name);
    }

    #[test]
    #[ignore = "requires mysql connection"]
    fn reapply_after_unapply() {
        let Some(mut conn) = get_test_conn() else {
            return;
        };
        let table_name = "test_reapply_migrations";
        cleanup_table(&mut conn, table_name);

        let mut state = MySqlMigrationState::with_table_name(&mut conn, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_unapplied("0001_initial").unwrap();
        state.mark_applied("0001_initial").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);

        cleanup_table(&mut conn, table_name);
    }
}
