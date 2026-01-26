use rusqlite::Connection;

use crate::migrator::MigrationStateStore;

const DEFAULT_TABLE_NAME: &str = "schema_migrations";

pub struct SqliteMigrationState<'a> {
    conn: &'a Connection,
    table_name: String,
}

impl<'a> SqliteMigrationState<'a> {
    pub fn new(conn: &'a Connection) -> Result<Self, String> {
        Self::with_table_name(conn, DEFAULT_TABLE_NAME)
    }

    pub fn with_table_name(conn: &'a Connection, table_name: &str) -> Result<Self, String> {
        let state = Self {
            conn,
            table_name: table_name.to_string(),
        };
        state.ensure_table()?;
        Ok(state)
    }

    fn ensure_table(&self) -> Result<(), String> {
        self.conn
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        migration_name TEXT PRIMARY KEY,
                        applied INTEGER NOT NULL DEFAULT 1
                    )",
                    self.table_name
                ),
                [],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl MigrationStateStore for SqliteMigrationState<'_> {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        let mut stmt = self
            .conn
            .prepare(&format!(
                "SELECT migration_name FROM {} WHERE applied = 1 ORDER BY rowid",
                self.table_name
            ))
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
            .execute(
                &format!(
                    "INSERT INTO {} (migration_name, applied) VALUES (?1, 1)
                     ON CONFLICT(migration_name) DO UPDATE SET applied = 1",
                    self.table_name
                ),
                [name],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        self.conn
            .execute(
                &format!(
                    "UPDATE {} SET applied = 0 WHERE migration_name = ?1",
                    self.table_name
                ),
                [name],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_table_on_init() {
        let conn = Connection::open_in_memory().unwrap();
        let _state = SqliteMigrationState::new(&conn).unwrap();

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);

        assert!(exists);
    }

    #[test]
    fn custom_table_name() {
        let conn = Connection::open_in_memory().unwrap();
        let _state = SqliteMigrationState::with_table_name(&conn, "my_migrations").unwrap();

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='my_migrations'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);

        assert!(exists);
    }

    #[test]
    fn mark_applied_and_query() {
        let conn = Connection::open_in_memory().unwrap();
        let mut state = SqliteMigrationState::new(&conn).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0002_add_users").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial", "0002_add_users"]);
    }

    #[test]
    fn mark_unapplied() {
        let conn = Connection::open_in_memory().unwrap();
        let mut state = SqliteMigrationState::new(&conn).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0002_add_users").unwrap();
        state.mark_unapplied("0002_add_users").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);
    }

    #[test]
    fn mark_applied_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        let mut state = SqliteMigrationState::new(&conn).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0001_initial").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);
    }

    #[test]
    fn reapply_after_unapply() {
        let conn = Connection::open_in_memory().unwrap();
        let mut state = SqliteMigrationState::new(&conn).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_unapplied("0001_initial").unwrap();
        state.mark_applied("0001_initial").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);
    }

    #[test]
    fn unapplied_migration_not_in_list() {
        let conn = Connection::open_in_memory().unwrap();
        let mut state = SqliteMigrationState::new(&conn).unwrap();

        state.mark_applied("0001_a").unwrap();
        state.mark_applied("0002_b").unwrap();
        state.mark_unapplied("0001_a").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0002_b"]);
    }
}
