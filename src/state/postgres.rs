use postgres::Client;

use crate::migrator::MigrationStateStore;

const DEFAULT_TABLE_NAME: &str = "schema_migrations";

pub struct PostgresMigrationState<'a> {
    client: &'a mut Client,
    table_name: String,
}

impl<'a> PostgresMigrationState<'a> {
    pub fn new(client: &'a mut Client) -> Result<Self, String> {
        Self::with_table_name(client, DEFAULT_TABLE_NAME)
    }

    pub fn with_table_name(client: &'a mut Client, table_name: &str) -> Result<Self, String> {
        let mut state = Self {
            client,
            table_name: table_name.to_string(),
        };
        state.ensure_table()?;
        Ok(state)
    }

    fn ensure_table(&mut self) -> Result<(), String> {
        self.client
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        migration_name TEXT PRIMARY KEY,
                        applied BOOLEAN NOT NULL DEFAULT TRUE
                    )",
                    self.table_name
                ),
                &[],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl MigrationStateStore for PostgresMigrationState<'_> {
    fn applied_migrations(&mut self) -> Result<Vec<String>, String> {
        let rows = self
            .client
            .query(
                &format!(
                    "SELECT migration_name FROM {} WHERE applied = TRUE ORDER BY migration_name",
                    self.table_name
                ),
                &[],
            )
            .map_err(|e| e.to_string())?;

        let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
        Ok(names)
    }

    fn mark_applied(&mut self, name: &str) -> Result<(), String> {
        self.client
            .execute(
                &format!(
                    "INSERT INTO {} (migration_name, applied) VALUES ($1, TRUE)
                     ON CONFLICT (migration_name) DO UPDATE SET applied = TRUE",
                    self.table_name
                ),
                &[&name],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn mark_unapplied(&mut self, name: &str) -> Result<(), String> {
        self.client
            .execute(
                &format!(
                    "UPDATE {} SET applied = FALSE WHERE migration_name = $1",
                    self.table_name
                ),
                &[&name],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres::{Client, NoTls};
    use std::env;

    fn get_test_client() -> Option<Client> {
        let host = env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
        let user = env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
        let password = env::var("POSTGRES_PASSWORD").ok();
        let dbname = env::var("POSTGRES_DB").unwrap_or_else(|_| "cetane_test".to_string());

        let mut config = format!("host={} user={} dbname={}", host, user, dbname);
        if let Some(pw) = password {
            config.push_str(&format!(" password={}", pw));
        }

        Client::connect(&config, NoTls).ok()
    }

    fn cleanup_table(client: &mut Client, table_name: &str) {
        let _ = client.execute(&format!("DROP TABLE IF EXISTS {}", table_name), &[]);
    }

    #[test]
    #[ignore = "requires postgres connection"]
    fn creates_table_on_init() {
        let Some(mut client) = get_test_client() else {
            return;
        };
        let table_name = "test_creates_table_migrations";
        cleanup_table(&mut client, table_name);

        let _state = PostgresMigrationState::with_table_name(&mut client, table_name).unwrap();

        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                &[&table_name],
            )
            .map(|row| row.get(0))
            .unwrap_or(false);

        assert!(exists);
        cleanup_table(&mut client, table_name);
    }

    #[test]
    #[ignore = "requires postgres connection"]
    fn mark_applied_and_query() {
        let Some(mut client) = get_test_client() else {
            return;
        };
        let table_name = "test_mark_applied_migrations";
        cleanup_table(&mut client, table_name);

        let mut state = PostgresMigrationState::with_table_name(&mut client, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0002_add_users").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert!(applied.contains(&"0001_initial".to_string()));
        assert!(applied.contains(&"0002_add_users".to_string()));

        cleanup_table(&mut client, table_name);
    }

    #[test]
    #[ignore = "requires postgres connection"]
    fn mark_unapplied() {
        let Some(mut client) = get_test_client() else {
            return;
        };
        let table_name = "test_mark_unapplied_migrations";
        cleanup_table(&mut client, table_name);

        let mut state = PostgresMigrationState::with_table_name(&mut client, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0002_add_users").unwrap();
        state.mark_unapplied("0002_add_users").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);

        cleanup_table(&mut client, table_name);
    }

    #[test]
    #[ignore = "requires postgres connection"]
    fn mark_applied_is_idempotent() {
        let Some(mut client) = get_test_client() else {
            return;
        };
        let table_name = "test_idempotent_migrations";
        cleanup_table(&mut client, table_name);

        let mut state = PostgresMigrationState::with_table_name(&mut client, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_applied("0001_initial").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);

        cleanup_table(&mut client, table_name);
    }

    #[test]
    #[ignore = "requires postgres connection"]
    fn reapply_after_unapply() {
        let Some(mut client) = get_test_client() else {
            return;
        };
        let table_name = "test_reapply_migrations";
        cleanup_table(&mut client, table_name);

        let mut state = PostgresMigrationState::with_table_name(&mut client, table_name).unwrap();

        state.mark_applied("0001_initial").unwrap();
        state.mark_unapplied("0001_initial").unwrap();
        state.mark_applied("0001_initial").unwrap();

        let applied = state.applied_migrations().unwrap();
        assert_eq!(applied, vec!["0001_initial"]);

        cleanup_table(&mut client, table_name);
    }
}
