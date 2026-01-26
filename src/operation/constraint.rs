use crate::backend::Backend;
use crate::field::ReferentialAction;
use crate::operation::Operation;

#[derive(Debug, Clone)]
pub enum Constraint {
    Check {
        name: String,
        expression: String,
    },
    Unique {
        name: String,
        columns: Vec<String>,
    },
    ForeignKey {
        name: String,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    },
}

impl Constraint {
    pub fn check(name: impl Into<String>, expression: impl Into<String>) -> Self {
        Constraint::Check {
            name: name.into(),
            expression: expression.into(),
        }
    }

    pub fn unique(name: impl Into<String>, columns: Vec<String>) -> Self {
        Constraint::Unique {
            name: name.into(),
            columns,
        }
    }

    pub fn foreign_key(
        name: impl Into<String>,
        columns: Vec<String>,
        ref_table: impl Into<String>,
        ref_columns: Vec<String>,
    ) -> Self {
        Constraint::ForeignKey {
            name: name.into(),
            columns,
            ref_table: ref_table.into(),
            ref_columns,
            on_delete: ReferentialAction::default(),
            on_update: ReferentialAction::default(),
        }
    }

    pub fn on_delete(mut self, action: ReferentialAction) -> Self {
        if let Constraint::ForeignKey {
            ref mut on_delete, ..
        } = self
        {
            *on_delete = action;
        }
        self
    }

    pub fn on_update(mut self, action: ReferentialAction) -> Self {
        if let Constraint::ForeignKey {
            ref mut on_update, ..
        } = self
        {
            *on_update = action;
        }
        self
    }

    pub fn name(&self) -> &str {
        match self {
            Constraint::Check { name, .. } => name,
            Constraint::Unique { name, .. } => name,
            Constraint::ForeignKey { name, .. } => name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AddConstraint {
    pub table: String,
    pub constraint: Constraint,
}

impl AddConstraint {
    pub fn new(table: impl Into<String>, constraint: Constraint) -> Self {
        Self {
            table: table.into(),
            constraint,
        }
    }
}

impl Operation for AddConstraint {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        vec![backend.add_constraint_sql(&self.table, &self.constraint)]
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        Some(vec![
            backend.drop_constraint_sql(&self.table, self.constraint.name())
        ])
    }

    fn describe(&self) -> String {
        format!(
            "Add constraint {} to {}",
            self.constraint.name(),
            self.table
        )
    }
}

#[derive(Debug, Clone)]
pub struct RemoveConstraint {
    pub table: String,
    pub name: String,
    pub constraint: Option<Constraint>,
}

impl RemoveConstraint {
    pub fn new(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            name: name.into(),
            constraint: None,
        }
    }

    pub fn with_definition(mut self, constraint: Constraint) -> Self {
        self.constraint = Some(constraint);
        self
    }
}

impl Operation for RemoveConstraint {
    fn forward(&self, backend: &dyn Backend) -> Vec<String> {
        vec![backend.drop_constraint_sql(&self.table, &self.name)]
    }

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>> {
        self.constraint
            .as_ref()
            .map(|c| vec![backend.add_constraint_sql(&self.table, c)])
    }

    fn describe(&self) -> String {
        format!("Remove constraint {} from {}", self.name, self.table)
    }

    fn is_reversible(&self) -> bool {
        self.constraint.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Sqlite;
    use crate::field::ReferentialAction;

    #[test]
    fn add_check_constraint() {
        let constraint = Constraint::check("chk_age", "age >= 0");
        let op = AddConstraint::new("users", constraint);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("CHECK"));
        assert!(sql[0].contains("age >= 0"));
    }

    #[test]
    fn add_unique_constraint_generates_index() {
        let constraint = Constraint::unique("uq_email", vec!["email".to_string()]);
        let op = AddConstraint::new("users", constraint);

        let sql = op.forward(&Sqlite);
        assert!(sql[0].contains("UNIQUE INDEX"));
    }

    #[test]
    fn add_constraint_is_reversible() {
        let constraint = Constraint::unique("uq_email", vec!["email".to_string()]);
        let op = AddConstraint::new("users", constraint);

        let reverse = op.backward(&Sqlite).unwrap();
        assert!(reverse[0].contains("DROP INDEX"));
    }

    #[test]
    fn remove_constraint_without_definition_not_reversible() {
        let op = RemoveConstraint::new("users", "uq_email");
        assert!(!op.is_reversible());
    }

    #[test]
    fn remove_constraint_with_definition_is_reversible() {
        let constraint = Constraint::unique("uq_email", vec!["email".to_string()]);
        let op = RemoveConstraint::new("users", "uq_email").with_definition(constraint);

        assert!(op.is_reversible());
    }

    #[test]
    fn foreign_key_constraint() {
        let constraint = Constraint::foreign_key(
            "fk_posts_user",
            vec!["user_id".to_string()],
            "users",
            vec!["id".to_string()],
        )
        .on_delete(ReferentialAction::Cascade)
        .on_update(ReferentialAction::SetNull);

        let op = AddConstraint::new("posts", constraint);
        let sql = op.forward(&Sqlite);

        assert!(sql[0].contains("FOREIGN KEY"));
        assert!(sql[0].contains("REFERENCES"));
        assert!(sql[0].contains("ON DELETE CASCADE"));
        assert!(sql[0].contains("ON UPDATE SET NULL"));
    }

    #[test]
    fn constraint_name_accessor() {
        let check = Constraint::check("chk_age", "age >= 0");
        assert_eq!(check.name(), "chk_age");

        let unique = Constraint::unique("uq_email", vec!["email".to_string()]);
        assert_eq!(unique.name(), "uq_email");

        let fk = Constraint::foreign_key(
            "fk_user",
            vec!["user_id".to_string()],
            "users",
            vec!["id".to_string()],
        );
        assert_eq!(fk.name(), "fk_user");
    }

    #[test]
    fn on_delete_on_non_fk_is_noop() {
        let constraint = Constraint::check("chk", "x > 0").on_delete(ReferentialAction::Cascade);
        if let Constraint::Check { name, .. } = constraint {
            assert_eq!(name, "chk");
        } else {
            panic!("Expected Check constraint");
        }
    }

    #[test]
    fn on_update_on_non_fk_is_noop() {
        let constraint = Constraint::unique("uq", vec![]).on_update(ReferentialAction::Cascade);
        if let Constraint::Unique { name, .. } = constraint {
            assert_eq!(name, "uq");
        } else {
            panic!("Expected Unique constraint");
        }
    }

    #[test]
    fn add_constraint_describe() {
        let constraint = Constraint::check("chk_age", "age >= 0");
        let op = AddConstraint::new("users", constraint);
        assert_eq!(op.describe(), "Add constraint chk_age to users");
    }

    #[test]
    fn remove_constraint_describe() {
        let op = RemoveConstraint::new("users", "chk_age");
        assert_eq!(op.describe(), "Remove constraint chk_age from users");
    }

    #[test]
    fn remove_constraint_backward() {
        let constraint = Constraint::check("chk_age", "age >= 0");
        let op = RemoveConstraint::new("users", "chk_age").with_definition(constraint);

        let backward = op.backward(&Sqlite).unwrap();
        assert!(backward[0].contains("CHECK"));
    }
}
