mod types;

pub use types::FieldType;

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<String>,
    pub references: Option<ForeignKey>,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ReferentialAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl Field {
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
            references: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default = Some(value.into());
        self
    }

    pub fn references(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.references = Some(ForeignKey {
            table: table.into(),
            column: column.into(),
            on_delete: ReferentialAction::default(),
            on_update: ReferentialAction::default(),
        });
        self
    }

    pub fn on_delete(mut self, action: ReferentialAction) -> Self {
        if let Some(ref mut fk) = self.references {
            fk.on_delete = action;
        }
        self
    }

    pub fn on_update(mut self, action: ReferentialAction) -> Self {
        if let Some(ref mut fk) = self.references {
            fk.on_update = action;
        }
        self
    }
}

impl ReferentialAction {
    pub fn as_sql(&self) -> &'static str {
        match self {
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::SetDefault => "SET DEFAULT",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_builder_pattern() {
        let field = Field::new("email", FieldType::Text).not_null().unique();

        assert_eq!(field.name, "email");
        assert!(!field.nullable);
        assert!(field.unique);
        assert!(!field.primary_key);
    }

    #[test]
    fn primary_key_implies_not_null() {
        let field = Field::new("id", FieldType::Serial).primary_key();

        assert!(field.primary_key);
        assert!(!field.nullable);
    }

    #[test]
    fn field_with_default() {
        let field = Field::new("status", FieldType::Text)
            .not_null()
            .default("'active'");

        assert_eq!(field.default, Some("'active'".to_string()));
    }

    #[test]
    fn field_with_foreign_key() {
        let field = Field::new("user_id", FieldType::Integer)
            .references("users", "id")
            .on_delete(ReferentialAction::Cascade);

        let fk = field.references.unwrap();
        assert_eq!(fk.table, "users");
        assert_eq!(fk.column, "id");
        assert_eq!(fk.on_delete, ReferentialAction::Cascade);
    }

    #[test]
    fn field_with_on_update() {
        let field = Field::new("user_id", FieldType::Integer)
            .references("users", "id")
            .on_update(ReferentialAction::Cascade);

        let fk = field.references.unwrap();
        assert_eq!(fk.on_update, ReferentialAction::Cascade);
    }

    #[test]
    fn on_delete_without_references_is_noop() {
        let field = Field::new("x", FieldType::Integer).on_delete(ReferentialAction::Cascade);
        assert!(field.references.is_none());
    }

    #[test]
    fn on_update_without_references_is_noop() {
        let field = Field::new("x", FieldType::Integer).on_update(ReferentialAction::Cascade);
        assert!(field.references.is_none());
    }

    #[test]
    fn referential_action_as_sql() {
        assert_eq!(ReferentialAction::NoAction.as_sql(), "NO ACTION");
        assert_eq!(ReferentialAction::Restrict.as_sql(), "RESTRICT");
        assert_eq!(ReferentialAction::Cascade.as_sql(), "CASCADE");
        assert_eq!(ReferentialAction::SetNull.as_sql(), "SET NULL");
        assert_eq!(ReferentialAction::SetDefault.as_sql(), "SET DEFAULT");
    }

    #[test]
    fn referential_action_default() {
        assert_eq!(ReferentialAction::default(), ReferentialAction::NoAction);
    }
}
