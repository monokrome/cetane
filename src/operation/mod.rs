mod constraint;
mod field;
mod index;
mod sql;
mod table;

pub use constraint::{AddConstraint, Constraint, RemoveConstraint};
pub use field::{AddField, AlterField, RemoveField, RenameField};
pub use index::{AddIndex, Index, IndexOrder, RemoveIndex};
pub use sql::RunSql;
pub use table::{CreateTable, DropTable, RenameTable};

use crate::backend::Backend;

pub trait Operation: Send + Sync {
    fn forward(&self, backend: &dyn Backend) -> Vec<String>;

    fn backward(&self, backend: &dyn Backend) -> Option<Vec<String>>;

    fn describe(&self) -> String;

    fn is_reversible(&self) -> bool {
        true
    }
}
