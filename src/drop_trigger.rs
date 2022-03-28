use std::fmt::{Display, Formatter};

/// The data for a `drop trigger` command
#[derive(PartialEq, Debug, Clone)]
pub struct DropTrigger {
    /// the name of the trigger
    pub name: String,
    /// the table the trigger is associated with.
    pub table: String,
    /// only drop if the trigger exists
    pub if_exists: bool,
}

impl Display for DropTrigger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP TRIGGER{} {} ON {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name,
            self.table
        )
    }
}
