use std::fmt::{Display, Formatter};

/// data for the `CreateTrigger` statement.
#[derive(PartialEq, Debug, Clone)]
pub struct CreateTrigger {
    /// only create if it does not exist.
    pub not_exists: bool,
    /// the name of the trigger.
    pub name: String,
    /// the class the implements the trigger.
    pub class: String,
}

impl Display for CreateTrigger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TRIGGER {}{} USING {}",
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.class
        )
    }
}
