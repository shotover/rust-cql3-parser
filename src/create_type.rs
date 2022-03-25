use crate::common::ColumnDefinition;
use itertools::Itertools;
use std::fmt::{Display, Formatter};

/// The data for a `CREATE TYPE` statement.
#[derive(PartialEq, Debug, Clone)]
pub struct CreateType {
    /// only if the type does not exist.
    pub not_exists: bool,
    /// the name of the type
    pub name: String,
    /// the definition of the type.
    pub columns: Vec<ColumnDefinition>,
}

impl Display for CreateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TYPE {}{} ({})",
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.columns.iter().map(|x| x.to_string()).join(", "),
        )
    }
}
