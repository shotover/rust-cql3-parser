use crate::alter_column::AlterColumnType;
use crate::common::{ColumnDefinition, FQName};
use itertools::Itertools;
use std::fmt::{Display, Formatter};

/// data for an `AlterType` statement
#[derive(PartialEq, Debug, Clone)]
pub struct AlterType {
    /// the name of the type to alter
    pub name: FQName,
    /// the operation to perform on the type.
    pub operation: AlterTypeOperation,
}

impl Display for AlterType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TYPE {} {}", self.name, self.operation)
    }
}

/// the alter type operations.
#[derive(PartialEq, Debug, Clone)]
pub enum AlterTypeOperation {
    /// Alter the column type
    AlterColumnType(AlterColumnType),
    /// Add a columm
    Add(Vec<ColumnDefinition>),
    /// rename a column
    Rename(Vec<(String, String)>),
}

impl Display for AlterTypeOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTypeOperation::AlterColumnType(column_type) => write!(f, "{}", column_type),
            AlterTypeOperation::Add(columns) => write!(
                f,
                "ADD {}",
                columns.iter().map(|x| x.to_string()).join(", ")
            ),
            AlterTypeOperation::Rename(pairs) => write!(
                f,
                "RENAME {}",
                pairs
                    .iter()
                    .map(|(x, y)| format!("{} TO {}", x, y))
                    .join(" AND ")
            ),
        }
    }
}
