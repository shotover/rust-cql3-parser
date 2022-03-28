use crate::common::ColumnDefinition;
use crate::common::DataType;
use itertools::Itertools;
use std::fmt::{Display, Formatter};

/// Data for the create function statement
#[derive(PartialEq, Debug, Clone)]
pub struct CreateFunction {
    /// if specified the 'OR REPLACE' clause will be added.
    pub or_replace: bool,
    /// if specified the 'NOT EXISTS' clause will be added.
    pub not_exists: bool,
    /// the name of the function.
    pub name: String,
    /// the parameters for the function.
    pub params: Vec<ColumnDefinition>,
    /// if set the function should return `NULL`` when called with `NULL`` otherwise
    /// the function should process the input.
    pub return_null: bool,
    /// the data type the function returns.
    pub return_type: DataType,
    /// the language the function is written in.
    pub language: String,
    /// the code block containing the function
    pub code_block: String,
}

impl Display for CreateFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE {}FUNCTION {}{} ({}) {} ON NULL INPUT RETURNS {} LANGUAGE {} AS {}",
            if self.or_replace { "OR REPLACE " } else { "" },
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.params.iter().map(|x| x.to_string()).join(", "),
            if self.return_null {
                "RETURNS NULL"
            } else {
                "CALLED"
            },
            self.return_type,
            self.language,
            self.code_block
        )
    }
}
