use std::fmt::{Display, Formatter};
use itertools::Itertools;
use crate::cassandra_ast::common::{ColumnDefinition, PrimaryKey, WithItem};

/// The data for a `Create table` statement
#[derive(PartialEq, Debug, Clone)]
pub struct CreateTable {
    /// only create if the table does not exist
    pub if_not_exists: bool,
    /// the name of the table
    pub name: String,
    /// the column definitions.
    pub columns: Vec<ColumnDefinition>,
    /// the primary key if not specified in the column definitions.
    pub key: Option<PrimaryKey>,
    /// the list of `WITH` options.
    pub with_clause: Vec<WithItem>,
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut v: Vec<String> = self.columns.iter().map(|x| x.to_string()).collect();
        if self.key.is_some() {
            v.push(self.key.as_ref().unwrap().to_string());
        }
        write!(
            f,
            "{}{} ({}){}",
            if self.if_not_exists {
                "IF NOT EXISTS ".to_string()
            } else {
                "".to_string()
            },
            self.name,
            v.join(", "),
            if !self.with_clause.is_empty() {
                format!(
                    " WITH {}",
                    self.with_clause.iter().map(|x| x.to_string()).join(" AND ")
                )
            } else {
                "".to_string()
            }
        )
    }
}
