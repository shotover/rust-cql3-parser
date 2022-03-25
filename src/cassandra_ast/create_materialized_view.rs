use std::fmt::{Display, Formatter};
use itertools::Itertools;
use crate::cassandra_ast::common::PrimaryKey;
use crate::cassandra_ast::common::{RelationElement, WithItem};

/// the data to create a materialized view
#[derive(PartialEq, Debug, Clone)]
pub struct CreateMaterializedView {
    /// only create if it does not exist.
    pub if_not_exists: bool,
    /// the name of the materialized view.
    pub name: String,
    /// the columns in the view.
    pub columns: Vec<String>,
    /// the table to extract the view from.
    pub table: String,
    /// the where clause to select.  Note: all elements of the primary key must be listed
    /// in the where clause as `column ISNOT NULL`
    pub where_clause: Vec<RelationElement>,
    /// the primary key for the view
    pub key: PrimaryKey,
    /// the with options.
    pub with_clause: Vec<WithItem>,
}

impl Display for CreateMaterializedView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE MATERIALIZED VIEW {}{} AS SELECT {} FROM {} WHERE {} {}{}",
            if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.columns.join(", "),
            self.table,
            self.where_clause.iter().join(" AND "),
            self.key,
            if self.with_clause.is_empty() {
                "".to_string()
            } else {
                format!(
                    " WITH {}",
                    self.with_clause.iter().map(|x| x.to_string()).join(" AND ")
                )
            }
        )
    }
}
