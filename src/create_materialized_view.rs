use crate::common::{FQName, Identifier, PrimaryKey};
use crate::common::{RelationElement, WithItem};
use itertools::Itertools;
use std::fmt::{Display, Formatter};

/// the data to create a materialized view
#[derive(PartialEq, Debug, Clone)]
pub struct CreateMaterializedView {
    /// only create if it does not exist.
    pub if_not_exists: bool,
    /// the name of the materialized view.
    pub name: FQName,
    /// the columns in the view.
    pub columns: Vec<Identifier>,
    /// the table to extract the view from.
    pub table: FQName,
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
            self.columns.iter().map(|c| c.to_string()).join(", "),
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
