use std::fmt::{Display, Formatter};
use itertools::Itertools;
use crate::cassandra_ast::begin_batch::BeginBatch;
use crate::cassandra_ast::common::RelationElement;

/// the data for a delete statement.
#[derive(PartialEq, Debug, Clone)]
pub struct Delete {
    /// if set the statement starts with `BEGIN BATCH`
    pub begin_batch: Option<BeginBatch>,
    /// an optional list of columns to delete
    pub columns: Option<Vec<IndexedColumn>>,
    /// the table to delete from
    pub table_name: String,
    /// an optional timestamp to use for the deletion.
    pub timestamp: Option<u64>,
    /// the were clause for the delete.
    pub where_clause: Vec<RelationElement>,
    /// if present a list of key,values for the `IF` clause
    pub if_clause: Option<Vec<RelationElement>>,
    /// if true and if_clause is NONE then `IF EXISTS` is added
    pub if_exists : bool,
}

impl Display for Delete {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}DELETE {}FROM {}{} WHERE {}{}",
            self.begin_batch
                .as_ref()
                .map_or("".to_string(), |x| x.to_string()),
            self.columns
                .as_ref()
                .map_or("".to_string(), |x| format!("{} ", x.iter().join(", "))),
            self.table_name,
            self.timestamp
                .as_ref()
                .map_or("".to_string(), |x| format!(" USING TIMESTAMP {}", x)),
            self.where_clause.iter().join(" AND "),
            if self.if_clause.is_some() {
                format!(
                    " IF {}",
                    self.if_clause.as_ref().unwrap().iter().join(" AND ")
                )
            } else if self.if_exists {
                " IF EXISTS".to_string()
            } else {
                "".to_string()
            }
        )
    }
}

/// Defines an indexed column.  Indexed columns comprise a column name and an optional index into
/// the column.  This is expressed as `column[idx]`
#[derive(PartialEq, Debug, Clone)]
pub struct IndexedColumn {
    /// the column name
    pub column: String,
    /// the optional index in to the column
    pub idx: Option<String>,
}

impl Display for IndexedColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.idx {
            Some(x) => write!(f, "{}[{}]", self.column, x),
            None => write!(f, "{}", self.column),
        }
    }
}
