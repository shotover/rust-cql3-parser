use crate::begin_batch::BeginBatch;
use crate::common::{Operand, RelationElement, TtlTimestamp};
use crate::delete::IndexedColumn;
use itertools::Itertools;
use std::fmt::{Display, Formatter};

/// data for `Update` statements
#[derive(PartialEq, Debug, Clone)]
pub struct Update {
    /// if present then statement starts with BEGIN BATCH
    pub begin_batch: Option<BeginBatch>,
    /// the table name to update
    pub table_name: String,
    /// if present then the TTL Timestamp for the update
    pub using_ttl: Option<TtlTimestamp>,
    /// the column assignments for the update.
    pub assignments: Vec<AssignmentElement>,
    /// the where clause
    pub where_clause: Vec<RelationElement>,
    /// if present a list of key,values for the `IF` clause
    pub if_clause: Vec<RelationElement>,
    /// if true and `if_clause` is NONE then  `IF EXISTS` is added to the statement
    pub if_exists: bool,
}

impl Display for Update {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}UPDATE {}{} SET {} WHERE {}{}",
            self.begin_batch
                .as_ref()
                .map_or("".to_string(), |x| x.to_string()),
            self.table_name,
            self.using_ttl
                .as_ref()
                .map_or("".to_string(), |x| x.to_string()),
            self.assignments.iter().map(|a| a.to_string()).join(", "),
            self.where_clause.iter().join(" AND "),
            if !self.if_clause.is_empty() {
                format!(" IF {}", self.if_clause.iter().join(" AND "))
            } else if self.if_exists {
                " IF EXISTS".to_string()
            } else {
                "".to_string()
            }
        )
    }
}

/// defines an assignment element comprising the column, the value, and an optional +/- value operator.
#[derive(PartialEq, Debug, Clone)]
pub struct AssignmentElement {
    /// the column to set the value for.
    pub name: IndexedColumn,
    /// the column value
    pub value: Operand,
    /// an optional +/- value
    pub operator: Option<AssignmentOperator>,
}

impl Display for AssignmentElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.operator {
            Some(x) => write!(f, "{} = {}{}", self.name, self.value, x),
            None => write!(f, "{} = {}", self.name, self.value),
        }
    }
}

/// Defines the optional +/- value for an assignment
#[derive(PartialEq, Debug, Clone)]
pub enum AssignmentOperator {
    Plus(Operand),
    Minus(Operand),
}

impl Display for AssignmentOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AssignmentOperator::Plus(op) => write!(f, " + {}", op),
            AssignmentOperator::Minus(op) => write!(f, " - {}", op),
        }
    }
}
