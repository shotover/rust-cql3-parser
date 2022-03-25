use std::fmt::{Display, Formatter};
use itertools::Itertools;
use crate::cassandra_ast::begin_batch::BeginBatch;
use crate::cassandra_ast::common::{Operand, TtlTimestamp};

/// the data for insert statements.
#[derive(PartialEq, Debug, Clone)]
pub struct Insert {
    /// if set the statement starts with `BEGIN BATCH`
    pub begin_batch: Option<BeginBatch>,
    /// the table name
    pub table_name: String,
    /// an the list of of column names to insert into.
    pub columns: Vec<String>,
    /// the `VALUES` to insert
    pub values: InsertValues,
    /// if set the timestamp for `USING TTL`
    pub using_ttl: Option<TtlTimestamp>,
    /// if true then `IF NOT EXISTS` is added to the statement
    pub if_not_exists : bool,
}

impl Display for Insert {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}INSERT INTO {} ({}) {}{}{}",
            self.begin_batch
                .as_ref()
                .map_or("".to_string(), |x| x.to_string()),
            self.table_name,
            self.columns.join(", "),
            self.values,
            if self.if_not_exists {
                " IF NOT EXISTS"
            } else {
                ""
            },
            self.using_ttl
                .as_ref()
                .map_or("".to_string(), |x| x.to_string()),
        )
    }
}

/// The structure that describs the values to insert.
#[derive(PartialEq, Debug, Clone)]
pub enum InsertValues {
    /// this is the standard list of values.
    VALUES(Vec<Operand>),
    /// this option allows JSON string to define the values.
    JSON(String),
}

impl Display for InsertValues {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertValues::VALUES(columns) => {
                write!(f, "VALUES ({})", columns.iter().join(", "))
            }
            InsertValues::JSON(text) => {
                write!(f, "JSON {}", text)
            }
        }
    }
}
