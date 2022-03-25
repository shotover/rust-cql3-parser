use std::fmt::{Display, Formatter};
use itertools::Itertools;

/// The data necessary to create a keyspace.
#[derive(PartialEq, Debug, Clone)]
pub struct CreateKeyspace {
    /// the name of the keyspace
    pub name: String,
    /// replication strategy options.
    pub replication: Vec<(String, String)>,
    /// if specified the DURABLE WRITES option will be output.
    pub durable_writes: Option<bool>,
    /// only create if the keyspace does not exist.
    pub if_not_exists: bool,
}

impl Display for CreateKeyspace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.durable_writes.is_some() {
            write!(
                f,
                "KEYSPACE {}{} WITH REPLICATION = {{{}}} AND DURABLE_WRITES = {}",
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                self.name,
                self.replication
                    .iter()
                    .map(|(x, y)| format!("{}:{}", x, y))
                    .join(", "),
                if self.durable_writes.unwrap() {
                    "TRUE"
                } else {
                    "FALSE"
                }
            )
        } else {
            write!(
                f,
                "KEYSPACE {}{} WITH REPLICATION = {{{}}}",
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                self.name,
                self.replication
                    .iter()
                    .map(|(x, y)| format!("{}:{}", x, y))
                    .join(", ")
            )
        }
    }
}
