use std::fmt::{Display, Formatter};
use itertools::Itertools;
use crate::cassandra_ast::common::WithItem;

/// The data for an `AlterMaterializedView` statement
#[derive(PartialEq, Debug, Clone)]
pub struct AlterMaterializedView {
    /// the name of the materialzied view.
    pub name: String,
    /// the with options for the view.
    pub with_clause: Vec<WithItem>,
}

impl Display for AlterMaterializedView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ALTER MATERIALIZED VIEW {}{}",
            self.name,
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
