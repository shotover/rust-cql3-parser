use std::fmt::{Display, Formatter};
use itertools::Itertools;
use crate::cassandra_ast::common::DataType;

#[derive(PartialEq, Debug, Clone)]
pub struct Aggregate {
    pub or_replace: bool,
    pub not_exists: bool,
    pub name: String,
    pub data_type: DataType,
    pub sfunc: String,
    pub stype: DataType,
    pub finalfunc: String,
    pub init_cond: InitCondition,
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE {}AGGREGATE {}{} ({}) SFUNC {} STYPE {} FINALFUNC {} INITCOND {}",
            if self.or_replace { "OR REPLACE " } else { "" },
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.data_type,
            self.sfunc,
            self.stype,
            self.finalfunc,
            self.init_cond
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum InitCondition {
    Constant(String),
    List(Vec<InitCondition>),
    Map(Vec<(String, InitCondition)>),
}

impl Display for InitCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InitCondition::Constant(name) => write!(f, "{}", name),
            InitCondition::List(lst) => {
                write!(f, "({})", lst.iter().map(|x| x.to_string()).join(", "))
            }
            InitCondition::Map(entries) => write!(
                f,
                "({})",
                entries
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, v))
                    .join(", ")
            ),
        }
    }
}
