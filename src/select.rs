use crate::common::{Operand, OrderClause, RelationElement};
use itertools::Itertools;
use std::fmt::{Display, Formatter};

/// data for select statements
#[derive(PartialEq, Debug, Clone)]
pub struct Select {
    /// if true DISTINCT results
    pub distinct: bool,
    /// if true JSON reslts
    pub json: bool,
    /// The table name.
    pub table_name: String,
    /// the list of elements to select.
    pub columns: Vec<SelectElement>,
    /// the where clause
    pub where_clause: Option<Vec<RelationElement>>,
    /// the optional ordering
    pub order: Option<OrderClause>,
    /// the number of items to return
    pub limit: Option<i32>,
    /// if true ALLOW FILTERING is displayed
    pub filtering: bool,
}

impl Select {
    /// return the column names selected
    pub fn select_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|e| match e {
                SelectElement::Star => false,
                SelectElement::DotStar(_) => false,
                SelectElement::Column(_) => true,
                SelectElement::Function(_) => false,
            })
            .map(|e| match e {
                SelectElement::Column(named) => named.to_string(),
                _ => unreachable!(),
            })
            .collect()
    }

    /// return the aliased column names.  If the column is not aliased the
    /// base column name is returned.
    pub fn select_alias(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|e| match e {
                SelectElement::Column(named) => {
                    named.alias.clone().unwrap_or_else(|| named.name.clone())
                }
                _ => "".to_string(),
            })
            .filter(|e| !e.as_str().eq(""))
            .collect()
    }
    /// return the column names from the where clause
    pub fn where_columns(&self) -> Vec<String> {
        match &self.where_clause {
            Some(x) => x
                .iter()
                .map(|e| match &e.obj {
                    Operand::Column(name) => name.clone(),
                    _ => "".to_string(),
                })
                .filter(|e| !e.eq(&""))
                .collect(),
            None => vec![],
        }
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SELECT {}{}{} FROM {}{}{}{}{}",
            if self.distinct { "DISTINCT " } else { "" },
            if self.json { "JSON " } else { "" },
            self.columns.iter().join(", "),
            self.table_name,
            self.where_clause
                .as_ref()
                .map_or("".to_string(), |x| format!(
                    " WHERE {}",
                    x.iter().join(" AND ")
                )),
            self.order
                .as_ref()
                .map_or("".to_string(), |x| format!(" ORDER BY {}", x)),
            self.limit
                .map_or("".to_string(), |x| format!(" LIMIT {}", x)),
            if self.filtering {
                " ALLOW FILTERING"
            } else {
                ""
            }
        )
    }
}

/// the selectable elements for a select statement
#[derive(PartialEq, Debug, Clone)]
pub enum SelectElement {
    /// All of the columns
    Star,
    /// a name followed by a '.*' (e.g. `foo.*`)
    DotStar(String),
    /// a named column.  May have an alias specified.
    Column(Named),
    /// a named column.  May have an alias specified.
    Function(Named),
}

impl Display for SelectElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectElement::Star => write!(f, "*"),
            SelectElement::DotStar(column) => write!(f, "{}.*", column),
            SelectElement::Column(named) | SelectElement::Function(named) => write!(f, "{}", named),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Named {
    pub name: String,
    pub alias: Option<String>,
}

/// the name an optional alias for a named item.
impl Named {
    pub fn alias_or_name(&self) -> String {
        match &self.alias {
            None => self.name.clone(),
            Some(alias) => alias.clone(),
        }
    }
}

impl Display for Named {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.alias {
            None => write!(f, "{}", self.name),
            Some(a) => write!(f, "{} AS {}", self.name, a),
        }
    }
}
