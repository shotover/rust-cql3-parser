use crate::common::{FQName, OrderClause, RelationElement};
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
    pub table_name: FQName,
    /// the list of elements to select.
    pub columns: Vec<SelectElement>,
    /// the where clause
    pub where_clause: Vec<RelationElement>,
    /// the optional ordering
    pub order: Option<OrderClause>,
    /// the number of items to return
    pub limit: Option<i32>,
    /// if true ALLOW FILTERING is displayed
    pub filtering: bool,
}

impl Select {
    /// return the column names selected
    /// does not return functions.
    pub fn select_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter_map(|e| {
                if let SelectElement::Column(named) = e {
                    Some(named.to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    /// return the aliased column names.  If the column is not aliased the
    /// base column name is returned.
    /// does not return functions.
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
            if !self.where_clause.is_empty() {
                format!(" WHERE {}", self.where_clause.iter().join(" AND "))
            } else {
                "".to_string()
            },
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
    /// a named column.  May have an alias specified.
    Column(Named),
    /// a named column.  May have an alias specified.
    Function(Named),
}

impl Display for SelectElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectElement::Star => write!(f, "*"),
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
    pub fn alias_or_name(&self) -> &str {
        match &self.alias {
            None => &self.name,
            Some(alias) => alias,
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
