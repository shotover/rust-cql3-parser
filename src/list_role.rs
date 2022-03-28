use std::fmt::{Display, Formatter};

/// https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlListRoles.html
#[derive(PartialEq, Debug, Clone)]
pub struct ListRole {
    /// List roles only for this role.
    pub of: Option<String>,
    /// if true the NORECURSIVE option has been set.
    pub no_recurse: bool,
}

impl Display for ListRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s: String = "".to_string();
        if self.of.is_some() {
            s = " OF ".to_string();
            s.push_str(self.of.as_ref().unwrap().as_str());
        }
        write!(
            f,
            "LIST ROLES{}{}",
            s.as_str(),
            if self.no_recurse { " NORECURSIVE" } else { "" }
        )
    }
}
