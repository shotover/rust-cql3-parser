use crate::common::Identifier;
use itertools::Itertools;
use std::fmt::{Display, Formatter};

/// the data for the `create role` statement.
#[derive(PartialEq, Debug, Clone)]
pub struct RoleCommon {
    /// the name of the role
    pub name: Identifier,
    /// if specified the password for the role
    pub password: Option<String>,
    /// if specified then the user is explicitly noted as `SUPERUER` or `NOSUPERUSER`
    pub superuser: Option<bool>,
    /// if specified the user LOGIN option is specified
    pub login: Option<bool>,
    /// the list of options for an external authenticator.
    pub options: Vec<(String, String)>,
    /// only create the role if it does not exist.
    pub if_not_exists: bool,
}

impl Display for RoleCommon {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut with = vec![];

        if let Some(password) = &self.password {
            with.push(format!("PASSWORD = {}", password));
        }
        if let Some(superuser) = self.superuser {
            with.push(format!(
                "SUPERUSER = {}",
                if superuser { "TRUE" } else { "FALSE" }
            ));
        }
        if let Some(login) = self.login {
            with.push(format!("LOGIN = {}", if login { "TRUE" } else { "FALSE" }));
        }
        if !self.options.is_empty() {
            let mut txt = "OPTIONS = {".to_string();
            txt.push_str(
                self.options
                    .iter()
                    .map(|(x, y)| format!("{}:{}", x, y))
                    .join(", ")
                    .as_str(),
            );
            txt.push('}');
            with.push(txt.to_string());
        }
        if with.is_empty() {
            write!(
                f,
                "ROLE {}{}",
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                self.name
            )
        } else {
            write!(
                f,
                "ROLE {}{} WITH {}",
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                self.name,
                with.iter().join(" AND ")
            )
        }
    }
}
