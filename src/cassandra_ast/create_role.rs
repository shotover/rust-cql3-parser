use std::fmt::{Display, Formatter};
use itertools::Itertools;

/// the data for the `create role` statement.
#[derive(PartialEq, Debug, Clone)]
pub struct CreateRole {
    /// the name of the role
    pub name: String,
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

impl Display for CreateRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut with = vec![];

        if self.password.is_some() {
            with.push(format!("PASSWORD = {}", self.password.as_ref().unwrap()));
        }
        if self.superuser.is_some() {
            with.push(format!(
                "SUPERUSER = {}",
                if self.superuser.unwrap() {
                    "TRUE"
                } else {
                    "FALSE"
                }
            ));
        }
        if self.login.is_some() {
            with.push(format!(
                "LOGIN = {}",
                if self.login.unwrap() { "TRUE" } else { "FALSE" }
            ));
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
