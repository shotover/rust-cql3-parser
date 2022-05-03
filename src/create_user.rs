use std::fmt::{Display, Formatter};

/// data for the `create user` statement.
#[derive(PartialEq, Debug, Clone)]
pub struct CreateUser {
    /// the user name
    pub name: String,
    /// the password for the user.
    pub password: Option<String>,
    /// if true the `SUPERUSER` option is specified
    pub superuser: bool,
    /// it true the `NOSUPERUSER` option is specified.
    pub no_superuser: bool,
    /// only create if the user does not exist.
    pub if_not_exists: bool,
}

impl Display for CreateUser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut with = String::new();

        if let Some(password) = &self.password {
            with.push_str(" PASSWORD ");
            with.push_str(password);
        }
        if self.superuser {
            with.push_str(" SUPERUSER");
        }
        if self.no_superuser {
            with.push_str(" NOSUPERUSER");
        }
        if with.is_empty() {
            write!(
                f,
                "USER {}{}",
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
                "USER {}{} WITH{}",
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                self.name,
                with
            )
        }
    }
}
