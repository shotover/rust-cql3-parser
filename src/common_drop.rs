use crate::common::FQName;

/// the data for many `Drop` commands
#[derive(PartialEq, Debug, Clone)]
pub struct CommonDrop {
    /// the name of the thing being dropped.
    pub name: FQName,
    /// only drop if th thing exists.
    pub if_exists: bool,
}

impl CommonDrop {
    pub fn get_text(&self, type_: &str) -> String {
        format!(
            "DROP {}{} {}",
            type_,
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name
        )
    }
}
