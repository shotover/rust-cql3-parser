use std::fmt::{Display, Formatter};

/// defines the `BEGIN BATCH` data
/// NOTE: It is possible to set bot LOGGED and UNLOGGED however this will yield an
/// unparsable statment.
#[derive(PartialEq, Debug, Clone)]
pub struct BeginBatch {
    /* the logged and unlogged can not be merged into a single statement as one or the other or
    neither may be selected */
    /// if true the `LOGGED` option will be displayed.
    pub logged: bool,
    /// if true the `UNLOGGED` option will be displayed.
    pub unlogged: bool,
    /// the optional timestamp for the `BEGIN BATCH` command
    pub timestamp: Option<u64>,
}

impl Default for BeginBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl BeginBatch {
    pub fn new() -> BeginBatch {
        BeginBatch {
            logged: false,
            unlogged: false,
            timestamp: None,
        }
    }
}

impl Display for BeginBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let modifiers = if self.logged {
            "LOGGED "
        } else if self.unlogged {
            "UNLOGGED "
        } else {
            ""
        };
        if let Some(timestamp) = self.timestamp {
            write!(f, "BEGIN {}BATCH USING TIMESTAMP {} ", modifiers, timestamp)
        } else {
            write!(f, "BEGIN {}BATCH ", modifiers)
        }
    }
}
