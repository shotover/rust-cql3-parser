use std::fmt::{Display, Formatter};

#[derive(PartialEq, Debug, Clone)]
pub enum BatchType {
    Logged,
    Counter,
    Unlogged,
}

/// defines the `BEGIN BATCH` data
#[derive(PartialEq, Debug, Clone)]
pub struct BeginBatch {
    pub ty: BatchType,
    /// the optional timestamp for the `BEGIN BATCH` command
    pub timestamp: Option<u64>,
}

impl Display for BeginBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let modifiers = match self.ty {
            BatchType::Logged => "",
            BatchType::Counter => "COUNTER ",
            BatchType::Unlogged => "UNLOGGED ",
        };

        if let Some(timestamp) = self.timestamp {
            write!(f, "BEGIN {}BATCH USING TIMESTAMP {} ", modifiers, timestamp)
        } else {
            write!(f, "BEGIN {}BATCH ", modifiers)
        }
    }
}
