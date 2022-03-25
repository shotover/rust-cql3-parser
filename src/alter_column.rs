use crate::common::DataType;
use std::fmt::{Display, Formatter};

/// data to alter a column type.
#[derive(PartialEq, Debug, Clone)]
pub struct AlterColumnType {
    /// the name of the column
    pub name: String,
    /// the data type to set the colum to.
    pub data_type: DataType,
}

impl Display for AlterColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER {} TYPE {}", self.name, self.data_type)
    }
}
