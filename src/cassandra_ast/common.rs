use std::fmt::{Display, Formatter};
use itertools::Itertools;

/// A column definition.
/// This is used in many places, however the primary_key value should only be used in
/// the `create table` calls.  In all other cases it will yield an invalid statment.
#[derive(PartialEq, Debug, Clone)]
pub struct ColumnDefinition {
    /// the name of the column
    pub name: String,
    /// the data type for the column
    pub data_type: DataType,
    /// if set this column is the primary key.
    pub primary_key: bool,
}

impl Display for ColumnDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}{}",
            self.name,
            self.data_type,
            if self.primary_key { " PRIMARY KEY" } else { "" }
        )
    }
}

/// the definition of a data type
#[derive(PartialEq, Debug, Clone)]
pub struct DataType {
    /// the name of the data type.
    pub name: DataTypeName,
    /// the definition of the data type.  Normally this is empty but may contain data types that
    /// comprise the named type. (e.g. `FROZEN<foo>` will have foo in the definition)
    pub definition: Vec<DataTypeName>,
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.definition.is_empty() {
            write!(f, "{}", self.name)
        } else {
            write!(f, "{}<{}>", self.name, self.definition.iter().join(", "))
        }
    }
}

/// An enumeration of data types.
#[derive(PartialEq, Debug, Clone)]
pub enum DataTypeName {
    TIMESTAMP,
    SET,
    ASCII,
    BIGINT,
    BLOB,
    BOOLEAN,
    COUNTER,
    DATE,
    DECIMAL,
    DOUBLE,
    FLOAT,
    FROZEN,
    INET,
    INT,
    LIST,
    MAP,
    SMALLINT,
    TEXT,
    TIME,
    TIMEUUID,
    TINYINT,
    TUPLE,
    VARCHAR,
    VARINT,
    UUID,
    /// defines a custom type.  Where the name is the name of the type.
    CUSTOM(String),
}

impl Display for DataTypeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeName::TIMESTAMP => write!(f, "TIMESTAMP"),
            DataTypeName::SET => write!(f, "SET"),
            DataTypeName::ASCII => write!(f, "ASCII"),
            DataTypeName::BIGINT => write!(f, "BIGINT"),
            DataTypeName::BLOB => write!(f, "BLOB"),
            DataTypeName::BOOLEAN => write!(f, "BOOLEAN"),
            DataTypeName::COUNTER => write!(f, "COUNTER"),
            DataTypeName::DATE => write!(f, "DATE"),
            DataTypeName::DECIMAL => write!(f, "DECIMAL"),
            DataTypeName::DOUBLE => write!(f, "DOUBLE"),
            DataTypeName::FLOAT => write!(f, "FLOAT"),
            DataTypeName::FROZEN => write!(f, "FROZEN"),
            DataTypeName::INET => write!(f, "INET"),
            DataTypeName::INT => write!(f, "INT"),
            DataTypeName::LIST => write!(f, "LIST"),
            DataTypeName::MAP => write!(f, "MAP"),
            DataTypeName::SMALLINT => write!(f, "SMALLINT"),
            DataTypeName::TEXT => write!(f, "TEXT"),
            DataTypeName::TIME => write!(f, "TIME"),
            DataTypeName::TIMEUUID => write!(f, "TIMEUUID"),
            DataTypeName::TINYINT => write!(f, "TINYINT"),
            DataTypeName::TUPLE => write!(f, "TUPLE"),
            DataTypeName::VARCHAR => write!(f, "VARCHAR"),
            DataTypeName::VARINT => write!(f, "VARINT"),
            DataTypeName::UUID => write!(f, "UUID"),
            DataTypeName::CUSTOM(name) => write!(f, "{}", name),
        }
    }
}

impl DataTypeName {
    pub fn from(name: &str) -> DataTypeName {
        match name.to_uppercase().as_str() {
            "ASCII" => DataTypeName::ASCII,
            "BIGINT" => DataTypeName::BIGINT,
            "BLOB" => DataTypeName::BLOB,
            "BOOLEAN" => DataTypeName::BOOLEAN,
            "COUNTER" => DataTypeName::COUNTER,
            "DATE" => DataTypeName::DATE,
            "DECIMAL" => DataTypeName::DECIMAL,
            "DOUBLE" => DataTypeName::DOUBLE,
            "FLOAT" => DataTypeName::FLOAT,
            "FROZEN" => DataTypeName::FROZEN,
            "INET" => DataTypeName::INET,
            "INT" => DataTypeName::INT,
            "LIST" => DataTypeName::LIST,
            "MAP" => DataTypeName::MAP,
            "SET" => DataTypeName::SET,
            "SMALLINT" => DataTypeName::SMALLINT,
            "TEXT" => DataTypeName::TEXT,
            "TIME" => DataTypeName::TIME,
            "TIMESTAMP" => DataTypeName::TIMESTAMP,
            "TIMEUUID" => DataTypeName::TIMEUUID,
            "TINYINT" => DataTypeName::TINYINT,
            "TUPLE" => DataTypeName::TUPLE,
            "UUID" => DataTypeName::UUID,
            "VARCHAR" => DataTypeName::VARCHAR,
            "VARINT" => DataTypeName::VARINT,
            _ => DataTypeName::CUSTOM(name.to_string()),
        }
    }
}

/// An object that can be on either side of an `Operator`
#[derive(PartialEq, Debug, Clone)]
pub enum Operand {
    /// A constant
    CONST(String),
    /// a map displays as `{ String:String, String:String, ... }`
    MAP(Vec<(String, String)>),
    /// a set of values.  Displays as `( String, String, ...)`
    SET(Vec<String>),
    /// a list of values.  Displays as `[String, String, ...]`
    LIST(Vec<String>),
    /// a tuple of values.  Displays as `{ Operand, Operand, ... }`
    TUPLE(Vec<Operand>),
    /// A column name
    COLUMN(String),
    /// A function name
    FUNC(String),
    /// the `NULL` value.
    NULL,
}

impl Display for Operand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operand::COLUMN(text) | Operand::FUNC(text) | Operand::CONST(text) => {
                write!(f, "{}", text)
            }
            Operand::MAP(entries) => {
                let mut result = String::from('{');
                result.push_str(
                    entries
                        .iter()
                        .map(|(x, y)| format!("{}:{}", x, y))
                        .join(", ")
                        .as_str(),
                );
                result.push('}');
                write!(f, "{}", result)
            }
            Operand::SET(values) => {
                let mut result = String::from('{');
                result.push_str(values.iter().join(", ").as_str());
                result.push('}');
                write!(f, "{}", result)
            }
            Operand::LIST(values) => {
                let mut result = String::from('[');
                result.push_str(values.iter().join(", ").as_str());
                result.push(']');
                write!(f, "{}", result)
            }
            Operand::TUPLE(values) => {
                let mut result = String::from('(');
                result.push_str(values.iter().join(", ").as_str());
                result.push(')');
                write!(f, "{}", result)
            }
            Operand::NULL => write!(f, "NULL"),
        }
    }
}

/// data item used in `Grant`, `ListPermissions` and `Revoke` statements.
#[derive(PartialEq, Debug, Clone)]
pub struct PrivilegeData {
    /// the privilege that is being manipulated
    pub privilege: Privilege,
    /// the resource on which the permission is applied
    pub resource: Option<Resource>,
    /// the role name that tis being modified.
    pub role: Option<String>,
}

/// the list of privileges recognized by the system.
#[derive(PartialEq, Debug, Clone)]
pub enum Privilege {
    ALL,
    ALTER,
    AUTHORIZE,
    DESCRIBE,
    EXECUTE,
    CREATE,
    DROP,
    MODIFY,
    SELECT,
}

impl Display for Privilege {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Privilege::ALL => write!(f, "ALL PERMISSIONS"),
            Privilege::ALTER => write!(f, "ALTER"),
            Privilege::AUTHORIZE => write!(f, "AUTHORIZE"),
            Privilege::DESCRIBE => write!(f, "DESCRIBE"),
            Privilege::EXECUTE => write!(f, "EXECUTE"),
            Privilege::CREATE => write!(f, "CREATE"),
            Privilege::DROP => write!(f, "DROP"),
            Privilege::MODIFY => write!(f, "MODIFY"),
            Privilege::SELECT => write!(f, "SELECT"),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct RelationElement {
    /// the column, function or column list on the left side
    pub obj: Operand,
    /// the relational operator
    pub oper: RelationOperator,
    /// the value, func, argument list, tuple list or tuple
    pub value: Vec<Operand>,
}

impl RelationElement {
    pub fn first_value(&self) -> &Operand {
        self.value.get(0).unwrap()
    }
}

impl Display for RelationElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.obj,
            self.oper,
            self.value.iter().join(", ")
        )
    }
}

impl RelationOperator {
    /// evaluates the expression for any PartialOrd implementation
    pub fn eval<T>(&self, left: &T, right: &T) -> bool
    where
        T: PartialOrd,
    {
        match self {
            RelationOperator::LessThan => left.lt(right),
            RelationOperator::LessThanOrEqual => left.le(right),
            RelationOperator::Equal => left.eq(right),
            RelationOperator::NotEqual => !left.eq(right),
            RelationOperator::GreaterThanOrEqual => left.ge(right),
            RelationOperator::GreaterThan => left.gt(right),
            RelationOperator::In => false,
            RelationOperator::Contains => false,
            RelationOperator::ContainsKey => false,
            RelationOperator::IsNot => false,
        }
    }
}

/// A relation operator used in `WHERE` and `IF` clauses.
#[derive(PartialEq, Debug, Clone)]
pub enum RelationOperator {
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
    GreaterThanOrEqual,
    GreaterThan,
    In,
    Contains,
    ContainsKey,
    /// this is not used in normal cases it is used in the MaterializedView to specify
    /// a collumn that must not be null.
    IsNot,
}

impl Display for RelationOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RelationOperator::LessThan => write!(f, "<"),
            RelationOperator::LessThanOrEqual => write!(f, "<="),
            RelationOperator::Equal => write!(f, "="),
            RelationOperator::NotEqual => write!(f, "<>"),
            RelationOperator::GreaterThanOrEqual => write!(f, ">="),
            RelationOperator::GreaterThan => write!(f, ">"),
            RelationOperator::In => write!(f, "IN"),
            RelationOperator::Contains => write!(f, "CONTAINS"),
            RelationOperator::ContainsKey => write!(f, "CONTAINS KEY"),
            RelationOperator::IsNot => write!(f, "IS NOT"),
        }
    }
}

/// the structure of the TTL / Timestamp option.
#[derive(PartialEq, Debug, Clone)]
pub struct TtlTimestamp {
    /// the optional time-to-live value
    pub ttl: Option<u64>,
    /// the optional timestamp value
    pub timestamp: Option<u64>,
}

impl Display for TtlTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let tl = match self.ttl {
            Some(t) => format!("TTL {}", t),
            _ => "".to_string(),
        };

        let tm = match self.timestamp {
            Some(t) => format!("TIMESTAMP {}", t),
            _ => "".to_string(),
        };

        if self.ttl.is_some() && self.timestamp.is_some() {
            write!(f, " USING {} AND {}", tl, tm)
        } else {
            write!(f, " USING {}", if self.ttl.is_some() { tl } else { tm })
        }
    }
}

/// The definition of the items in a WithElement
#[derive(PartialEq, Debug, Clone)]
pub enum WithItem {
    /// an option comprising the key (name) and the value for the option.
    Option { key: String, value: OptionValue },
    /// A clustering order clause.
    ClusterOrder(OrderClause),
    /// the ID the ID for the table/view.
    ID(String),
    /// use compact storage.
    CompactStorage,
}

impl Display for WithItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WithItem::Option { key, value } => write!(f, "{} = {}", key, value),
            WithItem::ClusterOrder(order) => write!(f, "CLUSTERING ORDER BY ({})", order),
            WithItem::ID(txt) => write!(f, "ID = {}", txt),
            WithItem::CompactStorage => write!(f, "COMPACT STORAGE"),
        }
    }
}

/// the order clause
#[derive(PartialEq, Debug, Clone)]
pub struct OrderClause {
    /// the column to order by.
    pub name: String,
    /// if `true` then the order is descending,
    pub desc: bool,
}

impl Display for OrderClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            self.name,
            if self.desc { "DESC" } else { "ASC" }
        )
    }
}

/// the definition of an option value, is either literal string or a map of Key,value pairs.
#[derive(PartialEq, Debug, Clone)]
pub enum OptionValue {
    Literal(String),
    Map(Vec<(String, String)>),
}

impl Display for OptionValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OptionValue::Literal(txt) => write!(f, "{}", txt),
            OptionValue::Map(items) => write!(
                f,
                "{{{}}}",
                items.iter().map(|(x, y)| format!("{}:{}", x, y)).join(", ")
            ),
        }
    }
}

/// The definition of a primary key.
/// There must be at least one column specified in the partition.
#[derive(PartialEq, Debug, Clone)]
pub struct PrimaryKey {
    pub partition: Vec<String>,
    pub clustering: Vec<String>,
}

impl Display for PrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.partition.is_empty() && self.clustering.is_empty() {
            write!(f, "")
        } else if self.partition.len() == 1 {
            if self.clustering.is_empty() {
                write!(f, "PRIMARY KEY ({})", self.partition.get(0).unwrap())
            } else {
                write!(
                    f,
                    "PRIMARY KEY ({}, {})",
                    self.partition.get(0).unwrap(),
                    self.clustering.join(", ")
                )
            }
        } else {
            write!(
                f,
                "PRIMARY KEY (({}), {})",
                self.partition.join(", "),
                self.clustering.join(", ")
            )
        }
    }
}

/// A list of resource types recognized by the system
#[derive(PartialEq, Debug, Clone)]
pub enum Resource {
    /// all the functins optionally within a keyspace
    AllFunctions(Option<String>),
    /// all the keyspaces
    AllKeyspaces,
    /// all the roles
    AllRoles,
    /// the specific function.
    Function(String),
    /// the specific keyspace
    Keyspace(String),
    /// the specified role.
    Role(String),
    /// the specified table.
    Table(String),
}

impl Display for Resource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Resource::AllFunctions(str) => {
                if str.is_some() {
                    write!(f, "ALL FUNCTIONS IN KEYSPACE {}", str.as_ref().unwrap())
                } else {
                    write!(f, "ALL FUNCTIONS")
                }
            }
            Resource::AllKeyspaces => write!(f, "ALL KEYSPACES"),
            Resource::AllRoles => write!(f, "ALL ROLES"),
            Resource::Function(func) => write!(f, "FUNCTION {}", func),
            Resource::Keyspace(keyspace) => write!(f, "KEYSPACE {}", keyspace),
            Resource::Role(role) => write!(f, "ROLE {}", role),
            Resource::Table(table) => write!(f, "TABLE {}", table),
        }
    }
}
