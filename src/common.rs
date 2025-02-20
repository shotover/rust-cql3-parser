use bigdecimal::BigDecimal;
use bytes::Bytes;
use hex;
use itertools::Itertools;
use num_bigint::BigInt;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use uuid::Uuid;

/// A column definition.
/// This is used in many places, however the primary_key value should only be used in
/// the `create table` calls.  In all other cases it will yield an invalid statement.
#[derive(PartialEq, Debug, Clone)]
pub struct ColumnDefinition {
    /// the name of the column
    pub name: Identifier,
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
    Timestamp,
    Set,
    Ascii,
    BigInt,
    Blob,
    Boolean,
    Counter,
    Date,
    Decimal,
    Double,
    Float,
    Frozen,
    Inet,
    Int,
    List,
    Map,
    SmallInt,
    Text,
    Time,
    TimeUuid,
    TinyInt,
    Tuple,
    VarChar,
    VarInt,
    Uuid,
    /// defines a custom type.  Where the name is the name of the type.
    Custom(String),
}

impl Display for DataTypeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeName::Timestamp => write!(f, "TIMESTAMP"),
            DataTypeName::Set => write!(f, "SET"),
            DataTypeName::Ascii => write!(f, "ASCII"),
            DataTypeName::BigInt => write!(f, "BIGINT"),
            DataTypeName::Blob => write!(f, "BLOB"),
            DataTypeName::Boolean => write!(f, "BOOLEAN"),
            DataTypeName::Counter => write!(f, "COUNTER"),
            DataTypeName::Date => write!(f, "DATE"),
            DataTypeName::Decimal => write!(f, "DECIMAL"),
            DataTypeName::Double => write!(f, "DOUBLE"),
            DataTypeName::Float => write!(f, "FLOAT"),
            DataTypeName::Frozen => write!(f, "FROZEN"),
            DataTypeName::Inet => write!(f, "INET"),
            DataTypeName::Int => write!(f, "INT"),
            DataTypeName::List => write!(f, "LIST"),
            DataTypeName::Map => write!(f, "MAP"),
            DataTypeName::SmallInt => write!(f, "SMALLINT"),
            DataTypeName::Text => write!(f, "TEXT"),
            DataTypeName::Time => write!(f, "TIME"),
            DataTypeName::TimeUuid => write!(f, "TIMEUUID"),
            DataTypeName::TinyInt => write!(f, "TINYINT"),
            DataTypeName::Tuple => write!(f, "TUPLE"),
            DataTypeName::VarChar => write!(f, "VARCHAR"),
            DataTypeName::VarInt => write!(f, "VARINT"),
            DataTypeName::Uuid => write!(f, "UUID"),
            DataTypeName::Custom(name) => write!(f, "{}", name),
        }
    }
}

impl DataTypeName {
    pub fn from(name: &str) -> DataTypeName {
        match name.to_uppercase().as_str() {
            "ASCII" => DataTypeName::Ascii,
            "BIGINT" => DataTypeName::BigInt,
            "BLOB" => DataTypeName::Blob,
            "BOOLEAN" => DataTypeName::Boolean,
            "COUNTER" => DataTypeName::Counter,
            "DATE" => DataTypeName::Date,
            "DECIMAL" => DataTypeName::Decimal,
            "DOUBLE" => DataTypeName::Double,
            "FLOAT" => DataTypeName::Float,
            "FROZEN" => DataTypeName::Frozen,
            "INET" => DataTypeName::Inet,
            "INT" => DataTypeName::Int,
            "LIST" => DataTypeName::List,
            "MAP" => DataTypeName::Map,
            "SET" => DataTypeName::Set,
            "SMALLINT" => DataTypeName::SmallInt,
            "TEXT" => DataTypeName::Text,
            "TIME" => DataTypeName::Time,
            "TIMESTAMP" => DataTypeName::Timestamp,
            "TIMEUUID" => DataTypeName::TimeUuid,
            "TINYINT" => DataTypeName::TinyInt,
            "TUPLE" => DataTypeName::Tuple,
            "UUID" => DataTypeName::Uuid,
            "VARCHAR" => DataTypeName::VarChar,
            "VARINT" => DataTypeName::VarInt,
            _ => DataTypeName::Custom(name.to_string()),
        }
    }
}

/// An object that can be on either side of an `Operator`
#[derive(PartialEq, Debug, Clone, Eq, Ord, PartialOrd)]
pub enum Operand {
    /// A constant
    Const(String),
    /// a map displays as `{ String:String, String:String, ... }`
    Map(Vec<(String, String)>),
    /// a set of values.  Displays as `( String, String, ...)`
    Set(Vec<String>),
    /// a list of values.  Displays as `[String, String, ...]`
    List(Vec<String>),
    /// a tuple of values.  Displays as `{ Operand, Operand, ... }`
    Tuple(Vec<Operand>),
    /// A column name
    Column(Identifier),
    /// A function call e.g. foo(bar)
    Func(String),
    /// A parameter.  The string will either be '?' or ':name'
    Param(String),
    /// the `NULL` value.
    Null,
}

/// this is _NOT_ the same as `Operand::Const(string)`  This conversion encloses the value in
/// single quotes.
impl From<&str> for Operand {
    fn from(txt: &str) -> Self {
        Operand::Const(format!("'{}'", txt))
    }
}

impl From<&Bytes> for Operand {
    fn from(b: &Bytes) -> Self {
        Operand::from_hex(&hex::encode(b))
    }
}

impl From<&bool> for Operand {
    fn from(b: &bool) -> Self {
        Operand::Const(if *b {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        })
    }
}

impl From<&u128> for Operand {
    fn from(i: &u128) -> Self {
        Operand::Const(i.to_string())
    }
}
impl From<&u64> for Operand {
    fn from(i: &u64) -> Self {
        Operand::Const(i.to_string())
    }
}
impl From<&u32> for Operand {
    fn from(i: &u32) -> Self {
        Operand::Const(i.to_string())
    }
}

impl From<&u16> for Operand {
    fn from(i: &u16) -> Self {
        Operand::Const(i.to_string())
    }
}

impl From<&u8> for Operand {
    fn from(i: &u8) -> Self {
        Operand::Const(i.to_string())
    }
}
impl From<&i128> for Operand {
    fn from(i: &i128) -> Self {
        Operand::Const(i.to_string())
    }
}

impl From<&i64> for Operand {
    fn from(i: &i64) -> Self {
        Operand::Const(i.to_string())
    }
}
impl From<&i32> for Operand {
    fn from(i: &i32) -> Self {
        Operand::Const(i.to_string())
    }
}

impl From<&i16> for Operand {
    fn from(i: &i16) -> Self {
        Operand::Const(i.to_string())
    }
}

impl From<&i8> for Operand {
    fn from(i: &i8) -> Self {
        Operand::Const(i.to_string())
    }
}

impl From<&f64> for Operand {
    fn from(i: &f64) -> Self {
        Operand::Const(i.to_string())
    }
}
impl From<&f32> for Operand {
    fn from(i: &f32) -> Self {
        Operand::Const(i.to_string())
    }
}

impl From<&BigInt> for Operand {
    fn from(b: &BigInt) -> Self {
        Operand::Const(b.to_string())
    }
}

impl From<&BigDecimal> for Operand {
    fn from(b: &BigDecimal) -> Self {
        Operand::Const(b.to_string())
    }
}

impl From<&IpAddr> for Operand {
    fn from(addr: &IpAddr) -> Self {
        Operand::from(addr.to_string().as_str())
    }
}

impl From<&Uuid> for Operand {
    fn from(uuid: &Uuid) -> Self {
        Operand::from(uuid.to_string().as_str())
    }
}

impl Operand {
    /// creates creates a properly formatted Operand::Const for a hex string.
    fn from_hex(hex_str: &str) -> Operand {
        Operand::Const(format!("0x{}", hex_str))
    }

    /// unescapes a CQL string
    /// Specifically converts `''` to `'` and removes the leading and
    /// trailing delimiters.  For all other strings this is method returns
    /// the argument.  Valid delimiters are `'` and `$$`
    pub fn unescape(value: &str) -> String {
        if value.starts_with('\'') {
            let mut chars = value.chars();
            chars.next();
            chars.next_back();
            chars.as_str().replace("''", "'")
        } else if value.starts_with("$$") {
            /* to convert to a VarChar type we have to strip the delimiters off the front and back
            of the string.  Soe remove one char (front and back) in the case of `'` and two in the case of `$$`
             */
            let mut chars = value.chars();
            chars.next();
            chars.next();
            chars.next_back();
            chars.next_back();
            chars.as_str().to_string()
        } else {
            value.to_string()
        }
    }

    /// creates an Operand::Const from an unquoted string.
    /// if the string contains a `'` it will be quoted by the `$$` pattern.  if it contains `$$` and `'`
    /// it will be quoted by the `'` pattern and all existing `'` will be replaced with `''` (two single quotes).
    pub fn escape(txt: &str) -> Operand {
        if txt.contains('\'') {
            if txt.contains("$$") {
                Operand::Const(format!("'{}'", txt.replace('\'', "''")))
            } else {
                Operand::Const(format!("$${}$$", txt))
            }
        } else {
            Operand::Const(txt.to_string())
        }
    }
}

impl Display for Operand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operand::Column(id) => {
                write!(f, "{}", id)
            }
            Operand::Const(text) | Operand::Param(text) | Operand::Func(text) => {
                write!(f, "{}", text)
            }
            Operand::Map(entries) => {
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
            Operand::Set(values) => {
                let mut result = String::from('{');
                result.push_str(values.iter().join(", ").as_str());
                result.push('}');
                write!(f, "{}", result)
            }
            Operand::List(values) => {
                let mut result = String::from('[');
                result.push_str(values.iter().join(", ").as_str());
                result.push(']');
                write!(f, "{}", result)
            }
            Operand::Tuple(values) => {
                let mut result = String::from('(');
                result.push_str(values.iter().join(", ").as_str());
                result.push(')');
                write!(f, "{}", result)
            }
            Operand::Null => write!(f, "NULL"),
        }
    }
}

/// data item used in `Grant`, `ListPermissions` and `Revoke` statements.
#[derive(PartialEq, Debug, Clone)]
pub struct Privilege {
    /// the privilege that is being manipulated
    pub privilege: PrivilegeType,
    /// the resource on which the permission is applied
    pub resource: Option<Resource>,
    /// the role name that tis being modified.
    pub role: Option<String>,
}

/// the list of privileges recognized by the system.
#[derive(PartialEq, Debug, Clone)]
pub enum PrivilegeType {
    All,
    Alter,
    Authorize,
    Describe,
    Execute,
    Create,
    Drop,
    Modify,
    Select,
}

impl Display for PrivilegeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PrivilegeType::All => write!(f, "ALL PERMISSIONS"),
            PrivilegeType::Alter => write!(f, "ALTER"),
            PrivilegeType::Authorize => write!(f, "AUTHORIZE"),
            PrivilegeType::Describe => write!(f, "DESCRIBE"),
            PrivilegeType::Execute => write!(f, "EXECUTE"),
            PrivilegeType::Create => write!(f, "CREATE"),
            PrivilegeType::Drop => write!(f, "DROP"),
            PrivilegeType::Modify => write!(f, "MODIFY"),
            PrivilegeType::Select => write!(f, "SELECT"),
        }
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Ord, PartialOrd)]
pub struct RelationElement {
    /// the column, function or column list on the left side
    pub obj: Operand,
    /// the relational operator
    pub oper: RelationOperator,
    /// the value, func, argument list, tuple list or tuple
    pub value: Operand,
}

impl Display for RelationElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.obj, self.oper, self.value)
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
#[derive(PartialEq, Debug, Clone, Eq, PartialOrd, Ord)]
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
    pub name: Identifier,
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
    pub partition: Vec<Identifier>,
    pub clustering: Vec<Identifier>,
}

impl Display for PrimaryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.partition.is_empty() && self.clustering.is_empty() {
            write!(f, "")
        } else if self.partition.len() == 1 {
            if self.clustering.is_empty() {
                write!(f, "PRIMARY KEY ({})", self.partition[0])
            } else {
                write!(
                    f,
                    "PRIMARY KEY ({}, {})",
                    self.partition[0],
                    self.clustering.iter().map(|c| c.to_string()).join(", ")
                )
            }
        } else {
            write!(
                f,
                "PRIMARY KEY (({}), {})",
                self.partition.iter().map(|c| c.to_string()).join(", "),
                self.clustering.iter().map(|c| c.to_string()).join(", ")
            )
        }
    }
}

/// A list of resource types recognized by the system
#[derive(PartialEq, Debug, Clone)]
pub enum Resource {
    /// all the functions optionally within a keyspace
    AllFunctions(Option<String>),
    /// all the keyspaces
    AllKeyspaces,
    /// all the roles
    AllRoles,
    /// the specific function.
    Function(FQName),
    /// the specific keyspace
    Keyspace(Identifier),
    /// the specified role.
    Role(String),
    /// the specified table.
    Table(FQName),
}

impl Display for Resource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Resource::AllFunctions(str) => {
                if let Some(str) = str {
                    write!(f, "ALL FUNCTIONS IN KEYSPACE {}", str)
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

pub struct WhereClause {}
impl WhereClause {
    /// return a map of column names to relation elements
    pub fn get_column_relation_element_map(
        where_clause: &[RelationElement],
    ) -> BTreeMap<Identifier, Vec<RelationElement>> {
        let mut result: BTreeMap<Identifier, Vec<RelationElement>> = BTreeMap::new();

        for relation_element in where_clause {
            if let Operand::Column(key) = &relation_element.obj {
                if let Some(value) = result.get_mut(key) {
                    value.push(relation_element.clone());
                } else {
                    result.insert(key.clone(), vec![relation_element.clone()]);
                }
            }
        }

        result
    }

    /// get the unordered set of column names for found in the where clause
    pub fn get_column_list(where_clause: Vec<RelationElement>) -> HashSet<Identifier> {
        where_clause
            .into_iter()
            .filter_map(|relation_element| match relation_element.obj {
                Operand::Column(name) => Some(name),
                _ => None,
            })
            .collect()
    }
}

/// a fully qualified name.
#[derive(PartialEq, Debug, Clone, Hash, Eq)]
pub struct FQName {
    pub keyspace: Option<Identifier>,
    pub name: Identifier,
}

impl FQName {
    /// parses the FQName from a string.  Breaks the string at the first dot (`.`) and makes the left
    /// string the keyspace and the second string the name. If no dot is present the entire string
    /// is the name.
    pub fn parse(txt: &str) -> FQName {
        let parts = txt.split('.').collect_vec();
        if parts.len() > 1 {
            FQName::new(parts[0], parts[1])
        } else {
            FQName::simple(txt)
        }
    }

    pub fn simple(name: &str) -> FQName {
        FQName {
            keyspace: None,
            name: Identifier::parse(name),
        }
    }

    pub fn new(keyspace: &str, name: &str) -> FQName {
        FQName {
            keyspace: Some(Identifier::parse(keyspace)),
            name: Identifier::parse(name),
        }
    }
}

impl Display for FQName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(keyspace) = &self.keyspace {
            write!(f, "{}.{}", keyspace, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

impl From<&FQName> for std::string::String {
    fn from(fqname: &FQName) -> Self {
        fqname.to_string()
    }
}

impl From<FQName> for std::string::String {
    fn from(fqname: FQName) -> Self {
        fqname.to_string()
    }
}

#[derive(Debug, PartialEq)]
pub struct FQNameRef<'a> {
    pub keyspace: Option<IdentifierRef<'a>>,
    pub name: IdentifierRef<'a>,
}

impl PartialEq<FQName> for FQNameRef<'_> {
    fn eq(&self, other: &FQName) -> bool {
        self.keyspace == other.keyspace.as_ref().map(|x| x.as_ref()) && self.name == other.name
    }
}

impl PartialEq<FQNameRef<'_>> for FQName {
    fn eq(&self, other: &FQNameRef<'_>) -> bool {
        self.keyspace.as_ref().map(|x| x.as_ref()) == other.keyspace && self.name == other.name
    }
}

/// Identifers are either Quoted or Unquoted.
///  * Unquoted Identifiers:  are case insensitive
///  * Quoted Identifiers: are case sensitive.  double quotes appearing within the quoted string are escaped by doubling (i.e. `"foo""bar" is interpreted as `foo"bar`)
///
/// Quoted lower lower case is equivalent to unquoted mixed case.
/// Quoted( myid ) == Unquoted( myid )
/// Quoted( myid ) == Unquoted( "myId" )
/// Quoted( myid ) != Quoted( "myId" )
///
/// It is possible to create an Unquoted identifier with an embedded quote (e.g. `Identifier::Unquoted( "foo\"bar" )`).
/// *Note* that a quote as the first character in an Unquoted Identifier can cause problems if the Identifier is converted
/// to a string and then parsed again as the second parse will create a Quoted identifier.
#[derive(Debug, Clone, Eq, Ord, PartialOrd)]
pub enum Identifier {
    /// This variant is case sensitive
    /// "fOo""bAr""" is stored as fOo"bAr"
    Quoted(String),
    /// This variant is case insensitive
    /// Only ascii alphanumeric and _ characters are allowed in this variant
    /// fOo_bAr is stored as fOo_bAr
    Unquoted(String),
}

impl Identifier {
    /// parses strings as returned by the parser into Quoted or Unquoted Identifiers.
    ///  * Unquoted Identifiers:  are case insensitive
    ///  * Quoted Identifiers: are case sensitive.  double quotes appearing within the quoted string
    ///    are escaped by doubling (i.e. `"foo""bar" is interpreted as `foo"bar`)
    ///
    /// If the string starts with `"` it is assumed to be a quoted identifier, the leading and trailing quotes are removed
    /// and the internal doubled quotes (`""`) are converted to simple quotes (`"`).
    pub fn parse(text: &str) -> Identifier {
        if text.starts_with('"') {
            let mut chars = text.chars();
            chars.next();
            chars.next_back();
            Identifier::Quoted(chars.as_str().replace("\"\"", "\""))
        } else {
            Identifier::Unquoted(text.to_string())
        }
    }

    fn as_ref(&'_ self) -> IdentifierRef<'_> {
        match self {
            Self::Quoted(x) => IdentifierRef::Quoted(x),
            Self::Unquoted(x) => IdentifierRef::Unquoted(x),
        }
    }
}

impl PartialEq for Identifier {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Hash for Identifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Identifier::Quoted(a) => a.hash(state),
            Identifier::Unquoted(a) => a.to_lowercase().hash(state),
        }
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Identifier::Quoted(txt) => write!(f, "\"{}\"", txt.replace('\"', "\"\"")),
            Identifier::Unquoted(txt) => write!(f, "{}", txt),
        }
    }
}

impl Default for Identifier {
    fn default() -> Self {
        Identifier::Unquoted("".to_string())
    }
}

impl From<&str> for Identifier {
    fn from(txt: &str) -> Self {
        Identifier::parse(txt)
    }
}

impl From<&String> for Identifier {
    fn from(txt: &String) -> Self {
        Identifier::parse(txt)
    }
}

/// An alternative to [Identifier] that holds &str instead of String.
/// Allows for allocationless comparison of [Identifier].
#[derive(Debug)]
pub enum IdentifierRef<'a> {
    /// This variant is case sensitive
    /// "fOo""bAr""" is stored as fOo"bAr"
    Quoted(&'a str),
    /// This variant is case insensitive
    /// Only ascii alphanumeric and _ characters are allowed in this variant
    /// fOo_bAr is stored as fOo_bAr
    Unquoted(&'a str),
}

impl PartialEq for IdentifierRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (IdentifierRef::Quoted(a), IdentifierRef::Quoted(b)) => a == b,
            (IdentifierRef::Unquoted(a), IdentifierRef::Unquoted(b)) => {
                a.to_lowercase() == b.to_lowercase()
            }
            (IdentifierRef::Quoted(a), IdentifierRef::Unquoted(b)) => a == &b.to_lowercase(),
            (IdentifierRef::Unquoted(a), IdentifierRef::Quoted(b)) => &a.to_lowercase() == b,
        }
    }
}

impl PartialEq<Identifier> for IdentifierRef<'_> {
    fn eq(&self, other: &Identifier) -> bool {
        self == &other.as_ref()
    }
}

impl PartialEq<IdentifierRef<'_>> for Identifier {
    fn eq(&self, other: &IdentifierRef<'_>) -> bool {
        &self.as_ref() == other
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{FQName, Identifier, Operand};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    pub fn test_operand_unescape() {
        let tests = [
            (
                "'Women''s Tour of New Zealand'",
                "Women's Tour of New Zealand",
            ),
            (
                "$$Women's Tour of New Zealand$$",
                "Women's Tour of New Zealand",
            ),
            (
                "$$Women''s Tour of New Zealand$$",
                "Women''s Tour of New Zealand",
            ),
            ("55", "55"),
        ];
        for (arg, expected) in tests {
            assert_eq!(expected, Operand::unescape(arg).as_str());
        }
        assert_eq!(
            Operand::Null.to_string(),
            Operand::unescape(Operand::Null.to_string().as_str())
        );
    }

    #[test]
    pub fn test_operand_escape() {
        let tests = [
            (
                "$$Women's Tour of New Zealand$$",
                "Women's Tour of New Zealand",
            ),
            (
                "'Women''s Tour of New Zealand makes big $$'",
                "Women's Tour of New Zealand makes big $$",
            ),
            ("55", "55"),
        ];
        for (expected, arg) in tests {
            assert_eq!(Operand::Const(expected.to_string()), Operand::escape(arg));
        }
    }

    #[test]
    pub fn test_identifier_parse_quoted() {
        let args = [
            (r#""""hello"", she said""#, r#""hello", she said"#),
            (r#""CaseSpecific""#, "CaseSpecific"),
        ];

        for (arg, expected) in args {
            let x = Identifier::parse(arg);
            assert_eq!(arg, x.to_string());
            if let Identifier::Quoted(txt) = x {
                assert_eq!(expected, txt);
            } else {
                panic!("Should  be quoted");
            }
        }
    }

    #[test]
    pub fn test_identifier_parse_unquoted() {
        let args = ["just_A_name", "CaseSpecific"];

        for arg in args {
            let x = Identifier::parse(arg);
            assert_eq!(arg, x.to_string());
            if let Identifier::Unquoted(txt) = x {
                assert_eq!(arg, txt);
            } else {
                panic!("Should  be unquoted");
            }
        }
    }

    fn assert_identifier_equality(left: &Identifier, right: &Identifier) {
        assert_eq!(left, right);
        assert_eq!(right, left);
        let mut left_hasher = DefaultHasher::new();
        left.hash(&mut left_hasher);

        let mut right_hasher = DefaultHasher::new();
        right.hash(&mut right_hasher);
        assert_eq!(left_hasher.finish(), right_hasher.finish());
    }

    fn assert_identifier_inequality(left: &Identifier, right: &Identifier) {
        assert!(!left.eq(right));
        assert!(!right.eq(left));
    }

    #[test]
    pub fn test_identifier_equality() {
        let lower_case_unquoted = Identifier::Unquoted("myid".to_string());
        let mixed_case_unquoted = Identifier::Unquoted("myId".to_string());
        let lower_case_quoted = Identifier::Quoted("myid".to_string());
        let mixed_case_quoted = Identifier::Quoted("MyId".to_string());

        let quote_in_unquoted = Identifier::Unquoted("\"now\"".to_string());
        let quote_in_quoted = Identifier::Quoted("\"now\"".to_string());

        assert_identifier_equality(&lower_case_unquoted, &mixed_case_unquoted);

        assert_identifier_equality(&lower_case_unquoted, &lower_case_quoted);
        assert_identifier_inequality(&mixed_case_quoted, &mixed_case_unquoted);
        assert_identifier_inequality(&mixed_case_quoted, &lower_case_unquoted);
        assert_identifier_inequality(&mixed_case_quoted, &lower_case_quoted);

        assert_identifier_equality(&quote_in_quoted, &quote_in_unquoted);
    }

    #[test]
    pub fn test_fqname_parse() {
        let name = FQName::parse("myid");
        assert_eq!(FQName::simple("myid"), name);

        let name = FQName::parse("myId");
        assert_eq!(FQName::simple("myId"), name);
        assert_eq!(Identifier::Unquoted("myId".to_string()), name.name);

        let name = FQName::parse(r#""myId""#);
        assert_eq!(FQName::simple("\"myId\""), name);
        assert_eq!(Identifier::Quoted("myId".to_string()), name.name);

        assert_eq!(FQName::new("myid", "name"), FQName::parse("myid.name"));

        let name = FQName::parse("myId.Name");
        assert_eq!(FQName::new("myId", "Name"), name);
        assert_eq!(
            Some(Identifier::Unquoted("MyId".to_string())),
            name.keyspace
        );
        assert_eq!(Identifier::Unquoted("Name".to_string()), name.name);

        let name = FQName::parse("\"myId\".Name");
        assert_eq!(FQName::new("\"myId\"", "Name"), name);
        assert_eq!(Some(Identifier::Quoted("myId".to_string())), name.keyspace);
        assert_eq!(Identifier::Unquoted("Name".to_string()), name.name);

        let name = FQName::parse("\"myId\".\"Name\"");
        assert_eq!(FQName::new("\"myId\"", "\"Name\""), name);
        assert_eq!(Some(Identifier::Quoted("myId".to_string())), name.keyspace);
        assert_eq!(Identifier::Quoted("Name".to_string()), name.name);
    }
}
