use itertools::Itertools;
use std::fmt::{Display, Formatter};
use tree_sitter::{Node, Tree, TreeCursor};

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraStatement {
    AlterKeyspace(KeyspaceData),
    AlterMaterializedView(AlterMaterializedViewData),
    AlterRole(RoleData),
    AlterTable(AlterTableData),
    AlterType(AlterTypeData),
    AlterUser(UserData),
    ApplyBatch,
    CreateAggregate(AggregateData),
    CreateFunction(FunctionData),
    CreateIndex(IndexData),
    CreateKeyspace(KeyspaceData),
    CreateMaterializedView(CreateMaterializedViewData),
    CreateRole(RoleData),
    CreateTable(CreateTableData),
    CreateTrigger(TriggerData),
    CreateType(TypeData),
    CreateUser(UserData),
    DeleteStatement(DeleteStatementData),
    DropAggregate(DropData),
    DropFunction(DropData),
    DropIndex(DropData),
    DropKeyspace(DropData),
    DropMaterializedView(DropData),
    DropRole(DropData),
    DropTable(DropData),
    DropTrigger(DropTriggerData),
    DropType(DropData),
    DropUser(DropData),
    Grant(PrivilegeData),
    InsertStatement(InsertStatementData),
    ListPermissions(PrivilegeData),
    ListRoles(ListRoleData),
    Revoke(PrivilegeData),
    SelectStatement(SelectStatementData),
    Truncate(String),
    Update(UpdateStatementData),
    UseStatement(String),
    UNKNOWN(String),
}

#[derive(PartialEq, Debug, Clone)]
pub struct ListRoleData {
    pub of: Option<String>,
    pub no_recurse: bool,
}

impl Display for ListRoleData {
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

#[derive(PartialEq, Debug, Clone)]
pub struct PrivilegeData {
    pub privilege: Privilege,
    pub resource: Option<Resource>,
    pub role: Option<String>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct UpdateStatementData {
    pub modifiers: StatementModifiers,
    pub begin_batch: Option<BeginBatch>,
    pub table_name: String,
    pub using_ttl: Option<TtlTimestamp>,
    pub assignments: Vec<AssignmentElement>,
    pub where_clause: Vec<RelationElement>,
    pub if_spec: Option<Vec<(String, String)>>,
}

impl Display for UpdateStatementData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}UPDATE {}{} SET {} WHERE {}{}",
            self.begin_batch.as_ref().map_or("".to_string(), |x| x.to_string()),
        self.table_name,
            self.using_ttl.as_ref().map_or("".to_string(), |x| x.to_string() ),

        self.assignments
                .iter()
                .map(|a| a.to_string())
                .join(","),

        self.where_clause.iter().join(" AND "),
        if self.if_spec.is_some() {
                format!( " IF {}",
                    self.if_spec
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(x, y)| format!("{} = {}", x, y))
                    .join(" AND ")
            )
        } else if self.modifiers.exists {
            " IF EXISTS".to_string()
        } else {"".to_string()}
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct AssignmentElement {
    pub name: IndexedColumn,
    pub value: Operand,
    pub operator: Option<AssignmentOperator>,
}

impl Display for AssignmentElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.operator {
            Some(x) => write!(f, "{} = {}{}", self.name, self.value, x),
            None => write!(f, "{} = {}", self.name, self.value),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum AssignmentOperator {
    Plus(Operand),
    Minus(Operand),
}

impl Display for AssignmentOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AssignmentOperator::Plus(op) => write!(f, " + {}", op),
            AssignmentOperator::Minus(op) => write!(f, " - {}", op),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct IndexedColumn {
    column: String,
    value: Option<String>,
}

impl Display for IndexedColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.value {
            Some(x) => write!(f, "{}[{}]", self.column, x),
            None => write!(f, "{}", self.column),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeleteStatementData {
    pub modifiers: StatementModifiers,
    pub begin_batch: Option<BeginBatch>,
    pub columns: Option<Vec<IndexedColumn>>,
    pub table_name: String,
    pub timestamp: Option<u64>,
    pub where_clause: Vec<RelationElement>,
    pub if_spec: Option<Vec<(String, String)>>,
}

impl Display for DeleteStatementData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!( f, "{}DELETE {}FROM {}{} WHERE {}{}",
        self.begin_batch.as_ref().map_or( "".to_string(),|x|x.to_string()),
        self.columns.as_ref().map_or("".to_string(), |x| format!( "{} ",x.iter().join(", "))),
        self.table_name,
        self.timestamp.as_ref().map_or("".to_string(), |x| format!(" USING TIMESTAMP {}", x)),
        self.where_clause.iter().join(" AND "),
        if self.if_spec.is_some() {
                format!( " IF {}",
                self.if_spec
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(x, y)| format!("{} = {}", x, y))
                    .join(" AND ")
            )
        } else if self.modifiers.exists {
            " IF EXISTS".to_string()
        } else { "".to_string() }
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct InsertStatementData {
    pub begin_batch: Option<BeginBatch>,
    pub modifiers: StatementModifiers,
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Option<InsertValues>,
    pub using_ttl: Option<TtlTimestamp>,
}

impl Display for InsertStatementData {

    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!( f, "{}INSERT INTO {} {}{}{}{}",
            self.begin_batch.as_ref().map_or("".to_string(), |x| x.to_string()),
        self.table_name,
        self.columns.as_ref().map_or( "".to_string(), |x|
            format!( "({}) ", x.iter().join(", "))),
        self.values.as_ref().unwrap(),
        if self.modifiers.not_exists {" IF NOT EXISTS" } else {""},
        self.using_ttl.as_ref().map_or( "".to_string(),|x| x.to_string()),
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct TtlTimestamp {
    ttl: Option<u64>,
    timestamp: Option<u64>,
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

#[derive(PartialEq, Debug, Clone)]
pub struct BeginBatch {
    logged: bool,
    unlogged: bool,
    timestamp: Option<u64>,
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
        if self.timestamp.is_some() {
            write!(
                f,
                "BEGIN {}BATCH USING TIMESTAMP {} ",
                modifiers,
                self.timestamp.unwrap()
            )
        } else {
            write!(f, "BEGIN {}BATCH ", modifiers)
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum InsertValues {
    VALUES(Vec<Operand>),
    JSON(String),
}

impl Display for InsertValues {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertValues::VALUES(columns) => {
                write!(f, "VALUES ({})", columns.iter().join(", "))
            }
            InsertValues::JSON(text) => {
                write!(f, "JSON {}", text)
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum Operand {
    CONST(String),
    MAP(Vec<(String, String)>),
    SET(Vec<String>),
    LIST(Vec<String>),
    TUPLE(Vec<Operand>),
    COLUMN(String),
    FUNC(String),
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

#[derive(PartialEq, Debug, Clone)]
pub struct SelectStatementData {
    pub modifiers: StatementModifiers,
    pub table_name: String,
    pub elements: Vec<SelectElement>,
    pub where_clause: Option<Vec<RelationElement>>,
    pub order: Option<OrderClause>,
}

impl SelectStatementData {
    /// return the column names selected
    pub fn select_names(&self) -> Vec<String> {
        self.elements
            .iter()
            .filter(|e| match e {
                SelectElement::Star => false,
                SelectElement::DotStar(_) => false,
                SelectElement::Column(_) => true,
                SelectElement::Function(_) => false,
            }).map(|e| match e {
            SelectElement::Column(named) => named.to_string(),
            _ => unreachable!()
        } )
            .collect()
    }

    /// return the aliased column names.  If the column is not aliased the
    /// base column name is returned.
    pub fn select_alias(&self) -> Vec<String> {
        self.elements
            .iter()
            .map(|e| match e {
                SelectElement::Column(named) => {
                    if named.alias.is_some() {
                        named.alias.clone()
                        //Some(named.alias..as_ref().unwrap().clone())
                    } else {
                        Some(named.name.clone())
                    }
                }
                _ => None,
            })
            .filter(|e| e.is_some())
            .map(|e| e.unwrap())
            .collect()
    }
    /// return the column names from the where clause
    pub fn where_columns(&self) -> Vec<String> {
        match &self.where_clause {
            Some(x) => x
                .iter()
                .map(|e| match &e.obj {
                    Operand::COLUMN(name) => Some(name.clone()),
                    _ => None,
                })
                .filter(|e| e.is_some())
                .map(|e| e.unwrap())
                .collect(),
            None => vec![],
        }
    }
}

impl Display for SelectStatementData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT {}{}{} FROM {}{}{}{}{}",
        if self.modifiers.distinct {"DISTINCT " }else{""},
        if self.modifiers.json {"JSON "}else{""},
        self.elements.iter().join(", "),
        self.table_name,
        self.where_clause.as_ref().map_or( "".to_string(), |x|
            format!( " WHERE {}", x.iter().join(" AND ")
            )),
        self.order.as_ref().map_or("".to_string(), |x| format!(" ORDER BY {}", x)),
        self.modifiers.limit.map_or("".to_string(), |x| format!(" LIMIT {}", x)),
        if self.modifiers.filtering { " ALLOW FILTERING" }else{""}
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum SelectElement {
    Star,
    DotStar(String),
    Column(Named),
    Function(Named),
}

impl Display for SelectElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectElement::Star => write!(f, "*"),
            SelectElement::DotStar(column) => write!(f, "{}.*", column ),
            SelectElement::Column(named) | SelectElement::Function(named) => write!(f, "{}", named ),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Named {
    pub(crate) name: String,
    pub(crate) alias: Option<String>,
}

impl Named {
    pub fn alias_or_name(&self) -> String {
        match &self.alias {
            None => self.name.clone(),
            Some(alias) => alias.clone(),
        }
    }
}

impl Display for Named {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.alias {
            None => write!(f, "{}", self.name),
            Some(a) => write!(f, "{} AS {}", self.name, a),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct OrderClause {
    name: String,
    desc: bool,
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

#[derive(PartialEq, Debug, Clone)]
pub enum RelationOperator {
    LT,
    LE,
    EQ,
    NE,
    GE,
    GT,
    IN,
    Contains,
    ContainsKey,
    IsNot,
}

impl RelationOperator {
    pub fn eval<T>(&self, left: &T, right: &T) -> bool
    where
        T: PartialOrd,
    {
        match self {
            RelationOperator::LT => left.lt(right),
            RelationOperator::LE => left.le(right),
            RelationOperator::EQ => left.eq(right),
            RelationOperator::NE => !left.eq(right),
            RelationOperator::GE => left.ge(right),
            RelationOperator::GT => left.gt(right),
            RelationOperator::IN => false,
            RelationOperator::Contains => false,
            RelationOperator::ContainsKey => false,
            RelationOperator::IsNot => false,
        }
    }
}

impl Display for RelationOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RelationOperator::LT => write!(f, "<"),
            RelationOperator::LE => write!(f, "<="),
            RelationOperator::EQ => write!(f, "="),
            RelationOperator::NE => write!(f, "<>"),
            RelationOperator::GE => write!(f, ">="),
            RelationOperator::GT => write!(f, ">"),
            RelationOperator::IN => write!(f, "IN"),
            RelationOperator::Contains => write!(f, "CONTAINS"),
            RelationOperator::ContainsKey => write!(f, "CONTAINS KEY"),
            RelationOperator::IsNot => write!(f, "IS NOT"),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct StatementModifiers {
    pub distinct: bool,
    pub json: bool,
    pub limit: Option<i32>,
    pub filtering: bool,
    pub not_exists: bool,
    pub exists: bool,
}

impl Default for StatementModifiers {
    fn default() -> Self {
        Self::new()
    }
}

impl StatementModifiers {
    pub fn new() -> StatementModifiers {
        StatementModifiers {
            distinct: false,
            json: false,
            limit: None,
            filtering: false,
            not_exists: false,
            exists: false,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct AlterTypeData {
    name: String,
    operation: AlterTypeOperation,
}

impl Display for AlterTypeData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TYPE {} {}", self.name, self.operation)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum AlterTypeOperation {
    AlterColumnType(AlterColumnTypeData),
    Add(Vec<ColumnDefinition>),
    Rename(Vec<(String, String)>),
}

impl Display for AlterTypeOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTypeOperation::AlterColumnType(column_type) => write!(f, "{}", column_type),
            AlterTypeOperation::Add(columns) => write!(
                f,
                "ADD {}",
                columns.iter().map(|x| x.to_string()).join(", ")
            ),
            AlterTypeOperation::Rename(pairs) => write!(
                f,
                "RENAME {}",
                pairs
                    .iter()
                    .map(|(x, y)| format!("{} TO {}", x, y))
                    .join(" AND ")
            ),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct AlterColumnTypeData {
    name: String,
    data_type: DataType,
}

impl Display for AlterColumnTypeData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER {} TYPE {}", self.name, self.data_type)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct AlterTableData {
    name: String,
    operation: AlterTableOperation,
}

#[derive(PartialEq, Debug, Clone)]
enum AlterTableOperation {
    Add(Vec<ColumnDefinition>),
    DropColumns(Vec<String>),
    DropCompactStorage,
    Rename((String, String)),
    With(WithElement),
}

impl Display for AlterTableOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableOperation::Add(columns) => write!(
                f,
                "ADD {}",
                columns.iter().map(|x| x.to_string()).join(", ")
            ),
            AlterTableOperation::DropColumns(columns) => write!(f, "DROP {}", columns.join(", ")),
            AlterTableOperation::DropCompactStorage => write!(f, "DROP COMPACT STORAGE"),
            AlterTableOperation::Rename((from, to)) => write!(f, "RENAME {} TO {}", from, to),
            AlterTableOperation::With(with_element) => write!(
                f,
                "WITH {}",
                with_element.iter().map(|x| x.to_string()).join(" AND ")
            ),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct AlterMaterializedViewData {
    name: String,
    with: WithElement,
}

impl Display for AlterMaterializedViewData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ALTER MATERIALIZED VIEW {}{}",
            self.name,
            if self.with.is_empty() {
                "".to_string()
            } else {
                format!(
                    " WITH {}",
                    self.with.iter().map(|x| x.to_string()).join(" AND ")
                )
            }
        )
    }
}

/*
   /*
   seq(
               kw("ALTER"),
               kw( "MATERIALIZED"),
               kw( "VIEW"),
               $.materialized_view_name,
               optional( $.with_element),
           ),
    */

*/
#[derive(PartialEq, Debug, Clone)]
pub struct CreateMaterializedViewData {
    if_not_exists: bool,
    name: String,
    columns: Vec<String>,
    table: String,
    where_clause: Vec<RelationElement>,
    key: PrimaryKey,
    with: WithElement,
}

impl Display for CreateMaterializedViewData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE MATERIALIZED VIEW {}{} AS SELECT {} FROM {} WHERE {} {}{}",
            if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.columns.join(", "),
            self.table,
            self.where_clause.iter().join(" AND "),
            self.key,
            if self.with.is_empty() {
                "".to_string()
            } else {
                format!(
                    " WITH {}",
                    self.with.iter().map(|x| x.to_string()).join(" AND ")
                )
            }
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct CreateTableData {
    if_not_exists: bool,
    name: String,
    columns: Vec<ColumnDefinition>,
    key: Option<PrimaryKey>,
    with: WithElement,
}

impl Display for CreateTableData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut v: Vec<String> = self.columns.iter().map(|x| x.to_string()).collect();
        if self.key.is_some() {
            v.push(self.key.as_ref().unwrap().to_string());
        }
        write!(
            f,
            "{}{} ({}){}",
            if self.if_not_exists {
                "IF NOT EXISTS ".to_string()
            } else {
                "".to_string()
            },
            self.name,
            v.join(", "),
            if !self.with.is_empty() {
                format!(
                    " WITH {}",
                    self.with.iter().map(|x| x.to_string()).join(" AND ")
                )
            } else {
                "".to_string()
            }
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct TypeData {
    not_exists: bool,
    name: String,
    columns: Vec<ColumnDefinition>,
}

impl Display for TypeData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TYPE {}{} ({})",
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.columns.iter().map(|x| x.to_string()).join(", "),
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct ColumnDefinition {
    name: String,
    data_type: DataType,
    primary_key: bool,
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
#[derive(PartialEq, Debug, Clone)]
pub struct DataType {
    name: DataTypeName,
    definition: Vec<DataTypeName>,
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

#[derive(PartialEq, Debug, Clone)]
pub struct PrimaryKey {
    partition: Vec<String>,
    clustering: Vec<String>,
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

#[derive(PartialEq, Debug, Clone)]
pub struct TriggerData {
    not_exists: bool,
    name: String,
    class: String,
}

impl Display for TriggerData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE TRIGGER {}{} USING {}",
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.class
        )
    }
}
pub type WithElement = Vec<WithItem>;

#[derive(PartialEq, Debug, Clone)]
pub enum WithItem {
    Option { key: String, value: OptionValue },
    ClusterOrder(OrderClause),
    ID(String),
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

#[derive(PartialEq, Debug, Clone)]
pub enum OptionValue {
    Literal(String),
    Hash(Vec<(String, String)>),
}

impl Display for OptionValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OptionValue::Literal(txt) => write!(f, "{}", txt),
            OptionValue::Hash(items) => write!(
                f,
                "{{{}}}",
                items.iter().map(|(x, y)| format!("{}:{}", x, y)).join(", ")
            ),
        }
    }
}
#[derive(PartialEq, Debug, Clone)]
pub struct RoleData {
    name: String,
    password: Option<String>,
    superuser: Option<bool>,
    login: Option<bool>,
    options: Vec<(String, String)>,
    if_not_exists: bool,
}

impl Display for RoleData {
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

#[derive(PartialEq, Debug, Clone)]
pub struct KeyspaceData {
    name: String,
    replication: Vec<(String, String)>,
    durable_writes: Option<bool>,
    if_not_exists: bool,
}
impl Display for KeyspaceData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.durable_writes.is_some() {
            write!(
                f,
                "KEYSPACE {}{} WITH REPLICATION = {{{}}} AND DURABLE_WRITES = {}",
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                self.name,
                self.replication
                    .iter()
                    .map(|(x, y)| format!("{}:{}", x, y))
                    .join(", "),
                if self.durable_writes.unwrap() {
                    "TRUE"
                } else {
                    "FALSE"
                }
            )
        } else {
            write!(
                f,
                "KEYSPACE {}{} WITH REPLICATION = {{{}}}",
                if self.if_not_exists {
                    "IF NOT EXISTS "
                } else {
                    ""
                },
                self.name,
                self.replication
                    .iter()
                    .map(|(x, y)| format!("{}:{}", x, y))
                    .join(", ")
            )
        }
    }
}
#[derive(PartialEq, Debug, Clone)]
pub struct UserData {
    name: String,
    password: Option<String>,
    superuser: bool,
    no_superuser: bool,
    if_not_exists: bool,
}

impl Display for UserData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut with = String::new();

        if self.password.is_some() {
            with.push_str(" PASSWORD ");
            with.push_str(self.password.as_ref().unwrap().as_str());
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

#[derive(PartialEq, Debug, Clone)]
pub struct DropTriggerData {
    name: String,
    table: String,
    if_exists: bool,
}

impl Display for DropTriggerData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP TRIGGER{} {} ON {}",
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name,
            self.table
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct DropData {
    name: String,
    if_exists: bool,
}

impl DropData {
    pub fn get_text(&self, type_: &str) -> String {
        format!(
            "DROP {}{} {}",
            type_,
            if self.if_exists { " IF EXISTS" } else { "" },
            self.name
        )
    }
}

struct NodeFuncs {}

impl NodeFuncs {
    pub fn as_string(node: &Node, source: &str) -> String {
        node.utf8_text(source.as_bytes()).unwrap().to_string()
    }

    pub fn as_boolean(node: &Node, source: &str) -> bool {
        NodeFuncs::as_string(node, source).to_uppercase().eq("TRUE")
    }
}
impl CassandraStatement {
    pub fn from_tree(tree: &Tree, source: &str) -> CassandraStatement {
        let mut node = tree.root_node();
        if node.kind().eq("source_file") {
            node = node.child(0).unwrap();
        }
        CassandraStatement::from_node(&node, source)
    }

    pub fn from_node(node: &Node, source: &str) -> CassandraStatement {
        match node.kind() {
            "alter_keyspace" => CassandraStatement::AlterKeyspace(
                CassandraParser::parse_keyspace_data(node, source),
            ),
            "alter_materialized_view" => CassandraStatement::AlterMaterializedView(
                CassandraParser::parse_alter_materialized_view(node, source),
            ),
            "alter_role" => {
                CassandraStatement::AlterRole(CassandraParser::parse_role_data(node, source))
            }
            "alter_table" => {
                CassandraStatement::AlterTable(CassandraParser::parse_alter_table(node, source))
            }
            "alter_type" => {
                CassandraStatement::AlterType(CassandraParser::parse_alter_type(node, source))
            }
            "alter_user" => {
                CassandraStatement::AlterUser(CassandraParser::parse_user_data(node, source))
            }
            "apply_batch" => CassandraStatement::ApplyBatch,
            "create_aggregate" => CassandraStatement::CreateAggregate(
                CassandraParser::parse_aggregate_data(node, source),
            ),
            "create_function" => CassandraStatement::CreateFunction(
                CassandraParser::parse_function_data(node, source),
            ),
            "create_index" => {
                CassandraStatement::CreateIndex(CassandraParser::parse_index_data(node, source))
            }
            "create_keyspace" => CassandraStatement::CreateKeyspace(
                CassandraParser::parse_keyspace_data(node, source),
            ),
            "create_materialized_view" => CassandraStatement::CreateMaterializedView(
                CassandraParser::parse_create_materialized_vew(node, source),
            ),
            "create_role" => {
                CassandraStatement::CreateRole(CassandraParser::parse_role_data(node, source))
            }
            "create_table" => {
                CassandraStatement::CreateTable(CassandraParser::parse_create_table(node, source))
            }
            "create_trigger" => {
                CassandraStatement::CreateTrigger(CassandraParser::parse_trigger_data(node, source))
            }
            "create_type" => {
                CassandraStatement::CreateType(CassandraParser::parse_type_data(node, source))
            }
            "create_user" => {
                CassandraStatement::CreateUser(CassandraParser::parse_user_data(node, source))
            }
            "delete_statement" => CassandraStatement::DeleteStatement(
                CassandraParser::parse_delete_statement(node, source),
            ),
            "drop_aggregate" => CassandraStatement::DropAggregate(
                CassandraParser::parse_standard_drop(node, source),
            ),
            "drop_function" => CassandraStatement::DropFunction(
                CassandraParser::parse_standard_drop(node, source),
            ),
            "drop_index" => {
                CassandraStatement::DropIndex(CassandraParser::parse_standard_drop(node, source))
            }
            "drop_keyspace" => CassandraStatement::DropKeyspace(
                CassandraParser::parse_standard_drop(node, source),
            ),
            "drop_materialized_view" => CassandraStatement::DropMaterializedView(
                CassandraParser::parse_standard_drop(node, source),
            ),
            "drop_role" => {
                CassandraStatement::DropRole(CassandraParser::parse_standard_drop(node, source))
            }
            "drop_table" => {
                CassandraStatement::DropTable(CassandraParser::parse_standard_drop(node, source))
            }
            "drop_trigger" => {
                CassandraStatement::DropTrigger(CassandraParser::parse_drop_trigger(node, source))
            }
            "drop_type" => {
                CassandraStatement::DropType(CassandraParser::parse_standard_drop(node, source))
            }
            "drop_user" => {
                CassandraStatement::DropUser(CassandraParser::parse_standard_drop(node, source))
            }
            "grant" => {
                CassandraStatement::Grant(CassandraParser::parse_privilege_data(node, source))
            }
            "insert_statement" => CassandraStatement::InsertStatement(
                CassandraParser::parse_insert_statement(node, source),
            ),
            "list_permissions" => CassandraStatement::ListPermissions(
                CassandraParser::parse_privilege_data(node, source),
            ),
            "list_roles" => {
                CassandraStatement::ListRoles(CassandraParser::parse_list_role_data(node, source))
            }
            "revoke" => {
                CassandraStatement::Revoke(CassandraParser::parse_privilege_data(node, source))
            }
            "select_statement" => CassandraStatement::SelectStatement(
                CassandraParser::parse_select_statement(node, source),
            ),
            "truncate" => {
                let mut cursor = node.walk();
                cursor.goto_first_child();
                // consume until 'table_name'
                while !cursor.node().kind().eq("table_name") {
                    cursor.goto_next_sibling();
                }
                CassandraStatement::Truncate(CassandraParser::parse_table_name(
                    &cursor.node(),
                    source,
                ))
            }
            "update" => {
                CassandraStatement::Update(CassandraParser::parse_update_statement(node, source))
            }
            "use" => {
                let mut cursor = node.walk();
                cursor.goto_first_child();
                // consume 'USE'
                if cursor.goto_next_sibling() {
                    CassandraStatement::UseStatement(NodeFuncs::as_string(&cursor.node(), source))
                } else {
                    CassandraStatement::UNKNOWN(
                        "Keyspace not provided with USE statement".to_string(),
                    )
                }
            }
            _ => CassandraStatement::UNKNOWN(source.to_string()),
        }
    }
}

struct CassandraParser {}
impl CassandraParser {
    fn parse_alter_materialized_view(node: &Node, source: &str) -> AlterMaterializedViewData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume ALTER
        cursor.goto_next_sibling();
        // consume MATERIALIZED
        cursor.goto_next_sibling();
        // consume VIEW
        cursor.goto_next_sibling();
        AlterMaterializedViewData {
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            with: if cursor.goto_next_sibling() {
                CassandraParser::parse_with_element(&cursor.node(), source)
            } else {
                vec![]
            },
        }
    }
    fn parse_init_condition(node: &Node, source: &str) -> InitCondition {
        let mut cursor = node.walk();
        if cursor.node().kind().eq("init_cond_definition") {
            cursor.goto_first_child();
        }
        match cursor.node().kind() {
            "constant" => InitCondition::Constant(NodeFuncs::as_string(&cursor.node(), source)),
            "init_cond_list" => {
                let mut entries = vec![];
                cursor.goto_first_child();
                // consume the '('
                while cursor.goto_next_sibling() {
                    if cursor.node().kind().eq("constant") {
                        entries.push(InitCondition::Constant(NodeFuncs::as_string(
                            &cursor.node(),
                            source,
                        )));
                    }
                }
                InitCondition::List(entries)
            }
            "init_cond_nested_list" => {
                let mut entries = vec![];
                cursor.goto_first_child();
                while cursor.goto_next_sibling() {
                    if cursor.node().kind().eq("init_cond_list") {
                        entries.push(CassandraParser::parse_init_condition(
                            &cursor.node(),
                            source,
                        ));
                    }
                }
                InitCondition::List(entries)
            }
            "init_cond_hash" => {
                let mut entries = vec![];
                cursor.goto_first_child();
                while cursor.goto_next_sibling() {
                    if cursor.node().kind().eq("init_cond_hash_item") {
                        cursor.goto_first_child();
                        let key = NodeFuncs::as_string(&cursor.node(), source);
                        cursor.goto_next_sibling();
                        //consume ','
                        cursor.goto_next_sibling();
                        let value = CassandraParser::parse_init_condition(&cursor.node(), source);
                        entries.push((key, value));
                        cursor.goto_parent();
                    }
                }
                InitCondition::Map(entries)
            }
            _ => unreachable!(),
        }
    }

    fn parse_aggregate_data(node: &Node, source: &str) -> AggregateData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'CREATE'
        cursor.goto_next_sibling();
        AggregateData {
            or_replace: if cursor.node().kind().eq("OR") {
                // consume 'OR'
                cursor.goto_next_sibling();
                // consume 'REPLACE'
                cursor.goto_next_sibling();
                true
            } else {
                false
            },
            not_exists: {
                // consume 'FUNCTION'
                cursor.goto_next_sibling();
                if cursor.node().kind().eq("IF") {
                    // consume 'IF'
                    cursor.goto_next_sibling();
                    // consume 'NOT'
                    cursor.goto_next_sibling();
                    // consume 'EXISTS'
                    cursor.goto_next_sibling();
                    true
                } else {
                    false
                }
            },
            name: { CassandraParser::parse_table_name(&cursor.node(), source) },
            data_type: {
                cursor.goto_next_sibling();
                // consume '('
                cursor.goto_next_sibling();
                CassandraParser::parse_data_type(&cursor.node(), source)
            },
            sfunc: {
                cursor.goto_next_sibling();
                // consume ')'
                cursor.goto_next_sibling();
                // consume 'SFUNC'
                cursor.goto_next_sibling();
                NodeFuncs::as_string(&cursor.node(), source)
            },
            stype: {
                cursor.goto_next_sibling();
                // consume 'STYPE'
                cursor.goto_next_sibling();
                CassandraParser::parse_data_type(&cursor.node(), source)
            },
            finalfunc: {
                cursor.goto_next_sibling();
                // consume 'FINALFUNC'
                cursor.goto_next_sibling();
                NodeFuncs::as_string(&cursor.node(), source)
            },
            init_cond: {
                cursor.goto_next_sibling();
                // consume 'INITCOND'
                cursor.goto_next_sibling();
                // on 'init_cond_definition;
                CassandraParser::parse_init_condition(&cursor.node(), source)
            },
        }
    }

    fn parse_function_data(node: &Node, source: &str) -> FunctionData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'CREATE'
        cursor.goto_next_sibling();
        FunctionData {
            or_replace: if cursor.node().kind().eq("OR") {
                // consume 'OR'
                cursor.goto_next_sibling();
                // consume 'REPLACE'
                cursor.goto_next_sibling();
                true
            } else {
                false
            },
            not_exists: {
                // consume 'FUNCTION'
                cursor.goto_next_sibling();
                if cursor.node().kind().eq("IF") {
                    // consume 'IF'
                    cursor.goto_next_sibling();
                    // consume 'NOT'
                    cursor.goto_next_sibling();
                    // consume 'EXISTS'
                    cursor.goto_next_sibling();
                    true
                } else {
                    false
                }
            },
            name: { CassandraParser::parse_table_name(&cursor.node(), source) },
            params: {
                let mut params = vec![];
                while !cursor.node().kind().eq(")") {
                    if cursor.node().kind().eq("typed_name") {
                        params.push(CassandraParser::parse_column_definition(
                            &cursor.node(),
                            source,
                        ));
                    }
                    cursor.goto_next_sibling();
                }
                params
            },
            return_null: {
                // consume ')'
                cursor.goto_next_sibling();
                // parse the returns mode
                // '[CALLED |RETURNS NULL]', 'ON', 'NULL', 'INPUT'
                cursor.goto_first_child();
                let return_null = cursor.node().kind().eq("RETURNS");
                cursor.goto_parent();
                return_null
            },
            return_type: {
                cursor.goto_next_sibling();
                // consume 'RETURNS'
                cursor.goto_next_sibling();
                CassandraParser::parse_data_type(&cursor.node(), source)
            },
            language: {
                cursor.goto_next_sibling();
                // consume 'LANGUAGE'
                cursor.goto_next_sibling();
                NodeFuncs::as_string(&cursor.node(), source)
            },
            code_block: {
                cursor.goto_next_sibling();
                // consume 'AS'
                cursor.goto_next_sibling();
                NodeFuncs::as_string(&cursor.node(), source)
            },
        }
    }

    fn parse_alter_type(node: &Node, source: &str) -> AlterTypeData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'ALTER'
        cursor.goto_next_sibling();
        // consume 'TYPE'
        cursor.goto_next_sibling();
        AlterTypeData {
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            operation: {
                cursor.goto_next_sibling();
                // on 'alter_type_operation'
                cursor.goto_first_child();
                match cursor.node().kind() {
                    "alter_type_alter_type" => {
                        cursor.goto_first_child();
                        // consume 'ALTER'
                        cursor.goto_next_sibling();
                        AlterTypeOperation::AlterColumnType(AlterColumnTypeData {
                            name: NodeFuncs::as_string(&cursor.node(), source),
                            data_type: {
                                cursor.goto_next_sibling();
                                // consume 'TYPE'
                                cursor.goto_next_sibling();
                                CassandraParser::parse_data_type(&cursor.node(), source)
                            },
                        })
                    }
                    "alter_type_add" => {
                        let mut columns = vec![];
                        cursor.goto_first_child();
                        // consume ADD
                        while cursor.goto_next_sibling() {
                            if cursor.node().kind().eq("typed_name") {
                                columns.push(CassandraParser::parse_column_definition(
                                    &cursor.node(),
                                    source,
                                ));
                            }
                        }
                        AlterTypeOperation::Add(columns)
                    }
                    "alter_type_rename" => {
                        let mut pairs = vec![];
                        cursor.goto_first_child();
                        // consume RENAME
                        while cursor.goto_next_sibling() {
                            if cursor.node().kind().eq("alter_type_rename_item") {
                                cursor.goto_first_child();
                                let first = NodeFuncs::as_string(&cursor.node(), source);
                                cursor.goto_next_sibling();
                                // consume 'TO'
                                cursor.goto_next_sibling();
                                let second = NodeFuncs::as_string(&cursor.node(), source);
                                pairs.push((first, second));
                                cursor.goto_parent();
                            }
                        }
                        AlterTypeOperation::Rename(pairs)
                    }
                    _ => unreachable!(),
                }
            },
        }
    }

    fn parse_type_data(node: &Node, source: &str) -> TypeData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut result = TypeData {
            not_exists: CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor),
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            columns: vec![],
        };
        while cursor.goto_next_sibling() {
            if cursor.node().kind().eq("typed_name") {
                result
                    .columns
                    .push(CassandraParser::parse_column_definition(
                        &cursor.node(),
                        source,
                    ));
            }
        }
        result
    }

    fn parse_trigger_data(node: &Node, source: &str) -> TriggerData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        TriggerData {
            not_exists: CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor),
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            class: {
                cursor.goto_next_sibling();
                // consume 'USING'
                cursor.goto_next_sibling();
                NodeFuncs::as_string(&cursor.node(), source)
            },
        }
    }

    fn parse_alter_table_operation(node: &Node, source: &str) -> AlterTableOperation {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        match cursor.node().kind() {
            "alter_table_add" => {
                let mut columns: Vec<ColumnDefinition> = vec![];
                cursor.goto_first_child();
                // consume 'ADD'
                while cursor.goto_next_sibling() {
                    if cursor.node().kind().eq("typed_name") {
                        columns.push(CassandraParser::parse_column_definition(
                            &cursor.node(),
                            source,
                        ));
                    }
                }
                AlterTableOperation::Add(columns)
            }
            "alter_table_drop_columns" => {
                cursor.goto_first_child();
                let mut columns: Vec<String> = vec![];
                // consume 'DROP'
                while cursor.goto_next_sibling() {
                    if cursor.node().kind().eq("object_name") {
                        columns.push(NodeFuncs::as_string(&cursor.node(), source));
                    }
                }
                AlterTableOperation::DropColumns(columns)
            }
            "alter_table_drop_compact_storage" => AlterTableOperation::DropCompactStorage,
            "alter_table_rename" => {
                cursor.goto_first_child();
                // consume the 'FROM'
                cursor.goto_next_sibling();
                let from = NodeFuncs::as_string(&cursor.node(), source);
                cursor.goto_next_sibling();
                // consume the 'TO'
                cursor.goto_next_sibling();
                let to = NodeFuncs::as_string(&cursor.node(), source);
                AlterTableOperation::Rename((from, to))
            }
            "with_element" => AlterTableOperation::With(CassandraParser::parse_with_element(
                &cursor.node(),
                source,
            )),
            _ => unreachable!(),
        }
    }

    fn parse_alter_table(node: &Node, source: &str) -> AlterTableData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'ALTER'
        cursor.goto_next_sibling();
        // consume 'TABLE'
        cursor.goto_next_sibling();
        // get the name
        AlterTableData {
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            operation: {
                cursor.goto_next_sibling();
                CassandraParser::parse_alter_table_operation(&cursor.node(), source)
            },
        }
    }
    fn parse_primary_key_element(node: &Node, source: &str) -> PrimaryKey {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut primary_key = PrimaryKey {
            partition: vec![],
            clustering: vec![],
        };
        while cursor.goto_next_sibling() {
            if cursor.node().kind().eq("primary_key_definition") {
                cursor.goto_first_child();
                match cursor.node().kind() {
                    "compound_key" => {
                        cursor.goto_first_child();
                        primary_key
                            .partition
                            .push(NodeFuncs::as_string(&cursor.node(), source));
                        cursor.goto_next_sibling();
                        // consume the ','
                        cursor.goto_next_sibling();
                        // enter the clustering-key-list
                        let mut process = cursor.goto_first_child();
                        while process {
                            if !cursor.node().kind().eq(",") {
                                primary_key
                                    .clustering
                                    .push(NodeFuncs::as_string(&cursor.node(), source));
                            }
                            process = cursor.goto_next_sibling();
                        }
                    }
                    "composite_key" => {
                        cursor.goto_first_child();
                        let mut process = true;
                        while process {
                            match cursor.node().kind() {
                                "partition_key_list" => {
                                    cursor.goto_first_child();
                                    while process {
                                        if cursor.node().kind().eq("object_name") {
                                            primary_key
                                                .partition
                                                .push(NodeFuncs::as_string(&cursor.node(), source));
                                        }
                                        process = cursor.goto_next_sibling();
                                    }
                                    cursor.goto_parent();
                                }
                                "clustering_key_list" => {
                                    cursor.goto_first_child();
                                    while process {
                                        if cursor.node().kind().eq("object_name") {
                                            primary_key
                                                .clustering
                                                .push(NodeFuncs::as_string(&cursor.node(), source));
                                        }
                                        process = cursor.goto_next_sibling();
                                    }
                                    cursor.goto_parent();
                                }
                                _ => {}
                            }
                            process = cursor.goto_next_sibling();
                        }
                    }
                    _ => primary_key
                        .partition
                        .push(NodeFuncs::as_string(&cursor.node(), source)),
                }
            }
        }
        primary_key
    }
    fn parse_data_type(node: &Node, source: &str) -> DataType {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // extracting the name works because it is limited to a single child item so the text is correct
        let mut result = DataType {
            name: DataTypeName::from(NodeFuncs::as_string(&cursor.node(), source).as_str()),
            definition: vec![],
        };

        if cursor.goto_next_sibling() {
            cursor.goto_first_child();
            // consume the '<'
            while cursor.goto_next_sibling() {
                let kind = cursor.node().kind();
                if !(kind.eq(",") || kind.eq(">")) {
                    result.definition.push(DataTypeName::from(
                        NodeFuncs::as_string(&cursor.node(), source).as_str(),
                    ));
                }
            }
        }
        result
    }
    fn parse_column_definition(node: &Node, source: &str) -> ColumnDefinition {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        ColumnDefinition {
            name: NodeFuncs::as_string(&cursor.node(), source),
            data_type: {
                cursor.goto_next_sibling();
                CassandraParser::parse_data_type(&cursor.node(), source)
            },
            primary_key: cursor.goto_next_sibling(),
        }
    }

    fn parse_table_options(node: &Node, source: &str) -> Vec<WithItem> {
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();
        let mut result: Vec<WithItem> = vec![];
        while process {
            match cursor.node().kind() {
                "table_option_item" => {
                    cursor.goto_first_child();
                    let key = NodeFuncs::as_string(&cursor.node(), source);
                    cursor.goto_next_sibling();
                    // consume the '='
                    cursor.goto_next_sibling();
                    //
                    if cursor.node().kind().eq("table_option_value") {
                        if key.to_uppercase().eq("ID") {
                            result.push(WithItem::ID(NodeFuncs::as_string(&cursor.node(), source)));
                        } else {
                            result.push(WithItem::Option {
                                key,
                                value: OptionValue::Literal(NodeFuncs::as_string(
                                    &cursor.node(),
                                    source,
                                )),
                            });
                        }
                    } else if cursor.node().kind().eq("option_hash") {
                        result.push(WithItem::Option {
                            key,
                            value: OptionValue::Hash(CassandraParser::parse_map(
                                &cursor.node(),
                                source,
                            )),
                        });
                    }
                    cursor.goto_parent();
                }
                "clustering_order" => {
                    cursor.goto_first_child();
                    // consume CLUSTERING
                    cursor.goto_next_sibling();
                    // consume ORDER
                    cursor.goto_next_sibling();
                    // consume BY
                    cursor.goto_next_sibling();
                    // consume '('
                    cursor.goto_next_sibling();
                    result.push(WithItem::ClusterOrder(OrderClause {
                        name: NodeFuncs::as_string(&cursor.node(), source),
                        desc: {
                            // consume the name
                            if cursor.goto_next_sibling() {
                                cursor.node().kind().eq("DESC")
                            } else {
                                false
                            }
                        },
                    }));
                    cursor.goto_parent();
                }
                "compact_storage" => result.push(WithItem::CompactStorage),
                _ => {}
            }
            process = cursor.goto_next_sibling();
        }
        result
    }

    fn parse_materialized_where(node: &Node, source: &str) -> Vec<RelationElement> {
        let mut relations: Vec<RelationElement> = vec![];
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consumer the WHERE
        while cursor.goto_next_sibling() {
            if cursor.node().kind().eq("column_not_null") {
                cursor.goto_first_child();
                relations.push(RelationElement {
                    obj: Operand::COLUMN(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::IsNot,
                    value: vec![Operand::NULL],
                });
                cursor.goto_parent();
            }
            if cursor.node().kind().eq("relation_element") {
                relations.push(CassandraParser::parse_relation_element(
                    &cursor.node(),
                    source,
                ));
            }
        }
        relations
    }
    fn parse_create_materialized_vew(node: &Node, source: &str) -> CreateMaterializedViewData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'CREATE'
        cursor.goto_next_sibling();
        CreateMaterializedViewData {
            if_not_exists: CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor),
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            columns: {
                cursor.goto_next_sibling();
                // consume 'AS'
                cursor.goto_next_sibling();
                // consume 'select'
                cursor.goto_next_sibling();
                CassandraParser::parse_column_list(&cursor.node(), source)
            },
            table: {
                cursor.goto_next_sibling();
                // consume 'FROM'
                cursor.goto_next_sibling();
                CassandraParser::parse_table_name(&cursor.node(), source)
            },
            where_clause: {
                cursor.goto_next_sibling();
                CassandraParser::parse_materialized_where(&cursor.node(), source)
            },
            key: {
                cursor.goto_next_sibling();
                CassandraParser::parse_primary_key_element(&cursor.node(), source)
            },
            with: {
                if cursor.goto_next_sibling() {
                    CassandraParser::parse_with_element(&cursor.node(), source)
                } else {
                    vec![]
                }
            },
        }
    }

    fn parse_create_table(node: &Node, source: &str) -> CreateTableData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut result = CreateTableData {
            if_not_exists: CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor),
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            columns: vec![],
            key: None,
            with: vec![],
        };
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "column_definition_list" => {
                    let mut process = cursor.goto_first_child();

                    while process {
                        if cursor.node().kind().eq("column_definition") {
                            result
                                .columns
                                .push(CassandraParser::parse_column_definition(
                                    &cursor.node(),
                                    source,
                                ))
                        }
                        if cursor.node().kind().eq("primary_key_element") {
                            result.key = Some(CassandraParser::parse_primary_key_element(
                                &cursor.node(),
                                source,
                            ));
                        }
                        process = cursor.goto_next_sibling();
                    }
                    cursor.goto_parent();
                }
                "with_element" => {
                    result.with = CassandraParser::parse_with_element(&cursor.node(), source);
                }
                _ => {}
            }
        }
        result
    }

    fn parse_with_element(node: &Node, source: &str) -> WithElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        while cursor.goto_next_sibling() {
            if cursor.node().kind().eq("table_options") {
                return CassandraParser::parse_table_options(&cursor.node(), source);
            }
        }
        vec![]
    }
    fn parse_index_data(node: &Node, source: &str) -> IndexData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut result = IndexData {
            if_not_exists: CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor),
            name: None,
            table: "".to_string(),
            column: IndexColumnType::COLUMN("".to_string()),
        };
        let mut process = true;
        while process {
            match cursor.node().kind() {
                "short_index_name" => {
                    cursor.goto_first_child();
                    result.name = Some(NodeFuncs::as_string(&cursor.node(), source));
                    cursor.goto_parent();
                }
                "table_name" => {
                    cursor.goto_first_child();
                    result.table = CassandraParser::parse_dotted_name(&mut cursor, source);
                    cursor.goto_parent();
                }
                "index_column_spec" => {
                    cursor.goto_first_child();
                    result.column = match cursor.node().kind() {
                        "index_keys_spec" => {
                            cursor.goto_first_child();
                            cursor.goto_next_sibling();
                            // consume '('
                            cursor.goto_next_sibling();
                            IndexColumnType::KEYS(NodeFuncs::as_string(&cursor.node(), source))
                        }
                        "index_entries_s_spec" => {
                            cursor.goto_first_child();
                            cursor.goto_next_sibling();
                            // consume '('
                            cursor.goto_next_sibling();
                            IndexColumnType::ENTRIES(NodeFuncs::as_string(&cursor.node(), source))
                        }
                        "index_full_spec" => {
                            cursor.goto_next_sibling();
                            // consume '('
                            cursor.goto_first_child();
                            cursor.goto_next_sibling();
                            // consume '('
                            cursor.goto_next_sibling();
                            IndexColumnType::FULL(NodeFuncs::as_string(&cursor.node(), source))
                        }
                        _ => IndexColumnType::COLUMN(NodeFuncs::as_string(&cursor.node(), source)),
                    };
                    cursor.goto_parent();
                }
                _ => {}
            }
            process = cursor.goto_next_sibling();
        }
        result
    }
    fn parse_list_role_data(node: &Node, source: &str) -> ListRoleData {
        let mut cursor = node.walk();
        let mut result = ListRoleData {
            of: None,
            no_recurse: false,
        };
        cursor.goto_first_child();
        // consume 'LIST'
        cursor.goto_next_sibling();
        // consume 'ROLES'
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "role" => result.of = Some(NodeFuncs::as_string(&cursor.node(), source)),
                "NORECURSIVE" => result.no_recurse = true,
                _ => {}
            }
        }
        result
    }

    fn parse_resource(node: &Node, source: &str) -> Resource {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        match cursor.node().kind() {
            "ALL" => {
                cursor.goto_next_sibling();
                match cursor.node().kind() {
                    "FUNCTIONS" => {
                        if cursor.goto_next_sibling() {
                            // consume 'IN'
                            cursor.goto_next_sibling();
                            // consume 'KEYSPACE'
                            cursor.goto_next_sibling();
                            Resource::AllFunctions(Some(NodeFuncs::as_string(
                                &cursor.node(),
                                source,
                            )))
                        } else {
                            Resource::AllFunctions(None)
                        }
                    }
                    "KEYSPACES" => Resource::AllKeyspaces,
                    "ROLES" => Resource::AllRoles,
                    _ => unreachable!(),
                }
            }
            "FUNCTION" => {
                cursor.goto_next_sibling();
                Resource::Function(CassandraParser::parse_dotted_name(&mut cursor, source))
            }
            "KEYSPACE" => {
                cursor.goto_next_sibling();
                Resource::Keyspace(NodeFuncs::as_string(&cursor.node(), source))
            }
            "ROLE" => {
                cursor.goto_next_sibling();
                Resource::Role(NodeFuncs::as_string(&cursor.node(), source))
            }
            "TABLE" => {
                cursor.goto_next_sibling();
                Resource::Table(CassandraParser::parse_dotted_name(&mut cursor, source))
            }
            _ => Resource::Table(CassandraParser::parse_dotted_name(&mut cursor, source)),
        }
    }

    fn parse_role_data(node: &Node, source: &str) -> RoleData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor);
        let mut result = RoleData {
            name: NodeFuncs::as_string(&cursor.node(), source),
            password: None,
            superuser: None,
            login: None,
            options: vec![],
            if_not_exists,
        };
        cursor.goto_next_sibling();
        if cursor.node().kind().eq("role_with") {
            cursor.goto_first_child();
            // consume "WITH"
            while cursor.goto_next_sibling() {
                if cursor.node().kind().eq("role_with_option") {
                    cursor.goto_first_child();
                    match cursor.node().kind() {
                        "PASSWORD" => {
                            cursor.goto_next_sibling();
                            // consume the '='
                            cursor.goto_next_sibling();
                            result.password =
                                Some(NodeFuncs::as_string(&cursor.node(), source));
                            cursor.goto_next_sibling();
                        }
                        "LOGIN" => {
                            cursor.goto_next_sibling();
                            // consume the '='
                            cursor.goto_next_sibling();
                            result.login = Some(NodeFuncs::as_boolean(&cursor.node(), source));
                            cursor.goto_next_sibling();
                        }
                        "SUPERUSER" => {
                            cursor.goto_next_sibling();
                            // consume the '='
                            cursor.goto_next_sibling();
                            result.superuser =
                                Some(NodeFuncs::as_boolean(&cursor.node(), source));
                            cursor.goto_next_sibling();
                        }
                        "OPTIONS" => {
                            cursor.goto_next_sibling();
                            // consume the '='
                            cursor.goto_next_sibling();
                            result.options = CassandraParser::parse_map(&cursor.node(), source);
                            cursor.goto_next_sibling();
                        }
                        _ => unreachable!(),
                    }
                    cursor.goto_parent();
                }
            }
        }
        result
    }

    fn consume_2_keywords_and_check_not_exists(cursor: &mut TreeCursor) -> bool {
        let mut if_not_exists = false;
        // consume first keyword
        cursor.goto_next_sibling();
        // consume second keyword
        cursor.goto_next_sibling();
        if cursor.node().kind().eq("IF") {
            // consume 'IF'
            cursor.goto_next_sibling();
            // consume 'NOT'
            cursor.goto_next_sibling();
            // consume 'EXISTS'
            cursor.goto_next_sibling();
            if_not_exists = true;
        }
        if_not_exists
    }

    fn consume_2_keywords_and_check_exists(cursor: &mut TreeCursor) -> bool {
        let mut if_exists = false;
        // consume first keyword
        cursor.goto_next_sibling();
        // consume second keyword
        cursor.goto_next_sibling();
        if cursor.node().kind().eq("IF") {
            // consume 'IF'
            cursor.goto_next_sibling();
            // consume 'EXISTS'
            cursor.goto_next_sibling();
            if_exists = true;
        }
        if_exists
    }

    fn parse_keyspace_data(node: &Node, source: &str) -> KeyspaceData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor);
        let mut result = KeyspaceData {
            name: NodeFuncs::as_string(&cursor.node(), source),
            replication: vec![],
            durable_writes: None,
            if_not_exists,
        };
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "replication_list" => {
                    result.replication = CassandraParser::parse_map(&cursor.node(), source);
                }
                "durable_writes" => {
                    cursor.goto_first_child();
                    // consume "DURABLE_WRITES"
                    cursor.goto_next_sibling();
                    // consume "="
                    cursor.goto_next_sibling();
                    result.durable_writes = Some(NodeFuncs::as_boolean(&cursor.node(), source));
                    cursor.goto_parent();
                }
                _ => {}
            }
        }

        result
    }
    fn parse_user_data(node: &Node, source: &str) -> UserData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor);

        let mut result = UserData {
            name: NodeFuncs::as_string(&cursor.node(), source),
            password: None,
            superuser: false,
            no_superuser: false,
            if_not_exists,
        };
        cursor.goto_next_sibling();
        if cursor.node().kind().eq("user_with") {
            cursor.goto_first_child();
            // consume "WITH"
            while cursor.goto_next_sibling() {
                match cursor.node().kind() {
                    "user_password" => {
                        cursor.goto_first_child();
                        // consumer "PASSWORD"
                        cursor.goto_next_sibling();
                        result.password = Some(NodeFuncs::as_string(&cursor.node(), source));
                        cursor.goto_parent();
                    }
                    "user_super_user" => {
                        cursor.goto_first_child();
                        match cursor.node().kind() {
                            "SUPERUSER" => result.superuser = true,
                            "NOSUPERUSER" => result.no_superuser = true,
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        result
    }

    pub fn parse_update_statement(node: &Node, source: &str) -> UpdateStatementData {
        let mut statement_data = UpdateStatementData {
            begin_batch: None,
            modifiers: StatementModifiers::new(),
            table_name: String::from(""),
            using_ttl: None,
            assignments: vec![],
            where_clause: vec![],
            if_spec: None,
        };
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            match cursor.node().kind() {
                "begin_batch" => {
                    statement_data.begin_batch =
                        Some(CassandraParser::parse_begin_batch(&cursor.node(), source))
                }
                "UPDATE" => {
                    cursor.goto_next_sibling();
                    statement_data.table_name =
                        CassandraParser::parse_dotted_name(&mut cursor, source);
                }
                "using_ttl_timestamp" => {
                    statement_data.using_ttl =
                        Some(CassandraParser::parse_ttl_timestamp(&cursor.node(), source));
                }
                "assignment_element" => {
                    statement_data
                        .assignments
                        .push(CassandraParser::parse_assignment_element(
                            &cursor.node(),
                            source,
                        ));
                }
                "where_spec" => {
                    statement_data.where_clause =
                        CassandraParser::parse_where_spec(&cursor.node(), source);
                }
                "IF" => {
                    // consume EXISTS
                    cursor.goto_next_sibling();
                    statement_data.modifiers.exists = true;
                }
                "if_spec" => {
                    cursor.goto_first_child();
                    // consume IF
                    cursor.goto_next_sibling();
                    statement_data.if_spec =
                        CassandraParser::parse_if_condition_list(&cursor.node(), source);
                    cursor.goto_parent();
                }
                _ => {}
            }
            process = cursor.goto_next_sibling();
        }
        statement_data
    }

    fn parse_privilege(node: &Node, source: &str) -> Privilege {
        match NodeFuncs::as_string(node, source).to_uppercase().as_str() {
            "ALL" | "ALL PERMISSIONS" => Privilege::ALL,
            "ALTER" => Privilege::ALTER,
            "AUTHORIZE" => Privilege::AUTHORIZE,
            "DESCRIBE" => Privilege::DESCRIBE,
            "EXECUTE" => Privilege::EXECUTE,
            "CREATE" => Privilege::CREATE,
            "DROP" => Privilege::DROP,
            "MODIFY" => Privilege::MODIFY,
            "SELECT" => Privilege::SELECT,
            _ => unreachable!(),
        }
    }

    fn parse_privilege_data(node: &Node, source: &str) -> PrivilegeData {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let mut privilege: Option<Privilege> = None;
        let mut resource: Option<Resource> = None;
        let mut role: Option<String> = None;
        // consume 'GRANT/REVOKE'
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "privilege" => {
                    privilege = Some(CassandraParser::parse_privilege(&cursor.node(), source));
                }
                "resource" => {
                    resource = Some(CassandraParser::parse_resource(&cursor.node(), source));
                }
                "role" => role = Some(NodeFuncs::as_string(&cursor.node(), source)),
                _ => {}
            }
        }
        PrivilegeData {
            privilege: privilege.unwrap(),
            resource,
            role,
        }
    }

    fn parse_assignment_element(node: &Node, source: &str) -> AssignmentElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let name = CassandraParser::parse_indexed_column(&mut cursor, source);
        // consume the '='
        cursor.goto_next_sibling();
        let value = CassandraParser::parse_operand(&cursor.node(), source);
        let mut result = AssignmentElement {
            name,
            value,
            operator: None,
        };
        if cursor.goto_next_sibling() {
            // we have +/- value
            result.operator = Some(if cursor.node().kind().eq("+") {
                cursor.goto_next_sibling();
                AssignmentOperator::Plus(CassandraParser::parse_operand(&cursor.node(), source))
            } else {
                cursor.goto_next_sibling();
                AssignmentOperator::Minus(CassandraParser::parse_operand(&cursor.node(), source))
            });
        }
        result
    }

    pub fn parse_delete_statement(node: &Node, source: &str) -> DeleteStatementData {
        let mut statement_data = DeleteStatementData {
            begin_batch: None,
            modifiers: StatementModifiers::new(),
            table_name: String::from(""),
            columns: None,
            timestamp: None,
            where_clause: vec![],
            if_spec: None,
        };

        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            match cursor.node().kind() {
                "begin_batch" => {
                    statement_data.begin_batch =
                        Some(CassandraParser::parse_begin_batch(&cursor.node(), source))
                }
                "delete_column_list" => {
                    // goto delete_column_item
                    let mut delete_columns = vec![];
                    process = cursor.goto_first_child();
                    while process {
                        delete_columns.push(CassandraParser::parse_delete_column_item(
                            &cursor.node(),
                            source,
                        ));
                        // consume the column
                        cursor.goto_next_sibling();
                        process = cursor.goto_next_sibling();
                        // consume the ',' if any
                        cursor.goto_next_sibling();
                    }
                    // bring the cursor back to delete_column_list
                    cursor.goto_parent();
                    statement_data.columns = Some(delete_columns);
                }
                "from_spec" => {
                    statement_data.table_name =
                        CassandraParser::parse_from_spec(&cursor.node(), source);
                }
                "using_timestamp_spec" => {
                    statement_data.timestamp =
                        CassandraParser::parse_using_timestamp(&cursor.node(), source);
                }
                "where_spec" => {
                    statement_data.where_clause =
                        CassandraParser::parse_where_spec(&cursor.node(), source);
                }
                "IF" => {
                    // consume EXISTS
                    cursor.goto_next_sibling();
                    statement_data.modifiers.exists = true;
                }
                "if_spec" => {
                    cursor.goto_first_child();
                    // consume IF
                    cursor.goto_next_sibling();
                    statement_data.if_spec =
                        CassandraParser::parse_if_condition_list(&cursor.node(), source);
                    cursor.goto_parent();
                }
                _ => {}
            }
            process = cursor.goto_next_sibling();
        }
        statement_data
    }

    fn parse_if_condition_list(node: &Node, source: &str) -> Option<Vec<(String, String)>> {
        let mut result = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();
        while process {
            cursor.goto_first_child();
            let column = NodeFuncs::as_string(&cursor.node(), source);
            // consume the '='
            cursor.goto_next_sibling();
            cursor.goto_next_sibling();
            let value = NodeFuncs::as_string(&cursor.node(), source);
            result.push((column, value));
            cursor.goto_parent();
            process = cursor.goto_next_sibling();
            if process {
                // we found 'AND' so get real next node
                cursor.goto_next_sibling();
            }
        }
        Some(result)
    }

    fn parse_delete_column_item(node: &Node, source: &str) -> IndexedColumn {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        CassandraParser::parse_indexed_column(&mut cursor, source)
    }

    fn parse_indexed_column(cursor: &mut TreeCursor, source: &str) -> IndexedColumn {
        IndexedColumn {
            column: NodeFuncs::as_string(&cursor.node(), source),

            value: if cursor.goto_next_sibling() && cursor.node().kind().eq("[") {
                // consume '['
                cursor.goto_next_sibling();
                let result = Some(NodeFuncs::as_string(&cursor.node(), source));
                // consume ']'
                cursor.goto_next_sibling();
                result
            } else {
                None
            },
        }
    }

    pub fn parse_insert_statement(node: &Node, source: &str) -> InsertStatementData {
        let mut statement_data = InsertStatementData {
            begin_batch: None,
            modifiers: StatementModifiers::new(),
            table_name: String::from(""),
            columns: None,
            values: None,
            using_ttl: None,
        };

        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            match cursor.node().kind() {
                "begin_batch" => {
                    statement_data.begin_batch =
                        Some(CassandraParser::parse_begin_batch(&cursor.node(), source))
                }
                "table_name" => {
                    statement_data.table_name =
                        CassandraParser::parse_table_name(&cursor.node(), source);
                }
                "insert_column_spec" => {
                    cursor.goto_first_child();
                    // consume the '(' at the beginning
                    while cursor.goto_next_sibling() {
                        if cursor.node().kind().eq("column_list") {
                            statement_data.columns =
                                Some(CassandraParser::parse_column_list(&cursor.node(), source));
                        }
                    }
                    cursor.goto_parent();
                }
                "insert_values_spec" => {
                    cursor.goto_first_child();
                    match cursor.node().kind() {
                        "VALUES" => {
                            cursor.goto_next_sibling();
                            // consume the '('
                            cursor.goto_next_sibling();
                            let expression_list =
                                CassandraParser::parse_expression_list(&cursor.node(), source);
                            statement_data.values = Some(InsertValues::VALUES(expression_list));
                        }
                        "JSON" => {
                            cursor.goto_next_sibling();
                            statement_data.values = Some(InsertValues::JSON(NodeFuncs::as_string(
                                &cursor.node(),
                                source,
                            )));
                        }
                        _ => {}
                    }
                    cursor.goto_parent();
                }
                "IF" => {
                    // consume NOT
                    cursor.goto_next_sibling();
                    // consume EXISTS
                    cursor.goto_next_sibling();
                    statement_data.modifiers.not_exists = true;
                }
                "using_ttl_timestamp" => {
                    statement_data.using_ttl =
                        Some(CassandraParser::parse_ttl_timestamp(&cursor.node(), source));
                }
                _ => {}
            }
            process = cursor.goto_next_sibling();
        }
        statement_data
    }

    fn parse_column_list(node: &Node, source: &str) -> Vec<String> {
        let mut result: Vec<String> = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            if cursor.node().kind().eq("column") {
                result.push(NodeFuncs::as_string(&cursor.node(), source));
            }
            process = cursor.goto_next_sibling();
            // consume ',' if it is there
            cursor.goto_next_sibling();
        }
        result
    }

    fn parse_using_timestamp(node: &Node, source: &str) -> Option<u64> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "USING"
        cursor.goto_next_sibling();
        // consume "TIMESTAMP"
        cursor.goto_next_sibling();
        Some(
            NodeFuncs::as_string(&cursor.node(), source)
                .parse::<u64>()
                .unwrap(),
        )
    }

    fn parse_ttl_timestamp(node: &Node, source: &str) -> TtlTimestamp {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "USING"
        let mut ttl: Option<u64> = None;
        let mut timestamp: Option<u64> = None;
        while (ttl.is_none() || timestamp.is_none()) && cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "ttl" => {
                    ttl = Some(
                        NodeFuncs::as_string(&cursor.node(), source)
                            .parse::<u64>()
                            .unwrap(),
                    );
                }
                "time" => {
                    timestamp = Some(
                        NodeFuncs::as_string(&cursor.node(), source)
                            .parse::<u64>()
                            .unwrap(),
                    );
                }
                _ => {}
            }
        }
        TtlTimestamp { ttl, timestamp }
    }

    fn parse_from_spec(node: &Node, source: &str) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'FROM'
        cursor.goto_next_sibling();
        CassandraParser::parse_table_name(&cursor.node(), source)
    }

    fn parse_dotted_name(cursor: &mut TreeCursor, source: &str) -> String {
        let mut result = NodeFuncs::as_string(&cursor.node(), source);
        if cursor.goto_next_sibling() {
            // we have fully qualified name
            result.push('.');
            // consume '.'
            cursor.goto_next_sibling();
            result.push_str(NodeFuncs::as_string(&cursor.node(), source).as_str());
        }
        result
    }
    fn parse_table_name(node: &Node, source: &str) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        CassandraParser::parse_dotted_name(&mut cursor, source)
    }

    fn parse_function_args(node: &Node, source: &str) -> Vec<Operand> {
        let mut result = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            result.push(CassandraParser::parse_operand(&cursor.node(), source));
            process = cursor.goto_next_sibling();
            if process {
                // skip over the ','
                cursor.goto_next_sibling();
            }
        }
        result
    }

    fn parse_expression_list(node: &Node, source: &str) -> Vec<Operand> {
        let mut result = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            if cursor.node().kind().eq("expression") {
                cursor.goto_first_child();
                result.push(CassandraParser::parse_operand(&cursor.node(), source));
                cursor.goto_parent();
            }
            process = cursor.goto_next_sibling();
        }
        result
    }

    fn parse_operand(node: &Node, source: &str) -> Operand {
        match node.kind() {
            "constant" => Operand::CONST(NodeFuncs::as_string(node, source)),
            "column" => Operand::COLUMN(NodeFuncs::as_string(node, source)),
            "assignment_tuple" => {
                Operand::TUPLE(CassandraParser::parse_assignment_tuple(node, source))
            }
            "assignment_map" => Operand::MAP(CassandraParser::parse_assignment_map(node, source)),
            "assignment_list" => {
                Operand::LIST(CassandraParser::parse_assignment_list(node, source))
            }
            "assignment_set" => Operand::SET(CassandraParser::parse_assignment_set(node, source)),
            "function_args" => Operand::TUPLE(CassandraParser::parse_function_args(node, source)),
            "function_call" => Operand::FUNC(NodeFuncs::as_string(node, source)),
            // TODO should this be unreachable()?
            _ => Operand::CONST(NodeFuncs::as_string(node, source)),
        }
    }

    // parses lists of option_hash_item or replication_list_item
    fn parse_map(node: &Node, source: &str) -> Vec<(String, String)> {
        let mut cursor = node.walk();

        cursor.goto_first_child();
        let mut entries: Vec<(String, String)> = vec![];
        // { const : const, ... }
        // we are on the '{' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "}" | "," => {}
                "option_hash_item" | "replication_list_item" => {
                    cursor.goto_first_child();
                    let key = NodeFuncs::as_string(&cursor.node(), source);
                    cursor.goto_next_sibling();
                    // consume the ':'
                    cursor.goto_next_sibling();
                    let value = NodeFuncs::as_string(&cursor.node(), source);
                    entries.push((key, value));
                    cursor.goto_parent();
                }
                _ => unreachable!(),
            }
        }
        cursor.goto_parent();
        entries
    }

    fn parse_assignment_map(node: &Node, source: &str) -> Vec<(String, String)> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut entries: Vec<(String, String)> = vec![];
        cursor.goto_first_child();
        // { const : const, ... }
        // we are on the '{' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "}" | "," => {}
                _ => {
                    let key = NodeFuncs::as_string(&cursor.node(), source);
                    cursor.goto_next_sibling();
                    // consume the ':'
                    cursor.goto_next_sibling();
                    let value = NodeFuncs::as_string(&cursor.node(), source);
                    entries.push((key, value));
                }
            }
        }
        cursor.goto_parent();
        entries
    }

    fn parse_assignment_list(node: &Node, source: &str) -> Vec<String> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // [ const, const, ... ]
        let mut entries: Vec<String> = vec![];
        // we are on the '[' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "]" | "," => {}
                _ => {
                    entries.push(NodeFuncs::as_string(&cursor.node(), source));
                }
            }
        }
        entries
    }

    fn parse_assignment_set(node: &Node, source: &str) -> Vec<String> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // { const, const, ... }
        let mut entries: Vec<String> = vec![];
        // we are on the '{' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "}" | "," => {}
                _ => {
                    entries.push(NodeFuncs::as_string(&cursor.node(), source));
                }
            }
        }
        entries
    }

    fn parse_assignment_tuple(node: &Node, source: &str) -> Vec<Operand> {
        // ( expression, expression ... )
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume '('
        cursor.goto_next_sibling();
        // now on 'expression-list'
        CassandraParser::parse_expression_list(&cursor.node(), source)
    }

    fn parse_begin_batch(node: &Node, source: &str) -> BeginBatch {
        let mut result = BeginBatch::new();

        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume BEGIN
        cursor.goto_next_sibling();

        let node = cursor.node();
        result.logged = node.kind().eq("LOGGED");
        result.unlogged = node.kind().eq("UNLOGGED");
        if result.logged || result.unlogged {
            // used a node so advance
            cursor.goto_next_sibling();
        }
        // consume BATCH
        if cursor.goto_next_sibling() {
            // we should have using_timestamp_spec
            result.timestamp = CassandraParser::parse_using_timestamp(&cursor.node(), source)
        }

        result
    }

    pub fn parse_select_statement(node: &Node, source: &str) -> SelectStatementData {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let mut statement_data = SelectStatementData {
            modifiers: StatementModifiers::new(),
            elements: vec![],
            table_name: String::new(),
            where_clause: None,
            order: None,
        };
        // we are on SELECT so we can just start
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "DISTINCT" => statement_data.modifiers.distinct = true,
                "JSON" => statement_data.modifiers.json = true,
                "select_elements" => {
                    let mut process = cursor.goto_first_child();
                    while process {
                        match cursor.node().kind() {
                            "select_element" => {
                                statement_data
                                    .elements
                                    .push(CassandraParser::parse_select_element(
                                        &cursor.node(),
                                        source,
                                    ))
                            }
                            "*" => statement_data.elements.push(SelectElement::Star),
                            _ => {}
                        }
                        process = cursor.goto_next_sibling();
                    }
                    cursor.goto_parent();
                }
                "from_spec" => {
                    statement_data.table_name =
                        CassandraParser::parse_from_spec(&cursor.node(), source)
                }
                "where_spec" => {
                    statement_data.where_clause =
                        Some(CassandraParser::parse_where_spec(&cursor.node(), source))
                }
                "order_spec" => {
                    statement_data.order = CassandraParser::parse_order_spec(&cursor.node(), source)
                }
                "limit_spec" => {
                    cursor.goto_first_child();
                    // consume LIMIT
                    cursor.goto_next_sibling();
                    statement_data.modifiers.limit = Some(
                        NodeFuncs::as_string(&cursor.node(), source)
                            .parse::<i32>()
                            .unwrap(),
                    );
                    cursor.goto_parent();
                }
                "ALLOW" => {
                    // consume 'FILTERING'
                    cursor.goto_next_sibling();
                    statement_data.modifiers.filtering = true
                }
                _ => {}
            }
        }
        statement_data
    }

    fn parse_where_spec(node: &Node, source: &str) -> Vec<RelationElement> {
        // (where_spec (relation_elements (relation_element (constant))))
        let mut result = vec![];
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume the "WHERE"
        cursor.goto_next_sibling();
        // now on relation_elements.
        let mut process = cursor.goto_first_child();
        // now on first relation.
        while process {
            result.push(CassandraParser::parse_relation_element(
                &cursor.node(),
                source,
            ));
            process = cursor.goto_next_sibling();
            // consume the 'AND' if it exists
            cursor.goto_next_sibling();
        }
        result
    }

    fn parse_relation_element(node: &Node, source: &str) -> RelationElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        match cursor.node().kind() {
            "relation_contains_key" => {
                cursor.goto_first_child();
                RelationElement {
                    obj: Operand::COLUMN(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::ContainsKey,
                    value: {
                        // consume column value
                        cursor.goto_next_sibling();
                        // consume 'CONTAINS'
                        cursor.goto_next_sibling();
                        // consume 'KEY'
                        cursor.goto_next_sibling();
                        vec![Operand::CONST(NodeFuncs::as_string(&cursor.node(), source))]
                    },
                }
            }
            "relation_contains" => {
                cursor.goto_first_child();
                RelationElement {
                    obj: Operand::COLUMN(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::Contains,
                    value: {
                        // consume column value
                        cursor.goto_next_sibling();
                        // consume 'CONTAINS'
                        cursor.goto_next_sibling();
                        vec![Operand::CONST(NodeFuncs::as_string(&cursor.node(), source))]
                    },
                }
            }
            _ => {
                RelationElement {
                    obj: CassandraParser::parse_relation_value(&mut cursor, source),
                    oper: {
                        // consumer the obj
                        cursor.goto_next_sibling();
                        CassandraParser::parse_operator(&mut cursor)
                    },
                    value: {
                        // consume the oper
                        cursor.goto_next_sibling();
                        let mut values = vec![];
                        let inline_tuple = if cursor.node().kind().eq("(") {
                            // inline tuple or function_args
                            cursor.goto_next_sibling();
                            true
                        } else {
                            false
                        };
                        values.push(CassandraParser::parse_operand(&cursor.node(), source));
                        cursor.goto_next_sibling();
                        while cursor.node().kind().eq(",") {
                            cursor.goto_next_sibling();
                            values.push(CassandraParser::parse_operand(&cursor.node(), source));
                        }
                        if inline_tuple && values.len() > 1 {
                            vec![Operand::TUPLE(values)]
                        } else {
                            values
                        }
                    },
                }
            }
        }
    }

    fn parse_operator(cursor: &mut TreeCursor) -> RelationOperator {
        let node = cursor.node();
        let kind = node.kind();
        match kind {
            "<" => RelationOperator::LT,
            "<=" => RelationOperator::LE,
            "<>" => RelationOperator::NE,
            "=" => RelationOperator::EQ,
            ">=" => RelationOperator::GE,
            ">" => RelationOperator::GT,
            "IN" => RelationOperator::IN,

            _ => {
                unreachable!("Unknown operator: {}", kind);
            }
        }
    }

    fn parse_relation_value(cursor: &mut TreeCursor, source: &str) -> Operand {
        let node = cursor.node();
        let kind = node.kind();
        match kind {
            "column" => Operand::COLUMN(NodeFuncs::as_string(&node, source)),
            "function_call" => Operand::FUNC(NodeFuncs::as_string(&node, source)),
            "(" => {
                let mut values: Vec<Operand> = Vec::new();
                // consume '('
                cursor.goto_next_sibling();
                while !cursor.node().kind().eq(")") {
                    match cursor.node().kind() {
                        "," => {}
                        _ => values.push(CassandraParser::parse_relation_value(cursor, source)),
                    }
                    cursor.goto_next_sibling();
                }
                Operand::TUPLE(values)
            }
            _ => Operand::CONST(NodeFuncs::as_string(&node, source)),
        }
    }

    fn parse_order_spec(node: &Node, source: &str) -> Option<OrderClause> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "ORDER"
        cursor.goto_next_sibling();
        // consume "BY"
        cursor.goto_next_sibling();
        Some(OrderClause {
            name: NodeFuncs::as_string(&cursor.node(), source),
            desc: {
                // consume the name
                if cursor.goto_next_sibling() {
                    cursor.node().kind().eq("DESC")
                } else {
                    false
                }
            },
        })
    }

    fn parse_select_element(node: &Node, source: &str) -> SelectElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let type_ = cursor.node();

        let alias = if cursor.goto_next_sibling() {
            // we have an alias
            // consume 'AS'
            cursor.goto_next_sibling();
            Some(NodeFuncs::as_string(&cursor.node(), source))
        } else {
            None
        };
        match type_.kind() {
            "column" => SelectElement::Column(Named {
                name: NodeFuncs::as_string(&type_, source),
                alias,
            }),
            "function_call" => SelectElement::Function(Named {
                name: NodeFuncs::as_string(&type_, source),
                alias,
            }),
            _ => SelectElement::DotStar(NodeFuncs::as_string(&type_, source)),
        }
    }

    fn parse_standard_drop(node: &Node, source: &str) -> DropData {
        let mut cursor = node.walk();
        let mut if_exists = false;
        cursor.goto_first_child();
        // consume 'DROP'
        cursor.goto_next_sibling();
        // consume type
        if cursor.node().kind().eq("MATERIALIZED") {
            cursor.goto_next_sibling();
        }
        cursor.goto_next_sibling();
        if cursor.node().kind().eq("IF") {
            if_exists = true;
            // consume 'IF'
            cursor.goto_next_sibling();
            // consume 'EXISTS'
            cursor.goto_next_sibling();
        }
        DropData {
            name: CassandraParser::parse_dotted_name(&mut cursor, source),
            if_exists,
        }
    }

    fn parse_drop_trigger(node: &Node, source: &str) -> DropTriggerData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        DropTriggerData {
            if_exists: CassandraParser::consume_2_keywords_and_check_exists(&mut cursor),
            name: { CassandraParser::parse_table_name(&cursor.node(), source) },
            table: {
                cursor.goto_next_sibling();
                // consume 'ON'
                cursor.goto_next_sibling();
                CassandraParser::parse_table_name(&cursor.node(), source)
            },
        }
    }
}

impl Display for CassandraStatement {

    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CassandraStatement::AlterKeyspace(keyspace_data) => write!(f, "ALTER {}", keyspace_data),
            CassandraStatement::AlterMaterializedView(alter_data) => write!(f, "{}", alter_data),
            CassandraStatement::AlterRole(role_data) => write!(f,"ALTER {}", role_data),
            CassandraStatement::AlterTable(table_data) => {
                write!(f,"ALTER TABLE {} {}", table_data.name, table_data.operation)
            }
            CassandraStatement::AlterType(alter_type_data) => write!(f, "{}", alter_type_data),
            CassandraStatement::AlterUser(user_data) => write!(f,"ALTER {}", user_data),
            CassandraStatement::ApplyBatch => write!(f,"APPLY BATCH"),
            CassandraStatement::CreateAggregate(aggregate_data) => write!(f,"{}",aggregate_data),
            CassandraStatement::CreateFunction(function_data) => write!(f,"{}",function_data),
            CassandraStatement::CreateIndex(index_data) => write!(f,"{}",index_data),
            CassandraStatement::CreateKeyspace(keyspace_data) => {
                write!(f,"CREATE {}", keyspace_data)
            }
            CassandraStatement::CreateMaterializedView(view_data) => write!(f,"{}",view_data),
            CassandraStatement::CreateRole(role_data) => write!(f,"CREATE {}", role_data),
            CassandraStatement::CreateTable(table_data) => write!(f,"CREATE TABLE {}", table_data),
            CassandraStatement::CreateTrigger(trigger_data) => write!(f,"{}",trigger_data),
            CassandraStatement::CreateType(type_data) => write!(f,"{}",type_data),
            CassandraStatement::CreateUser(user_data) => write!(f,"CREATE {}", user_data),
            CassandraStatement::DeleteStatement(statement_data) => write!(f,"{}",statement_data),
            CassandraStatement::DropAggregate(drop_data) => write!(f,"{}",drop_data.get_text("AGGREGATE")),
            CassandraStatement::DropFunction(drop_data) => write!(f,"{}",drop_data.get_text("FUNCTION")),
            CassandraStatement::DropIndex(drop_data) => write!(f,"{}",drop_data.get_text("INDEX")),
            CassandraStatement::DropKeyspace(drop_data) => write!(f,"{}",drop_data.get_text("KEYSPACE")),
            CassandraStatement::DropMaterializedView(drop_data) => write!(f,"{}",drop_data.get_text("MATERIALIZED VIEW")),
            CassandraStatement::DropRole(drop_data) => write!(f,"{}",drop_data.get_text("ROLE")),
            CassandraStatement::DropTable(drop_data) => write!(f,"{}",drop_data.get_text("TABLE")),
            CassandraStatement::DropTrigger(drop_trigger_data) => write!(f,"{}",drop_trigger_data),
            CassandraStatement::DropType(drop_data) => write!(f,"{}",drop_data.get_text("TYPE")),
            CassandraStatement::DropUser(drop_data) => write!(f,"{}",drop_data.get_text("USER")),
            CassandraStatement::Grant(grant_data) => write!(f,"GRANT {} ON {} TO {}",
                grant_data.privilege,
                grant_data.resource.as_ref().unwrap(),
                &grant_data.role.as_ref().unwrap()
            ),
            CassandraStatement::InsertStatement(statement_data) => write!(f,"{}",statement_data),
            CassandraStatement::ListPermissions(grant_data) =>
                write!(f,"LIST {}{}{}", grant_data.privilege,
                grant_data.resource.as_ref().map_or( "".to_string(), |x| format!( " ON {}",x)),
                grant_data.role.as_ref().map_or( "".to_string(), |x| format!(" OF {}", x))),
            CassandraStatement::ListRoles(data) => write!(f,"{}",data),
            CassandraStatement::Revoke(grant_data) => write!(f,
                "REVOKE {} ON {} FROM {}",
                grant_data.privilege,
                grant_data.resource.as_ref().unwrap(),
                grant_data.role.as_ref().unwrap()
            ),
            CassandraStatement::SelectStatement(statement_data) => write!(f,"{}",statement_data),
            CassandraStatement::Truncate(table) => write!(f,"TRUNCATE TABLE {}", table),
            CassandraStatement::Update(statement_data) => write!(f,"{}",statement_data),
            CassandraStatement::UseStatement(keyspace) => write!(f,"USE {}", keyspace),
            CassandraStatement::UNKNOWN(query) => write!(f,"{}", query),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct FunctionData {
    or_replace: bool,
    not_exists: bool,
    name: String,
    params: Vec<ColumnDefinition>,
    return_null: bool,
    return_type: DataType,
    language: String,
    code_block: String,
}

impl Display for FunctionData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE {}FUNCTION {}{} ({}) {} ON NULL INPUT RETURNS {} LANGUAGE {} AS {}",
            if self.or_replace { "OR REPLACE " } else { "" },
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.params.iter().map(|x| x.to_string()).join(", "),
            if self.return_null {
                "RETURNS NULL"
            } else {
                "CALLED"
            },
            self.return_type,
            self.language,
            self.code_block
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum IndexColumnType {
    COLUMN(String),
    KEYS(String),
    ENTRIES(String),
    FULL(String),
}

impl Display for IndexColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexColumnType::COLUMN(name) => write!(f, "{}", name),
            IndexColumnType::KEYS(name) => write!(f, "KEYS( {} )", name),
            IndexColumnType::ENTRIES(name) => write!(f, "ENTRIES( {} )", name),
            IndexColumnType::FULL(name) => write!(f, "FULL( {} )", name),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct IndexData {
    if_not_exists: bool,
    name: Option<String>,
    table: String,
    column: IndexColumnType,
}

impl Display for IndexData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = if self.name.is_some() {
            format!("{} ", self.name.as_ref().unwrap().as_str())
        } else {
            "".to_string()
        };
        let exists = if self.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };

        write!(
            f,
            "CREATE INDEX {}{}ON {}( {} )",
            exists, name, self.table, self.column
        )
    }
}
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
pub enum Resource {
    // optional keyspace
    AllFunctions(Option<String>),
    AllKeyspaces,
    AllRoles,
    Function(String),
    Keyspace(String),
    Role(String),
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

#[derive(PartialEq, Debug, Clone)]
pub struct AggregateData {
    or_replace: bool,
    not_exists: bool,
    name: String,
    data_type: DataType,
    sfunc: String,
    stype: DataType,
    finalfunc: String,
    init_cond: InitCondition,
}

impl Display for AggregateData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE {}AGGREGATE {}{} ({}) SFUNC {} STYPE {} FINALFUNC {} INITCOND {}",
            if self.or_replace { "OR REPLACE " } else { "" },
            if self.not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            self.name,
            self.data_type,
            self.sfunc,
            self.stype,
            self.finalfunc,
            self.init_cond
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum InitCondition {
    Constant(String),
    List(Vec<InitCondition>),
    Map(Vec<(String, InitCondition)>),
}

impl Display for InitCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InitCondition::Constant(name) => write!(f, "{}", name),
            InitCondition::List(lst) => {
                write!(f, "({})", lst.iter().map(|x| x.to_string()).join(", "))
            }
            InitCondition::Map(entries) => write!(
                f,
                "({})",
                entries
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, v))
                    .join(", ")
            ),
        }
    }
}

pub struct CassandraAST {
    /// The query string
    text: String,
    /// the tree-sitter tree
    pub(crate) tree: Tree,
    /// the statement type of the query
    pub statement: CassandraStatement,
}

impl CassandraAST {
    /// create an AST from the query string
    pub fn new(cassandra_statement: String) -> CassandraAST {
        let language = tree_sitter_cql::language();
        let mut parser = tree_sitter::Parser::new();
        if parser.set_language(language).is_err() {
            panic!("language version mismatch");
        }

        // this code enables debug logging
        /*
        fn log( _x : LogType, message : &str) {
            println!("{}", message );
        }
        parser.set_logger( Some( Box::new( log)) );
        */
        let tree = parser.parse(&cassandra_statement, None).unwrap();

        CassandraAST {
            statement: CassandraStatement::from_tree(&tree, &cassandra_statement),
            text: cassandra_statement,
            tree,
        }
    }

    /// returns true if the parsing exposed an error in the query
    pub fn has_error(&self) -> bool {
        self.tree.root_node().has_error()
    }

    /// retrieves the query value for the node (word or phrase enclosed by the node)
    pub fn node_text(&self, node: &Node) -> String {
        node.utf8_text(self.text.as_bytes()).unwrap().to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::cassandra_ast::{CassandraAST, CassandraStatement, Named, SelectElement};

    fn test_parsing(expected: &[&str], statements: &[&str]) {
        for i in 0..statements.len() {
            let ast = CassandraAST::new(statements[i].to_string());
            assert!(
                !ast.has_error(),
                "AST has error\n{}\n{} ",
                statements[i],
                ast.tree.root_node().to_sexp()
            );
            let stmt = ast.statement;
            let stmt_str = stmt.to_string();
            assert_eq!(expected[i], stmt_str);
        }
    }
    #[test]
    fn test_select_statements() {
        let stmts = [
            "SELECT DISTINCT JSON * FROM table",
            "SELECT column FROM table",
            "SELECT column AS column2 FROM table",
            "SELECT func(*) FROM table",
            "SELECT column AS column2, func(*) AS func2 FROM table;",
            "SELECT column FROM table WHERE col < 5",
            "SELECT column FROM table WHERE col <= 'hello'",
            "SELECT column FROM table WHERE col = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;",
            "SELECT column FROM table WHERE col <> -5",
            "SELECT column FROM table WHERE col >= 3.5",
            "SELECT column FROM table WHERE col = X'E0'",
            "SELECT column FROM table WHERE col = 0XFF",
            "SELECT column FROM table WHERE col = 0Xef",
            "SELECT column FROM table WHERE col = true",
            "SELECT column FROM table WHERE col = false",
            "SELECT column FROM table WHERE col = null",
            "SELECT column FROM table WHERE col = null AND col2 = 'jinx'",
            "SELECT column FROM table WHERE col = $$ a code's block $$",
            "SELECT column FROM table WHERE func(*) < 5",
            "SELECT column FROM table WHERE func(*) <= 'hello'",
            "SELECT column FROM table WHERE func(*) = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;",
            "SELECT column FROM table WHERE func(*) <> -5",
            "SELECT column FROM table WHERE func(*) >= 3.5",
            "SELECT column FROM table WHERE func(*) = X'e0'",
            "SELECT column FROM table WHERE func(*) = 0XFF",
            "SELECT column FROM table WHERE func(*) = 0Xff",
            "SELECT column FROM table WHERE func(*) = true",
            "SELECT column FROM table WHERE func(*) = false",
            "SELECT column FROM table WHERE func(*) = func2(*)",
            "SELECT column FROM table WHERE col IN ( 'literal', 5, func(*), true )",
            "SELECT column FROM table WHERE (col1, col2) IN (( 5, 'stuff'), (6, 'other'));",
            "SELECT column FROM table WHERE (col1, col2) >= ( 5, 'stuff'), (6, 'other')",
            "SELECT column FROM table WHERE col1 CONTAINS 'foo'",
            "SELECT column FROM table WHERE col1 CONTAINS KEY 'foo'",
            "SELECT column FROM table ORDER BY col1",
            "SELECT column FROM table ORDER BY col1 ASC",
            "SELECT column FROM table ORDER BY col1 DESC",
            "SELECT column FROM table LIMIT 5",
            "SELECT column FROM table ALLOW FILTERING",
        ];
        let expected = [
            "SELECT DISTINCT JSON * FROM table",
            "SELECT column FROM table",
            "SELECT column AS column2 FROM table",
            "SELECT func(*) FROM table",
            "SELECT column AS column2, func(*) AS func2 FROM table",
            "SELECT column FROM table WHERE col < 5",
            "SELECT column FROM table WHERE col <= 'hello'",
            "SELECT column FROM table WHERE col = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2",
            "SELECT column FROM table WHERE col <> -5",
            "SELECT column FROM table WHERE col >= 3.5",
            "SELECT column FROM table WHERE col = X'E0'",
            "SELECT column FROM table WHERE col = 0XFF",
            "SELECT column FROM table WHERE col = 0Xef",
            "SELECT column FROM table WHERE col = true",
            "SELECT column FROM table WHERE col = false",
            "SELECT column FROM table WHERE col = null",
            "SELECT column FROM table WHERE col = null AND col2 = 'jinx'",
            "SELECT column FROM table WHERE col = $$ a code's block $$",
            "SELECT column FROM table WHERE func(*) < 5",
            "SELECT column FROM table WHERE func(*) <= 'hello'",
            "SELECT column FROM table WHERE func(*) = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2",
            "SELECT column FROM table WHERE func(*) <> -5",
            "SELECT column FROM table WHERE func(*) >= 3.5",
            "SELECT column FROM table WHERE func(*) = X'e0'",
            "SELECT column FROM table WHERE func(*) = 0XFF",
            "SELECT column FROM table WHERE func(*) = 0Xff",
            "SELECT column FROM table WHERE func(*) = true",
            "SELECT column FROM table WHERE func(*) = false",
            "SELECT column FROM table WHERE func(*) = func2(*)",
            "SELECT column FROM table WHERE col IN ('literal', 5, func(*), true)",
            "SELECT column FROM table WHERE (col1, col2) IN ((5, 'stuff'), (6, 'other'))",
            "SELECT column FROM table WHERE (col1, col2) >= (5, 'stuff'), (6, 'other')",
            "SELECT column FROM table WHERE col1 CONTAINS 'foo'",
            "SELECT column FROM table WHERE col1 CONTAINS KEY 'foo'",
            "SELECT column FROM table ORDER BY col1 ASC",
            "SELECT column FROM table ORDER BY col1 ASC",
            "SELECT column FROM table ORDER BY col1 DESC",
            "SELECT column FROM table LIMIT 5",
            "SELECT column FROM table ALLOW FILTERING",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_insert_statements() {
        let stmts = [
        "BEGIN LOGGED BATCH USING TIMESTAMP 5 INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5);",
        "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) IF NOT EXISTS",
        "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) USING TIMESTAMP 3",
        "INSERT INTO table VALUES ('hello', 5)",
        "INSERT INTO table (col1, col2) JSON $$ json code $$",
        "INSERT INTO table (col1, col2) VALUES ({ 5 : 6 }, 'foo')",
        "INSERT INTO table (col1, col2) VALUES ({ 5, 6 }, 'foo')",
        "INSERT INTO table (col1, col2) VALUES ([ 5, 6 ], 'foo')",
        "INSERT INTO table (col1, col2) VALUES (( 5, 6 ), 'foo')",
    ];
        let expected = [
        "BEGIN LOGGED BATCH USING TIMESTAMP 5 INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5)",
        "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) IF NOT EXISTS",
        "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) USING TIMESTAMP 3",
        "INSERT INTO table VALUES ('hello', 5)",
        "INSERT INTO table (col1, col2) JSON $$ json code $$",
        "INSERT INTO table (col1, col2) VALUES ({5:6}, 'foo')",
        "INSERT INTO table (col1, col2) VALUES ({5, 6}, 'foo')",
        "INSERT INTO table (col1, col2) VALUES ([5, 6], 'foo')",
        "INSERT INTO table (col1, col2) VALUES ((5, 6), 'foo')",
    ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_delete_statements() {
        let stmts = [
        "BEGIN LOGGED BATCH USING TIMESTAMP 5 DELETE column [ 'hello' ] from table WHERE column2 = 'foo' IF EXISTS",
        "BEGIN UNLOGGED BATCH DELETE column [ 6 ] from keyspace.table USING TIMESTAMP 5 WHERE column2='foo' IF column3 = 'stuff'",
        "BEGIN BATCH DELETE column [ 'hello' ] from keyspace.table WHERE column2='foo'",
        "DELETE from table WHERE column2='foo'",
        "DELETE column, column3 from keyspace.table WHERE column2='foo'",
        "DELETE column, column3 from keyspace.table WHERE column2='foo' IF column4 = 'bar'",
    ];
        let expected  = [
        "BEGIN LOGGED BATCH USING TIMESTAMP 5 DELETE column['hello'] FROM table WHERE column2 = 'foo' IF EXISTS",
        "BEGIN UNLOGGED BATCH DELETE column[6] FROM keyspace.table USING TIMESTAMP 5 WHERE column2 = 'foo' IF column3 = 'stuff'",
        "BEGIN BATCH DELETE column['hello'] FROM keyspace.table WHERE column2 = 'foo'",
        "DELETE FROM table WHERE column2 = 'foo'",
        "DELETE column, column3 FROM keyspace.table WHERE column2 = 'foo'",
        "DELETE column, column3 FROM keyspace.table WHERE column2 = 'foo' IF column4 = 'bar'",
    ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn x() {
        let qry = "CREATE MATERIALIZED VIEW view AS SELECT col1, col2 FROM tbl WHERE col3 IS NOT NULL PRIMARY KEY (col1)";
        let ast = CassandraAST::new(qry.to_string());
        let stmt = ast.statement;
        let stmt_str = stmt.to_string();
        assert_eq!(qry, stmt_str);
    }

    #[test]
    fn test_get_statement_type() {
        let stmts = ["Not a valid statement"];
        let types = [CassandraStatement::UNKNOWN(
            "Not a valid statement".to_string(),
        )];

        for i in 0..stmts.len() {
            let ast = CassandraAST::new(stmts.get(i).unwrap().to_string());
            assert_eq!(*types.get(i).unwrap(), ast.statement);
        }
    }

    #[test]
    fn test_has_error() {
        let ast = CassandraAST::new("SELECT foo from bar.baz where fu='something'".to_string());
        assert!(!ast.has_error());
        let ast = CassandraAST::new("Not a valid statement".to_string());
        assert!(ast.has_error());
    }

    #[test]
    fn test_truncate() {
        let stmts = [
            "TRUNCATE foo",
            "TRUNCATE TABLE foo",
            "TRUNCATE keyspace.foo",
            "TRUNCATE TABLE keyspace.foo",
        ];
        let expected = [
            "TRUNCATE TABLE foo",
            "TRUNCATE TABLE foo",
            "TRUNCATE TABLE keyspace.foo",
            "TRUNCATE TABLE keyspace.foo",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_use() {
        let stmts = ["USE keyspace"];
        let expected = ["USE keyspace"];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_aggregate() {
        let stmts = [
            "DROP AGGREGATE IF EXISTS aggregate;",
            "DROP AGGREGATE aggregate;",
            "DROP AGGREGATE IF EXISTS keyspace.aggregate;",
            "DROP AGGREGATE keyspace.aggregate;",
        ];
        let expected = [
            "DROP AGGREGATE IF EXISTS aggregate",
            "DROP AGGREGATE aggregate",
            "DROP AGGREGATE IF EXISTS keyspace.aggregate",
            "DROP AGGREGATE keyspace.aggregate",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_function() {
        let stmts = [
            "DROP FUNCTION func;",
            "DROP FUNCTION keyspace.func;",
            "DROP FUNCTION IF EXISTS func;",
            "DROP FUNCTION IF EXISTS keyspace.func;",
        ];
        let expected = [
            "DROP FUNCTION func",
            "DROP FUNCTION keyspace.func",
            "DROP FUNCTION IF EXISTS func",
            "DROP FUNCTION IF EXISTS keyspace.func",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_index() {
        let stmts = [
            "DROP INDEX idx;",
            "DROP INDEX keyspace.idx;",
            "DROP INDEX IF EXISTS idx;",
            "DROP INDEX IF EXISTS keyspace.idx;",
        ];
        let expected = [
            "DROP INDEX idx",
            "DROP INDEX keyspace.idx",
            "DROP INDEX IF EXISTS idx",
            "DROP INDEX IF EXISTS keyspace.idx",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_keyspace() {
        let stmts = [
            "DROP KEYSPACE keyspace",
            "DROP KEYSPACE IF EXISTS keyspace;",
        ];
        let expected = ["DROP KEYSPACE keyspace", "DROP KEYSPACE IF EXISTS keyspace"];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_materialized_view() {
        let stmts = [
            "DROP MATERIALIZED VIEW view;",
            "DROP MATERIALIZED VIEW IF EXISTS view;",
            "DROP MATERIALIZED VIEW keyspace.view;",
            "DROP MATERIALIZED VIEW IF EXISTS keyspace.view;",
        ];
        let expected = [
            "DROP MATERIALIZED VIEW view",
            "DROP MATERIALIZED VIEW IF EXISTS view",
            "DROP MATERIALIZED VIEW keyspace.view",
            "DROP MATERIALIZED VIEW IF EXISTS keyspace.view",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_role() {
        let stmts = ["DROP ROLE role;", "DROP ROLE if exists role;"];
        let expected = ["DROP ROLE role", "DROP ROLE IF EXISTS role"];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_table() {
        let stmts = [
            "DROP TABLE table;",
            "DROP TABLE IF EXISTS table;",
            "DROP TABLE keyspace.table;",
            "DROP TABLE IF EXISTS keyspace.table;",
        ];
        let expected = [
            "DROP TABLE table",
            "DROP TABLE IF EXISTS table",
            "DROP TABLE keyspace.table",
            "DROP TABLE IF EXISTS keyspace.table",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_type() {
        let stmts = [
            "DROP TYPE type;",
            "DROP TYPE IF EXISTS type;",
            "DROP TYPE keyspace.type;",
            "DROP TYPE IF EXISTS keyspace.type;",
        ];
        let expected = [
            "DROP TYPE type",
            "DROP TYPE IF EXISTS type",
            "DROP TYPE keyspace.type",
            "DROP TYPE IF EXISTS keyspace.type",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_user() {
        let stmts = ["DROP USER user;", "DROP USER IF EXISTS user;"];
        let expected = ["DROP USER user", "DROP USER IF EXISTS user"];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_update_statements() {
        let stmts = [
        "BEGIN LOGGED BATCH USING TIMESTAMP 5 UPDATE keyspace.table SET col1 = 'foo' WHERE col2=5;",
        "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2=5;",
        "UPDATE keyspace.table SET col1 = 'foo' WHERE col2=5 IF EXISTS;",
        "UPDATE keyspace.table SET col1 = 'foo' WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = { 5 : 'hello', 'world' : 5b6962dd-3f90-4c93-8f61-eabfa4a803e2 } WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = {  'hello',  5b6962dd-3f90-4c93-8f61-eabfa4a803e2 } WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = [  'hello',  5b6962dd-3f90-4c93-8f61-eabfa4a803e2 ] WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = col2+5 WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = col2+{ 5 : 'hello', 'world' : 5b6962dd-3f90-4c93-8f61-eabfa4a803e2 } WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = { 5 : 'hello', 'world' : 5b6962dd-3f90-4c93-8f61-eabfa4a803e2 } - col2 WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = col2 + {  'hello',  5b6962dd-3f90-4c93-8f61-eabfa4a803e2 }  WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = {  'hello',  5b6962dd-3f90-4c93-8f61-eabfa4a803e2 } - col2 WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = col2+[  'hello',  5b6962dd-3f90-4c93-8f61-eabfa4a803e2 ] WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1 = [  'hello',  5b6962dd-3f90-4c93-8f61-eabfa4a803e2 ]+col2 WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table SET col1[5] = 'hello' WHERE col2=5 IF col3=7;",
        "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2=5;"
    ];
        let expected = [
        "BEGIN LOGGED BATCH USING TIMESTAMP 5 UPDATE keyspace.table SET col1 = 'foo' WHERE col2 = 5",
        "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2 = 5",
        "UPDATE keyspace.table SET col1 = 'foo' WHERE col2 = 5 IF EXISTS",
        "UPDATE keyspace.table SET col1 = 'foo' WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = {5:'hello', 'world':5b6962dd-3f90-4c93-8f61-eabfa4a803e2} WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = {'hello', 5b6962dd-3f90-4c93-8f61-eabfa4a803e2} WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = ['hello', 5b6962dd-3f90-4c93-8f61-eabfa4a803e2] WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = col2 + 5 WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = col2 + {5:'hello', 'world':5b6962dd-3f90-4c93-8f61-eabfa4a803e2} WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = {5:'hello', 'world':5b6962dd-3f90-4c93-8f61-eabfa4a803e2} - col2 WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = col2 + {'hello', 5b6962dd-3f90-4c93-8f61-eabfa4a803e2} WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = {'hello', 5b6962dd-3f90-4c93-8f61-eabfa4a803e2} - col2 WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = col2 + ['hello', 5b6962dd-3f90-4c93-8f61-eabfa4a803e2] WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1 = ['hello', 5b6962dd-3f90-4c93-8f61-eabfa4a803e2] + col2 WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table SET col1[5] = 'hello' WHERE col2 = 5 IF col3 = 7",
        "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2 = 5"
    ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_role() {
        let stmts = [
            "CREATE ROLE if not exists role;",
            "CREATE ROLE 'role'",
            "CREATE ROLE 'role' WITH PASSWORD = 'password'",
            "CREATE ROLE 'role' WITH PASSWORD = 'password' AND LOGIN=false;",
            "CREATE ROLE 'role' WITH SUPERUSER=true;",
            "CREATE ROLE 'role' WITH OPTIONS={ 'foo' : 3.14, 'bar' : 'pi' }",
        ];
        let expected = [
            "CREATE ROLE IF NOT EXISTS role",
            "CREATE ROLE 'role'",
            "CREATE ROLE 'role' WITH PASSWORD = 'password'",
            "CREATE ROLE 'role' WITH PASSWORD = 'password' AND LOGIN = FALSE",
            "CREATE ROLE 'role' WITH SUPERUSER = TRUE",
            "CREATE ROLE 'role' WITH OPTIONS = {'foo':3.14, 'bar':'pi'}",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_alter_role() {
        let stmts = [
            "ALTER ROLE 'role'",
            "ALTER ROLE 'role' WITH PASSWORD = 'password';",
            "ALTER ROLE 'role' WITH PASSWORD = 'password' AND LOGIN=false;",
            "ALTER ROLE 'role' WITH SUPERUSER=true;",
            "ALTER ROLE 'role' WITH OPTIONS={ 'foo' : 3.14, 'bar' : 'pi' }",
        ];
        let expected = [
            "ALTER ROLE 'role'",
            "ALTER ROLE 'role' WITH PASSWORD = 'password'",
            "ALTER ROLE 'role' WITH PASSWORD = 'password' AND LOGIN = FALSE",
            "ALTER ROLE 'role' WITH SUPERUSER = TRUE",
            "ALTER ROLE 'role' WITH OPTIONS = {'foo':3.14, 'bar':'pi'}",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_user() {
        let stmts = [
            "CREATE USER if not exists username WITH PASSWORD 'password';",
            "CREATE USER username WITH PASSWORD 'password' superuser;",
            "CREATE USER username WITH PASSWORD 'password' nosuperuser;",
        ];
        let expected = [
            "CREATE USER IF NOT EXISTS username WITH PASSWORD 'password'",
            "CREATE USER username WITH PASSWORD 'password' SUPERUSER",
            "CREATE USER username WITH PASSWORD 'password' NOSUPERUSER",
        ];
        test_parsing(&expected, &stmts);
    }
    #[test]
    fn test_alter_user() {
        let stmts = [
            "ALTER USER username WITH PASSWORD 'password';",
            "ALTER USER username WITH PASSWORD 'password' superuser;",
            "ALTER USER username WITH PASSWORD 'password' nosuperuser;",
        ];
        let expected = [
            "ALTER USER username WITH PASSWORD 'password'",
            "ALTER USER username WITH PASSWORD 'password' SUPERUSER",
            "ALTER USER username WITH PASSWORD 'password' NOSUPERUSER",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_keyspace() {
        let stmts = [
        "CREATE KEYSPACE keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1  };",
        "CREATE KEYSPACE keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1  } AND DURABLE_WRITES = false;",
        "CREATE KEYSPACE if not exists keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1  };",
    ];
        let expected = [
        "CREATE KEYSPACE keyspace WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}",
        "CREATE KEYSPACE keyspace WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = FALSE",
        "CREATE KEYSPACE IF NOT EXISTS keyspace WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}",
    ];
        test_parsing(&expected, &stmts);
    }
    #[test]
    fn test_alter_keyspace() {
        let stmts = [
            "ALTER KEYSPACE keyspace WITH REPLICATION = { 'foo' : 'bar', 'baz' : 5};",
            "ALTER KEYSPACE keyspace WITH REPLICATION = { 'foo' : 5 } AND DURABLE_WRITES = true;",
        ];
        let expected = [
            "ALTER KEYSPACE keyspace WITH REPLICATION = {'foo':'bar', 'baz':5}",
            "ALTER KEYSPACE keyspace WITH REPLICATION = {'foo':5} AND DURABLE_WRITES = TRUE",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_grant() {
        let stmts = [
            "GRANT ALL ON 'keyspace'.table TO role;",
            "GRANT ALL PERMISSIONS ON 'keyspace'.table TO role;",
            "GRANT ALTER ON 'keyspace'.table TO role;",
            "GRANT AUTHORIZE ON 'keyspace'.table TO role;",
            "GRANT DESCRIBE ON 'keyspace'.table TO role;",
            "GRANT EXECUTE ON 'keyspace'.table TO role;",
            "GRANT CREATE ON 'keyspace'.table TO role;",
            "GRANT DROP ON 'keyspace'.table TO role;",
            "GRANT MODIFY ON 'keyspace'.table TO role;",
            "GRANT SELECT ON 'keyspace'.table TO role;",
            "GRANT ALL ON ALL FUNCTIONS TO role;",
            "GRANT ALL ON ALL FUNCTIONS IN KEYSPACE keyspace TO role;",
            "GRANT ALL ON ALL KEYSPACES TO role;",
            "GRANT ALL ON ALL ROLES TO role;",
            "GRANT ALL ON FUNCTION 'keyspace'.function TO role;",
            "GRANT ALL ON FUNCTION 'function' TO role;",
            "GRANT ALL ON KEYSPACE 'keyspace' TO role;",
            "GRANT ALL ON ROLE 'role' TO role;",
            "GRANT ALL ON TABLE 'keyspace'.table TO role;",
            "GRANT ALL ON TABLE 'table' TO role;",
            "GRANT ALL ON 'table' TO role;",
        ];
        let expected = [
            "GRANT ALL PERMISSIONS ON TABLE 'keyspace'.table TO role",
            "GRANT ALL PERMISSIONS ON TABLE 'keyspace'.table TO role",
            "GRANT ALTER ON TABLE 'keyspace'.table TO role",
            "GRANT AUTHORIZE ON TABLE 'keyspace'.table TO role",
            "GRANT DESCRIBE ON TABLE 'keyspace'.table TO role",
            "GRANT EXECUTE ON TABLE 'keyspace'.table TO role",
            "GRANT CREATE ON TABLE 'keyspace'.table TO role",
            "GRANT DROP ON TABLE 'keyspace'.table TO role",
            "GRANT MODIFY ON TABLE 'keyspace'.table TO role",
            "GRANT SELECT ON TABLE 'keyspace'.table TO role",
            "GRANT ALL PERMISSIONS ON ALL FUNCTIONS TO role",
            "GRANT ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE keyspace TO role",
            "GRANT ALL PERMISSIONS ON ALL KEYSPACES TO role",
            "GRANT ALL PERMISSIONS ON ALL ROLES TO role",
            "GRANT ALL PERMISSIONS ON FUNCTION 'keyspace'.function TO role",
            "GRANT ALL PERMISSIONS ON FUNCTION 'function' TO role",
            "GRANT ALL PERMISSIONS ON KEYSPACE 'keyspace' TO role",
            "GRANT ALL PERMISSIONS ON ROLE 'role' TO role",
            "GRANT ALL PERMISSIONS ON TABLE 'keyspace'.table TO role",
            "GRANT ALL PERMISSIONS ON TABLE 'table' TO role",
            "GRANT ALL PERMISSIONS ON TABLE 'table' TO role",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_revoke() {
        let stmts = [
            "REVOKE ALL ON TABLE 'keyspace'.table FROM role;",
            "REVOKE ALL PERMISSIONS ON  TABLE'keyspace'.table FROM role;",
            "REVOKE ALTER ON TABLE 'keyspace'.table FROM role;",
            "REVOKE AUTHORIZE ON TABLE 'keyspace'.table FROM role;",
            "REVOKE DESCRIBE ON TABLE 'keyspace'.table FROM role;",
            "REVOKE EXECUTE ON TABLE 'keyspace'.table FROM role;",
            "REVOKE CREATE ON TABLE 'keyspace'.table FROM role;",
            "REVOKE DROP ON TABLE 'keyspace'.table FROM role;",
            "REVOKE MODIFY ON TABLE 'keyspace'.table FROM role;",
            "REVOKE SELECT ON TABLE 'keyspace'.table FROM role;",
            "REVOKE ALL ON ALL FUNCTIONS FROM role;",
            "REVOKE ALL ON ALL FUNCTIONS IN KEYSPACE keyspace FROM role;",
            "REVOKE ALL ON ALL KEYSPACES FROM role;",
            "REVOKE ALL ON ALL ROLES FROM role;",
            "REVOKE ALL ON FUNCTION 'keyspace'.function FROM role;",
            "REVOKE ALL ON FUNCTION 'function' FROM role;",
            "REVOKE ALL ON KEYSPACE 'keyspace' FROM role;",
            "REVOKE ALL ON ROLE 'role' FROM role;",
            "REVOKE ALL ON TABLE 'keyspace'.table FROM role;",
            "REVOKE ALL ON TABLE 'table' FROM role;",
            "REVOKE ALL ON  TABLE'table' FROM role;",
        ];
        let expected = [
            "REVOKE ALL PERMISSIONS ON TABLE 'keyspace'.table FROM role",
            "REVOKE ALL PERMISSIONS ON TABLE 'keyspace'.table FROM role",
            "REVOKE ALTER ON TABLE 'keyspace'.table FROM role",
            "REVOKE AUTHORIZE ON TABLE 'keyspace'.table FROM role",
            "REVOKE DESCRIBE ON TABLE 'keyspace'.table FROM role",
            "REVOKE EXECUTE ON TABLE 'keyspace'.table FROM role",
            "REVOKE CREATE ON TABLE 'keyspace'.table FROM role",
            "REVOKE DROP ON TABLE 'keyspace'.table FROM role",
            "REVOKE MODIFY ON TABLE 'keyspace'.table FROM role",
            "REVOKE SELECT ON TABLE 'keyspace'.table FROM role",
            "REVOKE ALL PERMISSIONS ON ALL FUNCTIONS FROM role",
            "REVOKE ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE keyspace FROM role",
            "REVOKE ALL PERMISSIONS ON ALL KEYSPACES FROM role",
            "REVOKE ALL PERMISSIONS ON ALL ROLES FROM role",
            "REVOKE ALL PERMISSIONS ON FUNCTION 'keyspace'.function FROM role",
            "REVOKE ALL PERMISSIONS ON FUNCTION 'function' FROM role",
            "REVOKE ALL PERMISSIONS ON KEYSPACE 'keyspace' FROM role",
            "REVOKE ALL PERMISSIONS ON ROLE 'role' FROM role",
            "REVOKE ALL PERMISSIONS ON TABLE 'keyspace'.table FROM role",
            "REVOKE ALL PERMISSIONS ON TABLE 'table' FROM role",
            "REVOKE ALL PERMISSIONS ON TABLE 'table' FROM role",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_list_permissions() {
        let stmts = [
            "LIST ALL",
            "LIST ALL ON TABLE 'keyspace'.table OF role;",
            "LIST ALL PERMISSIONS ON  TABLE 'keyspace'.table OF role;",
            "LIST ALTER ON TABLE 'keyspace'.table OF role;",
            "LIST AUTHORIZE ON TABLE 'keyspace'.table OF role;",
            "LIST DESCRIBE ON TABLE 'keyspace'.table OF role;",
            "LIST EXECUTE ON TABLE 'keyspace'.table OF role;",
            "LIST CREATE ON TABLE 'keyspace'.table OF role;",
            "LIST DROP ON TABLE 'keyspace'.table OF role;",
            "LIST MODIFY ON TABLE 'keyspace'.table OF role;",
            "LIST SELECT ON TABLE 'keyspace'.table OF role;",
            "LIST ALL ON ALL FUNCTIONS OF role;",
            "LIST ALL ON ALL FUNCTIONS IN KEYSPACE keyspace OF role;",
            "LIST ALL ON ALL KEYSPACES OF role;",
            "LIST ALL ON ALL ROLES OF role;",
            "LIST ALL ON FUNCTION 'keyspace'.function OF role;",
            "LIST ALL ON FUNCTION 'function' OF role;",
            "LIST ALL ON KEYSPACE 'keyspace' OF role;",
            "LIST ALL ON ROLE 'role' OF role;",
            "LIST ALL ON TABLE 'keyspace'.table OF role;",
            "LIST ALL ON TABLE 'table' OF role;",
            "LIST ALL ON  TABLE 'table' OF role;",
        ];
        let expected = [
            "LIST ALL PERMISSIONS",
            "LIST ALL PERMISSIONS ON TABLE 'keyspace'.table OF role",
            "LIST ALL PERMISSIONS ON TABLE 'keyspace'.table OF role",
            "LIST ALTER ON TABLE 'keyspace'.table OF role",
            "LIST AUTHORIZE ON TABLE 'keyspace'.table OF role",
            "LIST DESCRIBE ON TABLE 'keyspace'.table OF role",
            "LIST EXECUTE ON TABLE 'keyspace'.table OF role",
            "LIST CREATE ON TABLE 'keyspace'.table OF role",
            "LIST DROP ON TABLE 'keyspace'.table OF role",
            "LIST MODIFY ON TABLE 'keyspace'.table OF role",
            "LIST SELECT ON TABLE 'keyspace'.table OF role",
            "LIST ALL PERMISSIONS ON ALL FUNCTIONS OF role",
            "LIST ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE keyspace OF role",
            "LIST ALL PERMISSIONS ON ALL KEYSPACES OF role",
            "LIST ALL PERMISSIONS ON ALL ROLES OF role",
            "LIST ALL PERMISSIONS ON FUNCTION 'keyspace'.function OF role",
            "LIST ALL PERMISSIONS ON FUNCTION 'function' OF role",
            "LIST ALL PERMISSIONS ON KEYSPACE 'keyspace' OF role",
            "LIST ALL PERMISSIONS ON ROLE 'role' OF role",
            "LIST ALL PERMISSIONS ON TABLE 'keyspace'.table OF role",
            "LIST ALL PERMISSIONS ON TABLE 'table' OF role",
            "LIST ALL PERMISSIONS ON TABLE 'table' OF role",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_list_roles() {
        let stmts = [
            "LIST ROLES;",
            "LIST ROLES NORECURSIVE;",
            "LIST ROLES OF role_name;",
            "LIST ROLES OF role_name NORECURSIVE",
        ];
        let expected = [
            "LIST ROLES",
            "LIST ROLES NORECURSIVE",
            "LIST ROLES OF role_name",
            "LIST ROLES OF role_name NORECURSIVE",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_apply_batch() {
        let stmts = ["Apply Batch;"];
        let expected = ["APPLY BATCH"];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_index() {
        let stmts = [
            "CREATE INDEX index_name ON keyspace.table (column);",
            "CREATE INDEX index_name ON table (column);",
            "CREATE INDEX ON table (column);",
            "CREATE INDEX ON table (keys ( key ) );",
            "CREATE INDEX ON table (entries ( spec ) );",
            "CREATE INDEX ON table (full ( spec ) );",
        ];
        let expected = [
            "CREATE INDEX index_name ON keyspace.table( column )",
            "CREATE INDEX index_name ON table( column )",
            "CREATE INDEX ON table( column )",
            "CREATE INDEX ON table( KEYS( key ) )",
            "CREATE INDEX ON table( ENTRIES( spec ) )",
            "CREATE INDEX ON table( FULL( spec ) )",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_table() {
        let stmts = [
        "CREATE TABLE IF NOT EXISTS keyspace.table (col1 text, col2 int, col3 FROZEN<col4>, PRIMARY KEY (col1, col2) );",
    "CREATE TABLE table (col1 text, col2 int, col3 FROZEN<col4>, PRIMARY KEY (col1, col2) ) WITH option = 'option' AND option2 = 3.5;",
    "CREATE TABLE table (col1 text, col2 int, col3 FROZEN<col4>, PRIMARY KEY (col1, col2) ) WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '100' } AND comment = 'Based on table';",
    "CREATE TABLE keyspace.table (col1 text, col2 int, col3 FROZEN<col4>, PRIMARY KEY (col1, col2) ) WITH CLUSTERING ORDER BY ( col2 )",
        "CREATE TABLE keyspace.table (col1 text, col2 int, col3 FROZEN<col4>, PRIMARY KEY (col1, col2) ) WITH option = 'option' AND option2 = 3.5 AND  CLUSTERING ORDER BY ( col2 )",
        "CREATE TABLE keyspace.table (col1 text, col2 int, PRIMARY KEY (col1) ) WITH option1='value' AND CLUSTERING ORDER BY ( col2 ) AND ID='someId' AND COMPACT STORAGE",
    ];
        let expected = [
        "CREATE TABLE IF NOT EXISTS keyspace.table (col1 TEXT, col2 INT, col3 FROZEN<col4>, PRIMARY KEY (col1, col2))",
        "CREATE TABLE table (col1 TEXT, col2 INT, col3 FROZEN<col4>, PRIMARY KEY (col1, col2)) WITH option = 'option' AND option2 = 3.5",
        "CREATE TABLE table (col1 TEXT, col2 INT, col3 FROZEN<col4>, PRIMARY KEY (col1, col2)) WITH caching = {'keys':'ALL', 'rows_per_partition':'100'} AND comment = 'Based on table'",
        "CREATE TABLE keyspace.table (col1 TEXT, col2 INT, col3 FROZEN<col4>, PRIMARY KEY (col1, col2)) WITH CLUSTERING ORDER BY (col2 ASC)",
        "CREATE TABLE keyspace.table (col1 TEXT, col2 INT, col3 FROZEN<col4>, PRIMARY KEY (col1, col2)) WITH option = 'option' AND option2 = 3.5 AND CLUSTERING ORDER BY (col2 ASC)",
        "CREATE TABLE keyspace.table (col1 TEXT, col2 INT, PRIMARY KEY (col1)) WITH option1 = 'value' AND CLUSTERING ORDER BY (col2 ASC) AND ID = 'someId' AND COMPACT STORAGE",
    ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_alter_table() {
        let stmts = [
            "ALTER TABLE keyspace.table ADD column1 UUID, column2 BIGINT;",
            "ALTER TABLE keyspace.table DROP column1, column2;",
            "ALTER TABLE keyspace.table DROP COMPACT STORAGE;",
            "ALTER TABLE keyspace.table RENAME column1 TO column2;",
            "ALTER TABLE keyspace.table WITH option1 = 'option' AND option2 = 3.5;",
        ];
        let expected = [
            "ALTER TABLE keyspace.table ADD column1 UUID, column2 BIGINT",
            "ALTER TABLE keyspace.table DROP column1, column2",
            "ALTER TABLE keyspace.table DROP COMPACT STORAGE",
            "ALTER TABLE keyspace.table RENAME column1 TO column2",
            "ALTER TABLE keyspace.table WITH option1 = 'option' AND option2 = 3.5",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_drop_trigger() {
        let stmts = [
            "DROP TRIGGER trigger_name ON table_name;",
            "DROP TRIGGER trigger_name ON ks.table_name;",
            "DROP TRIGGER keyspace.trigger_name ON table_name;",
            "DROP TRIGGER keyspace.trigger_name ON ks.table_name;",
            "DROP TRIGGER if exists trigger_name ON table_name;",
            "DROP TRIGGER if exists trigger_name ON ks.table_name;",
            "DROP TRIGGER if exists keyspace.trigger_name ON table_name;",
            "DROP TRIGGER if exists keyspace.trigger_name ON ks.table_name;",
        ];
        let expected = [
            "DROP TRIGGER trigger_name ON table_name",
            "DROP TRIGGER trigger_name ON ks.table_name",
            "DROP TRIGGER keyspace.trigger_name ON table_name",
            "DROP TRIGGER keyspace.trigger_name ON ks.table_name",
            "DROP TRIGGER IF EXISTS trigger_name ON table_name",
            "DROP TRIGGER IF EXISTS trigger_name ON ks.table_name",
            "DROP TRIGGER IF EXISTS keyspace.trigger_name ON table_name",
            "DROP TRIGGER IF EXISTS keyspace.trigger_name ON ks.table_name",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_trigger() {
        let stmts = [
            "CREATE TRIGGER trigger_name USING 'trigger_class'",
            "CREATE TRIGGER if not exists trigger_name USING 'trigger_class'",
            "CREATE TRIGGER if not exists keyspace.trigger_name USING 'trigger_class'",
        ];
        let expected = [
            "CREATE TRIGGER trigger_name USING 'trigger_class'",
            "CREATE TRIGGER IF NOT EXISTS trigger_name USING 'trigger_class'",
            "CREATE TRIGGER IF NOT EXISTS keyspace.trigger_name USING 'trigger_class'",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_type() {
        let stmts = [
        "CREATE TYPE if not exists keyspace.type ( 'col1' TIMESTAMP);",
    "CREATE TYPE if not exists keyspace.type ( col1 SET);",
    "CREATE TYPE keyspace.type ( col1 ASCII);",
    "CREATE TYPE keyspace.type ( col1 BIGINT);",
    "CREATE TYPE keyspace.type ( col1 BLOB);",
    "CREATE TYPE keyspace.type ( col1 BOOLEAN);",
    "CREATE TYPE keyspace.type ( col1 COUNTER);",
    "CREATE TYPE keyspace.type ( col1 DATE);",
    "CREATE TYPE keyspace.type ( col1 DECIMAL);",
    "CREATE TYPE keyspace.type ( col1 DOUBLE);",
    "CREATE TYPE keyspace.type ( col1 FLOAT);",
    "CREATE TYPE keyspace.type ( col1 FROZEN);",
    "CREATE TYPE keyspace.type ( col1 INET);",
    "CREATE TYPE keyspace.type ( col1 INT);",
    "CREATE TYPE keyspace.type ( col1 LIST);",
    "CREATE TYPE keyspace.type ( col1 MAP);",
    "CREATE TYPE keyspace.type ( col1 SMALLINT);",
    "CREATE TYPE keyspace.type ( col1 TEXT);",
    "CREATE TYPE type ( col1 TIME);",
    "CREATE TYPE type ( col1 TIMEUUID);",
    "CREATE TYPE type ( col1 TINYINT);",
    "CREATE TYPE type ( col1 TUPLE);",
    "CREATE TYPE type ( col1 VARCHAR);",
    "CREATE TYPE type ( col1 VARINT);",
    "CREATE TYPE type ( col1 TIMESTAMP);",
    "CREATE TYPE type ( col1 UUID);",
    "CREATE TYPE type ( col1 'foo');",
    "CREATE TYPE if not exists keyspace.type ( col1 'foo' < 'subcol1', TIMESTAMP, BLOB > );",
    "CREATE TYPE type ( col1 UUID, Col2 int);",
    ];
        let expected = [
            "CREATE TYPE IF NOT EXISTS keyspace.type ('col1' TIMESTAMP)",
            "CREATE TYPE IF NOT EXISTS keyspace.type (col1 SET)",
            "CREATE TYPE keyspace.type (col1 ASCII)",
            "CREATE TYPE keyspace.type (col1 BIGINT)",
            "CREATE TYPE keyspace.type (col1 BLOB)",
            "CREATE TYPE keyspace.type (col1 BOOLEAN)",
            "CREATE TYPE keyspace.type (col1 COUNTER)",
            "CREATE TYPE keyspace.type (col1 DATE)",
            "CREATE TYPE keyspace.type (col1 DECIMAL)",
            "CREATE TYPE keyspace.type (col1 DOUBLE)",
            "CREATE TYPE keyspace.type (col1 FLOAT)",
            "CREATE TYPE keyspace.type (col1 FROZEN)",
            "CREATE TYPE keyspace.type (col1 INET)",
            "CREATE TYPE keyspace.type (col1 INT)",
            "CREATE TYPE keyspace.type (col1 LIST)",
            "CREATE TYPE keyspace.type (col1 MAP)",
            "CREATE TYPE keyspace.type (col1 SMALLINT)",
            "CREATE TYPE keyspace.type (col1 TEXT)",
            "CREATE TYPE type (col1 TIME)",
            "CREATE TYPE type (col1 TIMEUUID)",
            "CREATE TYPE type (col1 TINYINT)",
            "CREATE TYPE type (col1 TUPLE)",
            "CREATE TYPE type (col1 VARCHAR)",
            "CREATE TYPE type (col1 VARINT)",
            "CREATE TYPE type (col1 TIMESTAMP)",
            "CREATE TYPE type (col1 UUID)",
            "CREATE TYPE type (col1 'foo')",
            "CREATE TYPE IF NOT EXISTS keyspace.type (col1 'foo'<'subcol1', TIMESTAMP, BLOB>)",
            "CREATE TYPE type (col1 UUID, Col2 INT)",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_alter_type() {
        let stmts = [
            "ALTER TYPE keyspace.type ALTER column TYPE UUID;",
            "ALTER TYPE keyspace.type ADD column2 UUID, column3 TIMESTAMP;",
            "ALTER TYPE keyspace.type RENAME column1 TO column2;",
            "ALTER TYPE type ALTER column TYPE UUID;",
            "ALTER TYPE type ADD column2 UUID, column3 TIMESTAMP;",
            "ALTER TYPE type RENAME column1 TO column2;",
            "ALTER TYPE type RENAME column1 TO column2 AND col3 TO col4;",
        ];
        let expected = [
            "ALTER TYPE keyspace.type ALTER column TYPE UUID",
            "ALTER TYPE keyspace.type ADD column2 UUID, column3 TIMESTAMP",
            "ALTER TYPE keyspace.type RENAME column1 TO column2",
            "ALTER TYPE type ALTER column TYPE UUID",
            "ALTER TYPE type ADD column2 UUID, column3 TIMESTAMP",
            "ALTER TYPE type RENAME column1 TO column2",
            "ALTER TYPE type RENAME column1 TO column2 AND col3 TO col4",
        ];
        test_parsing(&expected, &stmts);
    }
    #[test]
    fn test_create_function() {
        let stmts = [
            "CREATE OR REPLACE FUNCTION keyspace.func ( param1 int , param2 text) CALLED ON NULL INPUT RETURNS INT LANGUAGE javascript AS $$ return 5; $$;",
        "CREATE OR REPLACE FUNCTION keyspace.func ( param1 int , param2 text) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE javascript AS $$ return 'hello'; $$;",
        "CREATE FUNCTION IF NOT EXISTS func ( param1 int , param2 text) CALLED ON NULL INPUT RETURNS INT LANGUAGE javascript AS $$ return 5; $$;",
        ];
        let expected = [
            "CREATE OR REPLACE FUNCTION keyspace.func (param1 INT, param2 TEXT) CALLED ON NULL INPUT RETURNS INT LANGUAGE javascript AS $$ return 5; $$",
            "CREATE OR REPLACE FUNCTION keyspace.func (param1 INT, param2 TEXT) RETURNS NULL ON NULL INPUT RETURNS TEXT LANGUAGE javascript AS $$ return 'hello'; $$",
            "CREATE FUNCTION IF NOT EXISTS func (param1 INT, param2 TEXT) CALLED ON NULL INPUT RETURNS INT LANGUAGE javascript AS $$ return 5; $$",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_aggregate() {
        let stmts = [
            "CREATE OR REPLACE AGGREGATE keyspace.aggregate ( UUID ) SFUNC sfunc STYPE TIMESTAMP FINALFUNC finalFunc INITCOND 5;",
        "CREATE AGGREGATE IF NOT EXISTS keyspace.aggregate  ( UUID ) SFUNC sfunc STYPE TIMESTAMP FINALFUNC finalFunc INITCOND 5;",
        "CREATE AGGREGATE keyspace.aggregate  ( ASCII ) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND ( 5, 'text', 6.3);",
        "CREATE AGGREGATE keyspace.aggregate  ( ASCII ) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND (( 5, 'text', 6.3),(4,'foo',3.14));",
        "CREATE AGGREGATE keyspace.aggregate  ( ASCII ) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND ( key : (5,7,9));",
            "CREATE AGGREGATE keyspace.aggregate  ( ASCII ) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND ( key1 : (5,7,9), key2 : (2,4,6));",
        ];
        let expected = [
            "CREATE OR REPLACE AGGREGATE keyspace.aggregate (UUID) SFUNC sfunc STYPE TIMESTAMP FINALFUNC finalFunc INITCOND 5",
            "CREATE AGGREGATE IF NOT EXISTS keyspace.aggregate (UUID) SFUNC sfunc STYPE TIMESTAMP FINALFUNC finalFunc INITCOND 5",
            "CREATE AGGREGATE keyspace.aggregate (ASCII) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND (5, 'text', 6.3)",
            "CREATE AGGREGATE keyspace.aggregate (ASCII) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND ((5, 'text', 6.3), (4, 'foo', 3.14))",
            "CREATE AGGREGATE keyspace.aggregate (ASCII) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND (key:(5, 7, 9))",
            "CREATE AGGREGATE keyspace.aggregate (ASCII) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND (key1:(5, 7, 9), key2:(2, 4, 6))",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_create_materialized_view() {
        let stmts = [
            "CREATE MATERIALIZED VIEW view AS SELECT col1, col2 FROM tbl WHERE col3 IS NOT NULL PRIMARY KEY (col1);",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col3 IS NOT NULL PRIMARY KEY (col1)",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col3 IS NOT NULL PRIMARY KEY (col1) WITH option1 = 'option';",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col3 IS NOT NULL PRIMARY KEY (col1) WITH option1 = 'option' AND CLUSTERING ORDER BY (col2 DESC);",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH option1 = 'option' AND option2 = 3.5 AND CLUSTERING ORDER BY (col2 DESC);",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH CLUSTERING ORDER BY (col2 DESC);",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH option1 = 'option' AND option2 = 3.5;",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1);",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL PRIMARY KEY (col1,col2)",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2, col3  FROM keyspace.table  WHERE col1 IS NOT NULL AND col4 IS NOT NULL PRIMARY KEY (col1, col4) WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : '100' } AND comment = 'Based on table' ;",
        ];
        let expected = [
            "CREATE MATERIALIZED VIEW view AS SELECT col1, col2 FROM tbl WHERE col3 IS NOT NULL PRIMARY KEY (col1)",
            "CREATE MATERIALIZED VIEW IF NOT EXISTS keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col3 IS NOT NULL PRIMARY KEY (col1)",
            "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col3 IS NOT NULL PRIMARY KEY (col1) WITH option1 = 'option'",
            "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col3 IS NOT NULL PRIMARY KEY (col1) WITH option1 = 'option' AND CLUSTERING ORDER BY (col2 DESC)",
            "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH option1 = 'option' AND option2 = 3.5 AND CLUSTERING ORDER BY (col2 DESC)",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH CLUSTERING ORDER BY (col2 DESC)",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH option1 = 'option' AND option2 = 3.5",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1)",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL PRIMARY KEY (col1, col2)",
        "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2, col3 FROM keyspace.table WHERE col1 IS NOT NULL AND col4 IS NOT NULL PRIMARY KEY (col1, col4) WITH caching = {'keys':'ALL', 'rows_per_partition':'100'} AND comment = 'Based on table'",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_alter_materialized_view() {
        let stmts = [
            "ALTER MATERIALIZED VIEW 'keyspace'.mview;",
            "ALTER MATERIALIZED VIEW mview;",
            "ALTER MATERIALIZED VIEW keyspace.mview WITH option1 = 'option' AND option2 = 3.5;",
        ];
        let expected = [
            "ALTER MATERIALIZED VIEW 'keyspace'.mview",
            "ALTER MATERIALIZED VIEW mview",
            "ALTER MATERIALIZED VIEW keyspace.mview WITH option1 = 'option' AND option2 = 3.5",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_select_element_display() {
        assert_eq!( "*", SelectElement::Star.to_string());
        assert_eq!( "col.*", SelectElement::DotStar("col".to_string()).to_string());
        assert_eq!( "col", SelectElement::Column(Named{ name: "col".to_string(), alias: None }).to_string());
        assert_eq!( "func", SelectElement::Function(Named{ name: "func".to_string(), alias: None }).to_string());
        assert_eq!( "col AS alias", SelectElement::Column(Named{ name: "col".to_string(), alias: Some("alias".to_string()) }).to_string());
        assert_eq!( "func AS alias", SelectElement::Function(Named{ name: "func".to_string(), alias: Some("alias".to_string()) }).to_string());
    }

}
