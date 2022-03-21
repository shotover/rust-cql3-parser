use itertools::Itertools;
use regex::Regex;
use std::fmt::{Display, Formatter};
use tree_sitter::{Node, Tree, TreeCursor};

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraStatement {
    AlterKeyspace(KeyspaceData),
    AlterMaterializedView,
    AlterRole(RoleData),
    AlterTable,
    AlterType,
    AlterUser(UserData),
    ApplyBatch,
    CreateAggregate,
    CreateFunction,
    CreateIndex,
    CreateKeyspace(KeyspaceData),
    CreateMaterializedView,
    CreateRole(RoleData),
    CreateTable,
    CreateTrigger,
    CreateType,
    CreateUser(UserData),
    DeleteStatement(DeleteStatementData),
    DropAggregate(DropData),
    DropFunction(DropData),
    DropIndex(DropData),
    DropKeyspace(DropData),
    DropMaterializedView(DropData),
    DropRole(DropData),
    DropTable(DropData),
    DropTrigger,
    DropType(DropData),
    DropUser(DropData),
    Grant(GrantRevokeData),
    InsertStatement(InsertStatementData),
    ListPermissions(GrantRevokeData),
    ListRoles,
    Revoke(GrantRevokeData),
    SelectStatement(SelectStatementData),
    Truncate(String),
    Update(UpdateStatementData),
    UseStatement(String),
    UNKNOWN(String),
}

#[derive(PartialEq, Debug, Clone)]
pub struct GrantRevokeData {
    pub privilege: Privilege,
    pub resource : Option<Resource>,
    pub role : Option<String>,
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

impl ToString for UpdateStatementData {
    fn to_string(&self) -> String {
        let mut result = String::new();
        if self.begin_batch.is_some() {
            result.push_str(self.begin_batch.as_ref().unwrap().to_string().as_str());
        }
        result.push_str("UPDATE ");
        result.push_str(self.table_name.as_str());
        if self.using_ttl.is_some() {
            result.push_str(self.using_ttl.as_ref().unwrap().to_string().as_str());
        }
        result.push_str(" SET ");
        result.push_str(
            self.assignments
                .iter()
                .map(|a| a.to_string())
                .join(",")
                .as_str(),
        );
        result.push_str(" WHERE ");
        result.push_str(self.where_clause.iter().join(" AND ").as_str());
        if self.if_spec.is_some() {
            result.push_str(" IF ");
            result.push_str(
                self.if_spec
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(x, y)| format!("{} = {}", x, y))
                    .join(" AND ")
                    .as_str(),
            );
        } else if self.modifiers.exists {
            result.push_str(" IF EXISTS");
        }
        result
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

impl ToString for DeleteStatementData {
    fn to_string(&self) -> String {
        let mut result = String::new();
        if self.begin_batch.is_some() {
            result.push_str(self.begin_batch.as_ref().unwrap().to_string().as_str());
        }
        result.push_str("DELETE ");
        if self.columns.is_some() {
            result.push_str(self.columns.as_ref().unwrap().iter().join(", ").as_str());
            result.push(' ');
        }
        result.push_str("FROM ");
        result.push_str(&self.table_name.as_str());
        if self.timestamp.is_some() {
            result.push_str(format!(" USING TIMESTAMP {}", self.timestamp.unwrap()).as_str());
        }
        result.push_str(" WHERE ");
        result.push_str(self.where_clause.iter().join(" AND ").as_str());

        if self.if_spec.is_some() {
            result.push_str(" IF ");
            result.push_str(
                self.if_spec
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(x, y)| format!("{} = {}", x, y))
                    .join(" AND ")
                    .as_str(),
            );
        } else if self.modifiers.exists {
            result.push_str(" IF EXISTS");
        }

        result
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

impl ToString for InsertStatementData {
    fn to_string(&self) -> String {
        let mut result = String::new();
        if self.begin_batch.is_some() {
            result.push_str(self.begin_batch.as_ref().unwrap().to_string().as_str());
        }
        result.push_str("INSERT INTO ");
        result.push_str(&self.table_name.as_str());
        if self.columns.is_some() {
            result.push_str(" (");
            result.push_str(self.columns.as_ref().unwrap().iter().join(", ").as_str());
            result.push(')');
        }
        result.push_str(self.values.as_ref().unwrap().to_string().as_str());
        if self.modifiers.not_exists {
            result.push_str(" IF NOT EXISTS");
        }
        if self.using_ttl.is_some() {
            result.push_str(self.using_ttl.as_ref().unwrap().to_string().as_str());
        }
        result
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
                write!(f, " VALUES ({})", columns.iter().join(", "))
            }
            InsertValues::JSON(text) => {
                write!(f, " JSON {}", text)
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
            .map(|e| match e {
                SelectElement::STAR => None,
                SelectElement::DOT_STAR(_) => None,
                SelectElement::COLUMN(named) => Some(named.name.clone()),
                SelectElement::FUNCTION(_) => None,
            })
            .filter(|e| e.is_some())
            .map(|e| e.unwrap())
            .collect()
    }

    /// return the aliased column names.  If the column is not aliased the
    /// base column name is returned.
    pub fn select_alias(&self) -> Vec<String> {
        self.elements
            .iter()
            .map(|e| match e {
                SelectElement::COLUMN(named) => {
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

impl ToString for SelectStatementData {
    fn to_string(&self) -> String {
        let mut result = String::new();
        result.push_str("SELECT ");
        if self.modifiers.distinct {
            result.push_str("DISTINCT ");
        }
        if self.modifiers.json {
            result.push_str("JSON ");
        }
        result.push_str(self.elements.iter().join(", ").as_str());
        result.push_str(" FROM ");
        result.push_str(self.table_name.as_str());
        if self.where_clause.is_some() {
            result.push_str(" WHERE ");
            result.push_str(
                self.where_clause
                    .as_ref()
                    .unwrap()
                    .iter()
                    .join(" AND ")
                    .as_str(),
            );
        }
        if self.order.is_some() {
            result.push_str(format!("{}", self.order.as_ref().unwrap()).as_str());
        }
        if self.modifiers.limit.is_some() {
            result.push_str(format!(" LIMIT {}", self.modifiers.limit.unwrap()).as_str());
        }
        if self.modifiers.filtering {
            result.push_str(" ALLOW FILTERING");
        }
        result
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum SelectElement {
    STAR,
    DOT_STAR(String),
    COLUMN(Named),
    FUNCTION(Named),
}

impl Display for SelectElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectElement::STAR => write!(f, "{}", "*"),
            SelectElement::DOT_STAR(column) => column.fmt(f),
            SelectElement::COLUMN(named) | SelectElement::FUNCTION(named) => named.fmt(f),
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
            " ORDER BY {} {}",
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
    CONTAINS,
    CONTAINS_KEY,
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
            RelationOperator::CONTAINS => false,
            RelationOperator::CONTAINS_KEY => false,
        }
    }
}

impl Display for RelationOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RelationOperator::LT => write!(f, "{}", "<"),
            RelationOperator::LE => write!(f, "{}", "<="),
            RelationOperator::EQ => write!(f, "{}", "="),
            RelationOperator::NE => write!(f, "{}", "<>"),
            RelationOperator::GE => write!(f, "{}", ">="),
            RelationOperator::GT => write!(f, "{}", ">"),
            RelationOperator::IN => write!(f, "{}", "IN"),
            RelationOperator::CONTAINS => write!(f, "CONTAINS"),
            RelationOperator::CONTAINS_KEY => write!(f, "CONTAINS KEY"),
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
    pub fn as_string(node: &Node, source: &String) -> String {
        node.utf8_text(source.as_bytes()).unwrap().to_string()
    }

    pub fn as_boolean(node: &Node, source: &String) -> bool {
        NodeFuncs::as_string(node, source).to_uppercase().eq("TRUE")
    }
}
impl CassandraStatement {
    pub fn from_tree(tree: &Tree, source: &String) -> CassandraStatement {
        let mut node = tree.root_node();
        if node.kind().eq("source_file") {
            node = node.child(0).unwrap();
        }
        CassandraStatement::from_node(&node, source)
    }

    pub fn from_node(node: &Node, source: &String) -> CassandraStatement {
        if node.has_error() {
            return CassandraStatement::UNKNOWN(source.clone());
        }
        match node.kind() {
            "alter_keyspace" => CassandraStatement::AlterKeyspace(
                CassandraParser::parse_keyspace_data(node, source),
            ),
            "alter_materialized_view" => CassandraStatement::AlterMaterializedView,
            "alter_role" => {
                CassandraStatement::AlterRole(CassandraParser::parse_role_data(node, source))
            }
            "alter_table" => CassandraStatement::AlterTable,
            "alter_type" => CassandraStatement::AlterType,
            "alter_user" => {
                CassandraStatement::AlterUser(CassandraParser::parse_user_data(node, source))
            }
            "apply_batch" => CassandraStatement::ApplyBatch,
            "create_aggregate" => CassandraStatement::CreateAggregate,
            "create_function" => CassandraStatement::CreateFunction,
            "create_index" => CassandraStatement::CreateIndex,
            "create_keyspace" => CassandraStatement::CreateKeyspace(
                CassandraParser::parse_keyspace_data(node, source),
            ),
            "create_materialized_view" => CassandraStatement::CreateMaterializedView,
            "create_role" => {
                CassandraStatement::CreateRole(CassandraParser::parse_role_data(node, source))
            }
            "create_table" => CassandraStatement::CreateTable,
            "create_trigger" => CassandraStatement::CreateTrigger,
            "create_type" => CassandraStatement::CreateType,
            "create_user" => {
                CassandraStatement::CreateUser(CassandraParser::parse_user_data(node, source))
            }
            "delete_statement" => CassandraStatement::DeleteStatement(
                CassandraParser::build_delete_statement(node, source),
            ),
            "drop_aggregate" => CassandraStatement::DropAggregate(
                CassandraParser::parse_standard_drop(&node, source),
            ),
            "drop_function" => CassandraStatement::DropFunction(
                CassandraParser::parse_standard_drop(&node, source),
            ),
            "drop_index" => {
                CassandraStatement::DropIndex(CassandraParser::parse_standard_drop(&node, source))
            }
            "drop_keyspace" => CassandraStatement::DropKeyspace(
                CassandraParser::parse_standard_drop(&node, source),
            ),
            "drop_materialized_view" => CassandraStatement::DropMaterializedView(
                CassandraParser::parse_standard_drop(&node, source),
            ),
            "drop_role" => {
                CassandraStatement::DropRole(CassandraParser::parse_standard_drop(&node, source))
            }
            "drop_table" => {
                CassandraStatement::DropTable(CassandraParser::parse_standard_drop(&node, source))
            }
            "drop_trigger" => CassandraStatement::DropTrigger,
            "drop_type" => {
                CassandraStatement::DropType(CassandraParser::parse_standard_drop(&node, source))
            }
            "drop_user" => {
                CassandraStatement::DropUser(CassandraParser::parse_standard_drop(&node, source))
            }
            "grant" => CassandraStatement::Grant(CassandraParser::parse_grant_revoke_data(&node,source)),
            "insert_statement" => CassandraStatement::InsertStatement(
                CassandraParser::build_insert_statement(node, source),
            ),
            "list_permissions" => CassandraStatement::ListPermissions(CassandraParser::parse_grant_revoke_data(&node,source)),
            "list_roles" => CassandraStatement::ListRoles,
            "revoke" => CassandraStatement::Revoke(CassandraParser::parse_grant_revoke_data(&node,source)),
            "select_statement" => CassandraStatement::SelectStatement(
                CassandraParser::build_select_statement(node, source),
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
                CassandraStatement::Update(CassandraParser::build_update_statement(node, source))
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
            _ => CassandraStatement::UNKNOWN(node.kind().to_string()),
        }
    }
}

struct CassandraParser {}
impl CassandraParser {

    fn parse_resource(node: &Node, source: &String) -> Resource {
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
                            Resource::ALL_FUNCTIONS(Some(NodeFuncs::as_string(&cursor.node(), source)))
                        } else {
                            Resource::ALL_FUNCTIONS(None)
                        }
                    }
                    "KEYSPACES" => Resource::ALL_KEYSPACES,
                    "ROLES" => Resource::ALL_ROLES,
                    _ => unreachable!(),
                }
            },
            "FUNCTION" => {
                cursor.goto_next_sibling();
                Resource::FUNCTION(CassandraParser::parse_dotted_name(&mut cursor, source))
            },
            "KEYSPACE" => {
                cursor.goto_next_sibling();
                Resource::KEYSPACE(NodeFuncs::as_string(&cursor.node(), source))
            },
            "ROLE" => {
                cursor.goto_next_sibling();
                Resource::ROLE(NodeFuncs::as_string(&cursor.node(), source))
            },
            "TABLE" => {
                cursor.goto_next_sibling();
                Resource::TABLE(CassandraParser::parse_dotted_name(&mut cursor, source))
            },
            _ => {
                Resource::TABLE(CassandraParser::parse_dotted_name(&mut cursor, source)) },
        }
    }

    fn parse_role_data(node: &Node, source: &String) -> RoleData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut if_not_exists = CassandraParser::consume_alter_create(&mut cursor);
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
                match cursor.node().kind() {
                    "role_with_option" => {
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
                    _ => {}
                }
            }
        }
        result
    }

    fn consume_alter_create(cursor: &mut TreeCursor) -> bool {
        let mut if_not_exists = false;
        // consume 'ALTER/CREATE'
        cursor.goto_next_sibling();
        // consume 'type'
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
    fn parse_keyspace_data(node: &Node, source: &String) -> KeyspaceData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_alter_create(&mut cursor);
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
    fn parse_user_data(node: &Node, source: &String) -> UserData {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_alter_create(&mut cursor);

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

    pub fn build_update_statement(node: &Node, source: &String) -> UpdateStatementData {
        /*
        optional( $.begin_batch),
               kw( "UPDATE"),
               dotted_name( $.object_name, $.object_name, "table"),
               optional( $.using_ttl_timestamp),
               kw( "SET"),
               commaSep1( $.assignment_element),
               $.where_spec,
               optional( choice( if_exists, $.if_spec))
        */
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

    fn parse_privilege(node: &Node, source: &String) -> Privilege {
       match NodeFuncs::as_string(node, source).to_uppercase().as_str() {
            "ALL" | "ALL PERMISSIONS" => Privilege::ALL,
            "ALTER" => Privilege::ALTER,
            "AUTHORIZE" => Privilege::AUTHORIZE,
            "DESCRIBE" => Privilege::DESCRIBE,
            "EXECUTE" => Privilege::EXECUTE,
            "CREATE" => Privilege::CREATE,
            "DROP" => Privilege::DROP,
            "MODIFY" => Privilege::MODIFY ,
            "SELECT"=> Privilege::SELECT,
            _ => unreachable!(),
        }
    }

    fn parse_grant_revoke_data(node: &Node, source: &String) -> GrantRevokeData {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let mut privilege: Option<Privilege> = None;
        let mut resource : Option<Resource> = None;
        let mut role : Option<String> = None;
        // consume 'GRANT/REVOKE'
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "privilege" =>  {
                    privilege = Some(CassandraParser::parse_privilege(&cursor.node(), source));
                },
                "resource" => {
                    resource = Some(CassandraParser::parse_resource( &cursor.node(), source));
                }
                "role" => role = Some(NodeFuncs::as_string( &cursor.node(), source )),
                _ => {},
            }
        }
        GrantRevokeData {
            privilege: privilege.unwrap(),
            resource,
            role
        }
    }

    fn parse_assignment_element(node: &Node, source: &String) -> AssignmentElement {
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

    pub fn build_delete_statement(node: &Node, source: &String) -> DeleteStatementData {
        /*
               optional( $.begin_batch ),
               kw("DELETE"),
               optional( $.delete_column_list ),
               $.from_spec,
               optional( $.using_timestamp_spec),
               $.where_spec,
               optional( choice( if_exists, $.if_spec))
        */
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

    fn parse_if_condition_list(node: &Node, source: &String) -> Option<Vec<(String, String)>> {
        let mut result = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();
        while process {
            cursor.goto_first_child();
            let column = NodeFuncs::as_string(&cursor.node(), &source);
            // consume the '='
            cursor.goto_next_sibling();
            cursor.goto_next_sibling();
            let value = NodeFuncs::as_string(&cursor.node(), &source);
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

    fn parse_delete_column_item(node: &Node, source: &String) -> IndexedColumn {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        CassandraParser::parse_indexed_column(&mut cursor, source)
    }

    fn parse_indexed_column(cursor: &mut TreeCursor, source: &String) -> IndexedColumn {
        IndexedColumn {
            column: NodeFuncs::as_string(&cursor.node(), &source),

            value: if cursor.goto_next_sibling() && cursor.node().kind().eq("[") {
                // consume '['
                cursor.goto_next_sibling();
                let result = Some(NodeFuncs::as_string(&cursor.node(), &source));
                // consume ']'
                cursor.goto_next_sibling();
                result
            } else {
                None
            },
        }
    }

    pub fn build_insert_statement(node: &Node, source: &String) -> InsertStatementData {
        /*
        optional( $.begin_batch),
               kw("INSERT"),
               kw("INTO"),
               $.table_name,
               optional( $.insert_column_spec ),
               $.insert_values_spec,
               optional( if_not_exists ),
               optional( $.using_ttl_timestamp )
        */
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

    // on column_list
    fn parse_column_list(node: &Node, source: &String) -> Vec<String> {
        let mut result: Vec<String> = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();

        while process {
            if cursor.node().kind().eq("column") {
                result.push(NodeFuncs::as_string(&cursor.node(), &source));
            }
            process = cursor.goto_next_sibling();
            // consume ',' if it is there
            cursor.goto_next_sibling();
        }
        result
    }

    fn parse_using_timestamp(node: &Node, source: &String) -> Option<u64> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "USING"
        cursor.goto_next_sibling();
        // consume "TIMESTAMP"
        cursor.goto_next_sibling();
        Some(
            NodeFuncs::as_string(&cursor.node(), &source)
                .parse::<u64>()
                .unwrap(),
        )
    }

    fn parse_ttl_timestamp(node: &Node, source: &String) -> TtlTimestamp {
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

    fn parse_from_spec(node: &Node, source: &String) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'FROM'
        cursor.goto_next_sibling();
        CassandraParser::parse_table_name(&cursor.node(), &source)
    }

    fn parse_dotted_name(cursor: &mut TreeCursor, source: &String) -> String {
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
    fn parse_table_name(node: &Node, source: &String) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        CassandraParser::parse_dotted_name(&mut cursor, source)
    }

    fn parse_function_args(node: &Node, source: &String) -> Vec<Operand> {
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

    fn parse_expression_list(node: &Node, source: &String) -> Vec<Operand> {
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

    fn parse_operand(node: &Node, source: &String) -> Operand {
        match node.kind() {
            "constant" => Operand::CONST(NodeFuncs::as_string(node, source)),
            "column" => Operand::COLUMN(NodeFuncs::as_string(node, &source)),
            "assignment_tuple" => {
                Operand::TUPLE(CassandraParser::parse_assignment_tuple(node, source))
            }
            "assignment_map" => Operand::MAP(CassandraParser::parse_assignment_map(node, source)),
            "assignment_list" => {
                Operand::LIST(CassandraParser::parse_assignment_list(node, source))
            }
            "assignment_set" => Operand::SET(CassandraParser::parse_assignment_set(node, source)),
            "function_args" => Operand::TUPLE(CassandraParser::parse_function_args(node, source)),
            "function_call" => Operand::FUNC(NodeFuncs::as_string(node, &source)),
            _ => Operand::CONST(NodeFuncs::as_string(node, source)),
        }
    }

    // parses lists of option_hash_item or replication_list_item
    fn parse_map(node: &Node, source: &String) -> Vec<(String, String)> {
        /*
               option_hash : $ => seq( "{", commaSep1( $.option_hash_item), "}"),
        option_hash_item : $ => seq( alias($._string_literal,"property"), ":", alias( choice( $._string_literal, $._float_literal), "value"), ),

         */
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
                    let key = NodeFuncs::as_string(&cursor.node(), &source);
                    cursor.goto_next_sibling();
                    // consume the ':'
                    cursor.goto_next_sibling();
                    let value = NodeFuncs::as_string(&cursor.node(), &source);
                    entries.push((key, value));
                    cursor.goto_parent();
                }
                _ => unreachable!(),
            }
        }
        cursor.goto_parent();
        entries
    }

    fn parse_assignment_map(node: &Node, source: &String) -> Vec<(String, String)> {
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
                    let key = NodeFuncs::as_string(&cursor.node(), &source);
                    cursor.goto_next_sibling();
                    // consume the ':'
                    cursor.goto_next_sibling();
                    let value = NodeFuncs::as_string(&cursor.node(), &source);
                    entries.push((key, value));
                }
            }
        }
        cursor.goto_parent();
        entries
    }

    fn parse_assignment_list(node: &Node, source: &String) -> Vec<String> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // [ const, const, ... ]
        let mut entries: Vec<String> = vec![];
        // we are on the '[' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "]" | "," => {}
                _ => {
                    entries.push(NodeFuncs::as_string(&cursor.node(), &source));
                }
            }
        }
        entries
    }

    fn parse_assignment_set(node: &Node, source: &String) -> Vec<String> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // { const, const, ... }
        let mut entries: Vec<String> = vec![];
        // we are on the '{' so we can just skip it
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "}" | "," => {}
                _ => {
                    entries.push(NodeFuncs::as_string(&cursor.node(), &source));
                }
            }
        }
        entries
    }

    fn parse_assignment_tuple(node: &Node, source: &String) -> Vec<Operand> {
        // ( expression, expression ... )
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume '('
        cursor.goto_next_sibling();
        // now on 'expression-list'
        CassandraParser::parse_expression_list(&cursor.node(), source)
    }

    fn parse_begin_batch(node: &Node, source: &String) -> BeginBatch {
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

    pub fn build_select_statement(node: &Node, source: &String) -> SelectStatementData {
        /*
        seq(
                kw("SELECT"),
                optional( kw("DISTINCT")),
                optional( kw("JSON") ),
                $.select_elements,
                $.from_spec,
                optional($.where_spec),
                optional($.order_spec),
                optional($.limit_spec ),
                optional(seq( kw("ALLOW"), kw("FILTERING"))),
            ),
         */
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
                                        &source,
                                    ))
                            }
                            "*" => statement_data.elements.push(SelectElement::STAR),
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
                        NodeFuncs::as_string(&cursor.node(), &source)
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
        return statement_data;
    }

    fn parse_where_spec(node: &Node, source: &String) -> Vec<RelationElement> {
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

    fn parse_relation_element(node: &Node, source: &String) -> RelationElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        match cursor.node().kind() {
            "relation_contains_key" => {
                cursor.goto_first_child();
                RelationElement {
                    obj: Operand::COLUMN(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::CONTAINS_KEY,
                    value: {
                        // consume column value
                        cursor.goto_next_sibling();
                        // consume 'CONTAINS'
                        cursor.goto_next_sibling();
                        // consume 'KEY'
                        cursor.goto_next_sibling();
                        let mut result = vec![];
                        result.push(Operand::CONST(NodeFuncs::as_string(&cursor.node(), source)));
                        result
                    },
                }
            }
            "relation_contains" => {
                cursor.goto_first_child();
                RelationElement {
                    obj: Operand::COLUMN(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::CONTAINS,
                    value: {
                        // consume column value
                        cursor.goto_next_sibling();
                        // consume 'CONTAINS'
                        cursor.goto_next_sibling();
                        let mut result = vec![];
                        result.push(Operand::CONST(NodeFuncs::as_string(&cursor.node(), source)));
                        result
                    },
                }
            }
            _ => {
                let result = RelationElement {
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
                            let mut result = vec![];
                            result.push(Operand::TUPLE(values));
                            result
                        } else {
                            values
                        }
                    },
                };
                return result;
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

    fn parse_relation_value(cursor: &mut TreeCursor, source: &String) -> Operand {
        let node = cursor.node();
        let kind = node.kind();
        match kind {
            "column" => Operand::COLUMN(NodeFuncs::as_string(&node, &source)),
            "function_call" => Operand::FUNC(NodeFuncs::as_string(&node, &source)),
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

    fn parse_order_spec(node: &Node, source: &String) -> Option<OrderClause> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume "ORDER"
        cursor.goto_next_sibling();
        // consume "BY"
        cursor.goto_next_sibling();
        Some(OrderClause {
            name: NodeFuncs::as_string(&cursor.node(), &source),
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

    fn parse_select_element(node: &Node, source: &String) -> SelectElement {
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
            "column" => SelectElement::COLUMN(Named {
                name: NodeFuncs::as_string(&type_, source),
                alias,
            }),
            "function_call" => SelectElement::FUNCTION(Named {
                name: NodeFuncs::as_string(&type_, source),
                alias,
            }),
            _ => SelectElement::DOT_STAR(NodeFuncs::as_string(&type_, source)),
        }
    }

    fn parse_standard_drop(node: &Node, source: &String) -> DropData {
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
}

impl ToString for CassandraStatement {
    fn to_string(&self) -> String {
        // TODO remove this
        let unimplemented = String::from("Unimplemented");
        match self {
            CassandraStatement::AlterKeyspace(keyspace_data) => format!("ALTER {}", keyspace_data),
            CassandraStatement::AlterMaterializedView => unimplemented,
            CassandraStatement::AlterRole(role_data) => format!("ALTER {}", role_data),
            CassandraStatement::AlterTable => unimplemented,
            CassandraStatement::AlterType => unimplemented,
            CassandraStatement::AlterUser(user_data) => format!("ALTER {}", user_data),
            CassandraStatement::ApplyBatch => String::from("APPLY BATCH"),
            CassandraStatement::CreateAggregate => unimplemented,
            CassandraStatement::CreateFunction => unimplemented,
            CassandraStatement::CreateIndex => unimplemented,
            CassandraStatement::CreateKeyspace(keyspace_data) => {
                format!("CREATE {}", keyspace_data)
            }
            CassandraStatement::CreateMaterializedView => unimplemented,
            CassandraStatement::CreateRole(role_data) => format!("CREATE {}", role_data),
            CassandraStatement::CreateTable => unimplemented,
            CassandraStatement::CreateTrigger => unimplemented,
            CassandraStatement::CreateType => unimplemented,
            CassandraStatement::CreateUser(user_data) => format!("CREATE {}", user_data),
            CassandraStatement::DeleteStatement(statement_data) => statement_data.to_string(),
            CassandraStatement::DropAggregate(drop_data) => drop_data.get_text("AGGREGATE"),
            CassandraStatement::DropFunction(drop_data) => drop_data.get_text("FUNCTION"),
            CassandraStatement::DropIndex(drop_data) => drop_data.get_text("INDEX"),
            CassandraStatement::DropKeyspace(drop_data) => drop_data.get_text("KEYSPACE"),
            CassandraStatement::DropMaterializedView(drop_data) => {
                drop_data.get_text("MATERIALIZED VIEW")
            }
            CassandraStatement::DropRole(drop_data) => drop_data.get_text("ROLE"),
            CassandraStatement::DropTable(drop_data) => drop_data.get_text("TABLE"),
            CassandraStatement::DropTrigger => unimplemented,
            CassandraStatement::DropType(drop_data) => drop_data.get_text("TYPE"),
            CassandraStatement::DropUser(drop_data) => drop_data.get_text("USER"),
            CassandraStatement::Grant(grant_data) => format!("GRANT {} ON {} TO {}", grant_data.privilege, grant_data.resource.as_ref().unwrap(), &grant_data.role.as_ref().unwrap()),
            CassandraStatement::InsertStatement(statement_data) => statement_data.to_string(),
            CassandraStatement::ListPermissions(grant_data) => {
                let mut result = format!("LIST {}", grant_data.privilege);
                if grant_data.resource.is_some() {
                    result.push_str(" ON ");
                    result.push_str( grant_data.resource.as_ref().unwrap().to_string().as_str() );
                }
                if grant_data.role.is_some() {
                    result.push_str(" OF ");
                    result.push_str( grant_data.role.as_ref().unwrap().as_str() );
                }
                result
            },
            CassandraStatement::ListRoles => unimplemented,
            CassandraStatement::Revoke(grant_data) => format!("REVOKE {} ON {} FROM {}", grant_data.privilege, grant_data.resource.as_ref().unwrap(), grant_data.role.as_ref().unwrap()),
            CassandraStatement::SelectStatement(statement_data) => statement_data.to_string(),
            CassandraStatement::Truncate(table) => format!("TRUNCATE TABLE {}", table).to_string(),
            CassandraStatement::Update(statement_data) => statement_data.to_string(),
            CassandraStatement::UseStatement(keyspace) => format!("USE {}", keyspace).to_string(),
            CassandraStatement::UNKNOWN(_) => unimplemented,
        }
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
            Privilege::ALTER => write!(f, "ALTER" ),
            Privilege::AUTHORIZE => write!(f, "AUTHORIZE" ),
            Privilege::DESCRIBE => write!(f, "DESCRIBE" ),
            Privilege::EXECUTE => write!(f, "EXECUTE" ),
            Privilege::CREATE => write!(f, "CREATE" ),
            Privilege::DROP => write!(f, "DROP" ),
            Privilege::MODIFY => write!(f, "MODIFY" ),
            Privilege::SELECT => write!(f, "SELECT" ),
            }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum Resource {
    // optional keyspace
    ALL_FUNCTIONS( Option<String> ),
    ALL_KEYSPACES,
    ALL_ROLES,
    FUNCTION( String),
    KEYSPACE( String),
    ROLE(String),
    TABLE( String ),
}

impl Display for Resource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Resource::ALL_FUNCTIONS(str) => {
                if str.is_some() {
                    write!(f, "ALL FUNCTIONS IN KEYSPACE {}", str.as_ref().unwrap())
                } else { write!(f, "ALL FUNCTIONS") }
            },
            Resource::ALL_KEYSPACES => write!(f, "ALL KEYSPACES"),
            Resource::ALL_ROLES => write!(f, "ALL ROLES"),
            Resource::FUNCTION(func) => write!(f, "FUNCTION {}", func),
            Resource::KEYSPACE(keyspace) => write!(f, "KEYSPACE {}", keyspace),
            Resource::ROLE(role) => write!(f, "ROLE {}", role),
            Resource::TABLE(table) => write!(f, "TABLE {}", table),
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
        node.utf8_text(&self.text.as_bytes()).unwrap().to_string()
    }
}

/// The SearchPattern object used for string pattern matching
pub struct SearchPattern {
    /// the plain text version of the name to search for.
    pub name_str: String,
    /// the regex version of the name to search for.
    pub name: Regex,
    /// the plain text version of  the child name to search for
    pub child_str: Option<String>,
    /// the regex version of the child name to search for.
    pub child: Option<Regex>,
}

impl SearchPattern {
    /// Creates a SearchPattern from a string.
    ///
    /// The string is a series of names separated by slashes
    /// (e.g. ` foo / bar` )  This will match all `bar`s somewhere under
    /// `foo`.
    /// The string is a regular expression so `foo|bar` will match either 'foo' or 'bar'.
    ///
    /// There is a child pattern (also a regular expression) that will verify if a node has
    /// the child but still return the node.  (e.g. `foo[bar]` will return all `foo` nodes
    /// that have a `bar` somewhere below them.
    pub fn from_str(pattern: &str) -> SearchPattern {
        let parts: Vec<&str> = pattern.split("[").collect();
        let name_pattern = format!("^{}$", parts[0].trim());
        let child_pattern = if parts.len() == 2 {
            let name: Vec<&str> = parts[1].split("]").collect();
            Some(format!("^{}$", name[0].trim()))
        } else {
            None
        };
        SearchPattern {
            name_str: name_pattern.clone(),
            name: Regex::new(name_pattern.as_str()).unwrap(),
            child: match &child_pattern {
                Some(pattern) => Some(Regex::new(pattern.as_str()).unwrap()),
                None => None,
            },
            child_str: child_pattern,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cassandra_ast::{CassandraAST, CassandraStatement};

    fn test_parsing(expected: &[&str], statements: &[&str]) {
        for i in 0..statements.len() {
            let ast = CassandraAST::new(statements[i].to_string());
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
        let qry = "ALTER KEYSPACE keyspace WITH REPLICATION = {'foo':'bar', 'baz':5}";
        let ast = CassandraAST::new(qry.to_string());
        let stmt = ast.statement;
        let stmt_str = stmt.to_string();
        assert_eq!(qry, stmt_str);
    }

    #[test]
    fn test_get_statement_type() {
        let stmts = [
            "ALTER MATERIALIZED VIEW 'keyspace'.mview;",
            "ALTER TABLE keyspace.table DROP column1, column2;",
            "ALTER TYPE type ALTER column TYPE UUID;",
            "APPLY BATCH;",
            "CREATE AGGREGATE keyspace.aggregate  ( ASCII ) SFUNC sfunc STYPE BIGINT FINALFUNC finalFunc INITCOND (( 5, 'text', 6.3),(4,'foo',3.14));",
            "CREATE FUNCTION IF NOT EXISTS func ( param1 int , param2 text) CALLED ON NULL INPUT RETURNS INT LANGUAGE javascript AS $$ return 5; $$;",
            "CREATE INDEX index_name ON keyspace.table (column);",
            "CREATE MATERIALIZED VIEW keyspace.view AS SELECT col1, col2 FROM ks_target.tbl_target WHERE col3 IS NOT NULL AND col4 IS NOT NULL AND col5 <> 'foo' PRIMARY KEY (col1) WITH option1 = 'option' AND option2 = 3.5 AND CLUSTERING ORDER BY (col2 DESC);",
            "CREATE TABLE table (col1 text, col2 int, col3 FROZEN<col4>, PRIMARY KEY (col1, col2) ) WITH option = 'option' AND option2 = 3.5;",
            "CREATE TRIGGER if not exists keyspace.trigger_name USING 'trigger_class';",
            "CREATE TYPE type ( col1 'foo');",
            "DROP TRIGGER trigger_name ON ks.table_name;",
            "LIST ROLES;",
            "Not a valid statement"];
        let types = [
            CassandraStatement::AlterMaterializedView,
            CassandraStatement::AlterTable,
            CassandraStatement::AlterType,
            CassandraStatement::ApplyBatch,
            CassandraStatement::CreateAggregate,
            CassandraStatement::CreateFunction,
            CassandraStatement::CreateIndex,
            CassandraStatement::CreateMaterializedView,
            CassandraStatement::CreateTable,
            CassandraStatement::CreateTrigger,
            CassandraStatement::CreateType,
            CassandraStatement::DropTrigger,
            CassandraStatement::ListRoles,
            CassandraStatement::UNKNOWN("Not a valid statement".to_string()),
        ];

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
}
