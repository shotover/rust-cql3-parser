use tree_sitter::{Node, Tree};
use std::fmt::{Display, Formatter};
use crate::cassandra_ast::{CassandraParser, NodeFuncs};
use crate::cassandra_ast::aggregate::Aggregate;
use crate::cassandra_ast::alter_materialized_view::AlterMaterializedView;
use crate::cassandra_ast::alter_table::AlterTable;
use crate::cassandra_ast::alter_type::AlterType;
use crate::cassandra_ast::common::PrivilegeData;
use crate::cassandra_ast::common_drop::CommonDrop;
use crate::cassandra_ast::create_functon::CreateFunction;
use crate::cassandra_ast::create_index::CreateIndex;
use crate::cassandra_ast::create_keyspace::CreateKeyspace;
use crate::cassandra_ast::create_materialized_view::CreateMaterializedView;
use crate::cassandra_ast::create_role::CreateRole;
use crate::cassandra_ast::create_table::CreateTable;
use crate::cassandra_ast::create_trigger::CreateTrigger;
use crate::cassandra_ast::create_type::CreateType;
use crate::cassandra_ast::create_user::CreateUser;
use crate::cassandra_ast::delete::Delete;
use crate::cassandra_ast::drop_trigger::DropTrigger;
use crate::cassandra_ast::insert::Insert;
use crate::cassandra_ast::list_role::ListRole;
use crate::cassandra_ast::select::Select;
use crate::cassandra_ast::update::Update;

/// The Supported Cassandra CQL3 statements
/// Documentation for statements can be found at
/// https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlCommandsTOC.html
#[derive(PartialEq, Debug, Clone)]
pub enum CassandraStatement {
    AlterKeyspace(CreateKeyspace),
    AlterMaterializedView(AlterMaterializedView),
    AlterRole(CreateRole),
    AlterTable(AlterTable),
    AlterType(AlterType),
    AlterUser(CreateUser),
    ApplyBatch,
    CreateAggregate(Aggregate),
    CreateFunction(CreateFunction),
    CreateIndex(CreateIndex),
    CreateKeyspace(CreateKeyspace),
    CreateMaterializedView(CreateMaterializedView),
    CreateRole(CreateRole),
    CreateTable(CreateTable),
    CreateTrigger(CreateTrigger),
    CreateType(CreateType),
    CreateUser(CreateUser),
    DeleteStatement(Delete),
    DropAggregate(CommonDrop),
    DropFunction(CommonDrop),
    DropIndex(CommonDrop),
    DropKeyspace(CommonDrop),
    DropMaterializedView(CommonDrop),
    DropRole(CommonDrop),
    DropTable(CommonDrop),
    DropTrigger(DropTrigger),
    DropType(CommonDrop),
    DropUser(CommonDrop),
    Grant(PrivilegeData),
    Insert(Insert),
    ListPermissions(PrivilegeData),
    ListRoles(ListRole),
    Revoke(PrivilegeData),
    Select(Select),
    Truncate(String),
    Update(Update),
    Use(String),
    UNKNOWN(String),
}

impl CassandraStatement {
    /// extract the cassandra statement from an AST tree.
    pub fn from_tree(tree: &Tree, source: &str) -> CassandraStatement {
        let mut node = tree.root_node();
        if node.kind().eq("source_file") {
            node = node.child(0).unwrap();
        }
        CassandraStatement::from_node(&node, source)
    }

    /// extract the cassandra statement from an AST node.
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
            "drop_function" => {
                CassandraStatement::DropFunction(CassandraParser::parse_standard_drop(node, source))
            }
            "drop_index" => {
                CassandraStatement::DropIndex(CassandraParser::parse_standard_drop(node, source))
            }
            "drop_keyspace" => {
                CassandraStatement::DropKeyspace(CassandraParser::parse_standard_drop(node, source))
            }
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
            "insert_statement" => {
                CassandraStatement::Insert(CassandraParser::parse_insert_statement(node, source))
            }
            "list_permissions" => CassandraStatement::ListPermissions(
                CassandraParser::parse_privilege_data(node, source),
            ),
            "list_roles" => {
                CassandraStatement::ListRoles(CassandraParser::parse_list_role_data(node, source))
            }
            "revoke" => {
                CassandraStatement::Revoke(CassandraParser::parse_privilege_data(node, source))
            }
            "select_statement" => {
                CassandraStatement::Select(CassandraParser::parse_select_statement(node, source))
            }
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
                    CassandraStatement::Use(NodeFuncs::as_string(&cursor.node(), source))
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

impl Display for CassandraStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CassandraStatement::AlterKeyspace(keyspace_data) => {
                write!(f, "ALTER {}", keyspace_data)
            }
            CassandraStatement::AlterMaterializedView(alter_data) => write!(f, "{}", alter_data),
            CassandraStatement::AlterRole(role_data) => write!(f, "ALTER {}", role_data),
            CassandraStatement::AlterTable(table_data) => {
                write!(
                    f,
                    "ALTER TABLE {} {}",
                    table_data.name, table_data.operation
                )
            }
            CassandraStatement::AlterType(alter_type_data) => write!(f, "{}", alter_type_data),
            CassandraStatement::AlterUser(user_data) => write!(f, "ALTER {}", user_data),
            CassandraStatement::ApplyBatch => write!(f, "APPLY BATCH"),
            CassandraStatement::CreateAggregate(aggregate_data) => write!(f, "{}", aggregate_data),
            CassandraStatement::CreateFunction(function_data) => write!(f, "{}", function_data),
            CassandraStatement::CreateIndex(index_data) => write!(f, "{}", index_data),
            CassandraStatement::CreateKeyspace(keyspace_data) => {
                write!(f, "CREATE {}", keyspace_data)
            }
            CassandraStatement::CreateMaterializedView(view_data) => write!(f, "{}", view_data),
            CassandraStatement::CreateRole(role_data) => write!(f, "CREATE {}", role_data),
            CassandraStatement::CreateTable(table_data) => write!(f, "CREATE TABLE {}", table_data),
            CassandraStatement::CreateTrigger(trigger_data) => write!(f, "{}", trigger_data),
            CassandraStatement::CreateType(type_data) => write!(f, "{}", type_data),
            CassandraStatement::CreateUser(user_data) => write!(f, "CREATE {}", user_data),
            CassandraStatement::DeleteStatement(statement_data) => write!(f, "{}", statement_data),
            CassandraStatement::DropAggregate(drop_data) => {
                write!(f, "{}", drop_data.get_text("AGGREGATE"))
            }
            CassandraStatement::DropFunction(drop_data) => {
                write!(f, "{}", drop_data.get_text("FUNCTION"))
            }
            CassandraStatement::DropIndex(drop_data) => {
                write!(f, "{}", drop_data.get_text("INDEX"))
            }
            CassandraStatement::DropKeyspace(drop_data) => {
                write!(f, "{}", drop_data.get_text("KEYSPACE"))
            }
            CassandraStatement::DropMaterializedView(drop_data) => {
                write!(f, "{}", drop_data.get_text("MATERIALIZED VIEW"))
            }
            CassandraStatement::DropRole(drop_data) => write!(f, "{}", drop_data.get_text("ROLE")),
            CassandraStatement::DropTable(drop_data) => {
                write!(f, "{}", drop_data.get_text("TABLE"))
            }
            CassandraStatement::DropTrigger(drop_trigger_data) => {
                write!(f, "{}", drop_trigger_data)
            }
            CassandraStatement::DropType(drop_data) => write!(f, "{}", drop_data.get_text("TYPE")),
            CassandraStatement::DropUser(drop_data) => write!(f, "{}", drop_data.get_text("USER")),
            CassandraStatement::Grant(grant_data) => write!(
                f,
                "GRANT {} ON {} TO {}",
                grant_data.privilege,
                grant_data.resource.as_ref().unwrap(),
                &grant_data.role.as_ref().unwrap()
            ),
            CassandraStatement::Insert(statement_data) => write!(f, "{}", statement_data),
            CassandraStatement::ListPermissions(grant_data) => write!(
                f,
                "LIST {}{}{}",
                grant_data.privilege,
                grant_data
                    .resource
                    .as_ref()
                    .map_or("".to_string(), |x| format!(" ON {}", x)),
                grant_data
                    .role
                    .as_ref()
                    .map_or("".to_string(), |x| format!(" OF {}", x))
            ),
            CassandraStatement::ListRoles(data) => write!(f, "{}", data),
            CassandraStatement::Revoke(grant_data) => write!(
                f,
                "REVOKE {} ON {} FROM {}",
                grant_data.privilege,
                grant_data.resource.as_ref().unwrap(),
                grant_data.role.as_ref().unwrap()
            ),
            CassandraStatement::Select(statement_data) => write!(f, "{}", statement_data),
            CassandraStatement::Truncate(table) => write!(f, "TRUNCATE TABLE {}", table),
            CassandraStatement::Update(statement_data) => write!(f, "{}", statement_data),
            CassandraStatement::Use(keyspace) => write!(f, "USE {}", keyspace),
            CassandraStatement::UNKNOWN(query) => write!(f, "{}", query),
        }
    }
}
