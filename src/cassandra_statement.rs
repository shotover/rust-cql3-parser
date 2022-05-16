use crate::aggregate::Aggregate;
use crate::alter_materialized_view::AlterMaterializedView;
use crate::alter_table::AlterTable;
use crate::alter_type::AlterType;
use crate::cassandra_ast::{CassandraParser, ParsedStatement};
use crate::common::{FQName, Privilege};
use crate::common_drop::CommonDrop;
use crate::create_functon::CreateFunction;
use crate::create_index::CreateIndex;
use crate::create_keyspace::CreateKeyspace;
use crate::create_materialized_view::CreateMaterializedView;
use crate::create_table::CreateTable;
use crate::create_trigger::CreateTrigger;
use crate::create_type::CreateType;
use crate::create_user::CreateUser;
use crate::delete::Delete;
use crate::drop_trigger::DropTrigger;
use crate::insert::Insert;
use crate::list_role::ListRole;
use crate::role_common::RoleCommon;
use crate::select::Select;
use crate::update::Update;
use std::fmt::{Display, Formatter};
use tree_sitter::{Node, Tree};

/// The Supported Cassandra CQL3 statements
/// Documentation for statements can be found at
/// https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlCommandsTOC.html
#[derive(PartialEq, Debug, Clone)]
pub enum CassandraStatement {
    AlterKeyspace(CreateKeyspace),
    AlterMaterializedView(AlterMaterializedView),
    AlterRole(RoleCommon),
    AlterTable(AlterTable),
    AlterType(AlterType),
    AlterUser(CreateUser),
    ApplyBatch,
    CreateAggregate(Aggregate),
    CreateFunction(CreateFunction),
    CreateIndex(CreateIndex),
    CreateKeyspace(CreateKeyspace),
    CreateMaterializedView(CreateMaterializedView),
    CreateRole(RoleCommon),
    CreateTable(CreateTable),
    CreateTrigger(CreateTrigger),
    CreateType(CreateType),
    CreateUser(CreateUser),
    Delete(Delete),
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
    Grant(Privilege),
    Insert(Insert),
    ListPermissions(Privilege),
    ListRoles(ListRole),
    Revoke(Privilege),
    Select(Select),
    Truncate(FQName),
    Update(Update),
    Use(String),
    Unknown(String),
}

impl CassandraStatement {
    /// extract the cassandra statement from an AST tree.
    /// the boolean return value is `true` if there is a parsing error in the statement tree.
    pub fn from_tree(tree: &Tree, source: &str) -> Vec<ParsedStatement> {
        let mut result = vec![];
        let mut cursor = tree.root_node().walk();
        let mut process = cursor.goto_first_child();
        while process {
            result.push(ParsedStatement::new(cursor.node(), source));
            process = cursor.goto_next_sibling();
            while process && cursor.node().kind().eq(";") {
                process = cursor.goto_next_sibling();
            }
        }
        result
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
                CassandraStatement::AlterRole(CassandraParser::parse_create_role(node, source))
            }
            "alter_table" => {
                CassandraStatement::AlterTable(CassandraParser::parse_alter_table(node, source))
            }
            "alter_type" => {
                CassandraStatement::AlterType(CassandraParser::parse_alter_type(node, source))
            }
            "alter_user" => {
                CassandraStatement::AlterUser(CassandraParser::parse_create_user(node, source))
            }
            "apply_batch" => CassandraStatement::ApplyBatch,
            "create_aggregate" => CassandraStatement::CreateAggregate(
                CassandraParser::parse_create_aggregate(node, source),
            ),
            "create_function" => CassandraStatement::CreateFunction(
                CassandraParser::parse_function_data(node, source),
            ),
            "create_index" => {
                CassandraStatement::CreateIndex(CassandraParser::parse_index(node, source))
            }
            "create_keyspace" => CassandraStatement::CreateKeyspace(
                CassandraParser::parse_keyspace_data(node, source),
            ),
            "create_materialized_view" => CassandraStatement::CreateMaterializedView(
                CassandraParser::parse_create_materialized_vew(node, source),
            ),
            "create_role" => {
                CassandraStatement::CreateRole(CassandraParser::parse_create_role(node, source))
            }
            "create_table" => {
                CassandraStatement::CreateTable(CassandraParser::parse_create_table(node, source))
            }
            "create_trigger" => CassandraStatement::CreateTrigger(
                CassandraParser::parse_create_trigger(node, source),
            ),
            "create_type" => {
                CassandraStatement::CreateType(CassandraParser::parse_create_type(node, source))
            }
            "create_user" => {
                CassandraStatement::CreateUser(CassandraParser::parse_create_user(node, source))
            }
            "delete_statement" => {
                CassandraStatement::Delete(CassandraParser::parse_delete_statement(node, source))
            }
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
            "grant" => CassandraStatement::Grant(CassandraParser::parse_privilege(node, source)),
            "insert_statement" => {
                CassandraStatement::Insert(CassandraParser::parse_insert(node, source))
            }
            "list_permissions" => {
                CassandraStatement::ListPermissions(CassandraParser::parse_privilege(node, source))
            }
            "list_roles" => {
                CassandraStatement::ListRoles(CassandraParser::parse_list_role_data(node, source))
            }
            "revoke" => CassandraStatement::Revoke(CassandraParser::parse_privilege(node, source)),
            "select_statement" => {
                CassandraStatement::Select(CassandraParser::parse_select(node, source))
            }
            "truncate" => {
                CassandraStatement::Truncate(CassandraParser::parse_truncate(node, source))
            }
            "update" => CassandraStatement::Update(CassandraParser::parse_update(node, source)),
            "use" => CassandraStatement::Use(CassandraParser::parse_use(node, source)),
            _ => CassandraStatement::Unknown(source.to_string()),
        }
    }

    pub fn get_keyspace<'a>(&'a self, default: &'a str) -> &'a str {
        match self {
            CassandraStatement::AlterKeyspace(named) => &named.name,
            CassandraStatement::AlterMaterializedView(named) => {
                named.name.extract_keyspace(default)
            }
            CassandraStatement::AlterRole(_) => default,
            CassandraStatement::AlterTable(named) => named.name.extract_keyspace(default),
            CassandraStatement::AlterType(named) => named.name.extract_keyspace(default),
            CassandraStatement::AlterUser(_) => default,
            CassandraStatement::ApplyBatch => default,
            CassandraStatement::CreateAggregate(named) => named.name.extract_keyspace(default),
            CassandraStatement::CreateFunction(named) => named.name.extract_keyspace(default),
            CassandraStatement::CreateIndex(named) => named.table.extract_keyspace(default),
            CassandraStatement::CreateKeyspace(named) => &named.name,
            CassandraStatement::CreateMaterializedView(named) => {
                named.name.extract_keyspace(default)
            }
            CassandraStatement::CreateRole(_) => default,
            CassandraStatement::CreateTable(named) => named.name.extract_keyspace(default),
            CassandraStatement::CreateTrigger(named) => named.name.extract_keyspace(default),
            CassandraStatement::CreateType(named) => named.name.extract_keyspace(default),
            CassandraStatement::CreateUser(_) => default,
            CassandraStatement::Delete(named) => named.table_name.extract_keyspace(default),
            CassandraStatement::DropAggregate(named) => named.name.extract_keyspace(default),
            CassandraStatement::DropFunction(named) => named.name.extract_keyspace(default),
            CassandraStatement::DropIndex(named) => named.name.extract_keyspace(default),
            CassandraStatement::DropKeyspace(named) => &named.name.name,
            CassandraStatement::DropMaterializedView(named) => named.name.extract_keyspace(default),
            CassandraStatement::DropRole(_) => default,
            CassandraStatement::DropTable(named) => named.name.extract_keyspace(default),
            CassandraStatement::DropTrigger(named) => named.name.extract_keyspace(default),
            CassandraStatement::DropType(named) => named.name.extract_keyspace(default),
            CassandraStatement::DropUser(_) => default,
            CassandraStatement::Grant(_) => default,
            CassandraStatement::Insert(named) => named.table_name.extract_keyspace(default),
            CassandraStatement::ListPermissions(_) => default,
            CassandraStatement::ListRoles(_) => default,
            CassandraStatement::Revoke(_) => default,
            CassandraStatement::Select(named) => named.table_name.extract_keyspace(default),
            CassandraStatement::Truncate(named) => named.extract_keyspace(default),
            CassandraStatement::Update(named) => named.table_name.extract_keyspace(default),
            CassandraStatement::Use(named) => named,
            CassandraStatement::Unknown(_) => default,
        }
    }

    pub fn short_name(&self) -> &'static str {
        match self {
            CassandraStatement::AlterKeyspace(_) => "ALTER KEYSPACE",
            CassandraStatement::AlterMaterializedView(_) => "ALTER MATERIALIZED VIEW",
            CassandraStatement::AlterRole(_) => "ALTER ROLE",
            CassandraStatement::AlterTable(_) => "ALTER TABLE",
            CassandraStatement::AlterType(_) => "ALTER TYPE",
            CassandraStatement::AlterUser(_) => "ALTER USER",
            CassandraStatement::ApplyBatch => "APPLY BATCH",
            CassandraStatement::CreateAggregate(_) => "CREATE AGGREGATE",
            CassandraStatement::CreateFunction(_) => "CREATE FUNCTION",
            CassandraStatement::CreateIndex(_) => "CREATE INDEX",
            CassandraStatement::CreateKeyspace(_) => "CREATE KEYSPACE",
            CassandraStatement::CreateMaterializedView(_) => "CREATE MATERIALIZED VIEW",
            CassandraStatement::CreateRole(_) => "CREATE ROLE",
            CassandraStatement::CreateTable(_) => "CREATE TABLE",
            CassandraStatement::CreateTrigger(_) => "CREATE TRIGGER",
            CassandraStatement::CreateType(_) => "CREATE TYPE",
            CassandraStatement::CreateUser(_) => "CREATE USER",
            CassandraStatement::Delete(_) => "DELETE",
            CassandraStatement::DropAggregate(_) => "DROP AGGREGATE",
            CassandraStatement::DropFunction(_) => "DROP FUNCTION",
            CassandraStatement::DropIndex(_) => "DROP INDEX",
            CassandraStatement::DropKeyspace(_) => "DROP KEYSPACE",
            CassandraStatement::DropMaterializedView(_) => "DROP MATERIALIZED VIEW",
            CassandraStatement::DropRole(_) => "DROP ROLE",
            CassandraStatement::DropTable(_) => "DROP TABLE",
            CassandraStatement::DropTrigger(_) => "DROP TRIGGER",
            CassandraStatement::DropType(_) => "DROP TYPE",
            CassandraStatement::DropUser(_) => "DROP USER",
            CassandraStatement::Grant(_) => "GRANT",
            CassandraStatement::Insert(_) => "INSERT",
            CassandraStatement::ListPermissions(_) => "LIST PERMISSIONS",
            CassandraStatement::ListRoles(_) => "LIST ROLES",
            CassandraStatement::Revoke(_) => "REVOKE",
            CassandraStatement::Select(_) => "SELECT",
            CassandraStatement::Truncate(_) => "TRUNCATE",
            CassandraStatement::Update(_) => "UPDATE",
            CassandraStatement::Use(_) => "USE",
            CassandraStatement::Unknown(_) => "UNRECOGNIZED CQL",
        }
    }

    /// returns the table name from the statement if there is one.
    pub fn get_table_name(&self) -> Option<&FQName> {
        match self {
            CassandraStatement::AlterTable(t) => Some(&t.name),
            CassandraStatement::CreateIndex(i) => Some(&i.table),
            CassandraStatement::CreateMaterializedView(m) => Some(&m.table),
            CassandraStatement::CreateTable(t) => Some(&t.name),
            CassandraStatement::Delete(d) => Some(&d.table_name),
            CassandraStatement::DropTable(t) => Some(&t.name),
            CassandraStatement::DropTrigger(t) => Some(&t.table),
            CassandraStatement::Insert(i) => Some(&i.table_name),
            CassandraStatement::Select(s) => Some(&s.table_name),
            CassandraStatement::Truncate(t) => Some(t),
            CassandraStatement::Update(u) => Some(&u.table_name),
            _ => None,
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
            CassandraStatement::Delete(statement_data) => write!(f, "{}", statement_data),
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
            CassandraStatement::Unknown(query) => write!(f, "{}", query),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cassandra_ast::CassandraAST;

    // only tests single results
    fn test_parsing(expected: &[&str], statements: &[&str]) {
        for i in 0..statements.len() {
            let ast = CassandraAST::new(statements[i]);
            assert!(
                !ast.has_error(),
                "AST has error\n{}\n{} ",
                statements[i],
                ast.tree.root_node().to_sexp()
            );
            let stmt = &ast.statements[0];
            assert!(!stmt.has_error);
            let stmt_str = stmt.statement.to_string();
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
            "SELECT column from table where col=?",
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
            "SELECT column FROM table WHERE col = 0XFF",
            "SELECT column FROM table WHERE col = 0Xef",
            "SELECT column FROM table WHERE col = true",
            "SELECT column FROM table WHERE col = false",
            "SELECT column FROM table WHERE col = NULL",
            "SELECT column FROM table WHERE col = NULL AND col2 = 'jinx'",
            "SELECT column FROM table WHERE col = $$ a code's block $$",
            "SELECT column FROM table WHERE func(*) < 5",
            "SELECT column FROM table WHERE func(*) <= 'hello'",
            "SELECT column FROM table WHERE func(*) = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2",
            "SELECT column FROM table WHERE func(*) <> -5",
            "SELECT column FROM table WHERE func(*) >= 3.5",
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
            "SELECT column FROM table WHERE col = ?",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn test_insert_statements() {
        let stmts = [
            "BEGIN LOGGED BATCH USING TIMESTAMP 5 INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5);",
            "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) IF NOT EXISTS",
            "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) USING TIMESTAMP 3",
            "INSERT INTO table (col1, col2) JSON $$ json code $$",
            "INSERT INTO table (col1, col2) VALUES ({ 5 : 6 }, 'foo')",
            "INSERT INTO table (col1, col2) VALUES ({ 5, 6 }, 'foo')",
            "INSERT INTO table (col1, col2) VALUES ([ 5, 6 ], 'foo')",
            "INSERT INTO table (col1, col2) VALUES (( 5, 6 ), 'foo')",
            "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', ?) IF NOT EXISTS",
    ];
        let expected = [
            "BEGIN LOGGED BATCH USING TIMESTAMP 5 INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5)",
            "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) IF NOT EXISTS",
            "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', 5) USING TIMESTAMP 3",
            "INSERT INTO table (col1, col2) JSON $$ json code $$",
            "INSERT INTO table (col1, col2) VALUES ({5:6}, 'foo')",
            "INSERT INTO table (col1, col2) VALUES ({5, 6}, 'foo')",
            "INSERT INTO table (col1, col2) VALUES ([5, 6], 'foo')",
            "INSERT INTO table (col1, col2) VALUES ((5, 6), 'foo')",
            "INSERT INTO keyspace.table (col1, col2) VALUES ('hello', ?) IF NOT EXISTS",
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
            "DELETE column, column3 from keyspace.table WHERE column2=?",
            "DELETE column, column3 from keyspace.table WHERE column2='foo' IF column4 = ?",
        ];
        let expected  = [
            "BEGIN LOGGED BATCH USING TIMESTAMP 5 DELETE column['hello'] FROM table WHERE column2 = 'foo' IF EXISTS",
            "BEGIN UNLOGGED BATCH DELETE column[6] FROM keyspace.table USING TIMESTAMP 5 WHERE column2 = 'foo' IF column3 = 'stuff'",
            "BEGIN BATCH DELETE column['hello'] FROM keyspace.table WHERE column2 = 'foo'",
            "DELETE FROM table WHERE column2 = 'foo'",
            "DELETE column, column3 FROM keyspace.table WHERE column2 = 'foo'",
            "DELETE column, column3 FROM keyspace.table WHERE column2 = 'foo' IF column4 = 'bar'",
            "DELETE column, column3 FROM keyspace.table WHERE column2 = ?",
            "DELETE column, column3 FROM keyspace.table WHERE column2 = 'foo' IF column4 = ?",
        ];
        test_parsing(&expected, &stmts);
    }

    #[test]
    fn x() {
        let qry = "DELETE column, column3 FROM keyspace.table WHERE column2 = 'foo' IF column4 = ?";
        let ast = CassandraAST::new(qry);
        let stmt = &ast.statements[0];
        let stmt_str = stmt.statement.to_string();
        assert_eq!(qry, stmt_str);
    }

    #[test]
    fn test_has_error() {
        let ast = CassandraAST::new("SELECT foo from bar.baz where fu='something'");
        assert!(!ast.has_error());
        let ast = CassandraAST::new("Not a valid statement");
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
            "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2=5;",
            "UPDATE foo SET c = 'yo', v = 123 WHERE z = 1",
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
            "UPDATE keyspace.table USING TIMESTAMP 3 SET col1 = 'foo' WHERE col2 = 5",
            "UPDATE foo SET c = 'yo', v = 123 WHERE z = 1",
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
}
