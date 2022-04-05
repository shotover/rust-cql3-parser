use crate::aggregate::{Aggregate, InitCondition};
use crate::alter_column::AlterColumnType;
use crate::alter_materialized_view::AlterMaterializedView;
use crate::alter_table::{AlterTable, AlterTableOperation};
use crate::alter_type::{AlterType, AlterTypeOperation};
use crate::begin_batch::BeginBatch;
use crate::cassandra_statement::CassandraStatement;
use crate::common::{
    ColumnDefinition, DataType, DataTypeName, Operand, OptionValue, OrderClause, PrimaryKey,
    Privilege, PrivilegeType, RelationElement, RelationOperator, Resource, TtlTimestamp, WithItem,
};
use crate::common_drop::CommonDrop;
use crate::create_functon::CreateFunction;
use crate::create_index::{CreateIndex, IndexColumnType};
use crate::create_keyspace::CreateKeyspace;
use crate::create_materialized_view::CreateMaterializedView;
use crate::create_table::CreateTable;
use crate::create_trigger::CreateTrigger;
use crate::create_type::CreateType;
use crate::create_user::CreateUser;
use crate::delete::{Delete, IndexedColumn};
use crate::drop_trigger::DropTrigger;
use crate::insert::{Insert, InsertValues};
use crate::list_role::ListRole;
use crate::role_common::RoleCommon;
use crate::select::{Named, Select, SelectElement};
use crate::update::{AssignmentElement, AssignmentOperator, Update};
use tree_sitter::{Node, Tree, TreeCursor};

/// Functions for common manipulation of the nodes in the AST tree.
struct NodeFuncs {}
impl NodeFuncs {
    /// get the string value of the node
    pub fn as_string(node: &Node, source: &str) -> String {
        node.utf8_text(source.as_bytes()).unwrap().to_string()
    }
    /// the the value of the node as a boolean
    pub fn as_boolean(node: &Node, source: &str) -> bool {
        NodeFuncs::as_string(node, source).to_uppercase().eq("TRUE")
    }
}

/// The parser that walks the AST tree and produces a CassandraStatement.
pub struct CassandraParser {}
impl CassandraParser {
    pub fn parse_truncate(node: &Node, source: &str) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume until 'table_name'
        while !cursor.node().kind().eq("table_name") {
            cursor.goto_next_sibling();
        }
        CassandraParser::parse_table_name(&cursor.node(), source)
    }

    pub fn parse_use(node: &Node, source: &str) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'USE'
        cursor.goto_next_sibling();
        NodeFuncs::as_string(&cursor.node(), source)
    }

    /// parse the alter materialized view command
    pub fn parse_alter_materialized_view(node: &Node, source: &str) -> AlterMaterializedView {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume ALTER
        cursor.goto_next_sibling();
        // consume MATERIALIZED
        cursor.goto_next_sibling();
        // consume VIEW
        cursor.goto_next_sibling();
        AlterMaterializedView {
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            with_clause: if cursor.goto_next_sibling() {
                CassandraParser::parse_with_element(&cursor.node(), source)
            } else {
                vec![]
            },
        }
    }
    /// parse init_condition for aggregate data.
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
    /// parse a create aggregate data statement
    pub fn parse_create_aggregate(node: &Node, source: &str) -> Aggregate {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'CREATE'
        cursor.goto_next_sibling();
        Aggregate {
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

    /// parse a create function statement
    pub fn parse_function_data(node: &Node, source: &str) -> CreateFunction {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'CREATE'
        cursor.goto_next_sibling();
        CreateFunction {
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

    /// parse an alter type statement
    pub fn parse_alter_type(node: &Node, source: &str) -> AlterType {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'ALTER'
        cursor.goto_next_sibling();
        // consume 'TYPE'
        cursor.goto_next_sibling();
        AlterType {
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
                        AlterTypeOperation::AlterColumnType(AlterColumnType {
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

    /// parse an create type statement
    pub fn parse_create_type(node: &Node, source: &str) -> CreateType {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut result = CreateType {
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

    /// parse a create trigger statement
    pub fn parse_create_trigger(node: &Node, source: &str) -> CreateTrigger {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        CreateTrigger {
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

    /// parse the alter table operation.
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

    /// parse an alter table statement.
    pub fn parse_alter_table(node: &Node, source: &str) -> AlterTable {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'ALTER'
        cursor.goto_next_sibling();
        // consume 'TABLE'
        cursor.goto_next_sibling();
        // get the name
        AlterTable {
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            operation: {
                cursor.goto_next_sibling();
                CassandraParser::parse_alter_table_operation(&cursor.node(), source)
            },
        }
    }

    /// parse the primary key.
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

    /// parse the data type
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

    /// parse a column definition
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

    /// parse table options
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
                            value: OptionValue::Map(CassandraParser::parse_map(
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

    /// parse materialized view where statement
    fn parse_materialized_where(node: &Node, source: &str) -> Vec<RelationElement> {
        let mut relations: Vec<RelationElement> = vec![];
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consumer the WHERE
        while cursor.goto_next_sibling() {
            if cursor.node().kind().eq("column_not_null") {
                cursor.goto_first_child();
                relations.push(RelationElement {
                    obj: Operand::Column(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::IsNot,
                    value: Operand::Null,
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

    /// parse a create materialized view statement
    pub fn parse_create_materialized_vew(node: &Node, source: &str) -> CreateMaterializedView {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'CREATE'
        cursor.goto_next_sibling();
        CreateMaterializedView {
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
            with_clause: {
                if cursor.goto_next_sibling() {
                    CassandraParser::parse_with_element(&cursor.node(), source)
                } else {
                    vec![]
                }
            },
        }
    }

    /// parse a create table statement
    pub fn parse_create_table(node: &Node, source: &str) -> CreateTable {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut result = CreateTable {
            if_not_exists: CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor),
            name: CassandraParser::parse_table_name(&cursor.node(), source),
            columns: vec![],
            key: None,
            with_clause: vec![],
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
                    result.with_clause =
                        CassandraParser::parse_with_element(&cursor.node(), source);
                }
                _ => {}
            }
        }
        result
    }

    /// parse the `with` element.
    fn parse_with_element(node: &Node, source: &str) -> Vec<WithItem> {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        while cursor.goto_next_sibling() {
            if cursor.node().kind().eq("table_options") {
                return CassandraParser::parse_table_options(&cursor.node(), source);
            }
        }
        vec![]
    }

    /// parse create index statement.
    pub fn parse_index(node: &Node, source: &str) -> CreateIndex {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let mut result = CreateIndex {
            if_not_exists: CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor),
            name: None,
            table: "".to_string(),
            column: IndexColumnType::Column("".to_string()),
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
                            IndexColumnType::Keys(NodeFuncs::as_string(&cursor.node(), source))
                        }
                        "index_entries_s_spec" => {
                            cursor.goto_first_child();
                            cursor.goto_next_sibling();
                            // consume '('
                            cursor.goto_next_sibling();
                            IndexColumnType::Entries(NodeFuncs::as_string(&cursor.node(), source))
                        }
                        "index_full_spec" => {
                            cursor.goto_next_sibling();
                            // consume '('
                            cursor.goto_first_child();
                            cursor.goto_next_sibling();
                            // consume '('
                            cursor.goto_next_sibling();
                            IndexColumnType::Full(NodeFuncs::as_string(&cursor.node(), source))
                        }
                        _ => IndexColumnType::Column(NodeFuncs::as_string(&cursor.node(), source)),
                    };
                    cursor.goto_parent();
                }
                _ => {}
            }
            process = cursor.goto_next_sibling();
        }
        result
    }

    /// parse the list roles statement
    pub fn parse_list_role_data(node: &Node, source: &str) -> ListRole {
        let mut cursor = node.walk();
        let mut result = ListRole {
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

    /// parse a resource type
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

    /// parse the create role statement
    pub fn parse_create_role(node: &Node, source: &str) -> RoleCommon {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor);
        let mut result = RoleCommon {
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
                            result.password = Some(NodeFuncs::as_string(&cursor.node(), source));
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
                            result.superuser = Some(NodeFuncs::as_boolean(&cursor.node(), source));
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

    /// consume 2 keywords and check the not exists flag.
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

    /// consume 2 keywords and check the if exists flag.
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

    /// parse the create keyspace command
    pub fn parse_keyspace_data(node: &Node, source: &str) -> CreateKeyspace {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor);
        let mut result = CreateKeyspace {
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

    /// parse the create user statement
    pub fn parse_create_user(node: &Node, source: &str) -> CreateUser {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        let if_not_exists = CassandraParser::consume_2_keywords_and_check_not_exists(&mut cursor);

        let mut result = CreateUser {
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

    fn parse_update_assignments(node: &Node, source: &str) -> Vec<AssignmentElement> {
        let mut result = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();
        while process {
            if cursor.node().kind().eq("assignment_element") {
                result.push(CassandraParser::parse_assignment_element(
                    &cursor.node(),
                    source,
                ));
            }
            process = cursor.goto_next_sibling();
        }
        result
    }

    fn check_begin_batch(cursor: &mut TreeCursor, source: &str) -> Option<BeginBatch> {
        if cursor.node().kind().eq("begin_batch") {
            let result = Some(CassandraParser::parse_begin_batch(&cursor.node(), source));
            cursor.goto_next_sibling();
            result
        } else {
            None
        }
    }
    /// parse the update statement.
    pub fn parse_update(node: &Node, source: &str) -> Update {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        Update {
            begin_batch: CassandraParser::check_begin_batch(&mut cursor, source),
            table_name: {
                // consume UPDATE
                cursor.goto_next_sibling();
                CassandraParser::parse_table_name(&cursor.node(), source)
            },
            using_ttl: {
                cursor.goto_next_sibling();
                if cursor.node().kind().eq("using_ttl_timestamp") {
                    let result = Some(CassandraParser::parse_ttl_timestamp(&cursor.node(), source));
                    cursor.goto_next_sibling();
                    result
                } else {
                    None
                }
            },
            assignments: { CassandraParser::parse_update_assignments(&cursor.node(), source) },
            where_clause: {
                cursor.goto_next_sibling();
                CassandraParser::parse_where_spec(&cursor.node(), source)
            },
            if_exists: {
                cursor.goto_next_sibling();
                if cursor.node().kind().eq("IF") {
                    // consume EXISTS
                    cursor.goto_next_sibling();
                    true
                } else {
                    false
                }
            },
            if_clause: if cursor.node().kind().eq("if_spec") {
                cursor.goto_first_child();
                // consume IF
                cursor.goto_next_sibling();
                CassandraParser::parse_if_condition_list(&cursor.node(), source)
            } else {
                None
            },
        }
    }

    /// parse the privilege
    fn parse_privilege_type(node: &Node, source: &str) -> PrivilegeType {
        match NodeFuncs::as_string(node, source).to_uppercase().as_str() {
            "ALL" | "ALL PERMISSIONS" => PrivilegeType::All,
            "ALTER" => PrivilegeType::Alter,
            "AUTHORIZE" => PrivilegeType::Authorize,
            "DESCRIBE" => PrivilegeType::Describe,
            "EXECUTE" => PrivilegeType::Execute,
            "CREATE" => PrivilegeType::Create,
            "DROP" => PrivilegeType::Drop,
            "MODIFY" => PrivilegeType::Modify,
            "SELECT" => PrivilegeType::Select,
            _ => unreachable!(),
        }
    }

    /// parse the privilege data.
    pub fn parse_privilege(node: &Node, source: &str) -> Privilege {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let mut privilege: Option<PrivilegeType> = None;
        let mut resource: Option<Resource> = None;
        let mut role: Option<String> = None;
        // consume 'GRANT/REVOKE'
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "privilege" => {
                    privilege = Some(CassandraParser::parse_privilege_type(
                        &cursor.node(),
                        source,
                    ));
                }
                "resource" => {
                    resource = Some(CassandraParser::parse_resource(&cursor.node(), source));
                }
                "role" => role = Some(NodeFuncs::as_string(&cursor.node(), source)),
                _ => {}
            }
        }
        Privilege {
            privilege: privilege.unwrap(),
            resource,
            role,
        }
    }

    /// parse an assignment element
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

    /// parse a delete statement.
    pub fn parse_delete_statement(node: &Node, source: &str) -> Delete {
        let mut statement_data = Delete {
            begin_batch: None,
            table_name: String::from(""),
            columns: None,
            timestamp: None,
            where_clause: vec![],
            if_clause: None,
            if_exists: false,
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
                    statement_data.if_exists = true;
                }
                "if_spec" => {
                    cursor.goto_first_child();
                    // consume IF
                    cursor.goto_next_sibling();
                    statement_data.if_clause =
                        CassandraParser::parse_if_condition_list(&cursor.node(), source);
                    cursor.goto_parent();
                }
                _ => {}
            }
            process = cursor.goto_next_sibling();
        }
        statement_data
    }

    /// parse an `IF` condition list
    fn parse_if_condition_list(node: &Node, source: &str) -> Option<Vec<RelationElement>> {
        let mut result: Vec<RelationElement> = vec![];
        let mut cursor = node.walk();
        let mut process = cursor.goto_first_child();
        while process {
            result.push(CassandraParser::parse_relation_element(
                &cursor.node(),
                source,
            ));
            process = cursor.goto_next_sibling();
            // consume the 'AND' if it exists
            cursor.goto_next_sibling();
        }
        Some(result)
    }

    /// parse a delete column item
    fn parse_delete_column_item(node: &Node, source: &str) -> IndexedColumn {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        CassandraParser::parse_indexed_column(&mut cursor, source)
    }

    /// parse an indexed column
    fn parse_indexed_column(cursor: &mut TreeCursor, source: &str) -> IndexedColumn {
        IndexedColumn {
            column: NodeFuncs::as_string(&cursor.node(), source),

            idx: if cursor.goto_next_sibling() && cursor.node().kind().eq("[") {
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

    /// parse an insert statement.
    pub fn parse_insert(node: &Node, source: &str) -> Insert {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        Insert {
            begin_batch: CassandraParser::check_begin_batch(&mut cursor, source),
            table_name: {
                // consume INSERT
                cursor.goto_next_sibling();
                // consume INTO
                cursor.goto_next_sibling();
                CassandraParser::parse_table_name(&cursor.node(), source)
            },
            columns: {
                cursor.goto_next_sibling();
                cursor.goto_first_child();
                // consume the '(' at the beginning
                cursor.goto_next_sibling();
                let result = CassandraParser::parse_column_list(&cursor.node(), source);
                cursor.goto_parent();
                result
            },
            values: {
                cursor.goto_next_sibling();
                cursor.goto_first_child();
                let result = match cursor.node().kind() {
                    "VALUES" => {
                        cursor.goto_next_sibling();
                        // consume the '('
                        cursor.goto_next_sibling();
                        let expression_list =
                            CassandraParser::parse_expression_list(&cursor.node(), source);
                        InsertValues::Values(expression_list)
                    }
                    "JSON" => {
                        cursor.goto_next_sibling();
                        InsertValues::Json(NodeFuncs::as_string(&cursor.node(), source))
                    }
                    _ => unreachable!(),
                };
                cursor.goto_parent();
                result
            },
            if_not_exists: {
                if cursor.goto_next_sibling() {
                    if cursor.node().kind().eq("IF") {
                        // consume IF
                        cursor.goto_next_sibling();
                        // consume NOT
                        cursor.goto_next_sibling();
                        // consume EXISTS
                        cursor.goto_next_sibling();
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            },
            using_ttl: {
                if cursor.node().kind().eq("using_ttl_timestamp") {
                    Some(CassandraParser::parse_ttl_timestamp(&cursor.node(), source))
                } else {
                    None
                }
            },
        }
    }

    /// parse a column list
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

    /// parse the using timestamp sttement.
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

    /// parse the using ttl timestamp element.
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

    /// parse the `FROM` clause
    fn parse_from_spec(node: &Node, source: &str) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume 'FROM'
        cursor.goto_next_sibling();
        CassandraParser::parse_table_name(&cursor.node(), source)
    }

    /// parse a name that may have a keyspace specified.
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

    /// parse a table name
    fn parse_table_name(node: &Node, source: &str) -> String {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        CassandraParser::parse_dotted_name(&mut cursor, source)
    }

    /// parse the function args.
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

    /// parse an expressin list.
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

    /// parse an operand
    fn parse_operand(node: &Node, source: &str) -> Operand {
        match node.kind() {
            "assignment_operand" | "constant" => {
                let txt = NodeFuncs::as_string(node, source);
                if txt.to_uppercase().eq("NULL") {
                    Operand::Null
                } else {
                    Operand::Const(txt)
                }
            }
            "bind_marker" => Operand::Param(NodeFuncs::as_string(node, source)),
            "object_name" | "column" => Operand::Column(NodeFuncs::as_string(node, source)),
            "assignment_tuple" => {
                Operand::Tuple(CassandraParser::parse_assignment_tuple(node, source))
            }
            "assignment_map" => Operand::Map(CassandraParser::parse_assignment_map(node, source)),
            "assignment_list" => {
                Operand::List(CassandraParser::parse_assignment_list(node, source))
            }
            "assignment_set" => Operand::Set(CassandraParser::parse_assignment_set(node, source)),
            "function_args" => Operand::Tuple(CassandraParser::parse_function_args(node, source)),
            "function_call" => Operand::Func(NodeFuncs::as_string(node, source)),
            _ => {
                unreachable!(node.kind())
            }
        }
    }

    /// parses lists of option_hash_item or replication_list_item
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

    /// parse an assignment map.
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

    /// parse an assignment list
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

    /// parse an assignment set
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

    /// parse and assignment tuple
    fn parse_assignment_tuple(node: &Node, source: &str) -> Vec<Operand> {
        // ( expression, expression ... )
        let mut cursor = node.walk();
        cursor.goto_first_child();
        // consume '('
        cursor.goto_next_sibling();
        // now on 'expression-list'
        CassandraParser::parse_expression_list(&cursor.node(), source)
    }

    /// parse a `BEGIN BATCH` clause
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

    /// parse a select statement
    pub fn parse_select(node: &Node, source: &str) -> Select {
        let mut cursor = node.walk();
        cursor.goto_first_child();

        let mut statement_data = Select {
            distinct: false,
            json: false,
            columns: vec![],
            table_name: String::new(),
            where_clause: None,
            order: None,
            limit: None,
            filtering: false,
        };
        // we are on SELECT so we can just start
        while cursor.goto_next_sibling() {
            match cursor.node().kind() {
                "DISTINCT" => statement_data.distinct = true,
                "JSON" => statement_data.json = true,
                "select_elements" => {
                    let mut process = cursor.goto_first_child();
                    while process {
                        match cursor.node().kind() {
                            "select_element" => {
                                statement_data
                                    .columns
                                    .push(CassandraParser::parse_select_element(
                                        &cursor.node(),
                                        source,
                                    ))
                            }
                            "*" => statement_data.columns.push(SelectElement::Star),
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
                    statement_data.limit = Some(
                        NodeFuncs::as_string(&cursor.node(), source)
                            .parse::<i32>()
                            .unwrap(),
                    );
                    cursor.goto_parent();
                }
                "ALLOW" => {
                    // consume 'FILTERING'
                    cursor.goto_next_sibling();
                    statement_data.filtering = true
                }
                _ => {}
            }
        }
        statement_data
    }

    /// parse the where clause
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

    /// parse a relaiton element.
    fn parse_relation_element(node: &Node, source: &str) -> RelationElement {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        match cursor.node().kind() {
            "relation_contains_key" => {
                cursor.goto_first_child();
                RelationElement {
                    obj: Operand::Column(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::ContainsKey,
                    value: {
                        // consume column value
                        cursor.goto_next_sibling();
                        // consume 'CONTAINS'
                        cursor.goto_next_sibling();
                        // consume 'KEY'
                        cursor.goto_next_sibling();
                        Operand::Const(NodeFuncs::as_string(&cursor.node(), source))
                    },
                }
            }
            "relation_contains" => {
                cursor.goto_first_child();
                RelationElement {
                    obj: Operand::Column(NodeFuncs::as_string(&cursor.node(), source)),
                    oper: RelationOperator::Contains,
                    value: {
                        // consume column value
                        cursor.goto_next_sibling();
                        // consume 'CONTAINS'
                        cursor.goto_next_sibling();
                        Operand::Const(NodeFuncs::as_string(&cursor.node(), source))
                    },
                }
            }
            _ => {
                RelationElement {
                    obj: CassandraParser::parse_relation_value(&mut cursor, source),
                    oper: {
                        // consume the obj
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
                        if values.len() > 1 {
                            if inline_tuple {
                                Operand::Tuple(values)
                            } else {
                                Operand::Collection(values)
                            }
                        } else {
                            values[0].clone()
                        }
                    },
                }
            }
        }
    }

    // Parse an Operator
    fn parse_operator(cursor: &mut TreeCursor) -> RelationOperator {
        let node = cursor.node();
        let kind = node.kind();
        match kind {
            "<" => RelationOperator::LessThan,
            "<=" => RelationOperator::LessThanOrEqual,
            "<>" => RelationOperator::NotEqual,
            "=" => RelationOperator::Equal,
            ">=" => RelationOperator::GreaterThanOrEqual,
            ">" => RelationOperator::GreaterThan,
            "IN" => RelationOperator::In,

            _ => {
                unreachable!("Unknown operator: {}", kind);
            }
        }
    }

    /// parse a relation value
    fn parse_relation_value(cursor: &mut TreeCursor, source: &str) -> Operand {
        let node = cursor.node();
        let kind = node.kind();
        match kind {
            "column" => Operand::Column(NodeFuncs::as_string(&node, source)),
            "function_call" => Operand::Func(NodeFuncs::as_string(&node, source)),
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
                Operand::Tuple(values)
            }
            _ => Operand::Const(NodeFuncs::as_string(&node, source)),
        }
    }

    /// parse an order clause
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

    /// parse a select element
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
            _ => unreachable!(),
        }
    }

    /// parse the standard drop specification.
    pub fn parse_standard_drop(node: &Node, source: &str) -> CommonDrop {
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
        CommonDrop {
            name: CassandraParser::parse_dotted_name(&mut cursor, source),
            if_exists,
        }
    }

    /// parse a drop trigger statement.
    pub fn parse_drop_trigger(node: &Node, source: &str) -> DropTrigger {
        let mut cursor = node.walk();
        cursor.goto_first_child();
        DropTrigger {
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

pub struct CassandraAST {
    /// The query string
    text: String,
    /// the tree-sitter tree
    pub(crate) tree: Tree,
    /// the statement type of the query
    pub statements: Vec<CassandraStatement>,
}

impl CassandraAST {
    /// create an AST from the query string
    pub fn new(cassandra_statement: &str) -> CassandraAST {
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

        let tree = parser.parse(cassandra_statement, None).unwrap();
        CassandraAST {
            statements: CassandraStatement::from_tree(&tree, cassandra_statement),
            text: cassandra_statement.to_string(),
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
