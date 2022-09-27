#![allow(clippy::derive_partial_eq_without_eq)]

pub mod aggregate;
pub mod alter_column;
pub mod alter_materialized_view;
pub mod alter_table;
pub mod alter_type;
pub mod begin_batch;
pub mod cassandra_ast;
pub mod cassandra_statement;
pub mod common;
pub mod common_drop;
pub mod create_function;
pub mod create_index;
pub mod create_keyspace;
pub mod create_materialized_view;
pub mod create_table;
pub mod create_trigger;
pub mod create_type;
pub mod create_user;
pub mod delete;
pub mod drop_trigger;
pub mod insert;
pub mod list_role;
pub mod role_common;
pub mod select;
pub mod update;
