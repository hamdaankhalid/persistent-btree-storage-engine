use crate::sql_data_types::SerialType;
use anyhow::Result;

// Given a create table stmt, return the schema name and the serial types in vector
pub fn find_schema_from_create_stmt(stmt: &str) -> Result<Vec<(String, SerialType)>> {
    todo!()
}
