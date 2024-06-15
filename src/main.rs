mod btree;
mod cell;
mod database;
mod page;
mod record;
mod sql_data_types;

use anyhow::{bail, Result};
use database::Database;
use env_logger::Env;

// Temporary Driver program so I can test my top level api's for the database without making a separate project using the LIB
fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let database: Database = Database::from_file(&args[1])?;

    let command = &args[2];
    match command.as_str() {
        ".tables" => {
            let tables = database.get_master_table()?;

            // string join table names with space in between
            let table_names: String = tables
                .iter()
                .map(|x| x.name.clone())
                .collect::<Vec<_>>()
                .join(", ");


            println!("{table_names}");
        }
        ".table" => {
            // Purely exists for Table exploration, not provided by lib api
            let table_name = &args[3];
            // currently only supports select * from <table_name>
            // run the processed query on the VM
            let table = database.get_table(table_name)?;

            let rows = table.get_rows(false)?;
            let num_rows = rows.len();

            println!("{num_rows} Rows for table {table_name}:");

            for row in rows {
                let row_data = row.clone().read_record()?;
                println!("{:?}", row_data);
            }
        }
        ".index" => {
            // Purely exists for Index exploration not provided by lib api
            let index_name = &args[3];
            let idx_table = database.get_index(index_name)?;

            let rows = idx_table.get_rows(false)?;
            let num_rows = rows.len();

            println!("{num_rows} Rows for Index {index_name}:");

            for row in rows {
                let row_data = row.clone().read_record()?;
                println!("{:?}", row_data);
            }
        }
        ".get" => {
            // Get(Table, Fields[], Filters[]))
            // support retrieval of columns from a table with a where clauses
            let table_name = &args[3];
            let columns = &args[4]; // , delimitted column names, and * for all
            let filters = &args[5]; // , delimitted ":" based key value pairs

            let table = database.get_table(table_name)?;

            // see if the where clauses can use any indices
            todo!()
        },
        ".set" => {
            todo!()
        },
        ".create" => {
            todo!()
        },
        ".delete" => {
            todo!()
        },
        _ => bail!("Unknown command: {command}"),
    }

    Ok(())
}
