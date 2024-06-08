mod btree;
mod cell;
mod database;
mod page;
mod record;
mod sql_data_types;

use anyhow::{bail, Result};
use database::Database;
use env_logger::Env;

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
    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", database.metadata.page_size);

            let tables = database.get_master_table()?;
            let num_tables = tables.len();

            println!("number of tables: {num_tables}");
        }
        ".tables" => {
            let tables = database.get_master_table()?;

            // join tables with space in between
            let table_names: String = tables
                .iter()
                .map(|x| x.name.clone())
                .collect::<Vec<_>>()
                .join(", ");

            println!("{table_names}");
        }
        ".table" => {
            let table_name = &args[3];
            // currently only supports select * from <table_name>
            // run the processed query on the VM
            let table = database.get_table(table_name)?;

            println!("Table {:?}", table);

            let rows = table.get_rows(false)?;
            let num_rows = rows.len();

            println!("{num_rows} Rows for table {table_name}:");

            for row in rows {
                let row_data = row.clone().read_record()?;
                println!("{:?}", row_data);
            }
        }
        ".index" => {
            let index_name = &args[3];
            let idx_table = database.get_index(index_name)?;

            println!("Index {:?}", idx_table);
        }
        _ => bail!("Unknown command: {command}"),
    }

    Ok(())
}
