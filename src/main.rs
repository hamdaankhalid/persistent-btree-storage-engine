mod page;
mod btree;
mod cell;
mod database;
mod record;
mod sql_data_types;

use database::Database;
use anyhow::{bail, Result};
use env_logger::Env;
use log::debug;

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();
    // Parse arguments

    let num = 263u16;
    // convert each byte to hex a decimal representation
    let bytes = num.to_be_bytes();
    let hex_bytes: Vec<String> = bytes.iter().map(|b| format!("{:02x}", b)).collect();

    let num2 = 300u16;
    let bytes2 = num2.to_be_bytes();
    let hex_bytes_2: Vec<String> = bytes2.iter().map(|b| format!("{:02x}", b)).collect();

    let num3 = 2u16;
    let bytes3 = num3.to_be_bytes();
    let hex_bytes_3: Vec<String> = bytes3.iter().map(|b| format!("{:02x}", b)).collect();

    debug!("Byte Repr 263 {:?} \n Byte Repr of 300 {:?} \n Byte Repr of 2 {:?}",
        hex_bytes, hex_bytes_2, hex_bytes_3);

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
                .join(" ");

            println!("{table_names}");
        }
        table_name => {
            // currently only supports select * from <table_name>
            // run the processed query on the VM
            let table = database.get_table(table_name)?;

            let rows = table.get_rows(false)?;
            let num_rows = rows.len();

            println!("{num_rows} Rows for table {table_name}:");
            /*
            for row in rows {
                let row_data = row.clone().read_record()?;
                println!("{:?}", row_data);
            };
            */
        }
    }

    Ok(())
}
