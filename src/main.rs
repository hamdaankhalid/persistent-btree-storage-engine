mod btree;
mod cell;
mod database;
mod page;
mod record;
mod sql_data_types;
mod sql_parser;

use anyhow::{bail, Result};
use database::Database;
use env_logger::Env;

enum SupportedOperators {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
}

struct ParsedFilterArgs {
    column_name: String,
    operator: SupportedOperators,
    value: String,
}

impl ParsedFilterArgs {
    fn from_string(raw_filter: &str) -> Result<ParsedFilterArgs> {
        if raw_filter.contains("=") {
            let parts = raw_filter.split("=").collect::<Vec<_>>();
            Ok(ParsedFilterArgs {
                column_name: parts[0].to_string(),
                operator: SupportedOperators::Equal,
                value: parts[1].to_string(),
            })
        } else if raw_filter.contains("!=") {
            let parts = raw_filter.split("!=").collect::<Vec<_>>();
            Ok(ParsedFilterArgs {
                column_name: parts[0].to_string(),
                operator: SupportedOperators::NotEqual,
                value: parts[1].to_string(),
            })
        } else if raw_filter.contains("<") {
            let parts = raw_filter.split("<").collect::<Vec<_>>();
            Ok(ParsedFilterArgs {
                column_name: parts[0].to_string(),
                operator: SupportedOperators::LessThan,
                value: parts[1].to_string(),
            })
        } else if raw_filter.contains(">") {
            let parts = raw_filter.split(">").collect::<Vec<_>>();
            Ok(ParsedFilterArgs {
                column_name: parts[0].to_string(),
                operator: SupportedOperators::GreaterThan,
                value: parts[1].to_string(),
            })
        } else {
            bail!("Unsupported Operator in filter: {raw_filter}")
        }
    }
}

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

            let columns_raw = args[4].as_str(); // "," delimitted column names, and * for all

            let filters_raw = if args.len() >= 7 {
                Some(&args[5]) // ","" delimitted "" based key value pairs, key and values are separated by =, <,>,!=
            } else {
                None
            };

            let index_filter = if args.len() >= 6 {
                // "," delimitted column and "=" P.S this is optional
                Some(&args[6])
            } else {
                None
            };

            let table = database.get_table(table_name)?;

            // see if the where clauses can use any indices
            let specific_columns = match columns_raw {
                "*" => None,
                _ => Some(columns_raw.split(",").collect::<Vec<_>>()),
            };

            let filters = filters_raw.map(|f| {
                f.split(",")
                    .map(|x| ParsedFilterArgs::from_string(x))
                    .collect::<Vec<_>>()
            });

            let index_filter = index_filter.map(|f| {
                // check what equality operator is being used in this  filter
                let parts = f.split("=").collect::<Vec<_>>();
                // Column, Value Tuple, Operator is implicitly equality
                (parts[0].to_string(), parts[1].to_string())
            });

            // lets make sure the table has the said columns being used by specific_columns and indices and what not

            todo!()
        }
        ".set" => {
            todo!()
        }
        ".create" => {
            todo!()
        }
        ".delete" => {
            todo!()
        }
        _ => bail!("Unknown command: {command}"),
    }

    Ok(())
}
