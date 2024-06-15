# SQLITE database but native built library format for Rust
Motivation, DB engine that I can use in rust projects without any SQL obfuscation. I'm too stupid to write my own Storage engine from scratch so I will use the SQLITE Spec for storage engine layer, then see what fun stuff I can do around the datbase engine/query layer.

## Roadmap

### Read Path
- Metadata and info command support. DONE
- Ability to read tables. DONE
- Abiltity to leverage Indices. WIP
- SQL-Like Rust interface to perform queries on tables.

### Write Path
- Transaction support
- WAL support
- ETC...

