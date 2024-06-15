# Extracting && Implementing the Sqlite Storage Engine

## Credits:
Used Codecrafters starting point to build an understanding of metadata for the file format, so thanks to them :)
Then I took that project and created this monstrosity from the initial project

## Out of scope
SQL frontend, and joins. I just want a simple Storage Engine based on persistent BTrees

## Motivation
I wanted to write a persistent Btree Storage engine, by implementing the SQLITE database file format I can use their page based BTree Schema along with all the well known exploration tools on it, and then write my API layer on top of my btree implementation.
This is a learning project, and I want to explore other ways of building a storage engine.

## Supported APIs
### Read Path
Get(Table, Fields[], Filters[]))

### Write Path
Create(DatabaseName)
Delete(DatabaseName)
Create(TableName, Schema)
Delete(TableName)
Set(Table, Fields, Values)
Transaction(Commands[])

### Read Path
- Ability to read tables. DONE
- Abiltity to leverage Indices for filters.

### Write Path -> NO CLUE HOW TO DO THIS STILL.. Need to dive deeper here
- Transaction support, for multiple entries at once
- WAL support
- ETC...

