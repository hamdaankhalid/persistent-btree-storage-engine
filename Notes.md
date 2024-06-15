# LLD

Main driver interacts with Database object, and issues commnds to it.

Database object keeps metadata and schema table btree after loading it on instantiation in memory.

Database on instantiation needs to use the file handle to seek and read.

Each btree is composed of root page, and has metadata about stuff like page size.

Btree should be able to load pages into memory as it is traversed so it needs its own File handle.

There is essentially 2 types of btrees, Table, and Index they both have the same interface of being able to traverse shit by reading disk.


#######
4 3 t 1 k

4 is the entire payload size

If the payload is indeex record format then,
3 t 1 k
