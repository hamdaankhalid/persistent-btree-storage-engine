# Rough notes I am collecting as I code shit

Index just holds the data and the row ID, the row ID
can then be used to perform an O(log(N)) lookup on table btree

we basically want to support 2 interfaces on any BTREE
- find One 
- find many
- find supports filtering on indices and properties
- if you choose to involve an index you can only support strict equality for now.
- for all other properties we can use lambda functions to do filtering.