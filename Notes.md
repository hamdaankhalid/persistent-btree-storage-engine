# Rough notes I am collecting as I code shit

Index just holds the data and the row ID, the row ID
can then be used to perform an O(log(N)) lookup on table btree

we basically want to support 2 interfaces on any BTREE
- find One 
- find many
- find supports filtering on indices and properties
- if you choose to involve an index you can only support strict equality for now.
- for all other properties we can use lambda functions to do filtering.


## How sqlite btrees and databases work

BST over a Binary  tree
                        A(1)
                B(-1)         C(5)
            D(-5)   E(0)     F(3) G(100))  -> O(LOG(N)) if you use the constraint of left is less than parent and right is more than parent

                A(1)
            B(-1)
        C(-5)
    D(-6)

Self Balancing trees
Red-Black, B-trees, B+trees

B-Tree
O(Log N)

Hashmap
O(1) Get/SEt

Sqlite Databases
Table -> a btree

Pages -> logical chunking of segments of the big database file, 512-65536 -> 4096 bytes

Database: 100 bytes database metadata

Schema table -> holkds root nodes for each table's btree

Index btree -> btree for indirection that is used to isolate oine or more porperties of the table btree to lookup stuff faster....

## CFG
Terminals, non-terminals, productions

- Terminals are basic symbols from which strings are formed... building blocks, atomic unit of your language -> Terminal would be the primitive tokens
- Non-terminals are syntactic variables that denote set of strings -> abstract concepts created by combining terminals into a hierarchial structure
- PRODUCTION  would be the rules that show how a non terminal is made up combnining terminals and/or non-terminals

