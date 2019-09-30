# TRUNCATE TABLES
## Description
This statement is used to empty the data of the specified table and partition
Grammar:

TRUNCATE TABLE [db.]tbl[ PARTITION(p1, p2, ...)];

Explain:
1. The statement empties the data, but retains the table or partition.
2. Unlike DELETE, this statement can only empty the specified tables or partitions as a whole, without adding filtering conditions.
3. Unlike DELETE, using this method to clear data will not affect query performance.
4. The data deleted by this operation is not recoverable.
5. When using this command, the table state should be NORMAL, i.e. SCHEMA CHANGE operations are not allowed.

## example

1. Clear the table TBL under example_db

TRUNCATE TABLE example_db.tbl;

2. P1 and P2 partitions of clearing TABLE tbl

TRUNCATE TABLE tbl PARTITION(p1, p2);

## keyword
TRUNCATE,TABLE
