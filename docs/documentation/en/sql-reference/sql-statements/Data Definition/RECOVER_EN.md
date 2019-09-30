# RECOVER
## Description
This statement is used to restore previously deleted databases, tables, or partitions
Grammar:
1)24674;"22797database;
RECOVER DATABASE db_name;
2) 恢复 table
RECOVER TABLE [db_name.]table_name;
3)24674;"22797partition
RECOVER PARTITION partition name FROM [dbu name.]table name;

Explain:
1. This operation can only recover the meta-information deleted in the previous period of time. The default is 3600 seconds.
2. If new meta-information of the same name and type is created after deleting meta-information, the previously deleted meta-information cannot be restored.

## example
1. Restore the database named example_db
RECOVER DATABASE example_db;

2. Restore table named example_tbl
RECOVER TABLE example_db.example_tbl;

3. Restore partition named P1 in example_tbl
RECOVER PARTITION p1 FROM example_tbl;

## keyword
RECOVER

