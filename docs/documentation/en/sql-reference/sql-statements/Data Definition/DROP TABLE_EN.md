# DROP TABLE
## Description
This statement is used to delete the table.
Grammar:
DROP TABLE [IF EXISTS] [db_name.]table_name;

Explain:
After executing DROP TABLE for a period of time, the deleted table can be restored through the RECOVER statement. See RECOVER statement for details

## example
1. Delete a table
DROP TABLE my_table;

2. If it exists, delete the table that specifies the database
DROP TABLE IF EXISTS example_db.my_table;

## keyword
DROP,TABLE

