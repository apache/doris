# SHOW PARTITIONS
## Description
This statement is used to display partition information
Grammar:
SHOW PARTITIONS FROM [db_name.]table_name [PARTITION partition_name];

## example
1. Display partition information for the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name;

1. Display information about the specified partition of the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name PARTITION p1;

## keyword
SHOW,PARTITIONS

