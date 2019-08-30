# SHOW DATA
## Description
This statement is used to show the amount of data
Grammar:
SHOW DATA [FROM db_name[.table_name]];

Explain:
1. If you do not specify the FROM clause, use the amount of data that shows the current DB subdivided into tables
2. If the FROM clause is specified, the amount of data subdivided into indices under the table is shown.
3. If you want to see the size of individual Partitions, see help show partitions

## example
1. Display the data volume and aggregate data volume of each table of default DB
SHOW DATA;

2. Display the subdivision data volume of the specified table below the specified DB
SHOW DATA FROM example_db.table_name;

## keyword
SHOW,DATA
