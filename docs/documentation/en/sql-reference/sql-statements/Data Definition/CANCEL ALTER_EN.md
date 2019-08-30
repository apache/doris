# CANCEL ALTER
## Description
This statement is used to undo an ALTER operation.
1. 撤销 ALTER TABLE COLUMN 操作
Grammar:
CANCEL ALTER TABLE COLUMN
FROM db_name.table_name

2. 撤销 ALTER TABLE ROLLUP 操作
Grammar:
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name

2. OTHER CLUSTER
Grammar:
(To be realized...


## example
[CANCEL ALTER TABLE COLUMN]
1. 撤销针对 my_table 的 ALTER COLUMN 操作。
CANCEL ALTER TABLE COLUMN
FROM example_db.my_table;

[CANCEL ALTER TABLE ROLLUP]
1. 撤销 my_table 下的 ADD ROLLUP 操作。
CANCEL ALTER TABLE ROLLUP
FROM example_db.my_table;

## keyword
CANCEL,ALTER,TABLE,COLUMN,ROLLUP

