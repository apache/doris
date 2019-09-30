# CANCEL ALTER
## description
    该语句用于撤销一个 ALTER 操作。
    1. 撤销 ALTER TABLE COLUMN 操作
    语法：
        CANCEL ALTER TABLE COLUMN
        FROM db_name.table_name
    
    2. 撤销 ALTER TABLE ROLLUP 操作
    语法：
        CANCEL ALTER TABLE ROLLUP
        FROM db_name.table_name
        
    2. 撤销 ALTER CLUSTER 操作
    语法：
        （待实现...）

        
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
    
