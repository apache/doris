# DROP TABLE
## description
    该语句用于删除 table 。
    语法：
        DROP TABLE [IF EXISTS] [db_name.]table_name;
        
    说明：
        执行 DROP TABLE 一段时间内，可以通过 RECOVER 语句恢复被删除的 table。详见 RECOVER 语句

## example
    1. 删除一个 table
        DROP TABLE my_table;
        
    2. 如果存在，删除指定 database 的 table
        DROP TABLE IF EXISTS example_db.my_table;

## keyword
    DROP,TABLE
    
