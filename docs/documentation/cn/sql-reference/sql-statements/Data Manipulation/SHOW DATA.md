# SHOW DATA
## description
    该语句用于展示数据量
    语法：
        SHOW DATA [FROM db_name[.table_name]];
        
    说明：
        1. 如果不指定 FROM 子句，使用展示当前 db 下细分到各个 table 的数据量
        2. 如果指定 FROM 子句，则展示 table 下细分到各个 index 的数据量
        3. 如果想查看各个 Partition 的大小，请参阅 help show partitions

## example
    1. 展示默认 db 的各个 table 的数据量及汇总数据量
        SHOW DATA;
        
    2. 展示指定 db 的下指定表的细分数据量
        SHOW DATA FROM example_db.table_name;

## keyword
    SHOW,DATA

