# SHOW PARTITIONS
## description
    该语句用于展示分区信息
    语法：
        SHOW PARTITIONS FROM [db_name.]table_name [PARTITION partition_name];

## example
    1. 展示指定 db 的下指定表的分区信息
        SHOW PARTITIONS FROM example_db.table_name;
        
    1. 展示指定 db 的下指定表的指定分区的信息
        SHOW PARTITIONS FROM example_db.table_name PARTITION p1;

## keyword
    SHOW,PARTITIONS
    
