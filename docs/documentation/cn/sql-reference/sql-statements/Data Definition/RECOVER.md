# RECOVER
## description
    该语句用于恢复之前删除的 database、table 或者 partition
    语法：
        1) 恢复 database
            RECOVER DATABASE db_name;
        2) 恢复 table
            RECOVER TABLE [db_name.]table_name;
        3) 恢复 partition
            RECOVER PARTITION partition_name FROM [db_name.]table_name;
    
    说明：
        1. 该操作仅能恢复之前一段时间内删除的元信息。默认为 3600 秒。
        2. 如果删除元信息后新建立了同名同类型的元信息，则之前删除的元信息不能被恢复

## example
    1. 恢复名为 example_db 的 database
        RECOVER DATABASE example_db;
        
    2. 恢复名为 example_tbl 的 table
        RECOVER TABLE example_db.example_tbl;
        
    3. 恢复表 example_tbl 中名为 p1 的 partition
        RECOVER PARTITION p1 FROM example_tbl;
        
## keyword
    RECOVER
    
