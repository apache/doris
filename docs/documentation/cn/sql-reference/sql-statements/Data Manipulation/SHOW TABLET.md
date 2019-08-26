# SHOW TABLET
## description
    该语句用于显示 tablet 相关的信息（仅管理员使用）
    语法：
        SHOW TABLET
        [FROM [db_name.]table_name | tablet_id] [partiton(partition_name_1, partition_name_1)]
        [where [version=1] [and backendid=10000] [and state="NORMAL|ROLLUP|CLONE|DECOMMISSION"]]
        [order by order_column]
        [limit [offset,]size]

    Now show tablet command supports to get tablet from partition, and filter results by 
    version/backendid/state fields and order by any columns.

## example
    1. 显示指定 db 的下指定表所有 tablet 信息
        SHOW TABLET FROM example_db.table_name;

        // get tablet from p1 and p2 partition
        SHOW TABLET FROM example_db.table_name partition(p1, p2);

        // return 10 results
        SHOW TABLET FROM example_db.table_name limit 10;

        // offset from 5 and return 10 results
        SHOW TABLET FROM example_db.table_name limit 5,10;

        // filter by backend id and version and state
        SHOW TABLET FROM example_db.table_name where backendid=10000 and version=1 and state="NORMAL";

        // order by version
        SHOW TABLET FROM example_db.table_name where backendid=10000 order by version;
        
    2. 显示指定 tablet id 为 10000 的 tablet 的父层级 id 信息
        SHOW TABLET 10000;

## keyword
    SHOW,TABLET,LIMIT

