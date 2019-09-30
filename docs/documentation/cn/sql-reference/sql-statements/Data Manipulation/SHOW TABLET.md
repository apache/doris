# SHOW TABLET
## description
    该语句用于显示 tablet 相关的信息（仅管理员使用）
    语法：
        SHOW TABLET
        [FROM [db_name.]table_name | tablet_id] [partiton(partition_name_1, partition_name_1)]
        [where [version=1] [and backendid=10000] [and state="NORMAL|ROLLUP|CLONE|DECOMMISSION"]]
        [order by order_column]
        [limit [offset,]size]

    现在show tablet命令支持按照按照以下字段进行过滤：partition, index name, version, backendid,
    state，同时支持按照任意字段进行排序，并且提供limit限制返回条数。

## example
    1. 显示指定 db 的下指定表所有 tablet 信息
        SHOW TABLET FROM example_db.table_name;

        // 获取partition p1和p2的tablet信息
        SHOW TABLET FROM example_db.table_name partition(p1, p2);

        // 获取10个结果
        SHOW TABLET FROM example_db.table_name limit 10;

        // 从偏移5开始获取10个结果
        SHOW TABLET FROM example_db.table_name limit 5,10;

        // 按照backendid/version/state字段进行过滤
        SHOW TABLET FROM example_db.table_name where backendid=10000 and version=1 and state="NORMAL";

        // 按照version字段进行排序
        SHOW TABLET FROM example_db.table_name where backendid=10000 order by version;

        // 获取index名字为t1_rollup的tablet相关信息
        SHOW TABLET FROM example_db.table_name where indexname="t1_rollup";
        
    2. 显示指定 tablet id 为 10000 的 tablet 的父层级 id 信息
        SHOW TABLET 10000;

## keyword
    SHOW,TABLET,LIMIT

