# ADMIN SHOW REPLICA STATUS
## description

    该语句用于展示一个表或分区的副本状态信息

    语法：

        ADMIN SHOW REPLICA STATUS FROM [db_name.]tbl_name [PARTITION (p1, ...)]
        [where_clause];

        where_clause:
            WHERE STATUS [!]= "replica_status"

        replica_status:
            OK:             replica 处于健康状态
            DEAD:           replica 所在 Backend 不可用
            VERSION_ERROR:  replica 数据版本有缺失
            SCHEMA_ERROR:   replica 的 schema hash 不正确
            MISSING:        replica 不存在

## example

    1. 查看表全部的副本状态

        ADMIN SHOW REPLICA STATUS FROM db1.tbl1;

    2. 查看表某个分区状态为 VERSION_ERROR 的副本

        ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)
        WHERE STATUS = "VERSION_ERROR";
        
    3. 查看表所有状态不健康的副本

        ADMIN SHOW REPLICA STATUS FROM tbl1
        WHERE STATUS != "OK";
        
## keyword
    ADMIN,SHOW,REPLICA,STATUS

# ADMIN SHOW REPLICA DISTRIBUTION
## description

    该语句用于展示一个表或分区副本分布状态

    语法：

        ADMIN SHOW REPLICA DISTRIBUTION FROM [db_name.]tbl_name [PARTITION (p1, ...)];

    说明：

        结果中的 Graph 列以图形的形式展示副本分布比例
        
## example

    1. 查看表的副本分布

        ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;

    2. 查看表的分区的副本分布

        ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);

## keyword
    ADMIN,SHOW,REPLICA,DISTRIBUTION

# ADMIN SHOW CONFIG
## description

    该语句用于展示当前集群的配置（当前仅支持展示 FE 的配置项）

    语法：

        ADMIN SHOW FRONTEND CONFIG;

    说明：

        结果中的各列含义如下：
        1. Key：        配置项名称
        2. Value：      配置项值
        3. Type：       配置项类型
        4. IsMutable：  是否可以通过 ADMIN SET CONFIG 命令设置
        5. MasterOnly： 是否仅适用于 Master FE
        6. Comment：    配置项说明
        
## example

    1. 查看当前FE节点的配置

        ADMIN SHOW FRONTEND CONFIG;

## keyword
    ADMIN,SHOW,CONFIG
