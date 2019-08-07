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

