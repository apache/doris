# ADMIN REPAIR
## description

    该语句用于尝试优先修复指定的表或分区

    语法：

        ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)]

    说明：

        1. 该语句仅表示让系统尝试以高优先级修复指定表或分区的分片副本，并不保证能够修复成功。用户可以通过 ADMIN SHOW REPLICA STATUS 命令查看修复情况。
        2. 默认的 timeout 是 14400 秒(4小时)。超时意味着系统将不再以高优先级修复指定表或分区的分片副本。需要重新使用该命令设置。

## example

    1. 尝试修复指定表

        ADMIN REPAIR TABLE tbl1;

    2. 尝试修复指定分区

        ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);
        
## keyword
    ADMIN,REPAIR

# ADMIN CANCEL REPAIR
## description

    该语句用于取消以高优先级修复指定表或分区

    语法：

        ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];

    说明：

        1. 该语句仅表示系统不再以高优先级修复指定表或分区的分片副本。系统仍会以默认调度方式修复副本。
        
## example

    1. 取消高优先级修复

        ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);

## keyword
    ADMIN,CANCEL,REPAIR

