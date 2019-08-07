# Colocate Join
## description
    Colocate/Local Join 就是指多个节点Join时没有数据移动和网络传输，每个节点只在本地进行Join，
    能够本地进行Join的前提是相同Join Key的数据导入时按照相同规则导入到固定的节点。

    1 How To Use:

        只需要在建表时增加 colocate_with 这个属性即可，colocate_with的值 可以设置成同一组colocate 表中的任意一个，
        不过需要保证colocate_with属性中的表要先建立。

        假如需要对table t1 和t2 进行Colocate Join，可以按以下语句建表：

            CREATE TABLE `t1` (
            `id` int(11) COMMENT "",
            `value` varchar(8) COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "colocate_with" = "t1"
            );

            CREATE TABLE `t2` (
            `id` int(11) COMMENT "",
            `value` varchar(8) COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "colocate_with" = "t1"
            );

    2 Colocate Join 目前的限制:

        1. Colcoate Table 必须是OLAP类型的表
        2. 相同colocate_with 属性的表的 BUCKET 数必须一样
        3. 相同colocate_with 属性的表的 副本数必须一样
        4. 相同colocate_with 属性的表的 DISTRIBUTED Columns的数据类型必须一样

    3 Colocate Join的适用场景:
        
        Colocate Join 十分适合几张表按照相同字段分桶，并高频根据相同字段Join的场景。 

    4 FAQ:

        Q: 支持多张表进行Colocate Join 吗? 
   
        A: 支持

        Q: 支持Colocate 表和正常表 Join 吗？

        A: 支持

        Q: Colocate 表支持用非分桶的Key进行Join吗？

        A: 支持：不符合Colocate Join条件的Join会使用Shuffle Join或Broadcast Join

        Q: 如何确定Join 是按照Colocate Join 执行的？

        A: explain的结果中Hash Join的孩子节点如果直接是OlapScanNode， 没有Exchange Node，就说明是Colocate Join

        Q: 如何修改colocate_with 属性？

        A: ALTER TABLE example_db.my_table set ("colocate_with"="target_table");

        Q: 如何禁用colcoate join?

        A: set disable_colocate_join = true; 就可以禁用Colocate Join，查询时就会使用Shuffle Join 和Broadcast Join

## keyword

    COLOCATE, JOIN, CREATE TABLE
