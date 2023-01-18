suite("test_limit_subquery") {

    sql "drop table if exists customer_repayment;"

    sql """
        CREATE TABLE `customer_repayment` (
        `repayment_id` bigint(20) NOT NULL COMMENT '',
        `register_date` datetime NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`repayment_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`repayment_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql "insert into customer_repayment values (1, '2023-01-18 00:00:00'), (2, '2023-01-18 00:00:01'), (3, '2023-01-18 00:00:02');"

    qt_sql1 """
        SELECT
            repayment_id 
        FROM
            customer_repayment 
        WHERE
            repayment_id IN ( SELECT repayment_id FROM customer_repayment ORDER BY register_date DESC LIMIT 0, 1 ) 
        ORDER BY
            register_date DESC;
        """

    qt_sql2 """
        SELECT
            repayment_id 
        FROM
            customer_repayment 
        WHERE
            repayment_id IN ( SELECT repayment_id FROM customer_repayment ORDER BY register_date DESC LIMIT 1, 1 ) 
        ORDER BY
            register_date DESC;
        """

}