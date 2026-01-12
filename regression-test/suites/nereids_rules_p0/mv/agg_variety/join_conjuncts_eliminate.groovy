package mv.agg_variety
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("join_conjuncts_eliminate") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"

    sql """
    drop table if exists customer
    """

    sql """
    CREATE TABLE customer (
        `cust_id` BIGINT NOT NULL COMMENT '',
        `cust_name` VARCHAR(100) NULL COMMENT '',
        `create_time` DATETIME NULL COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`cust_id`)
    DISTRIBUTED BY HASH(`cust_id`) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1"
    );
    """


    sql """
    drop table if exists data_event
    """

    sql"""
    CREATE TABLE data_event (
    `event_id` BIGINT NOT NULL COMMENT '',
    `cust_id` BIGINT NULL COMMENT '',
    `event_type_id` INT NULL COMMENT '',
    `bind_attr` VARCHAR(100) NULL COMMENT '',
    `sum_amount` DECIMAL(16,2) NULL COMMENT '',
    `created_date` DATETIME NULL COMMENT '',
    `create_time` DATETIME NULL COMMENT ''
     ) ENGINE=OLAP
     DUPLICATE KEY(`event_id`)
     DISTRIBUTED BY HASH(`event_id`) BUCKETS 10
     PROPERTIES (
         "replication_num" = "1"
     );
    """

    sql """
    INSERT INTO customer (cust_id, cust_name, create_time) VALUES
    (410723002257, '测试客户1', '2025-01-01 00:00:00'),
    (410723002258, '测试客户2', '2025-01-01 00:00:00');
    
    INSERT INTO data_event (event_id, cust_id, event_type_id, bind_attr, sum_amount, created_date, create_time) VALUES
    (1, 410723002257, 1, 'attr_A', 100.00, '2025-09-01 10:00:00', '2025-09-01 10:00:00'),
    (2, 410723002257, 1, 'attr_A', 200.00, '2025-09-02 11:00:00', '2025-09-02 11:00:00'),
    (3, 410723002257, 2, 'attr_B', 150.00, '2025-09-03 12:00:00', '2025-09-03 12:00:00'),
    (4, 410723002257, 2, 'attr_B', 250.00, '2025-09-04 13:00:00', '2025-09-04 13:00:00'),
    (5, 410723002257, 3, 'attr_C', 300.00, '2025-09-05 14:00:00', '2025-09-05 14:00:00'),
    (6, 410723002258, 1, 'attr_A', 50.00, '2025-09-01 15:00:00', '2025-09-01 15:00:00'),
    (7, 410723002258, 2, 'attr_B', 75.00, '2025-09-02 16:00:00', '2025-09-02 16:00:00'),
    (8, 410723002257, 1, 'attr_A', 400.00, '2025-08-01 10:00:00', '2025-08-01 10:00:00'),
    (9, 410723002257, 2, 'attr_B', 500.00, '2025-10-01 10:00:00', '2025-10-01 10:00:00');
    """

    sql "ANALYZE TABLE customer WITH SYNC"
    sql "ANALYZE TABLE data_event WITH SYNC"

    def query_sql = """
    select
        a.cust_id,
        a.event_type_id,
        a.bind_attr,
        sum(sum_amount) sum_amount,
        date_format(created_date, '%Y%m') month
    from data_event a
    inner join customer b on (a.cust_id = b.cust_id)
    where a.cust_id = 410723002257
        and date_format(a.created_date, '%Y%m') = '202509'
    group by a.cust_id, a.event_type_id, a.bind_attr, month;
    """

    create_async_mv(db, "mv1", """select 
    a.cust_id,
    a.event_type_id, 
    a.bind_attr,
    sum(sum_amount) sum_amount,
    date_format(a.created_date, '%Y%m') month,
    a.created_date
    from data_event a
    join customer b on(a.cust_id=b.cust_id)
    group by a.cust_id, a.event_type_id, a.bind_attr, month, a.created_date;
    """)

    create_async_mv(db, "mv2", """
    select 
        a.cust_id,
        a.event_type_id, 
        a.bind_attr,
        sum(sum_amount) sum_amount,
        date_format(a.created_date, '%Y%m') month
    from data_event a
    join customer b on(a.cust_id=b.cust_id)
    group by a.cust_id, a.event_type_id, a.bind_attr, month;
    """)


    order_qt_query1_1_before "${query_sql}"
    mv_rewrite_all_success_without_check_chosen(query_sql, ["mv1", "mv2"])
    order_qt_query1_1_after "${query_sql}"

    sql "DROP MATERIALIZED VIEW IF EXISTS mv1"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv2"
}
