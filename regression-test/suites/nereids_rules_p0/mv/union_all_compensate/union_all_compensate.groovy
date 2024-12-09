package mv.union_all_compensate
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

suite("union_all_compensate") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists test_table1
    """
    sql """
    CREATE TABLE `test_table1` (
      `data_date` datetime NOT NULL COMMENT '', 
      `slot_id` varchar(255) NULL,
      `num` int NULL
    ) ENGINE = OLAP DUPLICATE KEY(
      `data_date`,
      `slot_id`
    ) PARTITION BY RANGE(`data_date`) (
    FROM ("2024-09-01") TO ("2024-09-30") INTERVAL 1 DAY
    ) 
     DISTRIBUTED BY HASH (`data_date`, `slot_id`) BUCKETS 10 
     PROPERTIES (
      "is_being_synced" = "false", 
      "storage_medium" = "hdd", "storage_format" = "V2", 
      "inverted_index_storage_format" = "V2", 
      "light_schema_change" = "true", "disable_auto_compaction" = "false", 
      "enable_single_replica_compaction" = "false", 
      "group_commit_interval_ms" = "10000", 
      "group_commit_data_bytes" = "134217728", 
      'replication_num' = '1'
    );
    """

    sql """
    drop table if exists test_table2
    """
    sql """
    CREATE TABLE `test_table2` (
      `data_date` datetime NOT NULL COMMENT '', 
      `slot_id` varchar(255) NULL,
      `num` int NULL
    ) ENGINE = OLAP DUPLICATE KEY(
      `data_date`,
      `slot_id`
    ) PARTITION BY RANGE(`data_date`) (
        FROM ("2024-09-01") TO ("2024-09-30") INTERVAL 1 DAY
    ) 
     DISTRIBUTED BY HASH (`data_date`, `slot_id`) BUCKETS 10 
     PROPERTIES (
      "is_being_synced" = "false", 
      "storage_medium" = "hdd", "storage_format" = "V2", 
      "inverted_index_storage_format" = "V2", 
      "light_schema_change" = "true", "disable_auto_compaction" = "false", 
      "enable_single_replica_compaction" = "false", 
      "group_commit_interval_ms" = "10000", 
      "group_commit_data_bytes" = "134217728", 
      'replication_num' = '1'
    );
    """

    sql """
    insert into test_table1 values 
    ('2024-09-11 00:10:00', 'a', 1), 
    ('2024-09-11 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'b', 1),
    ('2024-09-13 00:20:00', 'b', 1),
    ('2024-09-13 00:30:00', 'b', 1),
    ('2024-09-13 00:20:00', 'b', 1),
    ('2024-09-13 00:30:00', 'b', 1),
    ('2024-09-14 00:20:00', 'b', 1),
    ('2024-09-14 00:30:00', 'b', 1),
    ('2024-09-14 00:20:00', 'b', 1),
    ('2024-09-14 00:30:00', 'b', 1),
    ('2024-09-15 00:20:00', 'b', 1),
    ('2024-09-15 00:30:00', 'b', 1),
    ('2024-09-15 00:20:00', 'b', 1),
    ('2024-09-15 00:30:00', 'b', 1);
    """

    sql """
    insert into test_table2 values 
    ('2024-09-11 00:10:00', 'a', 1), 
    ('2024-09-11 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'b', 1),
    ('2024-09-13 00:20:00', 'b', 1),
    ('2024-09-13 00:30:00', 'b', 1),
    ('2024-09-13 00:20:00', 'b', 1),
    ('2024-09-13 00:30:00', 'b', 1),
    ('2024-09-14 00:20:00', 'b', 1),
    ('2024-09-14 00:30:00', 'b', 1),
    ('2024-09-14 00:20:00', 'b', 1),
    ('2024-09-14 00:30:00', 'b', 1),
    ('2024-09-15 00:20:00', 'b', 1),
    ('2024-09-15 00:30:00', 'b', 1),
    ('2024-09-15 00:20:00', 'b', 1),
    ('2024-09-15 00:30:00', 'b', 1);
    """

    sql """analyze table test_table1 with sync"""
    sql """analyze table test_table2 with sync"""

    // Aggregate, scalar aggregate, should not compensate union all
    sql """ DROP MATERIALIZED VIEW IF EXISTS test_agg_mv"""
    sql"""
            CREATE MATERIALIZED VIEW test_agg_mv
             BUILD IMMEDIATE REFRESH ON MANUAL
             partition by(data_date) 
             DISTRIBUTED BY HASH(data_date) BUCKETS 3 
             PROPERTIES(
               "refresh_partition_num" = "1", 'replication_num' = '1'
             ) 
            AS
            SELECT 
              date_trunc(t1.data_date, 'day') as data_date, 
              to_date(t1.data_date) as dt, 
              t2.slot_id,  
              sum(t1.num) num_sum 
            FROM 
              test_table1 t1 
              inner join
              test_table2 t2 on t1.data_date = t2.data_date
            GROUP BY 
              date_trunc(t1.data_date, 'day'), 
              to_date(t1.data_date), 
              t2.slot_id;
    """
    waitingMTMVTaskFinishedByMvName("test_agg_mv")
    sql """analyze table test_agg_mv with sync"""

    def query1_0 =
            """
            select sum(t1.num) 
            FROM 
              test_table1 t1 
              inner join
              test_table2 t2 on t1.data_date = t2.data_date
            where to_date(t1.data_date) >= '2024-09-12';
            """
    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query1_0_before "${query1_0}"
    sql """set enable_materialized_view_rewrite = true;"""
    // equalsIgnoreNullable code is not ready in NullableAggregateFunction.java on 2.1, so should fail
    mv_rewrite_fail(query1_0, "test_agg_mv")
    order_qt_query1_0_after "${query1_0}"

    // Data modify
    sql """
    insert into test_table1 values
    ('2024-09-11 00:10:00', 'a', 1),
    ('2024-09-11 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'b', 1);
    """
    sql """analyze table test_table1 with sync"""

    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query1_1_before "${query1_0}"
    sql """set enable_materialized_view_rewrite = true;"""
    mv_rewrite_fail(query1_0, "test_agg_mv")
    order_qt_query1_1_after "${query1_0}"

    sql """alter table test_table1 modify column num set stats ('row_count'='20');"""
    sql """alter table test_table2 modify column num set stats ('row_count'='16');"""


    // Aggregate, if query group by expression doesn't use the partition column, but the invalid partition is in the
    // grace_period, should not compensate union all, but should rewritten successfully
    def query2_0 =
            """
            select t2.slot_id,
            sum(t1.num)
            FROM
              test_table1 t1
              inner join
              test_table2 t2 on t1.data_date = t2.data_date
            where to_date(t1.data_date) >= '2024-09-12'
            group by t2.slot_id;
            """
    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query2_0_before "${query2_0}"
    sql """set enable_materialized_view_rewrite = true;"""
    sql """ALTER MATERIALIZED VIEW test_agg_mv set("grace_period"="100000");"""
    mv_rewrite_success(query2_0, "test_agg_mv", true,
            is_partition_statistics_ready(db, ["test_table1", "test_table2", "test_agg_mv"]))
    order_qt_query2_0_after "${query2_0}"


    // Aggregate, if query group by expression doesn't use the partition column, and the invalid partition is not in the
    // grace_period, should not compensate union all, and should rewritten fail
    def query3_0 =
            """
            select t2.slot_id,
            sum(t1.num)
            FROM
              test_table1 t1
              inner join
              test_table2 t2 on t1.data_date = t2.data_date
            where to_date(t1.data_date) >= '2024-09-12'
            group by t2.slot_id;
            """
    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query3_0_before "${query3_0}"
    sql """set enable_materialized_view_rewrite = true;"""
    sql """ALTER MATERIALIZED VIEW test_agg_mv set("grace_period"="0");"""
    mv_rewrite_fail(query3_0, "test_agg_mv")
    order_qt_query3_0_after "${query3_0}"


    // Aggregate, if query group by expression use the partition column, but the invalid partition is in the
    // grace_period, should not compensate union all but should rewritten successfully
    def query4_0 =
            """
            select to_date(t1.data_date),
            sum(t1.num)
            FROM
              test_table1 t1
              inner join
              test_table2 t2 on t1.data_date = t2.data_date
            where to_date(t1.data_date) >= '2024-09-12'
            group by
            to_date(t1.data_date);
            """
    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query4_0_before "${query4_0}"
    sql """set enable_materialized_view_rewrite = true;"""
    sql """ALTER MATERIALIZED VIEW test_agg_mv set("grace_period"="100000");"""
    mv_rewrite_success(query4_0, "test_agg_mv", true,
            is_partition_statistics_ready(db, ["test_table1", "test_table2", "test_agg_mv"]))
    order_qt_query4_0_after "${query4_0}"


    // Aggregate, if query group by expression use the partition column, and the invalid partition is not in the
    // grace_period, should compensate union all, and should rewritten successfully
    def query5_0 =
            """
            select to_date(t1.data_date),
            sum(t1.num)
            FROM
              test_table1 t1
              inner join
              test_table2 t2 on t1.data_date = t2.data_date
            where to_date(t1.data_date) >= '2024-09-12'
            group by
            to_date(t1.data_date);
            """
    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query5_0_before "${query5_0}"
    sql """ALTER MATERIALIZED VIEW test_agg_mv set("grace_period"="0");"""
    sql """set enable_materialized_view_rewrite = true;"""
    mv_rewrite_success(query5_0, "test_agg_mv", true,
            is_partition_statistics_ready(db, ["test_table1", "test_table2", "test_agg_mv"]))
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS test_agg_mv"""


    sql """ DROP MATERIALIZED VIEW IF EXISTS test_join_mv"""
    sql """
    CREATE MATERIALIZED VIEW test_join_mv
    BUILD IMMEDIATE REFRESH ON MANUAL
    partition by(data_date)
    DISTRIBUTED BY HASH(data_date) BUCKETS 3
    PROPERTIES(
      "refresh_partition_num" = "1",
      'replication_num' = '1'
    )
    AS
    SELECT
      date_trunc(t3.data_date, 'day') as data_date,
      to_date(t3.data_date) as dt,
      t4.slot_id,
      t3.num
    FROM
      test_table1 t3
      left join
      test_table2 t4 on t3.data_date = t4.data_date
    """
    waitingMTMVTaskFinishedByMvName("test_join_mv")

    // Data modify
    sql """
    insert into test_table1 values
    ('2024-09-11 00:10:00', 'a', 1),
    ('2024-09-11 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'a', 1),
    ('2024-09-12 00:20:00', 'b', 1);
    """
    sql """analyze table test_table1 with sync"""

    // Join, if select expression not use the partition column, and the invalid partition is not in the
    // grace_period, should union all,and should rewritten successfully
    def query6_0 =
            """
            select
              t4.slot_id,
              t3.num
            FROM
              test_table1 t3
              left join
              test_table2 t4 on t3.data_date = t4.data_date
            where to_date(t3.data_date) >= '2024-09-12';
            """
    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query6_0_before "${query6_0}"
    sql """set enable_materialized_view_rewrite = true;"""
    mv_rewrite_success(query6_0, "test_join_mv", true,
            is_partition_statistics_ready(db, ["test_table1", "test_table2", "test_join_mv"]))
    order_qt_query6_0_after "${query6_0}"


    // Join, if select expression not use the partition column, and the invalid partition is in the
    // grace_period, should not compensate union all, and should rewritten successfully
    def query7_0 =
            """
            select
              t4.slot_id,
              t3.num
            FROM
              test_table1 t3
              left join
              test_table2 t4 on t3.data_date = t4.data_date
            where to_date(t3.data_date) >= '2024-09-12';
            """
    sql """set enable_materialized_view_rewrite = false;"""
    order_qt_query7_0_before "${query7_0}"
    sql """set enable_materialized_view_rewrite = true;"""
    sql """ALTER MATERIALIZED VIEW test_join_mv set("grace_period"="100000");"""
    mv_rewrite_success(query7_0, "test_join_mv", true,
            is_partition_statistics_ready(db, ["test_table1", "test_table2", "test_join_mv"]))
    order_qt_query7_0_after "${query7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS test_join_mv"""

}
