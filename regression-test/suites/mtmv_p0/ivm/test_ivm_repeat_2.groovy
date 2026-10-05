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

suite("test_ivm_repeat_2") {
    sql """drop materialized view if exists test_ivm_repeat_2_mv;"""
    sql """drop table if exists test_ivm_repeat_2_t;"""

    sql """
        CREATE TABLE test_ivm_repeat_2_t (
            id BIGINT NOT NULL,
            dt DATE NOT NULL,
            category VARCHAR(20) NOT NULL,
            amount DECIMAL(12, 2) NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_repeat_2_t VALUES
            (1, '2024-02-01', 'book', 10.00),
            (2, '2024-02-01', 'book', 20.00),
            (3, '2024-02-02', 'toy', 30.00),
            (4, '2024-02-02', 'stationery', 40.00),
            (5, '2024-02-03', 'food', 50.00);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_repeat_2_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT dt,
               category,
               GROUPING(dt) AS g_dt,
               GROUPING(category) AS g_category,
               GROUPING_ID(dt, category) AS gid,
               COUNT(*) AS event_count,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_2_t
        GROUP BY GROUPING SETS ((dt, category), (dt), ());
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_2_mv")
    order_qt_repeat_2_init """
        SELECT dt, category, g_dt, g_category, gid, event_count, total_amount
        FROM test_ivm_repeat_2_mv
    """
    order_qt_repeat_2_init_source """
        SELECT dt,
               category,
               GROUPING(dt) AS g_dt,
               GROUPING(category) AS g_category,
               GROUPING_ID(dt, category) AS gid,
               COUNT(*) AS event_count,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_2_t
        GROUP BY GROUPING SETS ((dt, category), (dt), ())
    """

    sql """
        INSERT INTO test_ivm_repeat_2_t VALUES
            (6, '2024-02-02', 'toy', 60.00),
            (7, '2024-02-03', 'food', 70.00);
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_2_mv")
    order_qt_repeat_2_insert """
        SELECT dt, category, g_dt, g_category, gid, event_count, total_amount
        FROM test_ivm_repeat_2_mv
    """
    order_qt_repeat_2_insert_source """
        SELECT dt,
               category,
               GROUPING(dt) AS g_dt,
               GROUPING(category) AS g_category,
               GROUPING_ID(dt, category) AS gid,
               COUNT(*) AS event_count,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_2_t
        GROUP BY GROUPING SETS ((dt, category), (dt), ())
    """

    sql """UPDATE test_ivm_repeat_2_t SET amount = 25.00 WHERE id = 2;"""
    sql """UPDATE test_ivm_repeat_2_t SET category = 'game', amount = 35.00 WHERE id = 3;"""
    sql """DELETE FROM test_ivm_repeat_2_t WHERE id = 4;"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_2_mv")
    order_qt_repeat_2_update_delete """
        SELECT dt, category, g_dt, g_category, gid, event_count, total_amount
        FROM test_ivm_repeat_2_mv
    """
    order_qt_repeat_2_update_delete_source """
        SELECT dt,
               category,
               GROUPING(dt) AS g_dt,
               GROUPING(category) AS g_category,
               GROUPING_ID(dt, category) AS gid,
               COUNT(*) AS event_count,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_2_t
        GROUP BY GROUPING SETS ((dt, category), (dt), ())
    """
    order_qt_repeat_2_empty_group """
        SELECT dt, category, g_dt, g_category, gid, event_count, total_amount
        FROM test_ivm_repeat_2_mv
        WHERE g_dt = 1 AND g_category = 1
    """
}
