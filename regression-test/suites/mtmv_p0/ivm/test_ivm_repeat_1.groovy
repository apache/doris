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

suite("test_ivm_repeat_1") {
    sql """drop materialized view if exists test_ivm_repeat_1_mv;"""
    sql """drop table if exists test_ivm_repeat_1_t;"""

    sql """
        CREATE TABLE test_ivm_repeat_1_t (
            id INT,
            region VARCHAR(16),
            product VARCHAR(16),
            amount INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_repeat_1_t VALUES
            (1, 'east', 'book', 10),
            (2, 'east', 'toy', 20),
            (3, 'west', 'book', 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_repeat_1_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT region,
               product,
               GROUPING(region) AS g_region,
               GROUPING(product) AS g_product,
               GROUPING_ID(region, product) AS gid,
               COUNT(*) AS cnt,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_1_t
        GROUP BY GROUPING SETS ((region, product), (region), ());
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_1_mv")
    order_qt_repeat_complete """
        SELECT region, product, g_region, g_product, gid, cnt, total_amount
        FROM test_ivm_repeat_1_mv
    """
    order_qt_repeat_complete_source """
        SELECT region,
               product,
               GROUPING(region) AS g_region,
               GROUPING(product) AS g_product,
               GROUPING_ID(region, product) AS gid,
               COUNT(*) AS cnt,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_1_t
        GROUP BY GROUPING SETS ((region, product), (region), ())
    """
    order_qt_repeat_complete_empty_group """
        SELECT region, product, g_region, g_product, gid, cnt, total_amount
        FROM test_ivm_repeat_1_mv
        WHERE g_region = 1 AND g_product = 1
    """

    sql """INSERT INTO test_ivm_repeat_1_t VALUES (4, 'west', 'toy', 40);"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_1_mv")
    order_qt_repeat_incr """
        SELECT region, product, g_region, g_product, gid, cnt, total_amount
        FROM test_ivm_repeat_1_mv
    """
    order_qt_repeat_incr_source """
        SELECT region,
               product,
               GROUPING(region) AS g_region,
               GROUPING(product) AS g_product,
               GROUPING_ID(region, product) AS gid,
               COUNT(*) AS cnt,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_1_t
        GROUP BY GROUPING SETS ((region, product), (region), ())
    """
    order_qt_repeat_incr_empty_group """
        SELECT region, product, g_region, g_product, gid, cnt, total_amount
        FROM test_ivm_repeat_1_mv
        WHERE g_region = 1 AND g_product = 1
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_1_mv")
    order_qt_repeat_complete2 """
        SELECT region, product, g_region, g_product, gid, cnt, total_amount
        FROM test_ivm_repeat_1_mv
    """
    order_qt_repeat_complete2_source """
        SELECT region,
               product,
               GROUPING(region) AS g_region,
               GROUPING(product) AS g_product,
               GROUPING_ID(region, product) AS gid,
               COUNT(*) AS cnt,
               SUM(amount) AS total_amount
        FROM test_ivm_repeat_1_t
        GROUP BY GROUPING SETS ((region, product), (region), ())
    """
    order_qt_repeat_complete2_empty_group """
        SELECT region, product, g_region, g_product, gid, cnt, total_amount
        FROM test_ivm_repeat_1_mv
        WHERE g_region = 1 AND g_product = 1
    """
}
