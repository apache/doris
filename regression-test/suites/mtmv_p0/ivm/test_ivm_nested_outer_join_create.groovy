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

suite("test_ivm_nested_outer_join_create") {
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set enable_materialized_view_rewrite = false"
    sql "set enable_ivm_complex_outer_join_delta = false"

    sql "drop materialized view if exists test_ivm_nested_outer_join_create_mv"
    sql "drop table if exists test_ivm_nested_outer_join_create_t1"
    sql "drop table if exists test_ivm_nested_outer_join_create_t2"
    sql "drop table if exists test_ivm_nested_outer_join_create_t3"

    sql """
        CREATE TABLE test_ivm_nested_outer_join_create_t1 (
            k INT,
            v INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        CREATE TABLE test_ivm_nested_outer_join_create_t2 (
            k INT,
            v INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        CREATE TABLE test_ivm_nested_outer_join_create_t3 (
            k INT,
            v INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    test {
        sql """
            CREATE MATERIALIZED VIEW test_ivm_nested_outer_join_create_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.k AS a_k, a.v AS a_v, b.k AS b_k, b.v AS b_v, c.k AS c_k, c.v AS c_v
            FROM test_ivm_nested_outer_join_create_t1 a
            FULL OUTER JOIN (
                test_ivm_nested_outer_join_create_t2 b
                FULL OUTER JOIN test_ivm_nested_outer_join_create_t3 c
                    ON b.k = c.k
            ) ON a.k = b.k
        """
        exception "null side"
    }

    sql "set enable_ivm_complex_outer_join_delta = true"
    sql """
        CREATE MATERIALIZED VIEW test_ivm_nested_outer_join_create_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT a.k AS a_k, a.v AS a_v, b.k AS b_k, b.v AS b_v, c.k AS c_k, c.v AS c_v
        FROM test_ivm_nested_outer_join_create_t1 a
        FULL OUTER JOIN (
            test_ivm_nested_outer_join_create_t2 b
            FULL OUTER JOIN test_ivm_nested_outer_join_create_t3 c
                ON b.k = c.k
        ) ON a.k = b.k
    """

    sql "set show_hidden_columns = true"
    def descResult = sql "desc test_ivm_nested_outer_join_create_mv all"
    assertTrue(descResult.toString().contains("__DORIS_IVM_ROW_ID_COL__"),
            "The created materialized view should use the IVM layout")
    sql "set show_hidden_columns = false"

    sql """
        INSERT INTO test_ivm_nested_outer_join_create_t1 VALUES
            (1, 10), (2, 20)
    """
    sql """
        INSERT INTO test_ivm_nested_outer_join_create_t2 VALUES
            (1, 100), (3, 300)
    """
    sql """
        INSERT INTO test_ivm_nested_outer_join_create_t3 VALUES
            (1, 1000), (4, 4000)
    """
    sql "REFRESH MATERIALIZED VIEW test_ivm_nested_outer_join_create_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_nested_outer_join_create_mv")

    order_qt_nested_outer_join_initial_source """
        SELECT a.k AS a_k, a.v AS a_v, b.k AS b_k, b.v AS b_v, c.k AS c_k, c.v AS c_v
        FROM test_ivm_nested_outer_join_create_t1 a
        FULL OUTER JOIN (
            test_ivm_nested_outer_join_create_t2 b
            FULL OUTER JOIN test_ivm_nested_outer_join_create_t3 c
                ON b.k = c.k
        ) ON a.k = b.k
        ORDER BY 1, 3, 5, 2, 4, 6
    """
    order_qt_nested_outer_join_initial_mv """
        SELECT a_k, a_v, b_k, b_v, c_k, c_v
        FROM test_ivm_nested_outer_join_create_mv
        ORDER BY 1, 3, 5, 2, 4, 6
    """

    sql "INSERT INTO test_ivm_nested_outer_join_create_t1 VALUES (5, 50)"
    sql "INSERT INTO test_ivm_nested_outer_join_create_t2 VALUES (5, 500)"
    sql "DELETE FROM test_ivm_nested_outer_join_create_t3 WHERE k = 1"
    sql "REFRESH MATERIALIZED VIEW test_ivm_nested_outer_join_create_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_nested_outer_join_create_mv")

    order_qt_nested_outer_join_second_source """
        SELECT a.k AS a_k, a.v AS a_v, b.k AS b_k, b.v AS b_v, c.k AS c_k, c.v AS c_v
        FROM test_ivm_nested_outer_join_create_t1 a
        FULL OUTER JOIN (
            test_ivm_nested_outer_join_create_t2 b
            FULL OUTER JOIN test_ivm_nested_outer_join_create_t3 c
                ON b.k = c.k
        ) ON a.k = b.k
        ORDER BY 1, 3, 5, 2, 4, 6
    """
    order_qt_nested_outer_join_second_mv """
        SELECT a_k, a_v, b_k, b_v, c_k, c_v
        FROM test_ivm_nested_outer_join_create_mv
        ORDER BY 1, 3, 5, 2, 4, 6
    """
}
