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

suite("test_ivm_outer_join_5") {
    sql """set enable_nereids_planner = true;"""
    sql """set enable_fallback_to_original_planner = false;"""
    sql """set enable_materialized_view_rewrite = false;"""
    sql """set enable_nereids_timeout = false;"""

    sql """drop materialized view if exists test_ivm_outer_join_5_mv;"""
    sql """drop table if exists test_ivm_outer_join_5_customers;"""
    sql """drop table if exists test_ivm_outer_join_5_profiles;"""

    sql """
        CREATE TABLE test_ivm_outer_join_5_customers (
            customer_id INT,
            region STRING,
            active TINYINT
        )
        UNIQUE KEY(customer_id)
        DISTRIBUTED BY HASH(customer_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_outer_join_5_profiles (
            customer_id INT,
            segment STRING
        )
        UNIQUE KEY(customer_id)
        DISTRIBUTED BY HASH(customer_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO test_ivm_outer_join_5_customers VALUES (1, 'APAC', 1), (2, 'EMEA', 1), (3, 'LATAM', 0);"""
    sql """INSERT INTO test_ivm_outer_join_5_profiles VALUES (1, 'retail'), (2, 'enterprise');"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_outer_join_5_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            test_ivm_outer_join_5_customers.customer_id AS customer_id,
            test_ivm_outer_join_5_customers.region AS region,
            test_ivm_outer_join_5_profiles.segment AS segment
        FROM test_ivm_outer_join_5_customers
        LEFT OUTER JOIN test_ivm_outer_join_5_profiles
            ON test_ivm_outer_join_5_customers.customer_id = test_ivm_outer_join_5_profiles.customer_id
        WHERE test_ivm_outer_join_5_customers.active = 1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_5_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_5_mv")
    order_qt_outer_join_5_after_complete_source """
        SELECT
            test_ivm_outer_join_5_customers.customer_id,
            test_ivm_outer_join_5_customers.region,
            test_ivm_outer_join_5_profiles.segment
        FROM test_ivm_outer_join_5_customers
        LEFT OUTER JOIN test_ivm_outer_join_5_profiles
            ON test_ivm_outer_join_5_customers.customer_id = test_ivm_outer_join_5_profiles.customer_id
        WHERE test_ivm_outer_join_5_customers.active = 1
        ORDER BY 1, 2, 3
    """
    order_qt_outer_join_5_after_complete_mv """
        SELECT customer_id, region, segment
        FROM test_ivm_outer_join_5_mv
        ORDER BY customer_id, region, segment
    """

    sql """INSERT INTO test_ivm_outer_join_5_customers VALUES (4, 'AMER', 1);"""
    sql """INSERT INTO test_ivm_outer_join_5_profiles VALUES (4, 'startup');"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_5_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_5_mv")
    order_qt_outer_join_5_after_dual_insert_source """
        SELECT
            test_ivm_outer_join_5_customers.customer_id,
            test_ivm_outer_join_5_customers.region,
            test_ivm_outer_join_5_profiles.segment
        FROM test_ivm_outer_join_5_customers
        LEFT OUTER JOIN test_ivm_outer_join_5_profiles
            ON test_ivm_outer_join_5_customers.customer_id = test_ivm_outer_join_5_profiles.customer_id
        WHERE test_ivm_outer_join_5_customers.active = 1
        ORDER BY 1, 2, 3
    """
    order_qt_outer_join_5_after_dual_insert_mv """
        SELECT customer_id, region, segment
        FROM test_ivm_outer_join_5_mv
        ORDER BY customer_id, region, segment
    """

    sql """INSERT INTO test_ivm_outer_join_5_profiles VALUES (2, 'public');"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_outer_join_5_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_outer_join_5_mv")
    order_qt_outer_join_5_after_right_update_source """
        SELECT
            test_ivm_outer_join_5_customers.customer_id,
            test_ivm_outer_join_5_customers.region,
            test_ivm_outer_join_5_profiles.segment
        FROM test_ivm_outer_join_5_customers
        LEFT OUTER JOIN test_ivm_outer_join_5_profiles
            ON test_ivm_outer_join_5_customers.customer_id = test_ivm_outer_join_5_profiles.customer_id
        WHERE test_ivm_outer_join_5_customers.active = 1
        ORDER BY 1, 2, 3
    """
    order_qt_outer_join_5_after_right_update_mv """
        SELECT customer_id, region, segment
        FROM test_ivm_outer_join_5_mv
        ORDER BY customer_id, region, segment
    """
}
