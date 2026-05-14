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

suite("test_ivm_explain_refresh") {
    sql """drop materialized view if exists test_ivm_explain_refresh_mv;"""
    sql """drop table if exists test_ivm_explain_refresh_t1;"""
    sql """drop table if exists test_ivm_explain_refresh_t2;"""

    sql """
        CREATE TABLE test_ivm_explain_refresh_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_explain_refresh_t2 (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_explain_refresh_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            test_ivm_explain_refresh_t1.k1 AS k1,
            test_ivm_explain_refresh_t1.v1 AS left_v1,
            test_ivm_explain_refresh_t2.v2 AS right_v2
        FROM test_ivm_explain_refresh_t1
        LEFT OUTER JOIN test_ivm_explain_refresh_t2
            ON test_ivm_explain_refresh_t1.k1 = test_ivm_explain_refresh_t2.k1;
    """

    explainIvmPlan("overview_after_create", """
        EXPLAIN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL
    """)

    explainIvmPlan("left_delta_plan", """
        EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL FOR DELTA 1
    """)

    explainIvmPlan("right_delta_plan", """
        EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL FOR DELTA 2
    """)
}
