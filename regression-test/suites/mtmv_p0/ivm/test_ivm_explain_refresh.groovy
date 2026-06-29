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
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
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
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
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

    sql """INSERT INTO test_ivm_explain_refresh_t1 VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_explain_refresh_t2 VALUES (1, 100), (3, 300);"""

    explainIvmPlan("left_delta_shape_plan", """
        EXPLAIN SHAPE PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL
    """)

    explainIvmPlan("right_delta_shape_plan", """
        EXPLAIN SHAPE PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL
    """)

    def overviewPlan = sql """
        EXPLAIN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL
    """
    assertTrue(overviewPlan.toString().contains("IVM_NORMALIZED_PLAN"))
    assertTrue(overviewPlan.toString().contains("IVM_DELTA_PLAN"))

    explain {
        sql "REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL"
        contains "PLAN FRAGMENT 0"
        contains "__DORIS_IVM_ROW_ID_COL__"
        contains "VRESULT SINK"
    }

    explain {
        sql "ANALYZED PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL"
        contains "LogicalResultSink"
    }

    explain {
        sql "ANALYZED PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL"
        contains "LogicalJoin"
        contains "LEFT_OUTER_JOIN"
        contains "test_ivm_explain_refresh_t1"
        contains "test_ivm_explain_refresh_t2"
    }

    explain {
        sql "LOGICAL PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL"
        contains "LogicalResultSink"
    }

    explain {
        sql "PHYSICAL PLAN REFRESH MATERIALIZED VIEW test_ivm_explain_refresh_mv INCREMENTAL"
        contains "PhysicalResultSink"
    }
}
