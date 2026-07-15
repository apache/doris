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

suite("test_ivm_subquery_alias_1") {
    sql """drop materialized view if exists test_ivm_subquery_alias_1_mv;"""
    sql """drop table if exists test_ivm_subquery_alias_1_base;"""

    sql """
        CREATE TABLE test_ivm_subquery_alias_1_base (
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

    sql """INSERT INTO test_ivm_subquery_alias_1_base VALUES (1, 10), (2, 20);"""

    explain {
        sql """
            ANALYZED PLAN
            SELECT s1.k1, s1.v1
            FROM (
                SELECT s0.k1, s0.v1
                FROM (
                    SELECT a.k1, a.v1
                    FROM test_ivm_subquery_alias_1_base a
                ) s0
            ) s1
        """
        contains "LogicalSubQueryAlias"
        contains "qualifier=[s0]"
        contains "qualifier=[s1]"
    }

    explain {
        sql """
            LOGICAL PLAN
            SELECT s1.k1, s1.v1
            FROM (
                SELECT s0.k1, s0.v1
                FROM (
                    SELECT a.k1, a.v1
                    FROM test_ivm_subquery_alias_1_base a
                ) s0
            ) s1
        """
        contains "LogicalProject"
        notContains "LogicalSubQueryAlias"
    }

    sql """
        CREATE MATERIALIZED VIEW test_ivm_subquery_alias_1_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT s1.k1, s1.v1
        FROM (
            SELECT s0.k1, s0.v1
            FROM (
                SELECT a.k1, a.v1
                FROM test_ivm_subquery_alias_1_base a
            ) s0
        ) s1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_subquery_alias_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_subquery_alias_1_mv")
    advance_ivm_stream_offset("test_ivm_subquery_alias_1_mv")
    order_qt_subquery_alias_scan_after_complete """
        SELECT k1, v1 FROM test_ivm_subquery_alias_1_mv
    """

    sql """INSERT INTO test_ivm_subquery_alias_1_base VALUES (2, 25), (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_subquery_alias_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_subquery_alias_1_mv")
    order_qt_subquery_alias_scan_after_incremental """
        SELECT k1, v1 FROM test_ivm_subquery_alias_1_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_subquery_alias_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_subquery_alias_1_mv")
    advance_ivm_stream_offset("test_ivm_subquery_alias_1_mv")
    order_qt_subquery_alias_scan_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_subquery_alias_1_mv
    """
}
