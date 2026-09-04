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

suite("test_ivm_subquery_alias_3") {
    sql """drop materialized view if exists test_ivm_subquery_alias_3_mv;"""
    sql """drop table if exists test_ivm_subquery_alias_3_t1;"""
    sql """drop table if exists test_ivm_subquery_alias_3_t2;"""

    sql """
        CREATE TABLE test_ivm_subquery_alias_3_t1 (
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
        CREATE TABLE test_ivm_subquery_alias_3_t2 (
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

    sql """INSERT INTO test_ivm_subquery_alias_3_t1 VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_subquery_alias_3_t2 VALUES (1, 100), (2, 200);"""

    explain {
        sql """
            ANALYZED PLAN
            SELECT o.k1, o.v1, c.v2
            FROM (
                SELECT a.k1, a.v1
                FROM test_ivm_subquery_alias_3_t1 a
            ) o
            INNER JOIN (
                SELECT b.k1, b.v2
                FROM test_ivm_subquery_alias_3_t2 b
            ) c
            ON o.k1 = c.k1
        """
        contains "LogicalSubQueryAlias"
        contains "qualifier=[o]"
        contains "qualifier=[c]"
    }

    explain {
        sql """
            LOGICAL PLAN
            SELECT o.k1, o.v1, c.v2
            FROM (
                SELECT a.k1, a.v1
                FROM test_ivm_subquery_alias_3_t1 a
            ) o
            INNER JOIN (
                SELECT b.k1, b.v2
                FROM test_ivm_subquery_alias_3_t2 b
            ) c
            ON o.k1 = c.k1
        """
        contains "LogicalJoin"
        notContains "LogicalSubQueryAlias"
    }

    sql """
        CREATE MATERIALIZED VIEW test_ivm_subquery_alias_3_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT o.k1, o.v1, c.v2
        FROM (
            SELECT a.k1, a.v1
            FROM test_ivm_subquery_alias_3_t1 a
        ) o
        INNER JOIN (
            SELECT b.k1, b.v2
            FROM test_ivm_subquery_alias_3_t2 b
        ) c
        ON o.k1 = c.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_subquery_alias_3_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_subquery_alias_3_mv")
    order_qt_subquery_alias_join_after_complete """
        SELECT k1, v1, v2 FROM test_ivm_subquery_alias_3_mv
    """

    sql """INSERT INTO test_ivm_subquery_alias_3_t2 VALUES (2, 220), (3, 300);"""
    sql """INSERT INTO test_ivm_subquery_alias_3_t1 VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_subquery_alias_3_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_subquery_alias_3_mv")
    order_qt_subquery_alias_join_after_incremental """
        SELECT k1, v1, v2 FROM test_ivm_subquery_alias_3_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_subquery_alias_3_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_subquery_alias_3_mv")
    order_qt_subquery_alias_join_after_complete_recovery """
        SELECT k1, v1, v2 FROM test_ivm_subquery_alias_3_mv
    """
}
