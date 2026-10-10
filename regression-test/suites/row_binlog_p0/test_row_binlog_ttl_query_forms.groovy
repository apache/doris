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

import org.awaitility.Awaitility
import java.util.concurrent.TimeUnit

suite("test_row_binlog_ttl_query_forms", "nonConcurrent") {
    sql "SET enable_sql_cache = true"
    sql "SET enable_query_cache = true"
    sql "DROP VIEW IF EXISTS row_binlog_ttl_query_view"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_query_forms"
    sql """
        CREATE TABLE row_binlog_ttl_query_forms (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.ttl_seconds" = "30",
            "disable_auto_compaction" = "true"
        )
    """
    sql """CREATE VIEW row_binlog_ttl_query_view AS
        SELECT k, v FROM row_binlog_ttl_query_forms@incr('incrementType' = 'DETAIL')"""
    sql """ALTER VIEW row_binlog_ttl_query_view AS WITH changes AS (
        SELECT k, v FROM row_binlog_ttl_query_forms@incr('incrementType' = 'DETAIL'))
        SELECT k, v FROM changes"""
    sql "INSERT INTO row_binlog_ttl_query_forms VALUES (1, 10), (2, 20)"

    def aggregate = """SELECT count(*), sum(v)
        FROM row_binlog_ttl_query_forms@incr('incrementType' = 'DETAIL')"""
    def throughView = "SELECT k, v FROM row_binlog_ttl_query_view ORDER BY k"
    def throughCte = """WITH changes AS (
        SELECT k, v FROM row_binlog_ttl_query_forms@incr('incrementType' = 'DETAIL'))
        SELECT k, v FROM changes ORDER BY k"""
    def throughUnion = """SELECT k, v FROM (
        SELECT k, v FROM row_binlog_ttl_query_forms@incr('incrementType' = 'DETAIL') WHERE k = 1
        UNION ALL
        SELECT k, v FROM row_binlog_ttl_query_forms@incr('incrementType' = 'DETAIL') WHERE k = 2
        ) changes ORDER BY k"""

    // Reuse the exact query text with both caches enabled and no intervening writes or DDL.
    qt_aggregate_before aggregate
    qt_aggregate_repeated aggregate
    qt_view_before throughView
    qt_cte_before throughCte
    qt_union_before throughUnion
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        (sql(aggregate)[0][0] as long) == 0L
    }
    qt_aggregate_expired aggregate
    qt_view_expired throughView
    qt_cte_expired throughCte
    qt_union_expired throughUnion
    qt_raw_retained "SELECT count(*) FROM binlog('table' = 'row_binlog_ttl_query_forms')"
    qt_base_preserved "SELECT k, v FROM row_binlog_ttl_query_forms ORDER BY k"

    // Each new statement must recompute its retention window, including scans nested in a view.
    sql """ALTER TABLE row_binlog_ttl_query_forms SET ("binlog.ttl_seconds" = "86400")"""
    qt_aggregate_extended aggregate
    qt_view_extended throughView
    qt_cte_extended throughCte
    qt_union_extended throughUnion
}
