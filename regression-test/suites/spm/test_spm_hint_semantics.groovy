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

suite("test_spm_hint_semantics", "p0") {
    sql "DROP TABLE IF EXISTS spm_hint_semantics_t"
    sql """CREATE TABLE spm_hint_semantics_t (k INT, a INT, b INT, ts BIGINT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_hint_semantics_t VALUES (1, 0, 1, 0), (2, 1, 0, 3600), (3, 0, 0, 7200)"

    def modeBefore = sql("SELECT @@sql_mode")[0][0].toString()
    def zoneBefore = sql("SELECT @@time_zone")[0][0].toString()
    sql "SET enable_spm_rewrite = true"
    sql "SET enable_spm_fallback = false"
    def createBaseline = { String bind, String plan ->
        sql('CREATE SESSION BASELINE PLAN "' + bind + '" WITH "' + plan + '"')[0][0]
    }
    def expectHit = { String query, id ->
        explain {
            sql query
            contains "SPM baseline hit: id=${id}"
        }
    }

    try {
        sql "SET sql_mode = ''"
        String nested = "SELECT s.k, s.a || s.b AS c FROM" +
                " (SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ k, a, b" +
                " FROM spm_hint_semantics_t) s ORDER BY s.k"
        def nestedId = createBaseline(nested, nested)
        expectHit(nested, nestedId)
        order_qt_nested_or nested

        sql "SET sql_mode = 'PIPES_AS_CONCAT'"
        String reversed = "WITH s AS (SELECT /*+ SET_VAR(sql_mode='') */ k, a, b" +
                " FROM spm_hint_semantics_t) SELECT k, a || b AS c FROM s ORDER BY k"
        def reversedId = createBaseline(reversed, reversed)
        expectHit(reversed, reversedId)
        order_qt_cte_concat reversed

        sql "SET sql_mode = ''"
        sql "SET time_zone = '+09:30'"
        String bind = "SELECT /*+ SET_VAR(time_zone='+00:00') */ k, from_unixtime(ts) AS dt" +
                " FROM spm_hint_semantics_t ORDER BY k"
        String plan = "SELECT /*+ SET_VAR(time_zone='+08:00') ORDERED */ k, from_unixtime(ts) AS dt" +
                " FROM spm_hint_semantics_t ORDER BY k"
        def zoneId = createBaseline(bind, plan)
        expectHit(bind, zoneId)
        order_qt_caller_zone bind
        qt_zone_after_hint "SELECT @@time_zone"

        String orderedHints = "SELECT /*+ SET_VAR(time_zone='+08:00') */" +
                "       /*+ SET_VAR(time_zone='+00:00') */ k, from_unixtime(ts) AS dt" +
                " FROM spm_hint_semantics_t ORDER BY k"
        def orderedId = createBaseline(orderedHints, orderedHints)
        expectHit(orderedHints, orderedId)
        order_qt_hint_text_order orderedHints

        // An explicit base-index pin keeps the original plan SQL and exercises tree fallback.
        String rawBind = "SELECT k, from_unixtime(ts) AS dt FROM spm_hint_semantics_t" +
                " INDEX spm_hint_semantics_t ORDER BY k"
        String rawPlan = "SELECT /*+ SET_VAR(time_zone='+08:00') ORDERED */ k, from_unixtime(ts) AS dt" +
                " FROM spm_hint_semantics_t INDEX spm_hint_semantics_t ORDER BY k"
        def rawId = createBaseline(rawBind, rawPlan)
        expectHit(rawBind, rawId)
        order_qt_plan_only_zone_removed rawBind
        qt_zone_after_fallback "SELECT @@time_zone"

        String protectedSql = "SELECT /*+ SET_VAR(enable_nereids_rules='') */ 1 AS c"
        test {
            sql('CREATE SESSION BASELINE PLAN "' + protectedSql + '"')
            exception "SPM-protected session variable"
        }
    } finally {
        sql "SET sql_mode = '${modeBefore}'"
        sql "SET time_zone = '${zoneBefore}'"
    }
}
