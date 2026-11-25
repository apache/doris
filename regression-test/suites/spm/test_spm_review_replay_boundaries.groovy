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

import org.apache.doris.regression.util.JdbcUtils

suite("test_spm_review_replay_boundaries", "spm") {
    sql "DROP TABLE IF EXISTS spm_review_replay_t"
    sql """CREATE TABLE spm_review_replay_t (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_review_replay_t VALUES (-1, 10), (1, 20), (1, 20)"
    sql "DROP TABLE IF EXISTS spm_review_replay_u"
    sql """CREATE TABLE spm_review_replay_u (k INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_review_replay_u VALUES (-1)"

    sql "SET enable_spm_rewrite = true"
    sql "SET enable_spm_fallback = false"
    sql "SET enable_sql_cache = false"

    def verifyReplay = { String tag, String bind, String plan, String query ->
        long id = sql("CREATE GLOBAL BASELINE PLAN \"${bind}\" WITH \"${plan}\"")[0][0] as Long
        try {
            quickTest(tag + "_rows", query, true)
            boolean hit = sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${id},")
            quickTest(tag + "_hit", "SELECT ${hit}", false)
        } finally {
            sql "DROP BASELINE PLAN IF EXISTS ${id}"
        }
    }

    String assertion = "SELECT k + 1 AS c FROM ASSERT_ROWS" +
            " (SELECT k FROM spm_review_replay_t WHERE k > 0 LIMIT 1) a"
    verifyReplay("assertion", assertion, assertion, assertion)

    String aggregate = "SELECT k, k > 0 OR max(NULL) AS c FROM spm_review_replay_t GROUP BY k"
    verifyReplay("aggregate", aggregate, aggregate, aggregate.replace("max(NULL)", "max(TRUE)"))

    String lambda = "SELECT k, k > 0 OR element_at(array_map(x -> NULL, array(k)), 1) AS c" +
            " FROM spm_review_replay_t"
    verifyReplay("lambda", lambda, lambda, lambda.replace("x -> NULL", "x -> TRUE"))

    String projected = "SELECT k, k > 0 OR element_at(a, 1) AS c" +
            " FROM (SELECT k, array(NULL) AS a FROM spm_review_replay_t) x"
    verifyReplay("projected_array", projected, projected, projected.replace("array(NULL)", "array(TRUE)"))

    // The inner NULL must retain its type for both the array and Boolean consumers.
    String mixed = "SELECT COALESCE(d.n, ARRAY(7)) AS a, d.n OR FALSE AS b FROM (SELECT NULL AS n) d"
    def verifyTypes = { String tag, String bind, String query ->
        sql "SET enable_spm_rewrite = false"
        def (nativeRows, nativeMeta) = JdbcUtils.executeToList(context.getConnection(), query)
        sql "SET enable_spm_rewrite = true"
        long id = sql("CREATE GLOBAL BASELINE PLAN \"${bind}\"")[0][0] as Long
        try {
            def (actualRows, actualMeta) = JdbcUtils.executeToList(context.getConnection(), query)
            boolean typesMatch = (1..actualMeta.getColumnCount()).every { int column ->
                actualMeta.getColumnTypeName(column) == nativeMeta.getColumnTypeName(column) &&
                        actualMeta.getPrecision(column) == nativeMeta.getPrecision(column) &&
                        actualMeta.getScale(column) == nativeMeta.getScale(column)
            }
            boolean hit = sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${id},")
            quickTest(tag + "_types_hit_rows", "SELECT ${typesMatch}, ${hit}, ${actualRows == nativeRows}", false)
        } finally {
            sql "DROP BASELINE PLAN IF EXISTS ${id}"
        }
    }
    verifyTypes("mixed_null_consumers", mixed, mixed.replace("ARRAY(7)", "ARRAY(8)"))
    verifyTypes("projected_array_variant_types", projected + " ORDER BY k, c",
            projected.replace("array(NULL)", "array(TRUE)") + " ORDER BY k, c")

    String usingJoin = "SELECT a.* FROM spm_review_replay_t a INNER JOIN spm_review_replay_u b USING(k)"
    verifyReplay("qualified_star", usingJoin, usingJoin, usingJoin)

    String subquery = "SELECT k FROM spm_review_replay_t WHERE k IN (SELECT k FROM spm_review_replay_u)"
    verifyReplay("manual_subquery", subquery, subquery.replace("(SELECT", "( SELECT").replace("u)", "u )"), subquery)

    test {
        sql """CREATE GLOBAL BASELINE PLAN "${subquery}" WITH
            "SELECT k FROM spm_review_replay_t WHERE k IN (SELECT k FROM spm_review_replay_u WHERE k > 0)" """
        exception "SPM cannot align the plan SQL"
    }
}
