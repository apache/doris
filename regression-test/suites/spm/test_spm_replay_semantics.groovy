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

suite("test_spm_replay_semantics", "spm") {
    sql "DROP TABLE IF EXISTS spm_replay_semantics_t"
    sql """CREATE TABLE spm_replay_semantics_t (k INT, b BOOLEAN, a INT, s STRING, v INT, d DATE)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO spm_replay_semantics_t VALUES
        (-1, TRUE, 1, 'a', 1, '2026-01-01'), (-1, NULL, 1, 'a', 1, '2026-01-01'),
        (-1, NULL, 2, 'b', 1, '2026-01-01'), (-1, TRUE, NULL, NULL, 0, '2026-01-01'),
        (1, TRUE, 1, 'a', 2, '2026-01-01')"""
    sql "DROP TABLE IF EXISTS spm_replay_semantics_u"
    sql """CREATE TABLE spm_replay_semantics_u (k INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_replay_semantics_u VALUES (1), (2)"

    sql "DROP TABLE IF EXISTS spm_replay_sink_order"
    sql """CREATE TABLE spm_replay_sink_order (a INT, b INT)
        DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_replay_sink_order VALUES (1, 2)"

    sql "DROP TABLE IF EXISTS spm_replay_container_literal"
    sql """CREATE TABLE spm_replay_container_literal (k INT, dates ARRAY<DATE>, words ARRAY<STRING>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_replay_container_literal VALUES (1, [DATE '2021-01-01'], ['original'])"

    sql "SET enable_spm_rewrite = true"
    sql "SET enable_spm_fallback = false"
    sql "SET enable_sql_cache = false"
    sql "SET spm_rewrite_timeout_ms = 60000"
    def create = { String bind, String plan ->
        sql("CREATE GLOBAL BASELINE PLAN \"${bind}\" WITH \"${plan}\"")[0][0] as Long
    }
    def withBaseline = { String bind, Closure check ->
        long id = create(bind, bind)
        try {
            check(id)
        } finally {
            sql "DROP BASELINE PLAN IF EXISTS ${id}"
        }
    }
    def explainHit = { String query, long id ->
        sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${id},")
    }
    def verify = { String tag, String bind, String query ->
        withBaseline(bind) { long id ->
            quickTest(tag + "_rows", query, true)
            boolean hit = explainHit(query, id)
            quickTest(tag + "_hit", "SELECT ${hit}", false)
        }
    }

    String inQuery = "SELECT k IN (SELECT k FROM spm_replay_semantics_u) AS x," +
            " k IN (SELECT k + 2 FROM spm_replay_semantics_u) AS y" +
            " FROM spm_replay_semantics_t WHERE k > 0 HAVING x"
    String inPlan = inQuery.replace("AS x,", "AS y,").replace("AS y FROM", "AS x FROM")
    test {
        sql """CREATE GLOBAL BASELINE PLAN "${inQuery}" WITH "${inPlan}" """
        exception "HAVING predicates"
    }
    String scalar = "SELECT (SELECT max(k) + 1 FROM spm_replay_semantics_u) AS x," +
            " (SELECT max(k) + 2 FROM spm_replay_semantics_u) AS y" +
            " FROM spm_replay_semantics_t HAVING x < 4"
    String scalarPlan = scalar.replace("AS x,", "AS y,").replace("AS y FROM", "AS x FROM")
    test {
        sql """CREATE GLOBAL BASELINE PLAN "${scalar}" WITH "${scalarPlan}" """
        exception "HAVING predicates"
    }

    String negation = "SELECT b, (NOT b) IS NULL AS c FROM spm_replay_semantics_t"
    verify("negation", negation, negation)

    String predicates = """SELECT b, k, a, s, b = (k IS NULL) AS is_null,
        b = (s LIKE 'a%') AS like_a, b = (a IN (1, 2)) AS in_a FROM spm_replay_semantics_t"""
    withBaseline(predicates) { long id ->
        order_qt_predicate_rows predicates
        qt_predicate_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
        boolean hit = explainHit(predicates, id)
        qt_predicate_hit "SELECT ${hit}"
    }

    String sinkOrder = "SELECT b, a FROM spm_replay_sink_order"
    withBaseline(sinkOrder) { long id ->
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), sinkOrder)
        qt_sink_order """SELECT ${rows[0][0]}, ${rows[0][1]},
            '${meta.getColumnLabel(1)}', '${meta.getColumnLabel(2)}'"""
        qt_sink_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
        boolean hit = explainHit(sinkOrder, id)
        qt_sink_hit "SELECT ${hit}"
    }

    String ambiguous = "SELECT k AS x, v AS x FROM spm_replay_semantics_t HAVING x > 0"
    String unambiguous = "SELECT k AS x, v AS y FROM spm_replay_semantics_t HAVING x > 0"
    test {
        sql ambiguous
        exception "ambiguous"
    }
    test {
        sql """CREATE GLOBAL BASELINE PLAN "${ambiguous}" WITH "${unambiguous}" """
        exception "the referenced output name 'x' is ambiguous"
    }

    def verifyOrdinal = { String tag, String original, String variant ->
        withBaseline(original) { long id ->
            quickTest(tag + "_frozen",
                    "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}", false)
            quickTest(tag + "_original", original, false)
            quickTest(tag + "_variant", variant, false)
            boolean originalHit = explainHit(original, id)
            boolean variantHit = explainHit(variant, id)
            quickTest(tag + "_hits", "SELECT ${originalHit}, ${variantHit}", false)
        }
    }
    String ordinal = "SELECT k * (1 - 2) AS score, k FROM spm_replay_semantics_u" +
            " ORDER BY 1 DESC NULLS LAST LIMIT 1"
    verifyOrdinal("ordinal", ordinal, ordinal.replace("(1 - 2)", "(3 - 2)"))

    String setOrdinal = "(SELECT 'web' AS channel, k FROM spm_replay_semantics_u)" +
            " UNION ALL (SELECT 'store' AS channel, k FROM spm_replay_semantics_u)" +
            " ORDER BY 1 ASC, 2 DESC LIMIT 3"
    verifyOrdinal("set_ordinal", setOrdinal, setOrdinal.replace("'web'", "'catalog'"))

    String recursive = "WITH RECURSIVE c(k) AS" +
            " (SELECT k FROM ASSERT_ROWS (SELECT k FROM spm_replay_semantics_u) a" +
            " UNION ALL SELECT CAST(k + 1 AS INT) FROM c WHERE k < 0) SELECT k FROM c"
    withBaseline(recursive) { long id ->
        boolean hit = explainHit(recursive, id)
        qt_recursive_assertion_hit "SELECT ${hit}"
        test {
            sql recursive
            exception "Expected"
        }
    }

    String concat = "SELECT k, length(group_concat(DISTINCT s)) AS n, sum(v) AS x" +
            " FROM spm_replay_semantics_t GROUP BY k"
    verify("concat", concat, concat)

    String count = "SELECT k, count(DISTINCT a) AS d, count(a) AS c FROM spm_replay_semantics_t GROUP BY k"
    verify("count", count, count)

    String keys = "SELECT k, k > 0 OR element_at(map_keys(map(NULL, k)), 1) AS c FROM spm_replay_semantics_t"
    verify("map_key", keys, keys.replace("map(NULL, k)", "map(TRUE, k)"))

    String nested = "SELECT k, k > 0 OR element_at(element_at(array(array(NULL)), 1), 1) AS c" +
            " FROM spm_replay_semantics_t"
    verify("nested_array", nested, nested.replace("array(NULL)", "array(TRUE)"))

    String window = "SELECT k, k > 0 OR first_value(NULL) OVER (ORDER BY k) AS c FROM spm_replay_semantics_t"
    verify("window", window, window.replace("first_value(NULL)", "first_value(TRUE)"))

    String having = "SELECT k + 1 FROM spm_replay_semantics_t HAVING k > 0"
    withBaseline(having) { long id ->
        String variant = having.replace("k + 1", "k + 2")
        boolean hit = explainHit(variant, id)
        qt_having_hit "SELECT ${hit}"
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), variant)
        String label = meta.getColumnLabel(1)
        qt_having_value_and_label "SELECT ${rows[0][0]}, '${label}'"
    }

    String date = "SELECT * REPLACE(DATE '2020-01-01' AS d) FROM spm_replay_semantics_t"
    withBaseline(date) { long id ->
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), date)
        boolean hit = explainHit(date, id)
        qt_date_type_and_hit "SELECT '${rows[0][5]}', '${meta.getColumnTypeName(6)}', ${hit}"
    }

    String container = "SELECT dates, element_at(dates, k) AS d, element_at(words, k) AS w" +
            " FROM (SELECT * REPLACE([DATE '2020-01-01'] AS dates, ['a''b'] AS words)" +
            " FROM spm_replay_container_literal) t"
    withBaseline(container) { long id ->
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), container)
        boolean escapedValuePreserved = rows[0][2] == "a'b"
        boolean hit = explainHit(container, id)
        qt_container_value_type_and_hit """SELECT '${rows[0][1]}', '${meta.getColumnTypeName(2)}',
            ${escapedValuePreserved}, ${hit}"""
        qt_container_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
    }

    String unspellableContainer = "SELECT * REPLACE({1, NULL} AS dates) FROM spm_replay_container_literal"
    withBaseline(unspellableContainer) { long id ->
        boolean hit = explainHit(unspellableContainer, id)
        qt_unspellable_container_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
        qt_unspellable_container_hit "SELECT ${hit}"
    }

    String previousMode = sql("SELECT @@sql_mode")[0][0].toString()
    String permissiveGroup = "SELECT k, v, count(*) AS n FROM spm_replay_semantics_t WHERE k > 0 GROUP BY k"
    try {
        sql "SET sql_mode = ''"
        withBaseline(permissiveGroup) { long id ->
            boolean hit = explainHit(permissiveGroup, id)
            qt_permissive_group_hit "SELECT ${hit}"
            sql "SET sql_mode = 'ONLY_FULL_GROUP_BY'"
            test {
                sql permissiveGroup
                exception "GROUP BY"
            }
        }
        String permissivePlan = permissiveGroup.replace("SELECT ", "SELECT /*+ SET_VAR(sql_mode=0) */ ")
        test {
            sql """CREATE GLOBAL BASELINE PLAN "${permissiveGroup}" WITH "${permissivePlan}" """
            exception "must appear in the GROUP BY clause"
        }
        test {
            sql permissiveGroup
            exception "must appear in the GROUP BY clause"
        }
        String validGroup = permissiveGroup + ", v"
        long validId = create(validGroup, validGroup.replace("SELECT ", "SELECT /*+ SET_VAR(sql_mode=0) */ "))
        try {
            boolean hit = explainHit(validGroup, validId)
            qt_valid_group_with_permissive_plan "SELECT ${hit}"
        } finally {
            sql "DROP BASELINE PLAN IF EXISTS ${validId}"
        }
    } finally {
        sql "SET sql_mode = '${previousMode}'"
    }
}
