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

    sql "DROP TABLE IF EXISTS spm_replay_assert_generate"
    sql """CREATE TABLE spm_replay_assert_generate (k INT, arr ARRAY<INT>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_replay_assert_generate VALUES (1, [10]), (2, [])"

    sql "DROP TABLE IF EXISTS spm_replay_stored_mode"
    sql """CREATE TABLE spm_replay_stored_mode (k INT, s STRING)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_replay_stored_mode VALUES (1, CONCAT('_#TEMP#_', CHAR(92))), (2, 'other')"

    sql "DROP TABLE IF EXISTS spm_replay_ids"
    sql """CREATE TABLE spm_replay_ids (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO spm_replay_ids VALUES (1, 2)"

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

    String assertedGenerator = "SELECT k + 1 AS c, lv.x AS x FROM ASSERT_ROWS" +
            " (SELECT k, arr FROM spm_replay_assert_generate) a LATERAL VIEW explode(arr) lv AS x"
    withBaseline(assertedGenerator) { long id ->
        qt_assert_generate_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
        boolean hit = explainHit(assertedGenerator, id)
        qt_assert_generate_hit "SELECT ${hit}"
        test {
            sql assertedGenerator
            exception "Expected EQ 1"
        }
        sql "TRUNCATE TABLE spm_replay_assert_generate"
        sql "INSERT INTO spm_replay_assert_generate VALUES (1, [10, 11])"
        order_qt_assert_generate_expanding assertedGenerator
        // Repeating CREATE of this readable baseline must keep one compact reservation.
        long firstRepeat = create(assertedGenerator, assertedGenerator)
        long secondRepeat = create(assertedGenerator, assertedGenerator)
        qt_repeat_id "SELECT ${firstRepeat == id}, ${secondRepeat == id}"
        qt_repeat_compact_states """SELECT COUNT(*) FROM __internal_schema.spm_baselines_seq
            WHERE baseline_id = ${id} AND id > 1"""
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

    def wholeContainers = [
        whole_map_key: "element_at(map_keys({NULL:1}), 1)",
        whole_nested_array: "element_at(element_at([[NULL]], 1), 1)",
        whole_nested_map: "element_at(element_at({'a':{'b':NULL}}, 'a'), 'b')",
        nested_map: "element_at(element_at(map('a', map('b', NULL)), 'a'), 'b')",
        any_value: "any_value(NULL)"
    ]
    wholeContainers.each { tag, payload ->
        String bind = "SELECT k, k > 0 OR ${payload} AS c FROM spm_replay_semantics_t"
        if (tag == "any_value") {
            bind += " GROUP BY k"
        }
        String variant = bind.replace("NULL", "TRUE")
        withBaseline(bind) { long id ->
            quickTest(tag + "_frozen", "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}")
            quickTest(tag + "_bind_rows", bind, true)
            quickTest(tag + "_variant_rows", variant, true)
            quickTest(tag + "_hit", "SELECT ${explainHit(bind, id)}, ${explainHit(variant, id)}")
        }
    }

    String decimalLiteral = "1.1234567890123456789012345678901234567"
    String decimal = "SELECT * REPLACE(${decimalLiteral} * ${decimalLiteral} AS k) FROM spm_replay_ids"
    def oldFold = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    def oldDecimal256 = sql("SELECT @@enable_decimal256")[0][0]
    try {
        sql "SET enable_fold_constant_by_be = true"
        sql "SET enable_decimal256 = false"
        withBaseline(decimal) { long id ->
            qt_decimal_settings_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
            sql "SET enable_decimal256 = true"
            def (rows, metadata) = JdbcUtils.executeToList(context.getConnection(), decimal)
            qt_decimal_settings_replayed """SELECT '${rows[0][0]}',
                ${metadata.getPrecision(1)}, ${metadata.getScale(1)}, ${explainHit(decimal, id)}"""
        }
    } finally {
        sql "SET enable_decimal256 = ${oldDecimal256}"
        sql "SET enable_fold_constant_by_be = ${oldFold}"
    }

    String starSubquery = "SELECT * REPLACE((SELECT max(k) + 1 FROM spm_replay_semantics_u) AS k)" +
            " FROM spm_replay_ids"
    long starId = create(starSubquery, starSubquery + " ")
    try {
        order_qt_star_subquery_whitespace starSubquery
        qt_star_subquery_hit "SELECT ${explainHit(starSubquery, starId)}"
    } finally {
        sql "DROP BASELINE PLAN IF EXISTS ${starId}"
    }
    String differentStarSubquery = starSubquery.replace("max(k) + 1", "max(k) + 2")
    test {
        sql """CREATE GLOBAL BASELINE PLAN "${starSubquery}" WITH "${differentStarSubquery}" """
        exception "cannot align the plan SQL with the bind SQL"
    }

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

    String columnCast = "SELECT * REPLACE(CAST(k AS BIGINT) AS k) FROM spm_replay_semantics_u"
    verify("column_cast", columnCast, columnCast)

    String functionCast = "SELECT * REPLACE(CAST(abs(1) AS BIGINT) AS k) FROM spm_replay_semantics_u"
    verify("function_cast", functionCast, functionCast)

    String invalidCast = "SELECT * REPLACE(CAST('abc' AS INT) AS k) FROM spm_replay_semantics_u"
    sql "SET enable_strict_cast = false"
    withBaseline(invalidCast) { long id ->
        qt_cast_mode_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
        boolean hit = explainHit(invalidCast, id)
        qt_cast_mode_hit "SELECT ${hit}"
        order_qt_permissive_cast invalidCast
        sql "SET enable_strict_cast = true"
        test {
            sql invalidCast
            exception "abc can't cast to INT in strict mode"
        }
    }
    sql "SET enable_strict_cast = false"

    def verifyConcreteCast = { String tag, String query ->
        withBaseline(query) { long id ->
            quickTest(tag + "_frozen",
                    "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}", false)
            boolean hit = explainHit(query, id)
            quickTest(tag + "_hit", "SELECT ${hit}", false)
            quickTest(tag + "_permissive", query, true)
            try {
                sql "SET enable_strict_cast = true"
                test {
                    sql query
                    exception "abc can't cast"
                }
            } finally {
                sql "SET enable_strict_cast = false"
            }
        }
    }
    verifyConcreteCast("generator_cast", "SELECT k, lv.x AS x FROM spm_replay_semantics_u" +
            " LATERAL VIEW explode(array(CAST('abc' AS INT))) lv AS x")
    verifyConcreteCast("implicit_cast", "SELECT * REPLACE('abc' + 1 AS k) FROM spm_replay_semantics_u")
    verifyConcreteCast("implicit_mixed_cast", "SELECT * REPLACE(k + 'abc' AS k) FROM spm_replay_semantics_u")

    String doubleResult = "SELECT a + b AS result FROM" +
            " (SELECT * REPLACE(CAST(9007199254740992 AS DOUBLE) AS b) FROM spm_replay_sink_order) p"
    withBaseline(doubleResult) { long id ->
        order_qt_double_result doubleResult
        boolean hit = explainHit(doubleResult, id)
        qt_double_result_hit "SELECT ${hit}"
        qt_double_result_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
    }

    String typedScalars = "SELECT * REPLACE(CAST(NULL AS BIGINT) AS a, CAST(1 AS BIGINT) AS b," +
            " CAST(1.25 AS FLOAT) AS k) FROM (SELECT a, b, a AS k FROM spm_replay_sink_order) p"
    withBaseline(typedScalars) { long id ->
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), typedScalars)
        boolean hit = explainHit(typedScalars, id)
        qt_typed_scalars """SELECT ${rows[0][0] == null}, ${rows[0][1]}, ${rows[0][2]},
            '${meta.getColumnTypeName(1)}', '${meta.getColumnTypeName(2)}', '${meta.getColumnTypeName(3)}', ${hit}"""
        qt_typed_scalars_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
    }

    String remainingScalars = "SELECT * REPLACE(" +
            "to_uuid_or_zero('00000000-0000-0000-0000-000000000001') AS a, round(1.234) AS b)" +
            " FROM spm_replay_sink_order"
    def (_, nativeScalarMeta) = JdbcUtils.executeToList(context.getConnection(), remainingScalars)
    withBaseline(remainingScalars) { long id ->
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), remainingScalars)
        boolean typesMatch = (1..2).every { int column ->
            meta.getColumnTypeName(column) == nativeScalarMeta.getColumnTypeName(column) &&
                    meta.getPrecision(column) == nativeScalarMeta.getPrecision(column) &&
                    meta.getScale(column) == nativeScalarMeta.getScale(column)
        }
        boolean hit = explainHit(remainingScalars, id)
        qt_uuid_decimal_values_and_types "SELECT '${rows[0][0]}', ${rows[0][1]}, ${typesMatch}, ${hit}"
        qt_uuid_decimal_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
    }

    String previousZone = sql("SELECT @@time_zone")[0][0].toString()
    String previousLocale = sql("SELECT @@lc_time_names")[0][0].toString()
    try {
        sql "SET time_zone = 'UTC'"
        String epoch = "SELECT * REPLACE(from_second(0) AS k, from_millisecond(1) AS b," +
                " from_microsecond(1) AS a, hour_from_unixtime(0) AS v," +
                " uuid_v7_to_datetime(to_uuid_or_zero('00000000-0000-7000-8000-000000000000')) AS d)" +
                " FROM spm_replay_semantics_t WHERE k = 1"
        withBaseline(epoch) { long id ->
            sql "SET time_zone = 'Asia/Shanghai'"
            order_qt_builtin_zone_values epoch
            boolean hit = explainHit(epoch, id)
            qt_builtin_zone_hit "SELECT ${hit}"
            qt_builtin_zone_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
        }

        sql "SET lc_time_names = 'en_US'"
        String locale = "SELECT * REPLACE(monthname(DATE '2024-01-01') AS a," +
                " dayname(DATE '2024-01-01') AS b) FROM spm_replay_sink_order"
        withBaseline(locale) { long id ->
            sql "SET lc_time_names = 'fr_FR'"
            order_qt_builtin_locale_values locale
            boolean hit = explainHit(locale, id)
            qt_builtin_locale_hit "SELECT ${hit}"
            qt_builtin_locale_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
        }
    } finally {
        sql "SET time_zone = '${previousZone}'"
        sql "SET lc_time_names = '${previousLocale}'"
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

    try {
        sql "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"
        String storedMode = "SELECT k FROM spm_replay_stored_mode WHERE s = '_#TEMP#_" + "\\" + "'"
        withBaseline(storedMode) { long id ->
            sql "SET sql_mode = '${previousMode}'"
            boolean visible = sql("SHOW BASELINE PLANS WHERE id = ${id}").any { (it[0] as Long) == id }
            qt_stored_mode_visible "SELECT ${visible}"
            sql "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"
            order_qt_stored_mode_rows storedMode
            boolean hit = explainHit(storedMode, id)
            qt_stored_mode_hit "SELECT ${hit}"
        }
    } finally {
        sql "SET sql_mode = '${previousMode}'"
    }

    String aliased = "SELECT * REPLACE(from_second(0) AS v)" +
            " FROM (SELECT k,v,7 AS b FROM spm_replay_ids) x"
    String savedZone = sql("SELECT @@time_zone")[0][0].toString()
    try {
        sql "SET time_zone = 'UTC'"
        for (String scope in ["SESSION", "GLOBAL"]) {
            long id = sql("CREATE ${scope} BASELINE PLAN \"${aliased}\" WITH \"${aliased} \"")[0][0] as Long
            try {
                String variant = aliased.replace("7 AS b", "8 AS b")
                boolean hits = explainHit(aliased, id) && explainHit(variant, id)
                quickTest("fresh_replay_ids_${scope.toLowerCase()}_hit", "SELECT ${hits}", false)
                quickTest("fresh_replay_ids_${scope.toLowerCase()}_rows", aliased, true)
                quickTest("fresh_replay_ids_${scope.toLowerCase()}_variant", variant, true)
            } finally {
                sql "DROP BASELINE PLAN IF EXISTS ${id}"
            }
        }

        for (String projection in ["", ", from_second(0) AS v"]) {
            String path = projection.isEmpty() ? "frozen" : "raw"
            String temporal = "SELECT * REPLACE(TIMESTAMP '2020-01-01 00:00:00.1' AS k" +
                    projection + ") FROM spm_replay_ids"
            String fractionVariant = temporal.replace("00:00:00.1", "00:00:00.2")
            withBaseline(temporal) { long id ->
                boolean originalHit = explainHit(temporal, id)
                boolean variantHit = explainHit(fractionVariant, id)
                quickTest("concrete_fraction_${path}_hits", "SELECT ${originalHit}, ${variantHit}", false)
                quickTest("concrete_fraction_${path}_original", temporal, true)
                quickTest("concrete_fraction_${path}_variant", fractionVariant, true)
                quickTest("concrete_fraction_${path}_frozen",
                        "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}", false)
            }
        }
    } finally {
        sql "SET time_zone = '${savedZone}'"
    }

    [
            if_array: "element_at(if(k < 0, array(NULL), array(NULL)), 1)",
            case_array: "element_at(case when k < 0 then [NULL] else [NULL] end, 1)",
            coalesce_map: "element_at(coalesce(map('x', NULL), map('x', NULL)), 'x')",
            nth_value: "nth_value(NULL, 1) OVER (ORDER BY k)"
    ].each { String name, String operand ->
        String query = "SELECT k, k < 0 OR ${operand} AS c FROM spm_replay_ids"
        String variant = query.replace("NULL", "TRUE")
        withBaseline(query) { long id ->
            boolean originalHit = explainHit(query, id)
            boolean variantHit = explainHit(variant, id)
            quickTest(name + "_hits", "SELECT ${originalHit}, ${variantHit}", false)
            quickTest(name + "_original_rows", query, true)
            quickTest(name + "_variant_rows", variant, true)
        }
    }

    String characters = "SELECT * REPLACE(CAST('a' AS STRING) AS k, CAST('a' AS VARCHAR(24)) AS v)" +
            " FROM spm_replay_ids"
    def nativeCharacterMeta = JdbcUtils.executeToList(context.getConnection(), characters)[1]
    withBaseline(characters) { long id ->
        def (rows, meta) = JdbcUtils.executeToList(context.getConnection(), characters)
        boolean typesMatch = (1..2).every { int column ->
            meta.getColumnTypeName(column) == nativeCharacterMeta.getColumnTypeName(column) &&
                    meta.getPrecision(column) == nativeCharacterMeta.getPrecision(column)
        }
        boolean hit = explainHit(characters, id)
        qt_character_values_and_types "SELECT '${rows[0][0]}', '${rows[0][1]}', ${typesMatch}, ${hit}"
        qt_character_frozen "SELECT plan_frozen FROM __internal_schema.spm_baselines WHERE id = ${id}"
    }

    String recreate = "SELECT k FROM spm_replay_ids WHERE k > 0"
    long firstId = create(recreate, recreate)
    sql "DROP BASELINE PLAN IF EXISTS ${firstId}"
    qt_drop_state_visible_at_both_keys """SELECT MAX(id < 0), MAX(id > 1)
        FROM __internal_schema.spm_baselines_seq
        WHERE baseline_id = ${firstId} AND dropped = 1 AND bind_sql_digest <> ''"""
    withBaseline(recreate) { long id ->
        boolean hit = explainHit(recreate, id)
        qt_recreated_identity_uses_new_id "SELECT ${id > firstId}, ${hit}"
        order_qt_recreated_identity_rows recreate
    }
}
