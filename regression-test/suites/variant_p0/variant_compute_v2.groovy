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

suite("variant_compute_v2") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    setBeConfigTemporary([enable_variant_v2: true]) {

    qt_constant_fold """
        SELECT CAST(parse_to_variant('{"folded":[1,true,null]}') AS STRING)
    """

    order_qt_constant_union """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant('{"constant":1}') AS v
            UNION ALL
            SELECT parse_to_variant('[2,null]') AS v
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_zero_row_union """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant('{"zero":0}') AS v FROM numbers("number" = "0")
            UNION ALL
            SELECT parse_to_variant('{"zero":1}') AS v
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_dynamic_encoded_union """
        SELECT branch, CAST(v AS STRING)
        FROM (
            SELECT 0 AS branch,
                   parse_to_variant(CONCAT('{"encoded":', number, '}')) AS v
            FROM numbers("number" = "2")
            UNION ALL
            SELECT 1 AS branch,
                   parse_to_variant(CONCAT('{"encoded":', number + 2, '}')) AS v
            FROM numbers("number" = "2")
        ) t
        ORDER BY branch, CAST(v AS STRING)
    """

    order_qt_dynamic_encoded_typed_nullable_union """
        SELECT branch, CAST(v AS STRING), v IS NULL
        FROM (
            SELECT 0 AS branch,
                   parse_to_variant(
                       CASE number WHEN 0 THEN CAST(NULL AS STRING)
                                   ELSE CAST(number AS STRING) END) AS v
            FROM numbers("number" = "2")
            UNION ALL
            SELECT 1 AS branch,
                   CAST(CASE number WHEN 0 THEN CAST(NULL AS BIGINT)
                                    ELSE number END AS VARIANT) AS v
            FROM numbers("number" = "2")
        ) t
        ORDER BY branch, v IS NULL, CAST(v AS STRING)
    """

    order_qt_dynamic_typed_union """
        SELECT branch, CAST(v AS STRING)
        FROM (
            SELECT 0 AS branch, CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "2")
            UNION ALL
            SELECT 1 AS branch, CAST(CAST(number + 2 AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "2")
        ) t
        ORDER BY branch, CAST(v AS STRING)
    """

    order_qt_dynamic_empty_branch_union """
        SELECT branch, CAST(v AS STRING)
        FROM (
            SELECT 0 AS branch, parse_to_variant(CAST(number AS STRING)) AS v
            FROM numbers("number" = "0")
            UNION ALL
            SELECT 1 AS branch, CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "1")
        ) t
        ORDER BY branch, CAST(v AS STRING)
    """

    order_qt_parse_cast_path """
        SELECT number,
               CAST(parse_to_variant(payload) AS STRING),
               CAST(parse_to_variant(payload)['object']['k'] AS INT),
               CAST(parse_to_variant(payload)['missing'] AS STRING)
        FROM (
            SELECT number,
                   CONCAT('{"object":{"k":', number,
                          '},"array":[true,null],"text":"v"}') AS payload
            FROM numbers("number" = "3")
        ) t
        ORDER BY number
    """

    qt_scalar_cast_round_trip """
        SELECT CAST(CAST(TRUE AS VARIANT) AS BOOLEAN),
               CAST(CAST(CAST(-7 AS TINYINT) AS VARIANT) AS TINYINT),
               CAST(CAST(CAST(-300 AS SMALLINT) AS VARIANT) AS SMALLINT),
               CAST(CAST(CAST(-70000 AS INT) AS VARIANT) AS INT),
               CAST(CAST(CAST(-9000000000 AS BIGINT) AS VARIANT) AS BIGINT),
               CAST(CAST(CAST('1234567890123456789012345' AS LARGEINT) AS VARIANT)
                    AS LARGEINT),
               CAST(CAST(CAST(1.25 AS FLOAT) AS VARIANT) AS FLOAT),
               CAST(CAST(CAST(-2.5 AS DOUBLE) AS VARIANT) AS DOUBLE),
               CAST(CAST(CAST(123.450 AS DECIMAL(9, 3)) AS VARIANT) AS DECIMAL(9, 3)),
               CAST(CAST(CAST('2024-02-29' AS DATE) AS VARIANT) AS DATE),
               CAST(CAST(CAST('2024-02-29 12:34:56.123456' AS DATETIMEV2(6)) AS VARIANT)
                    AS DATETIMEV2(6)),
               CAST(CAST(CAST('typed-string' AS VARCHAR(32)) AS VARIANT) AS STRING)
    """

    qt_encoded_scalar_casts_and_failures """
        SELECT CAST(parse_to_variant('true') AS BOOLEAN),
               CAST(parse_to_variant('-7') AS TINYINT),
               CAST(parse_to_variant('123.45') AS DECIMAL(9, 2)),
               CAST(parse_to_variant('"2024-02-29"') AS DATE),
               CAST(parse_to_variant('"2024-02-29 12:34:56.123456"') AS DATETIMEV2(6)),
               CAST(parse_to_variant('"not-an-int"') AS INT) IS NULL,
               CAST(parse_to_variant('{"a":1}') AS INT) IS NULL,
               CAST(parse_to_variant('null') AS INT) IS NULL,
               CAST(parse_to_variant('true') AS INT),
               CAST(parse_to_variant('false') AS INT),
               CAST(parse_to_variant('128') AS TINYINT) IS NULL,
               CAST(parse_to_variant('-129') AS TINYINT) IS NULL,
               CAST(parse_to_variant('9223372036854775808') AS BIGINT) IS NULL,
               CAST(parse_to_variant('123.45') AS DECIMAL(4, 2)) IS NULL,
               CAST(parse_to_variant(CAST(NULL AS STRING)) AS INT) IS NULL
    """

    qt_jsonb_cast_round_trip """
        SELECT CAST(CAST(CAST('{"b":2,"a":[1,null]}' AS JSON) AS VARIANT) AS STRING),
               CAST(CAST(CAST(CAST('{"b":2,"a":[1,null]}' AS JSON) AS VARIANT) AS JSON)
                    AS STRING),
               CAST(CAST(parse_to_variant('null') AS JSON) AS STRING)
    """

    qt_array_cast_round_trip """
        SELECT CAST(CAST(array(1, CAST(NULL AS INT), 3) AS VARIANT) AS ARRAY<INT>),
               CAST(parse_to_variant('[1,"2",null]') AS ARRAY<INT>),
               CAST(parse_to_variant('[1,"not-an-int",3]') AS ARRAY<INT>),
               CAST(CAST(array(array(1, CAST(NULL AS INT)), array(2, 3)) AS VARIANT)
                    AS ARRAY<ARRAY<INT>>),
               CAST(parse_to_variant('null') AS ARRAY<INT>) IS NULL,
               CAST(parse_to_variant('42') AS ARRAY<INT>) IS NULL
    """

    qt_typed_scalar_is_not_a_document """
        SELECT CAST(CAST('{"a":1}' AS VARIANT) AS STRING),
               element_at(CAST('{"a":1}' AS VARIANT), 'a') IS NULL,
               element_at(CAST(CAST(42 AS BIGINT) AS VARIANT), 0) IS NULL
    """

    order_qt_array_element """
        SELECT number,
               CAST(element_at(v, 1) AS STRING),
               CAST(element_at(v, -2) AS STRING),
               CAST(element_at(v, -1) AS STRING),
               element_at(v, 0) IS NULL,
               element_at(v, 4) IS NULL,
               element_at(v, CAST(NULL AS BIGINT)) IS NULL
        FROM (
            SELECT number, parse_to_variant(CONCAT('[', number, ',20,null]')) AS v
            FROM numbers("number" = "3")
        ) t
        ORDER BY number
    """

    qt_variant_selector_semantics """
        SELECT CAST(element_at(parse_to_variant('{"1":"object-key"}'), '1') AS STRING),
               element_at(parse_to_variant('{"1":"object-key"}'), 1) IS NULL
    """

    test {
        sql """
            SELECT variant_type(parse_to_variant(CONCAT('{"k":', number, '}')))
            FROM numbers("number" = "1")
        """
        exception "variant_type does not support ColumnVariantV2 execution"
    }

    setBeConfigTemporary([variant_throw_exeception_on_invalid_json: true]) {
        order_qt_parse_error_to_null """
            SELECT number,
                   CAST(parse_to_variant_error_to_null(payload) AS STRING),
                   parse_to_variant_error_to_null(payload) IS NULL
            FROM (
                SELECT number,
                       CASE number
                           WHEN 0 THEN '{"ok":1}'
                           WHEN 1 THEN 'not-json'
                           ELSE NULL
                       END AS payload
                FROM numbers("number" = "3")
            ) t
            ORDER BY number
        """
    }

    order_qt_direct_result """
        SELECT parse_to_variant(CONCAT('{"id":', number, '}'))
        FROM numbers("number" = "3")
    """

    order_qt_group_by """
        SELECT CAST(v AS STRING), COUNT(*)
        FROM (
            SELECT parse_to_variant(
                       CASE number
                           WHEN 0 THEN '{"a":1,"b":2}'
                           WHEN 1 THEN '{"b":2,"a":1}'
                           WHEN 2 THEN '1'
                           WHEN 3 THEN '1.0'
                           ELSE 'null'
                       END) AS v
            FROM numbers("number" = "6")
        ) t
        GROUP BY v
        ORDER BY CAST(v AS STRING)
    """

    order_qt_distinct """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT DISTINCT v
            FROM (
                SELECT parse_to_variant(
                           CASE number
                               WHEN 0 THEN '{"a":1,"b":2}'
                               WHEN 1 THEN '{"b":2,"a":1}'
                               ELSE '{"a":2}'
                           END) AS v
                FROM numbers("number" = "4")
            ) values_to_deduplicate
        ) distinct_values
        ORDER BY CAST(v AS STRING)
    """

    order_qt_typed_group_by """
        SELECT CAST(v AS STRING), COUNT(*)
        FROM (
            SELECT CAST(CAST(number % 2 AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "4")
        ) t
        GROUP BY v
        ORDER BY CAST(v AS STRING)
    """

    order_qt_encoded_typed_group_by_with_nulls """
        SELECT CAST(v AS STRING), v IS NULL, COUNT(*), COUNT(v)
        FROM (
            SELECT parse_to_variant(
                       CASE number WHEN 0 THEN '1'
                                   WHEN 1 THEN '1.0'
                                   WHEN 2 THEN 'null'
                                   ELSE CAST(NULL AS STRING) END) AS v
            FROM numbers("number" = "4")
            UNION ALL
            SELECT CAST(CASE number WHEN 0 THEN CAST(1 AS BIGINT)
                                    ELSE CAST(NULL AS BIGINT) END AS VARIANT) AS v
            FROM numbers("number" = "2")
        ) t
        GROUP BY v
        ORDER BY v IS NULL, CAST(v AS STRING)
    """

    order_qt_numeric_canonical_group_by """
        SELECT COUNT(*)
        FROM (
            SELECT parse_to_variant('1') AS v
            UNION ALL
            SELECT parse_to_variant('1.0') AS v
            UNION ALL
            SELECT CAST(CAST(1 AS BIGINT) AS VARIANT) AS v
            UNION ALL
            SELECT CAST(CAST(1.00 AS DECIMAL(9, 2)) AS VARIANT) AS v
        ) t
        GROUP BY v
        ORDER BY COUNT(*)
    """

    order_qt_exact_fraction_canonical_groups """
        SELECT GROUP_CONCAT(source_type ORDER BY source_type SEPARATOR ','), COUNT(*)
        FROM (
            SELECT 'double' AS source_type, CAST(CAST(1.5 AS DOUBLE) AS VARIANT) AS v
            UNION ALL
            SELECT 'float' AS source_type, CAST(CAST(1.5 AS FLOAT) AS VARIANT) AS v
            UNION ALL
            SELECT 'decimal' AS source_type,
                   CAST(CAST(1.50 AS DECIMAL(9, 2)) AS VARIANT) AS v
        ) t
        GROUP BY v
        ORDER BY GROUP_CONCAT(source_type ORDER BY source_type SEPARATOR ',')
    """

    order_qt_inexact_fraction_distinct_groups """
        SELECT COUNT(*)
        FROM (
            SELECT CAST(CAST(0.1 AS DOUBLE) AS VARIANT) AS v
            UNION ALL
            SELECT CAST(CAST(0.10 AS DECIMAL(9, 2)) AS VARIANT) AS v
        ) t
        GROUP BY v
        ORDER BY COUNT(*)
    """

    qt_signed_zero_canonical_group_by """
        SELECT COUNT(*)
        FROM (
            SELECT CAST(CAST(0.0 AS DOUBLE) AS VARIANT) AS v
            UNION ALL
            SELECT CAST(CAST(-0.0 AS DOUBLE) AS VARIANT) AS v
            UNION ALL
            SELECT CAST(CAST(0 AS BIGINT) AS VARIANT) AS v
        ) t
        GROUP BY v
    """

    qt_nan_canonical_group_by """
        SELECT COUNT(*)
        FROM (
            SELECT CAST(CAST('NaN' AS DOUBLE) AS VARIANT) AS v
            UNION ALL
            SELECT CAST(CAST('-NaN' AS DOUBLE) AS VARIANT) AS v
        ) t
        GROUP BY v
    """

    order_qt_mixed_shape_group_by """
        SELECT COUNT(*)
        FROM (
            SELECT parse_to_variant(
                       CASE number
                           WHEN 0 THEN 'true'
                           WHEN 1 THEN '"x"'
                           WHEN 2 THEN '[1,null]'
                           WHEN 3 THEN '{"a":1,"b":2}'
                           WHEN 4 THEN '{"b":2,"a":1}'
                           WHEN 5 THEN 'null'
                           ELSE CAST(NULL AS STRING)
                       END) AS v
            FROM numbers("number" = "8")
        ) t
        GROUP BY v
        ORDER BY COUNT(*)
    """

    order_qt_encoded_typed_distinct_with_nulls """
        SELECT CAST(v AS STRING), v IS NULL
        FROM (
            SELECT DISTINCT v
            FROM (
                SELECT parse_to_variant(
                           CASE number WHEN 0 THEN '1'
                                       WHEN 1 THEN '1.0'
                                       WHEN 2 THEN 'null'
                                       ELSE CAST(NULL AS STRING) END) AS v
                FROM numbers("number" = "4")
                UNION ALL
                SELECT CAST(CASE number WHEN 0 THEN CAST(1 AS BIGINT)
                                        ELSE CAST(NULL AS BIGINT) END AS VARIANT) AS v
                FROM numbers("number" = "2")
            ) values_to_deduplicate
        ) distinct_values
        ORDER BY v IS NULL, CAST(v AS STRING)
    """

    order_qt_sql_null_and_json_null_group_by """
        SELECT CAST(v AS STRING), v IS NULL, COUNT(*)
        FROM (
            SELECT parse_to_variant(
                       CASE number WHEN 2 THEN 'null' ELSE CAST(NULL AS STRING) END) AS v
            FROM numbers("number" = "3")
        ) t
        GROUP BY v
        ORDER BY v IS NULL, CAST(v AS STRING)
    """

    order_qt_grouping_sets_retained_and_nullified_variant """
        SELECT GROUPING(v), CAST(v AS STRING), v IS NULL, COUNT(*)
        FROM (
            SELECT parse_to_variant(CAST(number % 2 AS STRING)) AS v
            FROM numbers("number" = "4")
        ) t
        GROUP BY GROUPING SETS ((v), ())
        ORDER BY GROUPING(v), v IS NULL, CAST(v AS STRING)
    """

    qt_nested_variant_count """
        SELECT COUNT(CAST(parse_to_variant(CONCAT('[', number, ']')) AS ARRAY<VARIANT>)),
               COUNT(CASE WHEN number = 0 THEN CAST(NULL AS ARRAY<VARIANT>)
                          ELSE CAST(parse_to_variant(CONCAT('[', number, ']')) AS ARRAY<VARIANT>) END)
        FROM numbers("number" = "3")
    """

    qt_variant_group_concat_coercion """
        SELECT group_concat(parse_to_variant(CAST(number AS STRING))
                            ORDER BY number SEPARATOR ',')
        FROM numbers("number" = "3")
    """

    def conditionalSql = """
        SELECT number,
               CAST(IF(number % 2 = 0,
                       parse_to_variant(CONCAT('{\"if\":', number, '}')),
                       CAST(CAST(number AS BIGINT) AS VARIANT)) AS STRING),
               IF(number = 0, parse_to_variant(CAST(NULL AS STRING)),
                  CAST(CAST(number AS BIGINT) AS VARIANT)) IS NULL,
               CAST(CASE
                        WHEN number = 0 THEN CAST(NULL AS VARIANT)
                        WHEN number = 1 THEN parse_to_variant('null')
                        WHEN number = 2 THEN parse_to_variant('{\"case\":2}')
                        ELSE CAST(CAST(number AS BIGINT) AS VARIANT)
                    END AS STRING),
               CAST(IFNULL(
                        parse_to_variant(CASE number WHEN 0 THEN CAST(NULL AS STRING)
                                                     ELSE CAST(number AS STRING) END),
                        CAST(CAST(99 AS BIGINT) AS VARIANT)) AS STRING),
               CAST(COALESCE(
                        parse_to_variant(CASE number WHEN 0 THEN CAST(NULL AS STRING)
                                                     ELSE CAST(number AS STRING) END),
                        CAST(CAST(number + 10 AS BIGINT) AS VARIANT),
                        parse_to_variant('false')) AS STRING)
        FROM numbers("number" = "4")
        ORDER BY number
    """

    sql "SET short_circuit_evaluation = false"
    order_qt_conditionals_eager conditionalSql

    sql "SET short_circuit_evaluation = true"
    order_qt_conditionals_short_circuit conditionalSql

    order_qt_short_circuit_empty_variant_branches """
        SELECT number,
               CAST(IF(number >= 0,
                       parse_to_variant(CONCAT('{\"kept\":', number, '}')),
                       CAST(CAST(number AS BIGINT) AS VARIANT)) AS STRING),
               CAST(COALESCE(parse_to_variant(CAST(number AS STRING)),
                             CAST(CAST(number AS BIGINT) AS VARIANT)) AS STRING),
               IF(number >= 0, CAST(NULL AS VARIANT),
                  parse_to_variant(CAST(number AS STRING))) IS NULL
        FROM numbers("number" = "2")
        ORDER BY number
    """

    qt_direct_sql_null_variant """
        SELECT CAST(NULL AS VARIANT) IS NULL, CAST(CAST(NULL AS VARIANT) AS STRING)
    """


    qt_empty_and_nonempty_scalar_subqueries """
        SELECT empty_v IS NULL, CAST(empty_v AS STRING),
               json_null_v IS NULL, CAST(json_null_v AS STRING)
        FROM (
            SELECT (SELECT parse_to_variant(CAST(number AS STRING))
                    FROM numbers("number" = "0")) AS empty_v,
                   (SELECT parse_to_variant(
                               CASE number WHEN 0 THEN 'null' END)
                    FROM numbers("number" = "1")) AS json_null_v
        ) t
    """

    order_qt_hash_join """
        SELECT l.number, r.number
        FROM (
            SELECT number, parse_to_variant(CONCAT('{"k":', number % 3, '}')) AS v
            FROM numbers("number" = "6")
        ) l
        JOIN (
            SELECT number, parse_to_variant(CONCAT('{"k":', number, '}')) AS v
            FROM numbers("number" = "3")
        ) r
        ON l.v = r.v
        ORDER BY l.number, r.number
    """

    order_qt_encoded_typed_hash_join """
        SELECT l.number, r.number
        FROM (
            SELECT number, parse_to_variant(CAST(number % 3 AS STRING)) AS v
            FROM numbers("number" = "6")
        ) l
        JOIN (
            SELECT number, CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "3")
        ) r
        ON l.v = r.v
        ORDER BY l.number, r.number
    """

    order_qt_numeric_canonical_hash_join """
        SELECT l.id, r.id
        FROM (
            SELECT 0 AS id, parse_to_variant('1') AS v
            UNION ALL
            SELECT 1 AS id, parse_to_variant('1.0') AS v
        ) l
        JOIN (
            SELECT 2 AS id, CAST(CAST(1 AS BIGINT) AS VARIANT) AS v
            UNION ALL
            SELECT 3 AS id, CAST(CAST(1.00 AS DECIMAL(9, 2)) AS VARIANT) AS v
        ) r
        ON l.v = r.v
        ORDER BY l.id, r.id
    """

    order_qt_typed_hash_join """
        SELECT l.number, r.number
        FROM (
            SELECT number, CAST(CAST(number % 3 AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "6")
        ) l
        JOIN (
            SELECT number, CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "3")
        ) r
        ON l.v = r.v
        ORDER BY l.number, r.number
    """

    qt_sql_and_variant_null_hash_join """
        SELECT COUNT(*)
        FROM (
            SELECT 0 AS id, parse_to_variant(CAST(NULL AS STRING)) AS v
            UNION ALL
            SELECT 1 AS id, parse_to_variant('null') AS v
        ) l
        JOIN (
            SELECT 2 AS id, parse_to_variant(CAST(NULL AS STRING)) AS v
            UNION ALL
            SELECT 3 AS id, parse_to_variant('null') AS v
        ) r
        ON l.v = r.v
    """

    order_qt_empty_build_left_outer_join """
        SELECT l.number, r.v IS NULL, CAST(r.v AS STRING)
        FROM (
            SELECT number, parse_to_variant(CONCAT('{"k":', number, '}')) AS v
            FROM numbers("number" = "2")
        ) l
        LEFT OUTER JOIN (
            SELECT number, parse_to_variant(CONCAT('{"k":', number, '}')) AS v
            FROM numbers("number" = "0")
        ) r
        ON l.v = r.v
        ORDER BY l.number
    """

    qt_null_safe_hash_join """
        SELECT l.id, r.id
        FROM (
            SELECT 1 AS id, parse_to_variant(CAST(NULL AS STRING)) AS v
        ) l
        JOIN (
            SELECT 2 AS id, CAST(NULL AS VARIANT) AS v
        ) r
        ON l.v <=> r.v
    """

    qt_non_null_encoded_typed_null_safe_hash_join """
        SELECT l.id, r.id
        FROM (
            SELECT 1 AS id, parse_to_variant(CAST(number + 1 AS STRING)) AS v
            FROM numbers("number" = "1")
        ) l
        JOIN (
            SELECT 2 AS id, CAST(CAST(number + 1 AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "1")
        ) r
        ON l.v <=> r.v
    """

    qt_non_null_typed_null_safe_hash_join """
        SELECT l.id, r.id
        FROM (
            SELECT 1 AS id, CAST(CAST(number + 1 AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "1")
        ) l
        JOIN (
            SELECT 2 AS id, CAST(CAST(number + 1 AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "1")
        ) r
        ON l.v <=> r.v
    """

    order_qt_intersect """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant(CONCAT('{"k":', number % 4, '}')) AS v
            FROM numbers("number" = "6")
            INTERSECT
            SELECT parse_to_variant(CONCAT('{"k":', number, '}')) AS v
            FROM numbers("number" = "3")
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_except """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant(CONCAT('{"k":', number, '}')) AS v
            FROM numbers("number" = "4")
            EXCEPT
            SELECT parse_to_variant(CONCAT('{"k":', number, '}')) AS v
            FROM numbers("number" = "2")
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_encoded_typed_intersect """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant(CAST(number AS STRING)) AS v
            FROM numbers("number" = "3")
            INTERSECT
            SELECT CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "2")
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_typed_intersect """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "3")
            INTERSECT
            SELECT CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "2")
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_typed_except """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "4")
            EXCEPT
            SELECT CAST(CAST(number AS BIGINT) AS VARIANT) AS v
            FROM numbers("number" = "2")
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_explode """
        SELECT id, CAST(v AS STRING), CAST(element AS STRING)
        FROM (
            SELECT number AS id,
                   parse_to_variant(
                       CASE number
                           WHEN 0 THEN '[1,{"a":2},null]'
                           WHEN 1 THEN '[]'
                           WHEN 2 THEN '[true,"x"]'
                           WHEN 3 THEN 'null'
                           ELSE NULL
                       END) AS v
            FROM numbers("number" = "5")
        ) t
        LATERAL VIEW explode_variant_array(v) exploded AS element
        ORDER BY id, CAST(element AS STRING)
    """

    order_qt_explode_variant_array_corner_cases """
        SELECT id,
               element IS NULL,
               CAST(element AS STRING),
               CAST(element['k'] AS STRING),
               CAST(element[1] AS STRING)
        FROM (
            SELECT number AS id,
                   parse_to_variant(
                       CASE number
                           WHEN 0 THEN '[1,1.0,true,"x",null,{"k":2},[3,null]]'
                           WHEN 1 THEN '[]'
                           WHEN 2 THEN 'null'
                           WHEN 3 THEN '42'
                           WHEN 4 THEN '{"k":"not-an-array"}'
                           ELSE CAST(NULL AS STRING)
                       END) AS v
            FROM numbers("number" = "6")
        ) t
        LATERAL VIEW explode_variant_array(v) exploded AS element
        ORDER BY id, element IS NULL, CAST(element AS STRING)
    """

    order_qt_explode_variant_array_grouped_output """
        SELECT CAST(element AS STRING), element IS NULL, COUNT(*)
        FROM (
            SELECT parse_to_variant(
                       CASE number
                           WHEN 0 THEN '[1,null,{"a":1}]'
                           WHEN 1 THEN '[1.0,null,{"a":1}]'
                           ELSE '[]'
                       END) AS v
            FROM numbers("number" = "3")
        ) t
        LATERAL VIEW explode_variant_array(v) exploded AS element
        GROUP BY element
        ORDER BY element IS NULL, CAST(element AS STRING)
    """

    order_qt_multi_explode """
        SELECT id, CAST(left_element AS STRING), CAST(right_element AS STRING)
        FROM (
            SELECT number AS id,
                   parse_to_variant(
                       CASE number WHEN 0 THEN '[1,2]' WHEN 1 THEN '[]' ELSE NULL END)
                       AS left_array,
                   parse_to_variant(
                       CASE number WHEN 0 THEN '["a"]' WHEN 1 THEN '[true,false]' ELSE '[9]' END)
                       AS right_array
            FROM numbers("number" = "3")
        ) t
        LATERAL VIEW explode_variant_array(left_array, right_array) exploded
            AS left_element, right_element
        ORDER BY id, CAST(left_element AS STRING), CAST(right_element AS STRING)
    """

    order_qt_array_variant_payload_through_sort """
        SELECT id, values_to_sort
        FROM (
            SELECT number AS id,
                   CAST(parse_to_variant(
                            CASE number WHEN 0 THEN '[1,null]'
                                        WHEN 1 THEN '["x"]'
                                        ELSE CAST(NULL AS STRING) END)
                        AS ARRAY<VARIANT>) AS values_to_sort
            FROM numbers("number" = "3")
        ) t
        ORDER BY id
    """

    order_qt_array_variant_payload_through_topn """
        SELECT id, values_to_sort
        FROM (
            SELECT number AS id,
                   CAST(parse_to_variant(
                            CASE number WHEN 0 THEN '[1,null]'
                                        WHEN 1 THEN '["x"]'
                                        ELSE CAST(NULL AS STRING) END)
                        AS ARRAY<VARIANT>) AS values_to_sort
            FROM numbers("number" = "3")
        ) t
        ORDER BY id
        LIMIT 2
    """

    order_qt_nested_variant_container_constructors """
        SELECT id,
               array(values_to_nest),
               struct(values_to_nest),
               map('k', values_to_nest)
        FROM (
            SELECT number AS id,
                   CAST(parse_to_variant(
                            CASE number WHEN 0 THEN '[1,null]'
                                        WHEN 1 THEN '["x"]'
                                        ELSE CAST(NULL AS STRING) END)
                        AS ARRAY<VARIANT>) AS values_to_nest
            FROM numbers("number" = "3")
        ) t
        ORDER BY id
    """

    order_qt_explode_array_of_variant """
        SELECT id, element IS NULL, CAST(element AS STRING)
        FROM (
            SELECT number AS id,
                   CAST(parse_to_variant(
                            CASE number WHEN 0 THEN '[1,null]'
                                        WHEN 1 THEN '[]'
                                        ELSE CAST(NULL AS STRING) END)
                        AS ARRAY<VARIANT>) AS values_to_explode
            FROM numbers("number" = "3")
        ) t
        LATERAL VIEW explode(values_to_explode) exploded AS element
        ORDER BY id, element IS NULL, CAST(element AS STRING)
    """

    order_qt_explode_outer_array_of_variant """
        SELECT id, element IS NULL, CAST(element AS STRING)
        FROM (
            SELECT number AS id,
                   CAST(parse_to_variant(
                            CASE number WHEN 0 THEN '[1,null]'
                                        WHEN 1 THEN '[]'
                                        ELSE CAST(NULL AS STRING) END)
                        AS ARRAY<VARIANT>) AS values_to_explode
            FROM numbers("number" = "3")
        ) t
        LATERAL VIEW explode_outer(values_to_explode) exploded AS element
        ORDER BY id, element IS NULL, CAST(element AS STRING)
    """

    order_qt_explode_outer_sql_null """
        SELECT id, v IS NULL, CAST(v AS STRING), element IS NULL, CAST(element AS STRING)
        FROM (
            SELECT 0 AS id, parse_to_variant(CAST(NULL AS STRING)) AS v
            UNION ALL
            SELECT 1 AS id, parse_to_variant('null') AS v
            UNION ALL
            SELECT 2 AS id, parse_to_variant('[]') AS v
            UNION ALL
            SELECT 3 AS id, parse_to_variant('[null,1]') AS v
        ) t
        LATERAL VIEW explode_outer(v) exploded AS element
        ORDER BY id, element IS NULL, CAST(element AS STRING)
    """
    }
}
