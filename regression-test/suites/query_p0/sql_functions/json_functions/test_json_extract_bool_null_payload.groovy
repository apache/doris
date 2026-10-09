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

suite("test_json_extract_bool_null_payload") {
    sql "DROP TABLE IF EXISTS test_json_extract_bool_null_payload"
    sql """
        CREATE TABLE test_json_extract_bool_null_payload (
            id BIGINT,
            j STRING,
            a BIGINT,
            b BIGINT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_json_extract_bool_null_payload
        SELECT number,
               CASE WHEN number % 100 = 0
                    THEN '{"flag": true}'
                    ELSE CONCAT('{"n": ', number, '}')
               END,
               number + CASE number % 3
                            WHEN 0 THEN 10
                            WHEN 1 THEN -100
                            ELSE -200000
                        END,
               number
        FROM numbers("number" = "100000")
    """

    sql "SET enable_sql_cache = false"
    sql "SET short_circuit_evaluation = false"

    order_qt_or_payload_no_short_circuit """
        SELECT v, count(*) FROM (
            SELECT JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR (a > b) AS v
            FROM test_json_extract_bool_null_payload
        ) t
        GROUP BY v
        ORDER BY v
    """

    order_qt_and_or_payload_no_short_circuit """
        SELECT v, count(*) FROM (
            SELECT (JSON_PARSE_ERROR_TO_NULL(j) IS NOT NULL
                    AND JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag'))
                   OR (a > b) AS v
            FROM test_json_extract_bool_null_payload
        ) t
        GROUP BY v
        ORDER BY v
    """

    qt_case_count_no_short_circuit """
        SELECT count(*)
        FROM test_json_extract_bool_null_payload
        WHERE (CASE
                 WHEN JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR a > b THEN b
                 WHEN a < b THEN a
               END) IS NOT NULL
    """

    order_qt_case_branches_no_short_circuit """
        SELECT CASE
                 WHEN JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR a > b
                   THEN 'then_b'
                 WHEN a < b THEN 'then_a'
                 ELSE 'else'
               END AS branch,
               count(*)
        FROM test_json_extract_bool_null_payload
        GROUP BY branch
        ORDER BY branch
    """

    sql "SET short_circuit_evaluation = true"

    order_qt_or_payload_short_circuit """
        SELECT v, count(*) FROM (
            SELECT JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR (a > b) AS v
            FROM test_json_extract_bool_null_payload
        ) t
        GROUP BY v
        ORDER BY v
    """

    order_qt_and_or_payload_short_circuit """
        SELECT v, count(*) FROM (
            SELECT (JSON_PARSE_ERROR_TO_NULL(j) IS NOT NULL
                    AND JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag'))
                   OR (a > b) AS v
            FROM test_json_extract_bool_null_payload
        ) t
        GROUP BY v
        ORDER BY v
    """

    qt_case_count_short_circuit """
        SELECT count(*)
        FROM test_json_extract_bool_null_payload
        WHERE (CASE
                 WHEN JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR a > b THEN b
                 WHEN a < b THEN a
               END) IS NOT NULL
    """

    order_qt_case_branches_short_circuit """
        SELECT CASE
                 WHEN JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR a > b
                   THEN 'then_b'
                 WHEN a < b THEN 'then_a'
                 ELSE 'else'
               END AS branch,
               count(*)
        FROM test_json_extract_bool_null_payload
        GROUP BY branch
        ORDER BY branch
    """

    order_qt_nullable_or_truth_table """
        SELECT lhs.label, rhs.label, lhs.v OR rhs.v AS result
        FROM (
            SELECT 'false' AS label, CAST(false AS BOOLEAN) AS v
            UNION ALL
            SELECT 'null', CAST(NULL AS BOOLEAN)
            UNION ALL
            SELECT 'true', CAST(true AS BOOLEAN)
        ) lhs
        CROSS JOIN (
            SELECT 'false' AS label, CAST(false AS BOOLEAN) AS v
            UNION ALL
            SELECT 'null', CAST(NULL AS BOOLEAN)
            UNION ALL
            SELECT 'true', CAST(true AS BOOLEAN)
        ) rhs
        ORDER BY lhs.label, rhs.label
    """
}
