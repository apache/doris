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

// JSON_EXTRACT_BOOL(j, path) is CAST(JSON_EXTRACT(j, path) AS BOOLEAN). Rows whose key is
// missing cast to NULL, and the BE used to leave the nested boolean byte of such rows
// uninitialized. OR-ing that column with a TRUE condition produced a non-NULL boolean whose
// payload was a stale byte (3, 5, 65, ...) instead of 1, and a multi-branch CASE driven by
// that condition turned the byte into an out-of-range branch index and crashed the BE.
suite("test_json_extract_bool_null_payload") {
    def tbl = "test_json_extract_bool_null_payload"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
            id BIGINT,
            j  STRING,
            a  BIGINT,
            b  BIGINT
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // every row is valid JSON, only 1% of them carry the key; a > b holds for a third of the rows
    sql """
        INSERT INTO ${tbl}
        SELECT number,
               CASE WHEN number % 100 = 0 THEN '{"flag": true}' ELSE CONCAT('{"n": ', number, '}') END,
               number + CASE number % 3 WHEN 0 THEN 10 WHEN 1 THEN -100 ELSE -200000 END,
               number
        FROM numbers("number" = "100000")
    """

    // The OR may only ever produce NULL or 1 here: TRUE whenever a > b, NULL otherwise.
    qt_or_payload """
        SELECT v, count(*) FROM (
            SELECT JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR (a > b) AS v
            FROM ${tbl}
        ) t GROUP BY v ORDER BY v
    """
    qt_and_or_payload """
        SELECT v, count(*) FROM (
            SELECT (JSON_PARSE_ERROR_TO_NULL(j) IS NOT NULL
                    AND JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag')) OR (a > b) AS v
            FROM ${tbl}
        ) t GROUP BY v ORDER BY v
    """

    // A CASE with more than one non-literal branch keeps the CASE expression on the BE side.
    // Before the fix this query crashed the BE.
    qt_case_count """
        SELECT count(*)
        FROM ${tbl}
        WHERE (CASE
                 WHEN JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR a > b THEN b
                 WHEN a < b THEN a
               END) IS NOT NULL
    """
    qt_case_branches """
        SELECT CASE
                 WHEN JSON_EXTRACT_BOOL(JSON_PARSE_ERROR_TO_NULL(j), '\$.flag') OR a > b THEN 'then_b'
                 WHEN a < b THEN 'then_a'
                 ELSE 'else'
               END AS branch, count(*)
        FROM ${tbl}
        GROUP BY branch ORDER BY branch
    """

    sql "DROP TABLE IF EXISTS ${tbl}"
}
