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

suite("test_short_circuit_project", "p0") {
    // ---------- common table setup ----------
    def tableName = "cir_19608_short_circuit_project"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `uuid` BIGINT NULL,
            `interstitial_ad_watch_cnt_lt` BIGINT NULL,
            `splash_ad_watch_cnt_lt` BIGINT NULL,
            `splash_ad_revenue_lt` DOUBLE NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`uuid`)
        DISTRIBUTED BY HASH(`uuid`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "store_row_column" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (10001, 3, 4, 5.5)
    """

    // ---------- original test: IFNULL + ADD ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                IFNULL(interstitial_ad_watch_cnt_lt, 0) + IFNULL(splash_ad_watch_cnt_lt, 0),
                IFNULL(splash_ad_revenue_lt, 0) + IFNULL(splash_ad_watch_cnt_lt, 0)
            FROM ${tableName}
            WHERE uuid = 10001
        """
        contains "SHORT-CIRCUIT"
    }

    qt_sql """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            IFNULL(interstitial_ad_watch_cnt_lt, 0) + IFNULL(splash_ad_watch_cnt_lt, 0),
            IFNULL(splash_ad_revenue_lt, 0) + IFNULL(splash_ad_watch_cnt_lt, 0)
        FROM ${tableName}
        WHERE uuid = 10001
    """

    // ---------- extended table with richer types ----------
    def extTable = "short_circuit_project_ext"
    sql "DROP TABLE IF EXISTS ${extTable}"
    sql """
        CREATE TABLE ${extTable} (
            `id` BIGINT NOT NULL,
            `col_int` INT NULL,
            `col_bigint` BIGINT NULL,
            `col_double` DOUBLE NULL,
            `col_decimal` DECIMALV3(18, 4) NULL,
            `col_str` VARCHAR(200) NULL,
            `col_date` DATE NULL,
            `col_datetime` DATETIME NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "store_row_column" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO ${extTable} VALUES
        (1, 10, 200, 3.14, 99.9900, 'hello world', '2026-03-10', '2026-03-10 12:30:00'),
        (2, -5, 0, 0.0, 0.0000, '', '2000-01-01', '2000-01-01 00:00:00'),
        (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """

    // ---------- 1. Arithmetic operators: +, -, *, /, % ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                (col_int + col_bigint) * 2 - col_int,
                col_bigint % 7,
                col_double / 2.0 + col_int
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_arith """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            (col_int + col_bigint) * 2 - col_int,
            col_bigint % 7,
            col_double / 2.0 + col_int
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 2. CAST / type coercion ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                CAST(col_int AS VARCHAR(20)),
                CAST(col_str AS VARCHAR(5)),
                CAST(col_double AS BIGINT),
                CAST(col_int AS DOUBLE) + 0.5
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_cast """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            CAST(col_int AS VARCHAR(20)),
            CAST(col_str AS VARCHAR(5)),
            CAST(col_double AS BIGINT),
            CAST(col_int AS DOUBLE) + 0.5
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 3. CASE WHEN ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                CASE WHEN col_int > 5 THEN 'big' WHEN col_int > 0 THEN 'small' ELSE 'zero_or_neg' END,
                CASE WHEN col_double > 3.0 THEN col_double * 2 ELSE col_double END
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_case_when """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            CASE WHEN col_int > 5 THEN 'big' WHEN col_int > 0 THEN 'small' ELSE 'zero_or_neg' END,
            CASE WHEN col_double > 3.0 THEN col_double * 2 ELSE col_double END
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 4. String functions ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                CONCAT(UPPER(col_str), '!'),
                LENGTH(col_str),
                SUBSTR(col_str, 1, 5)
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_string_fn """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            CONCAT(UPPER(col_str), '!'),
            LENGTH(col_str),
            SUBSTR(col_str, 1, 5)
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 5. Math functions ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                ABS(col_int - 20),
                CEIL(col_double),
                FLOOR(col_double),
                ROUND(col_double, 1),
                POWER(col_int, 2)
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_math_fn """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            ABS(col_int - 20),
            CEIL(col_double),
            FLOOR(col_double),
            ROUND(col_double, 1),
            POWER(col_int, 2)
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 6. NULL handling: COALESCE, NULLIF ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                COALESCE(col_int, col_bigint, -1),
                NULLIF(col_int, 10),
                IFNULL(col_str, 'N/A'),
                COALESCE(NULL, col_double, 0)
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_null_fn """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            COALESCE(col_int, col_bigint, -1),
            NULLIF(col_int, 10),
            IFNULL(col_str, 'N/A'),
            COALESCE(NULL, col_double, 0)
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 7. IF / comparison: IF, GREATEST, LEAST ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                IF(col_int > col_bigint, col_int, col_bigint),
                GREATEST(col_int, col_bigint, 100),
                LEAST(col_double, CAST(col_int AS DOUBLE), 1.0)
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_if_cmp """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            IF(col_int > col_bigint, col_int, col_bigint),
            GREATEST(col_int, col_bigint, 100),
            LEAST(col_double, CAST(col_int AS DOUBLE), 1.0)
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 8. Deeply nested multi-operator expressions ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                CAST(IF(col_int > 0,
                    CONCAT(CAST(col_int + col_bigint AS VARCHAR(50)), '-pos'),
                    CONCAT(CAST(ABS(col_int) AS VARCHAR(50)), '-neg')) AS VARCHAR(100)),
                ROUND(COALESCE(col_double, 0) * IFNULL(col_decimal, 1.0), 2)
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_nested """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            CAST(IF(col_int > 0,
                CONCAT(CAST(col_int + col_bigint AS VARCHAR(50)), '-pos'),
                CONCAT(CAST(ABS(col_int) AS VARCHAR(50)), '-neg')) AS VARCHAR(100)),
            ROUND(COALESCE(col_double, 0) * IFNULL(col_decimal, 1.0), 2)
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 9. Simple projection (no intermediate layer) ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                col_int, col_str
            FROM ${extTable} WHERE id = 1
        """
        contains "SHORT-CIRCUIT"
    }

    qt_simple """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            col_int, col_str
        FROM ${extTable} WHERE id = 1
    """

    // ---------- 10. NULL row with complex expressions ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                IFNULL(col_int, 0) + IFNULL(col_bigint, 0),
                COALESCE(col_str, 'empty'),
                CASE WHEN col_int IS NULL THEN 'null_int' ELSE 'has_int' END,
                CAST(IFNULL(col_double, -1) AS BIGINT)
            FROM ${extTable} WHERE id = 3
        """
        contains "SHORT-CIRCUIT"
    }

    qt_null_row """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            IFNULL(col_int, 0) + IFNULL(col_bigint, 0),
            COALESCE(col_str, 'empty'),
            CASE WHEN col_int IS NULL THEN 'null_int' ELSE 'has_int' END,
            CAST(IFNULL(col_double, -1) AS BIGINT)
        FROM ${extTable} WHERE id = 3
    """

    // ---------- 11. Negative values with arithmetic ----------
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                ABS(col_int) + ABS(col_bigint),
                col_int * col_int,
                IFNULL(col_double, 0) + IFNULL(CAST(col_int AS DOUBLE), 0)
            FROM ${extTable} WHERE id = 2
        """
        contains "SHORT-CIRCUIT"
    }

    qt_neg_arith """
        SELECT /*+ SET_VAR(enable_nereids_planner=true) */
            ABS(col_int) + ABS(col_bigint),
            col_int * col_int,
            IFNULL(col_double, 0) + IFNULL(CAST(col_int AS DOUBLE), 0)
        FROM ${extTable} WHERE id = 2
    """
}
