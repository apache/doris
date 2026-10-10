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

suite("test_try_cast_decimal_overflow") {
    sql """DROP TABLE IF EXISTS test_try_cast_decimal_overflow"""
    sql """
        CREATE TABLE test_try_cast_decimal_overflow (
            id INT NOT NULL,
            d DECIMAL(6,3) NOT NULL,
            nullable_d DECIMAL(6,3),
            d64 DECIMAL(12,3) NOT NULL,
            d128 DECIMAL(30,3) NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_try_cast_decimal_overflow VALUES
            (1, 12.340, 12.340, 12.340, 12.340),
            (2, 123.456, 123.456, 123.456, 123.456),
            (3, -123.456, -123.456, -123.456, -123.456),
            (4, 99.994, 99.994, 99.994, 99.994),
            (5, 99.995, 99.995, 99.995, 99.995),
            (6, -99.995, -99.995, -99.995, -99.995),
            (7, 0, NULL, 0, 0)
    """

    sql "SET enable_sql_cache=false"
    sql "SET debug_skip_fold_constant=true"

    def queries = [
        valid: """
            SELECT id, d, TRY_CAST(d AS DECIMAL(4,2))
            FROM test_try_cast_decimal_overflow WHERE id = 1 ORDER BY id
        """,
        overflow: """
            SELECT id, d, TRY_CAST(d AS DECIMAL(4,2))
            FROM test_try_cast_decimal_overflow WHERE id = 2 ORDER BY id
        """,
        batch: """
            SELECT id, d, TRY_CAST(d AS DECIMAL(4,2)),
                   TRY_CAST(nullable_d AS DECIMAL(4,2)),
                   TRY_CAST(d64 AS DECIMAL(4,2)), TRY_CAST(d128 AS DECIMAL(4,2)),
                   TRY_CAST(d AS DECIMAL(4,3)), TRY_CAST(d AS DECIMAL(6,4))
            FROM test_try_cast_decimal_overflow ORDER BY id
        """,
        "const": """
            SELECT TRY_CAST(CAST(12.340 AS DECIMAL(6,3)) AS DECIMAL(4,2)),
                   TRY_CAST(CAST(123.456 AS DECIMAL(6,3)) AS DECIMAL(4,2))
        """
    ]

    // Check the non-strict baseline against the recorded results first.
    sql "SET enable_strict_cast=false"
    queries.each { tag, query -> quickTest(tag, query) }

    sql "SET enable_strict_cast=true"
    qt_valid queries.valid
    explain {
        verbose true
        sql queries.overflow
        contains "TRY_CAST"
    }
    // TRY_CAST must preserve the non-strict results even when strict CAST would fail.
    queries.values().each { query ->
        check_sqls_result_equal(query,
                query.replaceFirst("SELECT", "SELECT /*+ SET_VAR(enable_strict_cast=false) */"))
    }
    check_sqls_result_equal("""
        SELECT id, d, TRY_CAST(d AS DECIMAL(4,2))
        FROM test_try_cast_decimal_overflow WHERE id <= 2 ORDER BY id
    """, """
        SELECT /*+ SET_VAR(enable_strict_cast=false) */ id, d, TRY_CAST(d AS DECIMAL(4,2))
        FROM test_try_cast_decimal_overflow WHERE id <= 2 ORDER BY id
    """)

    test {
        sql """
            SELECT CAST(d AS DECIMAL(4,2))
            FROM test_try_cast_decimal_overflow WHERE id = 2
        """
        exception "Arithmetic overflow when converting value 123.456"
    }
    // TRY_CAST must not suppress errors raised while evaluating its child.
    test {
        sql """
            SELECT TRY_CAST(CAST(d AS DECIMAL(4,2)) AS DECIMAL(6,3))
            FROM test_try_cast_decimal_overflow WHERE id = 2
        """
        exception "Arithmetic overflow when converting value 123.456"
    }

    sql "SET enable_strict_cast=false"
    check_sqls_result_equal(queries.batch,
            queries.batch.replaceFirst("SELECT", "SELECT /*+ SET_VAR(enable_strict_cast=true) */"))
}
