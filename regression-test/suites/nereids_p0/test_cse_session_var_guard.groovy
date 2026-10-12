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

suite("test_cse_session_var_guard") {
    sql "DROP TABLE IF EXISTS cse_guard_demo"
    sql """CREATE TABLE cse_guard_demo (id INT, platform VARCHAR(65533))
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO cse_guard_demo VALUES
        (1, 'abc'), (2, ''), (3, NULL), (4, 'xyz'), (5, 'abc'), (6, ' '), (7, '中文')"""
    sql "DROP FUNCTION IF EXISTS cse_guard_replace_null(VARCHAR(65533))"
    sql "SET enable_decimal256 = false"
    sql """CREATE ALIAS FUNCTION cse_guard_replace_null(VARCHAR(65533))
        WITH PARAMETER(foo) AS IF(foo = '', NULL, foo)"""

    // Evaluate the original projection without guards as a result baseline.
    qt_without_guard """SELECT id, coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z
        FROM cse_guard_demo ORDER BY id"""

    // The function was created with decimal256 disabled. Changing it adds guards
    // to the expanded IF and COALESCE expressions, even for VARCHAR arguments.
    sql "SET enable_decimal256 = true"
    sql """EXPLAIN SELECT coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z FROM cse_guard_demo"""
    qt_with_guard """SELECT id, coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z
        FROM cse_guard_demo ORDER BY id"""

    // Reverse discovery order: guarded and unguarded roots still cannot depend
    // on aliases produced in their own projection layer.
    qt_guard_first """SELECT id, cse_guard_replace_null(coalesce(platform, '')) AS z,
        coalesce(platform, '') AS x, coalesce(platform, '') AS y
        FROM cse_guard_demo ORDER BY id"""

    // Reusing an entire guarded expression must remain supported.
    qt_repeated_guard """SELECT id,
        cse_guard_replace_null(coalesce(platform, '')) AS z1,
        cse_guard_replace_null(coalesce(platform, '')) AS z2,
        coalesce(platform, '') AS x, coalesce(platform, '') AS y
        FROM cse_guard_demo ORDER BY id"""

    // Common expressions below the guarded root may still be extracted into
    // earlier layers without separating a guarded root from its guard.
    qt_nested_guard """SELECT id,
        coalesce(nullif(platform, 'abc'), 'fallback') AS x,
        coalesce(nullif(platform, 'abc'), 'fallback') AS y,
        cse_guard_replace_null(coalesce(nullif(platform, 'abc'), 'fallback')) AS z
        FROM cse_guard_demo ORDER BY id"""

    // Ordinary CSE must retain reuse across multiple projection layers.
    qt_multiple_layers """SELECT id, id + 1 AS x,
        (id + 1) * 2 AS y, (id + 1) * 2 + (id + 1) AS z
        FROM cse_guard_demo ORDER BY id"""

    // Also cover a function created with decimal256 enabled and queried with it disabled.
    sql "DROP FUNCTION IF EXISTS cse_guard_replace_null(VARCHAR(65533))"
    sql """CREATE ALIAS FUNCTION cse_guard_replace_null(VARCHAR(65533))
        WITH PARAMETER(foo) AS IF(foo = '', NULL, foo)"""
    sql "SET enable_decimal256 = false"
    qt_reverse_session_guard """SELECT id, coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z
        FROM cse_guard_demo ORDER BY id"""
}
