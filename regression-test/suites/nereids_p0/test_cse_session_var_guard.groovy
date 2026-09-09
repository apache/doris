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
        (1, 'abc'), (2, ''), (3, NULL), (4, 'xyz'), (5, 'abc')"""
    sql "DROP FUNCTION IF EXISTS cse_guard_replace_null(VARCHAR(65533))"
    sql "SET enable_decimal256 = false"
    sql """CREATE ALIAS FUNCTION cse_guard_replace_null(VARCHAR(65533))
        WITH PARAMETER(foo) AS IF(foo = '', NULL, foo)"""

    // Matching settings provide the baseline; different settings introduce session guards.
    sql "SET enable_decimal256 = false"
    sql """EXPLAIN SELECT coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z FROM cse_guard_demo"""
    qt_guard_result_false """SELECT id, coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z
        FROM cse_guard_demo ORDER BY id"""
    qt_guard_first_false """SELECT id, cse_guard_replace_null(coalesce(platform, '')) AS z,
        coalesce(platform, '') AS x, coalesce(platform, '') AS y
        FROM cse_guard_demo ORDER BY id"""
    qt_guard_repeated_false """SELECT id,
        cse_guard_replace_null(coalesce(platform, '')) AS z1,
        cse_guard_replace_null(coalesce(platform, '')) AS z2,
        coalesce(platform, '') AS x, coalesce(platform, '') AS y
        FROM cse_guard_demo ORDER BY id"""

    sql "SET enable_decimal256 = true"
    sql """EXPLAIN SELECT coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z FROM cse_guard_demo"""
    qt_guard_result_true """SELECT id, coalesce(platform, '') AS x, coalesce(platform, '') AS y,
        cse_guard_replace_null(coalesce(platform, '')) AS z
        FROM cse_guard_demo ORDER BY id"""
    qt_guard_first_true """SELECT id, cse_guard_replace_null(coalesce(platform, '')) AS z,
        coalesce(platform, '') AS x, coalesce(platform, '') AS y
        FROM cse_guard_demo ORDER BY id"""
    qt_guard_repeated_true """SELECT id,
        cse_guard_replace_null(coalesce(platform, '')) AS z1,
        cse_guard_replace_null(coalesce(platform, '')) AS z2,
        coalesce(platform, '') AS x, coalesce(platform, '') AS y
        FROM cse_guard_demo ORDER BY id"""
}
