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

suite("condition_cache_eligibility") {
    sql "DROP TABLE IF EXISTS condition_cache_eligibility"
    sql """
        CREATE TABLE condition_cache_eligibility (
            k BIGINT,
            a ARRAY<INT>,
            dt1 DATETIME,
            dt2 DATETIME
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO condition_cache_eligibility
        SELECT number, [1,2,3,4,5,6,7], '2026-09-30 12:00:00', '2026-09-30 12:00:00'
        FROM numbers("number" = "10000")
    """

    sql "set enable_parallel_scan = false"
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
    sql "set enable_condition_cache = true"
    // CI fuzzes expression pushdown; these queries must exercise the storage predicates.
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_profile = false"

    sql "set experimental_enable_virtual_slot_for_cse = true"
    def virtualFirst = """
        SELECT k FROM condition_cache_eligibility
        WHERE abs(k + 1) + abs(k + 1) = 0 ORDER BY k
    """
    def virtualSecond = """
        SELECT k FROM condition_cache_eligibility
        WHERE abs(k - 9999) + abs(k - 9999) = 0 ORDER BY k
    """
    for (String query : [virtualFirst, virtualSecond]) {
        explain {
            sql query
            verbose true
            contains "virtualColumn=abs"
        }
    }
    // Compare with uncached base-column predicates to verify virtual-column results.
    def emptyReference = """
        SELECT /*+ SET_VAR(enable_condition_cache=false) */ k
        FROM condition_cache_eligibility WHERE k = -1 ORDER BY k
    """
    def matchingReference = """
        SELECT /*+ SET_VAR(enable_condition_cache=false) */ k
        FROM condition_cache_eligibility WHERE k = 9999 ORDER BY k
    """
    check_sqls_result_equal(virtualFirst, emptyReference)
    2.times {
        check_sqls_result_equal(virtualSecond, matchingReference)
    }

    // Seeded shuffle is repeatable within a block, but its count changes with block boundaries.
    // Cache eligibility and lookup accounting are covered by FE/BE unit tests. Compare seeded
    // expressions with an uncached execution rather than collecting profiles or random counts.
    for (int batchSize : [1, 1024]) {
        sql "set batch_size = ${batchSize}"
        for (String predicate : [
                "k < 0 OR rand(1) < 0.0001",
                "array_shuffle(a, 1) = [1,2,3,4,5,6,7]",
                "shuffle(a, 1) = [1,2,3,4,5,6,7]",
                "CAST(TIMEDIFF(dt1, dt2) AS DATE) = CURRENT_DATE()",
                "CAST(TIMEDIFF(dt1, dt2) AS DATETIME) = CAST(CURRENT_DATE() AS DATETIME)",
                "CAST(ARRAY(TIMEDIFF(dt1, dt2)) AS ARRAY<DATETIME>)[1] = CAST(CURRENT_DATE() AS DATETIME)",
                "CAST(MAP(1, TIMEDIFF(dt1, dt2)) AS MAP<INT, DATETIME>)[1] = CAST(CURRENT_DATE() AS DATETIME)",
                "STRUCT_ELEMENT(CAST(STRUCT(k, TIMEDIFF(dt1, dt2)) AS STRUCT<id:BIGINT, d:DATETIME>), 2)"
                        + " = CAST(CURRENT_DATE() AS DATETIME)"]) {
            2.times {
                // Keep one-row batches inexpensive under ASAN while retaining the volatile scan predicate.
                check_sqls_result_equal(
                        "SELECT count(*) FROM condition_cache_eligibility WHERE k < 1000 AND (${predicate})",
                        "SELECT /*+ SET_VAR(enable_condition_cache=false) */ count(*) "
                                + "FROM condition_cache_eligibility WHERE k < 1000 AND (${predicate})")
            }
        }
        // Unseeded values cannot be compared between executions. Check stable properties instead.
        quickRunTest("unseeded_invariants_${batchSize}", """
            SELECT count(*) FROM condition_cache_eligibility
            WHERE k < 1000 AND rand() >= 0 AND rand() < 1
                AND array_sort(array_shuffle(a)) = a
        """, true)
    }

    sql "DROP TABLE IF EXISTS condition_cache_rf_probe"
    sql """
        CREATE TABLE condition_cache_rf_probe (k BIGINT, s STRING NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO condition_cache_rf_probe
        SELECT number, '00:00:00' FROM numbers("number" = "10000")
    """
    sql "DROP TABLE IF EXISTS condition_cache_rf_build"
    sql """
        CREATE TABLE condition_cache_rf_build (d DATETIME NOT NULL)
        DUPLICATE KEY(d) DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Keep the expected join count stable even if these executions cross midnight.
    sql """
        INSERT INTO condition_cache_rf_build VALUES
            (CURRENT_DATE() - INTERVAL 1 DAY), (CURRENT_DATE()), (CURRENT_DATE() + INTERVAL 1 DAY)
    """
    sql "set enable_runtime_filter_prune = false"
    sql "set runtime_filter_wait_infinitely = true"
    sql "set disable_join_reorder = true"
    def rfQuery = """
        SELECT count(*) FROM condition_cache_rf_probe p
        JOIN [broadcast] condition_cache_rf_build b
        ON CAST(CAST(p.s AS TIME) AS DATETIME) = b.d
    """
    explain {
        sql rfQuery
        contains "RF"
        contains "CAST(CAST"
    }
    order_qt_volatile_rf_first rfQuery
    order_qt_volatile_rf_second rfQuery

    def deterministic = """
        SELECT count(*) FROM condition_cache_eligibility WHERE k % 1000 = 0
    """
    // Execute the same SQL twice so both scans have the same digest.
    order_qt_deterministic_first deterministic
    order_qt_deterministic_second deterministic

    order_qt_null_expressions """
        SELECT count(*) FROM condition_cache_eligibility
        WHERE CAST(CAST(NULL AS TIME) AS DATETIME) IS NULL
            AND array_shuffle(CAST(NULL AS ARRAY<INT>), 1) IS NULL
            AND shuffle(CAST(NULL AS ARRAY<INT>)) IS NULL
            AND array_shuffle(a, NULL) IS NULL
    """
}
