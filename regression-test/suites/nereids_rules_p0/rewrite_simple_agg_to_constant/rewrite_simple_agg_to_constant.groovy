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

suite("rewrite_simple_agg_to_constant") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP DATABASE IF EXISTS test_rewrite_simple_agg_constant"
    sql "CREATE DATABASE test_rewrite_simple_agg_constant"
    sql "USE test_rewrite_simple_agg_constant"

    // ========== Create test tables ==========

    // DUP_KEYS table with NOT NULL columns
    sql """
        CREATE TABLE dup_tbl (
            k1 INT NOT NULL,
            v1 INT NOT NULL,
            v2 BIGINT NOT NULL,
            v3 DATE NOT NULL,
            v4 VARCHAR(128)
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    // UNIQUE_KEYS table
    sql """
        CREATE TABLE uniq_tbl (
            k1 INT NOT NULL,
            v1 INT NOT NULL
        ) UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    // AGG_KEYS table
    sql """
        CREATE TABLE agg_tbl (
            k1 INT NOT NULL,
            v1 INT SUM NOT NULL
        ) AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    // ========== Insert test data ==========
    sql """
        INSERT INTO dup_tbl VALUES
        (1, 10, 100, '2024-01-01', 'aaa'),
        (2, 20, 200, '2024-06-15', 'bbb'),
        (3, 30, 300, '2024-12-31', null),
        (4, 40, 400, '2025-03-01', 'ddd'),
        (5, 50, 500, '2025-06-30', 'eee');
    """

    sql """
        INSERT INTO uniq_tbl VALUES (1, 10), (2, 20), (3, 30);
    """

    sql """
        INSERT INTO agg_tbl VALUES (1, 10), (2, 20), (3, 30);
    """

    // Wait a bit for tablet stats to be reported to FE
    sleep(3000)

    // ===================================================================
    // Warm up the SimpleAggCacheMgr async cache.
    //
    // The first call to getStats()/getRowCount() triggers an async load;
    // the result is not available until the internal SQL finishes.
    // We poll until explain shows "constant exprs", which proves the cache
    // entry is loaded and the rule can fire.
    // ===================================================================
    // Trigger cache loads for all columns we'll test
    sql "SELECT count(*) FROM dup_tbl"
    sql "SELECT min(v1), max(v1) FROM dup_tbl"
    sql "SELECT min(v2), max(v2) FROM dup_tbl"
    sql "SELECT min(v3), max(v3) FROM dup_tbl"

    // Poll until the rule fires (cache is warm)
    def warmUpSql = "SELECT count(*), min(v1), max(v2), min(v3) FROM dup_tbl"
    def cacheReady = false
    for (int i = 0; i < 30; i++) {
        def explainResult = sql "EXPLAIN ${warmUpSql}"
        if (explainResult.toString().contains("constant exprs")) {
            cacheReady = true
            break
        }
        sleep(1000)
    }
    if (!cacheReady) {
        if (isCloudMode()) {
            logger.info("SimpleAggCacheMgr cache did not warm up within 30s in cloud mode, skip remaining tests")
            return
        }
        assertTrue(false, "SimpleAggCacheMgr cache did not warm up within 30 seconds")
    }

    // ===================================================================
    // Positive tests: verify the rule IS applied.
    // The cache is confirmed warm from the poll above.
    // ===================================================================

    // count(*)
    explain {
        sql("SELECT count(*) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_count_star """SELECT count(*) FROM dup_tbl;"""

    // count(not-null column)
    explain {
        sql("SELECT count(k1) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_count_notnull """SELECT count(k1) FROM dup_tbl;"""

    // min(int)
    explain {
        sql("SELECT min(v1) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_min_int """SELECT min(v1) FROM dup_tbl;"""

    // max(int)
    explain {
        sql("SELECT max(v1) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_max_int """SELECT max(v1) FROM dup_tbl;"""

    // min(bigint)
    explain {
        sql("SELECT min(v2) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_min_bigint """SELECT min(v2) FROM dup_tbl;"""

    // max(bigint)
    explain {
        sql("SELECT max(v2) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_max_bigint """SELECT max(v2) FROM dup_tbl;"""

    // min(date)
    explain {
        sql("SELECT min(v3) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_min_date """SELECT min(v3) FROM dup_tbl;"""

    // max(date)
    explain {
        sql("SELECT max(v3) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_max_date """SELECT max(v3) FROM dup_tbl;"""

    // Mixed: count(*), min, max together
    explain {
        sql("SELECT count(*), min(v1), max(v2) FROM dup_tbl")
        contains "constant exprs"
    }
    order_qt_mixed """SELECT count(*), min(v1), max(v2) FROM dup_tbl;"""

    // ===================================================================
    // Negative tests: these queries should NEVER be rewritten.
    // The cache is confirmed warm (the poll + positive tests above proved
    // count/min/max for dup_tbl all hit cache). So if these plans do NOT
    // contain "constant exprs", the rule actively rejected them.
    // ===================================================================

    // Non-DUP_KEYS: UNIQUE_KEYS table should not be rewritten
    explain {
        sql("SELECT count(*) FROM uniq_tbl")
        notContains "constant exprs"
    }

    // Non-DUP_KEYS: AGG_KEYS table should not be rewritten
    explain {
        sql("SELECT count(*) FROM agg_tbl")
        notContains "constant exprs"
    }

    // GROUP BY present → should not be rewritten
    explain {
        sql("SELECT count(*) FROM dup_tbl GROUP BY k1")
        notContains "constant exprs"
    }

    // Unsupported aggregate function: SUM (cache for v1 is already warm from min/max above)
    explain {
        sql("SELECT sum(v1) FROM dup_tbl")
        notContains "constant exprs"
    }

    // Unsupported aggregate function: AVG
    explain {
        sql("SELECT avg(v1) FROM dup_tbl")
        notContains "constant exprs"
    }

    // DISTINCT count
    explain {
        sql("SELECT count(distinct k1) FROM dup_tbl")
        notContains "constant exprs"
    }

    // count(nullable column) → cannot guarantee count(v4) equals row count
    explain {
        sql("SELECT count(v4) FROM dup_tbl")
        notContains "constant exprs"
    }

    // min/max on string column → not supported (row count cache is warm)
    explain {
        sql("SELECT min(v4) FROM dup_tbl")
        notContains "constant exprs"
    }

    // Mixed supported (count) and unsupported (sum) → entire query NOT rewritten
    explain {
        sql("SELECT count(*), sum(v1) FROM dup_tbl")
        notContains "constant exprs"
    }

    // Manually specified partition → should not be rewritten
    explain {
        sql("SELECT count(*) FROM dup_tbl PARTITION(dup_tbl)")
        notContains "constant exprs"
    }
    explain {
        sql("SELECT min(v1), max(v2) FROM dup_tbl PARTITION(dup_tbl)")
        notContains "constant exprs"
    }

    // Manually specified tablet → should not be rewritten
    def tabletResult = sql "SHOW TABLETS FROM dup_tbl"
    def tabletId = tabletResult[0][0]
    explain {
        sql("SELECT count(*) FROM dup_tbl TABLET(${tabletId})")
        notContains "constant exprs"
    }
    explain {
        sql("SELECT min(v1), max(v2) FROM dup_tbl TABLET(${tabletId})")
        notContains "constant exprs"
    }

    // TABLESAMPLE → should not be rewritten
    explain {
        sql("SELECT count(*) FROM dup_tbl TABLESAMPLE(10 PERCENT)")
        notContains "constant exprs"
    }
    explain {
        sql("SELECT min(v1), max(v2) FROM dup_tbl TABLESAMPLE(3 ROWS)")
        notContains "constant exprs"
    }

    // Sync materialized view (indexSelected = true) → should not be rewritten
    createMV("""CREATE MATERIALIZED VIEW mv_dup_sum AS SELECT v1 as m1, sum(v2) as m2 FROM dup_tbl GROUP BY v1;""")
    explain {
        sql("SELECT count(*) FROM dup_tbl INDEX mv_dup_sum")
        notContains "constant exprs"
    }

    // ===================================================================
    // Verify disabling the rule works.
    // When the rule is disabled, even simple count(*) should NOT produce constant exprs.
    // ===================================================================
    explain {
        sql("SELECT /*+ SET_VAR(disable_nereids_rules=REWRITE_SIMPLE_AGG_TO_CONSTANT) */ count(*) FROM dup_tbl")
        notContains "constant exprs"
    }

    // ===================================================================
    // Correctness-only tests for queries that should NOT be rewritten.
    // The result must still be correct even though the rule does not fire.
    // ===================================================================

    // count(nullable column) — not rewritten, but result must be correct
    order_qt_count_nullable """SELECT count(v4) FROM dup_tbl;"""

    // Count on non-DUP_KEYS tables — result correctness
    order_qt_uniq_count """SELECT count(*) FROM uniq_tbl;"""
    order_qt_agg_count """SELECT count(*) FROM agg_tbl;"""

    // ===================================================================
    // Cache invalidation test: inserting new data should invalidate the
    // cached min/max stats so the rule no longer fires until cache refreshes.
    // ===================================================================
    sql "INSERT INTO dup_tbl VALUES (6, 60, 600, '2025-12-01', 'fff');"

    // Right after INSERT the cached stats are stale; explain should NOT
    // show "constant exprs" because the cache entry has been invalidated.
    explain {
        sql("SELECT min(v2), max(v2) FROM dup_tbl")
        notContains "constant exprs"
    }

}
