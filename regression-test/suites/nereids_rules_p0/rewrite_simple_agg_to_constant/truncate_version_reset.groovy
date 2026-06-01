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

/**
 * Regression test for: TRUNCATE TABLE must reset TableAttributes.visibleVersion.
 *
 * Bug: before the fix, truncateTableInternal() replaced partition data but did
 * not call olapTable.resetVisibleVersion().  As a result:
 *   - Partition.visibleVersion was reset to PARTITION_INIT_VERSION (1).
 *   - TableAttributes.visibleVersion kept its old, higher value.
 *   - TableAttributes.visibleVersionTime was never updated.
 *
 * Consequence for RewriteSimpleAggToConstantRule / SimpleAggCacheMgr:
 *   The cache entry was keyed by versionTime.  Because versionTime did not
 *   change at truncate time, the *caller* saw the same versionTime as the
 *   stale cached entry → cache HIT → the rule returned the pre-truncate
 *   count/min/max instead of the correct post-truncate values.
 *
 * The fix adds olapTable.resetVisibleVersion() inside truncateTableInternal(),
 * which bumps both visibleVersion (back to TABLE_INIT_VERSION = 1) and
 * visibleVersionTime (to System.currentTimeMillis()).  The new versionTime
 * differs from the cached entry's versionTime → cache MISS → the rule
 * correctly falls back to BE execution and returns the right result.
 */
suite("truncate_version_reset") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP DATABASE IF EXISTS test_truncate_version_reset"
    sql "CREATE DATABASE test_truncate_version_reset"
    sql "USE test_truncate_version_reset"

    sql """
        CREATE TABLE tbl (
            k1 INT NOT NULL,
            v1 INT NOT NULL
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    sql "INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);"

    // -----------------------------------------------------------------------
    // Warm up SimpleAggCacheMgr for count(*).
    // Poll until the rule fires (plan contains "constant exprs").
    // -----------------------------------------------------------------------
    sql "SELECT count(*) FROM tbl"

    def cacheReady = false
    for (int i = 0; i < 30; i++) {
        def explainResult = sql "EXPLAIN SELECT count(*) FROM tbl"
        if (explainResult.toString().contains("constant exprs")) {
            cacheReady = true
            break
        }
        sleep(1000)
    }
    if (!cacheReady) {
        if (isCloudMode()) {
            logger.info("SimpleAggCacheMgr did not warm up in cloud mode, skip")
            return
        }
        assertTrue(false, "SimpleAggCacheMgr cache did not warm up within 30 seconds")
    }

    // Confirm the cache is hot and the rule fires for count(*).
    explain {
        sql "SELECT count(*) FROM tbl"
        contains "constant exprs"
    }
    // Confirm the cached count is correct before truncate.
    order_qt_count_before_truncate "SELECT count(*) FROM tbl;"

    // -----------------------------------------------------------------------
    // TRUNCATE the table.
    // After the fix, resetVisibleVersion() is called inside
    // truncateTableInternal(), which updates visibleVersionTime.
    // The cache entry's versionTime no longer matches → cache is invalidated.
    // -----------------------------------------------------------------------
    sql "TRUNCATE TABLE tbl;"

    // count(*) must return 0.
    // Without the fix, the stale cache entry (count = 5) would be returned.
    order_qt_count_after_truncate "SELECT count(*) FROM tbl;"

    // -----------------------------------------------------------------------
    // Insert new rows after truncate, then verify count(*) reflects them.
    // This also validates that the version counter is correctly reset so
    // subsequent transactions start from the right next-version.
    // -----------------------------------------------------------------------
    sql "INSERT INTO tbl VALUES (10, 100), (20, 200);"

    // After insert the count must be 2.
    def count = sql "SELECT count(*) FROM tbl"
    assertEquals(2L, count[0][0] as long,
            "count(*) after truncate + insert should be 2, got ${count[0][0]}")
}
