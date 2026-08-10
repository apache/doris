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
 * TRUNCATE changes visible rows, so it must advance the table version metadata.
 * SimpleAggCacheMgr uses that metadata to invalidate cached aggregate values.
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

    // Truncate must invalidate the cached aggregate value.
    sql "TRUNCATE TABLE tbl;"

    // count(*) must return 0.
    // Without the fix, the stale cache entry (count = 5) would be returned.
    order_qt_count_after_truncate "SELECT count(*) FROM tbl;"

    // Insert new rows after truncate, then verify count(*) reflects them.
    sql "INSERT INTO tbl VALUES (10, 100), (20, 200);"

    // After insert the count must be 2.
    def count = sql "SELECT count(*) FROM tbl"
    assertEquals(2L, count[0][0] as long,
            "count(*) after truncate + insert should be 2, got ${count[0][0]}")
}
