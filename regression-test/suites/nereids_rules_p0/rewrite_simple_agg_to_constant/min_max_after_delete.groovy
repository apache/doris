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

// A no-filter MIN/MAX is rewritten to a constant from SimpleAggCacheMgr. That cache runs its
// internal SQL on the statistics connect context, which forces MIN/MAX onto the segment zone maps.
// A zone map still covers the rows a delete predicate filters out, so the cache used to store the
// max of a row that had been DELETEd and the rewrite handed that value back to the user.
suite("min_max_after_delete") {
    // The rewrite is only reached when the plan is built, so the SQL result cache would answer the
    // later queries without ever consulting SimpleAggCacheMgr.
    sql "SET enable_sql_cache=false"

    sql "DROP DATABASE IF EXISTS test_min_max_after_delete"
    sql "CREATE DATABASE test_min_max_after_delete"
    sql "USE test_min_max_after_delete"

    // disable_auto_compaction is what preserves the fault this test is about. The cached bound is
    // only wrong while the deleted row is still on disk behind its delete predicate, so that the
    // segment zone map still holds it. Auto compaction is on by default, and a DELETE alone is
    // enough to make cumulative compaction merge the preceding rowsets and base compaction apply
    // the predicate. If that wins the race against the async cache load, the unfixed code answers
    // correctly too and this test would pass without covering anything.
    sql """
        CREATE TABLE dup_min_max_deleted (
            k VARCHAR(20) NULL,
            d DATETIME NULL,
            n BIGINT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            'replication_num' = '1',
            'disable_auto_compaction' = 'true'
        )
    """

    sql """INSERT INTO dup_min_max_deleted VALUES ('keep_a', '2022-04-27 16:00:33', 1)"""
    sql """INSERT INTO dup_min_max_deleted VALUES ('keep_b', '2022-04-27 16:00:58', 2)"""
    // Holds both the max datetime and the max integer, and is deleted right below. DELETE leaves it
    // as a delete predicate, so its rows are still on disk behind that predicate.
    sql """INSERT INTO dup_min_max_deleted VALUES ('deleted_row', '2024-01-01 00:00:00', 100)"""
    sql """DELETE FROM dup_min_max_deleted WHERE k = 'deleted_row'"""

    // ===================================================================
    // Warm up the SimpleAggCacheMgr async cache.
    //
    // The first call to getStats() only triggers an async load. Poll until
    // explain shows "constant exprs", which proves the cache entry -- the
    // one that used to hold the deleted row's bound -- is in place.
    //
    // The query must push down MIN/MAX alone. Adding count(*) would make it
    // a MIX push-down, which the BE keeps behind its delete-predicate guard,
    // and the bug would not be exercised.
    // ===================================================================
    sql "SELECT min(d), max(d) FROM dup_min_max_deleted"
    sql "SELECT min(n), max(n) FROM dup_min_max_deleted"

    def minMaxSql = "SELECT min(d), max(d), min(n), max(n) FROM dup_min_max_deleted"
    def cacheReady = false
    for (int i = 0; i < 30; i++) {
        def explainResult = sql "EXPLAIN ${minMaxSql}"
        if (explainResult.toString().contains("constant exprs")) {
            cacheReady = true
            break
        }
        sleep(1000)
    }
    if (!cacheReady) {
        if (isCloudMode()) {
            logger.info("SimpleAggCacheMgr cache did not warm up within 30s in cloud mode, skip")
            return
        }
        assertTrue(false, "SimpleAggCacheMgr cache did not warm up within 30 seconds")
    }

    // The deleted row must not contribute to the answer, neither through the constant rewrite nor
    // through the rows the BE reads. Both values below come from the two surviving rows.
    explain {
        sql("${minMaxSql}")
        contains "constant exprs"
    }
    order_qt_min_max_after_delete """SELECT min(d), max(d), min(n), max(n) FROM dup_min_max_deleted"""

    // A delete predicate has to keep the pushed-down MIN/MAX off the zone maps, so disabling the
    // rewrite must agree with the rewritten answer above.
    order_qt_min_max_after_delete_rule_off """
        SELECT /*+ SET_VAR(disable_nereids_rules=REWRITE_SIMPLE_AGG_TO_CONSTANT) */
            min(d), max(d), min(n), max(n) FROM dup_min_max_deleted
    """
}
