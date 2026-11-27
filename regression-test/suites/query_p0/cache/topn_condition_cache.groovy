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

import org.apache.doris.regression.action.ProfileAction

suite("topn_condition_cache") {
    // TopN cache identities currently cover local storage snapshots only.
    if (isCloudMode()) {
        return
    }
    sql "DROP TABLE IF EXISTS topn_condition_cache"
    sql """
        CREATE TABLE topn_condition_cache (
            p INT NOT NULL, id BIGINT NOT NULL, score BIGINT NULL
        ) DUPLICATE KEY(p, id)
        PARTITION BY RANGE(p) (
            PARTITION p0 VALUES LESS THAN (1),
            PARTITION p1 VALUES LESS THAN (2)
        ) DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """
        INSERT INTO topn_condition_cache
        SELECT 0, number, number - 1000000 FROM numbers("number" = "100000")
        UNION ALL
        SELECT 1, number, number + 1000000 FROM numbers("number" = "100000")
    """
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"
    sql "set topn_filter_ratio = 1000000"
    sql "set enable_parallel_scan = false"
    def queries = [
        "SELECT score FROM topn_condition_cache ORDER BY score LIMIT 10",
        "SELECT score FROM topn_condition_cache WHERE id % 7 = 0 ORDER BY score LIMIT 10",
        "SELECT score FROM topn_condition_cache ORDER BY score DESC LIMIT 10",
        "SELECT score FROM topn_condition_cache ORDER BY score LIMIT 5 OFFSET 7",
        "SELECT score FROM topn_condition_cache WHERE id = -1 ORDER BY score LIMIT 10"
    ]
    for (int i = 0; i < queries.size(); i++) {
        sql "set enable_condition_cache = false"
        quickRunTest("reference_${i}", queries[i])
        sql "set enable_condition_cache = true"
        quickRunTest("learn_${i}", queries[i])
        quickRunTest("reuse_${i}", queries[i])
    }
    explain {
        sql queries[0]
        contains "TOPN OPT:"
    }
    def profileAction = new ProfileAction(context)
    def profiled = { String query, String tag ->
        String token = UUID.randomUUID().toString()
        sql "set enable_profile = true"
        sql "set profile_level = 2"
        quickRunTest(tag, "/* ${token} */ ${query}")
        def profile = profileAction.getProfileBySql(token, ["ConditionCacheLookup", "ConditionCacheHit"])
        sql "set enable_profile = false"
        return profile
    }
    def warm = profiled(queries[0], "warm_profile")
    def hits = (warm =~ /ConditionCacheHit: (\d+)/).findAll()
    assertTrue(hits.any { (it[1] as long) > 0 }, "No TopN condition-cache hit: ${warm}")

    // p1's files are unchanged, but p0 supplied all globally best rows. Its removal must
    // invalidate p1's old exclusions, rather than keying the proof only by p1's segments.
    sql "ALTER TABLE topn_condition_cache DROP PARTITION p0"
    def afterDrop = profiled(queries[0], "drop_best_partition")
    def dropHits = (afterDrop =~ /ConditionCacheHit: (\d+)/).findAll()
    assertTrue(!dropHits.isEmpty() && dropHits.every { (it[1] as long) == 0 },
            "Changed global snapshot reused exclusions: ${afterDrop}")
    qt_after_drop_reuse queries[0]

    sql "INSERT INTO topn_condition_cache VALUES (1, 200000, -3000000)"
    qt_after_insert queries[0]
    qt_after_insert_reuse queries[0]
    sql "set enable_parallel_scan = true"
    qt_parallel_scan queries[0]
    sql "set enable_condition_cache = false"
    qt_parallel_reference queries[0]
    sql "set enable_condition_cache = true"
    trigger_and_wait_compaction("topn_condition_cache", "full")
    qt_after_compaction queries[0]
    qt_after_compaction_reuse queries[0]

    sql "DROP TABLE IF EXISTS topn_condition_cache_nulls"
    sql """
        CREATE TABLE topn_condition_cache_nulls (id INT, score INT NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO topn_condition_cache_nulls
        SELECT number, IF(number % 11 = 0, NULL, number % 31)
        FROM numbers("number" = "20000")
    """
    for (String direction : ["ASC", "DESC"]) {
        for (String nullOrder : ["FIRST", "LAST"]) {
            def query = "SELECT score FROM topn_condition_cache_nulls " +
                    "ORDER BY score ${direction} NULLS ${nullOrder} LIMIT 10 OFFSET 3"
            sql "set enable_condition_cache = false"
            quickRunTest("null_reference_${direction}_${nullOrder}", query)
            sql "set enable_condition_cache = true"
            quickRunTest("null_learn_${direction}_${nullOrder}", query)
            quickRunTest("null_reuse_${direction}_${nullOrder}", query)
        }
    }
}
