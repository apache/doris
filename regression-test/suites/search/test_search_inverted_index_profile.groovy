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

import java.util.regex.Pattern
import org.apache.doris.regression.action.ProfileAction

suite("test_search_inverted_index_profile", "nonConcurrent") {
    def tableName = "test_search_ii_profile"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(200),
            content TEXT,
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """INSERT INTO ${tableName} VALUES
        (1, 'apple banana cherry', 'red fruit sweet delicious'),
        (2, 'banana grape mango', 'yellow fruit tropical summer'),
        (3, 'cherry plum peach', 'stone fruit summer garden'),
        (4, 'apple grape kiwi', 'green fruit fresh morning'),
        (5, 'mango pineapple coconut', 'tropical fruit exotic island'),
        (6, 'apple cherry plum', 'mixed fruit salad party'),
        (7, 'banana coconut papaya', 'smoothie blend tropical drink'),
        (8, 'grape cherry apple', 'wine fruit tart autumn')
    """
    sql "sync"

    // Helper: extract a numeric counter value from profile string
    def extractCounter = { String profileStr, String counterName ->
        // Profile HTML uses &nbsp; for spacing
        def pattern = Pattern.compile("${counterName}:(?:&nbsp;|\\s)*(\\d+)")
        def matcher = pattern.matcher(profileStr)
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1))
        }
        return -1L
    }

    def checkProfile = { String tag, String query, List<String> counters, Closure check ->
        sql "SET enable_profile=true"
        try {
            sql "/* ${tag} */ ${query}"
            def profile = new ProfileAction(context).getProfileBySql(tag, counters)
            check(profile)
        } finally {
            sql "SET enable_profile=false"
        }
    }

    // Disable both caches to check the index-open, search and cache-miss counters.
    sql """ set profile_level = 2 """
    sql """ set enable_sql_cache = false """
    sql """ set enable_inverted_index_searcher_cache = false """
    sql """ set enable_inverted_index_query_cache = false """
    sql """ set enable_segment_limit_pushdown = true """

    def queryId1 = "search_profile_miss_${System.currentTimeMillis()}"
    checkProfile(queryId1, """SELECT id FROM ${tableName}
            WHERE search('title:apple') ORDER BY id""",
            ["InvertedIndexQueryTime", "InvertedIndexSearcherSearchTime",
             "InvertedIndexSearcherCacheMiss"]) { profileString ->
        log.info("=== Cache-miss profile ===")

        def queryTime = extractCounter(profileString, "InvertedIndexQueryTime")
        def openTime = extractCounter(profileString, "InvertedIndexSearcherOpenTime")
        def searchTime = extractCounter(profileString, "InvertedIndexSearcherSearchTime")
        def searchInitTime = extractCounter(profileString, "InvertedIndexSearcherSearchInitTime")
        def searchExecTime = extractCounter(profileString, "InvertedIndexSearcherSearchExecTime")
        def cacheMiss = extractCounter(profileString, "InvertedIndexSearcherCacheMiss")
        def cacheHit = extractCounter(profileString, "InvertedIndexSearcherCacheHit")

        log.info("InvertedIndexQueryTime: {}", queryTime)
        log.info("InvertedIndexSearcherOpenTime: {}", openTime)
        log.info("InvertedIndexSearcherSearchTime: {}", searchTime)
        log.info("InvertedIndexSearcherSearchInitTime: {}", searchInitTime)
        log.info("InvertedIndexSearcherSearchExecTime: {}", searchExecTime)
        log.info("InvertedIndexSearcherCacheMiss: {}", cacheMiss)
        log.info("InvertedIndexSearcherCacheHit: {}", cacheHit)

        assertTrue(queryTime > 0,
            "InvertedIndexQueryTime should be > 0 for SEARCH(), got ${queryTime}")
        assertTrue(searchTime > 0,
            "InvertedIndexSearcherSearchTime should be > 0, got ${searchTime}")
        assertTrue(cacheMiss > 0,
            "InvertedIndexSearcherCacheMiss should be > 0 (cache disabled), got ${cacheMiss}")
    }

    // A repeated query must reuse the searcher and still record search time.
    sql """ set enable_inverted_index_searcher_cache = true """
    sql """ set enable_inverted_index_query_cache = false """

    // First run: populate searcher cache
    sql """SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */
           id FROM ${tableName} WHERE search('title:cherry') ORDER BY id"""

    // Second run: should hit searcher cache
    def queryId2 = "search_profile_hit_${System.currentTimeMillis()}"
    checkProfile(queryId2, """SELECT id FROM ${tableName}
            WHERE search('title:cherry') ORDER BY id""",
            ["InvertedIndexSearcherCacheHit", "InvertedIndexSearcherSearchTime"]) { profileString ->
        log.info("=== Cache-hit profile ===")

        def cacheHit = extractCounter(profileString, "InvertedIndexSearcherCacheHit")
        def cacheMiss = extractCounter(profileString, "InvertedIndexSearcherCacheMiss")
        def openTime = extractCounter(profileString, "InvertedIndexSearcherOpenTime")
        def searchTime = extractCounter(profileString, "InvertedIndexSearcherSearchTime")

        log.info("InvertedIndexSearcherCacheHit: {}", cacheHit)
        log.info("InvertedIndexSearcherCacheMiss: {}", cacheMiss)
        log.info("InvertedIndexSearcherOpenTime: {}", openTime)
        log.info("InvertedIndexSearcherSearchTime: {}", searchTime)

        assertTrue(cacheHit > 0,
            "InvertedIndexSearcherCacheHit should be > 0 on second run, got ${cacheHit}")
        assertTrue(searchTime > 0,
            "InvertedIndexSearcherSearchTime should still be > 0 on cache hit, got ${searchTime}")
    }

    // Check that opening and reusing a searcher keeps its I/O context valid.
    sql """ set enable_inverted_index_searcher_cache = true """
    sql """ set enable_inverted_index_query_cache = false """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("InvertedIndexReader.handle_searcher_cache.io_ctx")

        // First query: cache miss, debug point validates io_ctx consistency
        qt_io_ctx_miss """ SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */
            id FROM ${tableName} WHERE search('content:tropical') ORDER BY id """

        // Second query: cache hit, reuses the cached searcher
        // If io_ctx was stale, this would crash under ASAN
        qt_io_ctx_hit """ SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */
            id FROM ${tableName} WHERE search('content:tropical') ORDER BY id """

        // Third query: different DSL but same field — exercises resolver cache
        qt_io_ctx_multi """ SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */
            id FROM ${tableName} WHERE search('content:tropical OR content:fruit') ORDER BY id """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("InvertedIndexReader.handle_searcher_cache.io_ctx")
    }

    // A repeated DSL query must report a result-cache hit and query time.
    sql """ set enable_inverted_index_searcher_cache = true """
    sql """ set enable_inverted_index_query_cache = true """

    // First run: populate DSL cache
    sql """SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */
           id FROM ${tableName} WHERE search('title:banana') ORDER BY id"""

    // Second run: should hit DSL cache
    def queryId4 = "search_profile_dsl_hit_${System.currentTimeMillis()}"
    checkProfile(queryId4, """SELECT id FROM ${tableName}
            WHERE search('title:banana') ORDER BY id""",
            ["InvertedIndexQueryCacheHit", "InvertedIndexQueryTime"]) { profileString ->
        log.info("=== DSL cache-hit profile ===")

        def queryCacheHit = extractCounter(profileString, "InvertedIndexQueryCacheHit")
        def queryTime = extractCounter(profileString, "InvertedIndexQueryTime")

        log.info("InvertedIndexQueryCacheHit: {}", queryCacheHit)
        log.info("InvertedIndexQueryTime: {}", queryTime)

        assertTrue(queryCacheHit > 0,
            "InvertedIndexQueryCacheHit should be > 0 on DSL cache hit, got ${queryCacheHit}")
        assertTrue(queryTime > 0,
            "InvertedIndexQueryTime should be > 0 even on DSL cache hit, got ${queryTime}")
    }

    // Disabling the searcher cache must force a miss for a repeated query.
    sql """ set enable_inverted_index_searcher_cache = false """
    sql """ set enable_inverted_index_query_cache = false """

    // First run: cache miss (searcher cache disabled, nothing to hit)
    sql """SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */
           id FROM ${tableName} WHERE search('title:grape') ORDER BY id"""

    // Second run: should STILL be a cache miss because searcher cache is disabled
    def queryId5 = "search_profile_no_cache_${System.currentTimeMillis()}"
    checkProfile(queryId5, """SELECT id FROM ${tableName}
            WHERE search('title:grape') ORDER BY id""",
            ["InvertedIndexSearcherCacheHit", "InvertedIndexSearcherCacheMiss"]) { profileString ->
        log.info("=== Searcher cache disabled (2nd run) profile ===")

        def cacheHit = extractCounter(profileString, "InvertedIndexSearcherCacheHit")
        def cacheMiss = extractCounter(profileString, "InvertedIndexSearcherCacheMiss")

        log.info("InvertedIndexSearcherCacheHit: {}", cacheHit)
        log.info("InvertedIndexSearcherCacheMiss: {}", cacheMiss)

        assertTrue(cacheHit == 0,
            "InvertedIndexSearcherCacheHit should be 0 when cache disabled, got ${cacheHit}")
        assertTrue(cacheMiss > 0,
            "InvertedIndexSearcherCacheMiss should be > 0 when cache disabled, got ${cacheMiss}")
    }

    // SEARCH and MATCH must return the same rows for the same predicate.
    def search_result = sql """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */
        id FROM ${tableName} WHERE search('title:apple') ORDER BY id
    """
    def match_result = sql """
        SELECT id FROM ${tableName} WHERE title MATCH_ANY 'apple' ORDER BY id
    """
    assertEquals(search_result, match_result,
        "SEARCH('title:apple') and MATCH_ANY 'apple' should return identical rows")
}
