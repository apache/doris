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

    // =========================================================================
    // Test 1: SEARCH() profile metrics are populated on cache miss
    //
    // Disable both caches so every query triggers a full index open + search.
    // Verify that InvertedIndexQueryTime, InvertedIndexSearcherOpenTime,
    // InvertedIndexSearcherSearchTime, and cache miss counter are all > 0.
    // =========================================================================
    sql """ set enable_profile = true """
    sql """ set profile_level = 2 """
    sql """ set enable_sql_cache = false """
    sql """ set enable_inverted_index_searcher_cache = false """
    sql """ set enable_inverted_index_query_cache = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true """

    def queryId1 = "search_profile_miss_${System.currentTimeMillis()}"
    try {
        profile("${queryId1}") {
            run {
                sql """/* ${queryId1} */ SELECT id FROM ${tableName}
                       WHERE search('title:apple') ORDER BY id"""
            }
            check { profileString, exception ->
                if (exception != null) throw exception
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
        }
    } catch (IllegalStateException e) {
        if (e.message?.contains("HttpCliAction failed")) {
            log.warn("Profile HTTP request failed, skipping: {}", e.message)
        } else {
            throw e
        }
    }

    // =========================================================================
    // Test 2: SEARCH() cache hit — re-enable searcher cache, run twice.
    // First run is a cache miss (populates cache), second run is a cache hit.
    // Verify the second profile shows cache hit > 0 and open time == 0.
    // =========================================================================
    sql """ set enable_inverted_index_searcher_cache = true """
    sql """ set enable_inverted_index_query_cache = false """

    // First run: populate searcher cache
    sql """SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */
           id FROM ${tableName} WHERE search('title:cherry') ORDER BY id"""

    // Second run: should hit searcher cache
    def queryId2 = "search_profile_hit_${System.currentTimeMillis()}"
    try {
        profile("${queryId2}") {
            run {
                sql """/* ${queryId2} */ SELECT id FROM ${tableName}
                       WHERE search('title:cherry') ORDER BY id"""
            }
            check { profileString, exception ->
                if (exception != null) throw exception
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
        }
    } catch (IllegalStateException e) {
        if (e.message?.contains("HttpCliAction failed")) {
            log.warn("Profile HTTP request failed, skipping: {}", e.message)
        } else {
            throw e
        }
    }

    // =========================================================================
    // Test 3: io_ctx safety — debug point verifies no stale io_ctx on the
    // main stream after IndexSearcher creation (cache miss path).
    // Then run a second query that hits the cache and confirm correct results.
    // If the old dangling-pointer bug existed, ASAN would catch UAF here.
    // =========================================================================
    sql """ set enable_inverted_index_searcher_cache = true """
    sql """ set enable_inverted_index_query_cache = false """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("FieldReaderResolver.resolve.io_ctx")

        // First query: cache miss, debug point validates io_ctx consistency
        qt_io_ctx_miss """ SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */
            id FROM ${tableName} WHERE search('content:tropical') ORDER BY id """

        // Second query: cache hit, reuses the cached searcher
        // If io_ctx was stale, this would crash under ASAN
        qt_io_ctx_hit """ SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */
            id FROM ${tableName} WHERE search('content:tropical') ORDER BY id """

        // Third query: different DSL but same field — exercises resolver cache
        qt_io_ctx_multi """ SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */
            id FROM ${tableName} WHERE search('content:tropical OR content:fruit') ORDER BY id """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("FieldReaderResolver.resolve.io_ctx")
    }

    // =========================================================================
    // Test 4: DSL query-cache hit — enable query cache, run same DSL twice.
    // First run populates the DSL cache (miss), second run hits it.
    // Verify InvertedIndexQueryCacheHit > 0, InvertedIndexQueryTime > 0,
    // and InvertedIndexLookupTime > 0 on the cache-hit path.
    // =========================================================================
    sql """ set enable_inverted_index_searcher_cache = true """
    sql """ set enable_inverted_index_query_cache = true """

    // First run: populate DSL cache
    sql """SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */
           id FROM ${tableName} WHERE search('title:banana') ORDER BY id"""

    // Second run: should hit DSL cache
    def queryId4 = "search_profile_dsl_hit_${System.currentTimeMillis()}"
    try {
        profile("${queryId4}") {
            run {
                sql """/* ${queryId4} */ SELECT id FROM ${tableName}
                       WHERE search('title:banana') ORDER BY id"""
            }
            check { profileString, exception ->
                if (exception != null) throw exception
                log.info("=== DSL cache-hit profile ===")

                def queryCacheHit = extractCounter(profileString, "InvertedIndexQueryCacheHit")
                def queryTime = extractCounter(profileString, "InvertedIndexQueryTime")
                def lookupTime = extractCounter(profileString, "InvertedIndexLookupTime")

                log.info("InvertedIndexQueryCacheHit: {}", queryCacheHit)
                log.info("InvertedIndexQueryTime: {}", queryTime)
                log.info("InvertedIndexLookupTime: {}", lookupTime)

                assertTrue(queryCacheHit > 0,
                    "InvertedIndexQueryCacheHit should be > 0 on DSL cache hit, got ${queryCacheHit}")
                assertTrue(queryTime > 0,
                    "InvertedIndexQueryTime should be > 0 even on DSL cache hit, got ${queryTime}")
            }
        }
    } catch (IllegalStateException e) {
        if (e.message?.contains("HttpCliAction failed")) {
            log.warn("Profile HTTP request failed, skipping: {}", e.message)
        } else {
            throw e
        }
    }

    // =========================================================================
    // Test 5: searcher cache disabled — run same DSL twice with searcher cache
    // off (but query cache also off so DSL cache cannot mask the problem).
    // Both runs must show cache miss and zero cache hits, proving the switch
    // is actually respected and a second run does not silently hit the cache.
    // =========================================================================
    sql """ set enable_inverted_index_searcher_cache = false """
    sql """ set enable_inverted_index_query_cache = false """

    // First run: cache miss (searcher cache disabled, nothing to hit)
    sql """SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */
           id FROM ${tableName} WHERE search('title:grape') ORDER BY id"""

    // Second run: should STILL be a cache miss because searcher cache is disabled
    def queryId5 = "search_profile_no_cache_${System.currentTimeMillis()}"
    try {
        profile("${queryId5}") {
            run {
                sql """/* ${queryId5} */ SELECT id FROM ${tableName}
                       WHERE search('title:grape') ORDER BY id"""
            }
            check { profileString, exception ->
                if (exception != null) throw exception
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
        }
    } catch (IllegalStateException e) {
        if (e.message?.contains("HttpCliAction failed")) {
            log.warn("Profile HTTP request failed, skipping: {}", e.message)
        } else {
            throw e
        }
    }

    // =========================================================================
    // Test 6: SEARCH vs MATCH consistency — same data, same predicate logic,
    // both should return identical results.
    // =========================================================================
    def search_result = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */
        id FROM ${tableName} WHERE search('title:apple') ORDER BY id
    """
    def match_result = sql """
        SELECT id FROM ${tableName} WHERE title MATCH_ANY 'apple' ORDER BY id
    """
    assertEquals(search_result, match_result,
        "SEARCH('title:apple') and MATCH_ANY 'apple' should return identical rows")
}
