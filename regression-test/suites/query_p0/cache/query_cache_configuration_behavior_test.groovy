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

// ===================================================================================
// Query Cache configuration tests
//
// Verify that FE/BE Query Cache configuration boundaries do not break correctness:
//   1. enable_query_cache：
//      Session-level Query Cache master switch. When disabled, FE does not send query_cache_param to BE,
//      the profile should not contain HitCache; after re-enabling it, Query Cache should insert and hit entries.
//   2. query_cache_force_refresh：
//      Force-refresh Query Cache. When enabled, BE does not return existing cached results, but re-executes
//      the query and writes a new cache entry; after disabling it, the same query should hit the refreshed cache.
//   3. query_cache_entry_max_bytes：
//      Maximum bytes allowed for a single cache entry. When accumulated result bytes exceed the threshold,
//      BE uses pass-through and does not write Query Cache, while query results must remain correct.
//   4. query_cache_entry_max_rows：
//      Maximum rows allowed for a single cache entry. When accumulated result rows exceed the threshold,
//      Query Cache is also not written; raising the threshold should restore inserts and hits.
//   Test prerequisite: cache_last_version_interval_second:
//      This is the wait time FE uses to decide whether the latest table version is stable enough.
//      This suite temporarily sets it to 0 to avoid skipping cache analysis right after inserts and
//      to keep other configuration assertions stable.
// ===================================================================================

suite("query_cache_configuration_behavior_test") {

    // Query Cache keys on the BE side include tablet granularity. This case uses HitCache /
    // InsertCache in the profile to validate all cache entries involved in the query, so the test table
    // must ensure every scanned tablet has data. With explicit BUCKETS 1, each partition has only one tablet;
    // inserting data into every partition avoids unstable hit/insert statistics caused by empty tablets
    // with multiple buckets.
    def createSingleBucketTestTable = { String tableName ->
        sql """drop table if exists ${tableName} force"""
        sql """
            create table ${tableName}
            (
                id int,
                value int
            )
            partition by range(id)
            (
                partition p1 values[('1'), ('2')),
                partition p2 values[('2'), ('3')),
                partition p3 values[('3'), ('4')),
                partition p4 values[('4'), ('5')),
                partition p5 values[('5'), ('6'))
            )
            distributed by hash(id) BUCKETS 1
            properties(
                'replication_num'='1'
            )
        """

        sql """
            insert into ${tableName}
            values (1, 1), (1, 2),
                   (2, 1), (2, 2),
                   (3, 1), (3, 2),
                   (4, 1), (4, 2),
                   (5, 1), (5, 2)
        """

        sql "sync"
    }

    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        // SQL Cache is a FE-side whole-SQL result cache, while Query Cache is a BE-side cache by
        // cacheable subtree/tablet.
        // This case only verifies Query Cache, so SQL Cache is disabled to avoid interactions between
        // the two cache types.
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
        sql "set query_cache_force_refresh=false"
    }

    def assertMissAndInsertCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
                assertTrue(profileString.contains("InsertCache:  1"))
                assertTrue(profileString.contains("CacheTabletId"))
            }
        }
    }

    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  1"))
                assertFalse(profileString.contains("HitCache:  0"))
            }
        }
    }

    def assertPartHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  1"))
                assertTrue(profileString.contains("HitCache:  0"))
            }
        }
    }

    def assertNoCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    def assertNoInsertCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
                assertFalse(profileString.contains("InsertCache:  1"))
            }
        }
    }

    def assertInsertCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("InsertCache:  1"))
                assertTrue(profileString.contains("CacheTabletId"))
            }
        }
    }

    def noQueryCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertFalse(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
                assertFalse(profileString.contains("InsertCache:  1"))
                assertFalse(profileString.contains("CacheTabletId"))
            }
        }
    }

    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {
            combineFutures(
            extraThread("testEnableQueryCacheSwitch", {
                // Verify the enable_query_cache master switch:
                //   - false: query results are correct, but the profile has no Query Cache HitCache information;
                //   - true: the first execution misses and inserts, and later executions hit;
                //   - disabling and re-enabling it should not break already inserted cache entries.
                createSingleBucketTestTable "query_cache_enable_switch_table"

                setSessionVariables()

                def sql_str = """
                    select id, count(value)
                    from query_cache_enable_switch_table
                    group by id
                    order by id
                """

                sql """set enable_query_cache=false"""

                def expected = sql sql_str
                def disabledRes = sql sql_str
                assertEquals(expected, disabledRes)
                noQueryCache sql_str

                sql """set enable_query_cache=true"""

                assertMissAndInsertCache sql_str

                def firstRes = sql sql_str
                assertEquals(expected, firstRes)

                def secondRes = sql sql_str
                assertEquals(expected, secondRes)
                assertHasCache sql_str

                sql """set enable_query_cache=false"""

                def disabledAgainRes = sql sql_str
                assertEquals(expected, disabledAgainRes)
                noQueryCache sql_str

                sql """set enable_query_cache=true"""

                def reenabledRes = sql sql_str
                assertEquals(expected, reenabledRes)
                assertHasCache sql_str
            }),
            extraThread("testQueryCacheForceRefresh", {
                // Verify query_cache_force_refresh:
                //   - first insert and hit the existing cache in normal mode;
                //   - after enabling force refresh, do not return old cached results, but still re-execute
                //     and write a new cache;
                //   - after disabling force refresh, the same query should hit the newly written cache.
                createSingleBucketTestTable "query_cache_force_refresh_table"

                setSessionVariables()

                def sql_str = """
                    select id, count(value + 10)
                    from query_cache_force_refresh_table
                    group by id
                    order by id
                """

                sql """set enable_query_cache=false"""
                def expected = sql sql_str

                sql """set enable_query_cache=true"""
                sql """set query_cache_force_refresh=false"""

                assertMissAndInsertCache sql_str

                def firstRes = sql sql_str
                assertEquals(expected, firstRes)

                def secondRes = sql sql_str
                assertEquals(expected, secondRes)
                assertHasCache sql_str

                sql """set query_cache_force_refresh=true"""

                def refreshRes = sql sql_str
                assertEquals(expected, refreshRes)
                assertMissAndInsertCache sql_str

                sql """set query_cache_force_refresh=false"""

                def thirdRes = sql sql_str
                assertEquals(expected, thirdRes)
                assertHasCache sql_str
            }),
            extraThread("testQueryCacheEntryMaxBytes", {
                // Verify query_cache_entry_max_bytes:
                //   - when set to 0, any non-empty result exceeds the byte threshold, so BE only passes
                //     results through and does not write cache;
                //   - after increasing it to 10 MB, the same query should restore normal miss+insert
                //     behavior and then hit.
                def tb_name = "query_cache_entry_max_bytes_table"
                createSingleBucketTestTable tb_name

                setSessionVariables()
                sql """set query_cache_entry_max_bytes=0"""

                def sql_str = "select id, id + 1, 1, count(value+10) from ${tb_name} group by id, id+1 order by id;"
                def expected = sql sql_str
                def firstRes = sql sql_str
                assertEquals(expected, firstRes)
                assertNoInsertCache sql_str

                def secondRes = sql sql_str
                assertEquals(expected, secondRes)
                assertNoInsertCache sql_str

                sql """set query_cache_entry_max_bytes=10485760"""

                assertMissAndInsertCache sql_str

                def thirdRes = sql sql_str
                assertEquals(expected, thirdRes)

                def fourthRes = sql sql_str
                assertEquals(expected, fourthRes)
                assertHasCache sql_str
            }),
            extraThread("testQueryCacheEntryMaxRows", {
                // Verify query_cache_entry_max_rows:
                //   - insert multiple id groups into each single-bucket partition so the aggregate result
                //     exceeds one row;
                //   - when the threshold is 1, the cache entry exceeds the row limit and is not written;
                //   - after increasing it to 1000, insert and hit behavior is restored.
                def tb_name = "query_cache_entry_max_rows_table"

                sql """drop table if exists ${tb_name}"""
                sql """
                    create table ${tb_name}
                    (
                    id int,
                    value int
                    )
                    partition by range(id)
                    (
                    partition p1 values[('10'), ('20')),
                    partition p2 values[('20'), ('30')),
                    partition p3 values[('30'), ('40')),
                    partition p4 values[('40'), ('50')),
                    partition p5 values[('50'), ('60'))
                    )
                    distributed by hash(id) BUCKETS 1
                    properties(
                    'replication_num'='1'
                    );"""

                sql """
                    insert into ${tb_name}
                    values 
                    (10, 1), (11, 2),(12, 1), (13, 2),(14, 1), (15, 2),
                    (20, 1), (21, 2),(22, 1), (23, 2),(24, 1), (25, 2),
                    (30, 1), (31, 2),(32, 1), (33, 2),(34, 1), (35, 2),
                    (40, 1), (41, 2),(42, 1), (43, 2),(44, 1), (45, 2),
                    (50, 1), (51, 2),(52, 1), (53, 2),(54, 1), (55, 2);
                    """

                setSessionVariables()
                sql """set query_cache_entry_max_rows=1"""

                def sql_str = "select id, count(value) from ${tb_name} group by id order by id;"
                def expected = sql sql_str
                
                def firstRes = sql sql_str
                assertEquals(expected, firstRes)
                assertNoInsertCache sql_str

                def secondRes = sql sql_str
                assertEquals(expected, secondRes)
                assertNoInsertCache sql_str

                sql """set query_cache_entry_max_rows=1000"""

                assertMissAndInsertCache sql_str

                def thirdRes = sql sql_str
                assertEquals(expected, thirdRes)

                def fourthRes = sql sql_str
                assertEquals(expected, fourthRes)
                assertHasCache sql_str
            })
            ).get()
        })
    }
}
