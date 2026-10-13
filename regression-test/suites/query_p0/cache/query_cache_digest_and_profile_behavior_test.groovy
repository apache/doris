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
// Query Cache digest and profile tests
//
// Verify that the digest in the cache key (normalized SQL fingerprint) is correct:
//   1. Semantically equivalent SQL should have the same digest (between <=> >= and <=, conjunct order)
//   2. Semantically different SQL should have different digests (different predicate values, different HAVING)
//   3. Wrapper queries (such as Sort) share the digest of the bottom Agg->Scan subtree with bare queries
//   4. The digest stays the same with or without partition range predicates
//   5. Equivalent and non-equivalent HAVING clauses
//   6. Non-aggregate queries should not use Query Cache
//
// Core assertion methods:
//   - assertDigestEqual / assertDigestNotEqual: compare DIGEST values extracted from Explain
//   - assertMissAndInsertCache: the first execution should miss and have InsertCache=1
//   - assertHasCache: the second execution should hit
//   - noQueryCache: the profile contains no cache-related markers
// ===================================================================================

suite("query_cache_digest_and_profile_behavior_test") {
    def baseTable = "query_cache_digest_profile_base"
    def partTable = "query_cache_digest_profile_part"

    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_nereids_distribute_planner=true"
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
                assertTrue(profileString.contains("CacheTabletId"))
            }
        }
    }

    def assertContainsCacheHit = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  1"))
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
            }
        }
    }

    def getQueryCacheDigests = { String sqlStr ->
        def digests = []
        explain {
            sql sqlStr
            check { explainString ->
                logger.info("explainString: " + explainString)
                def matcher = explainString =~ /DIGEST:\s*([0-9a-f]+)/
                matcher.each { match ->
                    digests.add(match[1].toString())
                }
                return true
            }
        }
        return digests
    }

    def assertDigestEqual = { String sql1, String sql2 ->
        def digests1 = getQueryCacheDigests(sql1)
        def digests2 = getQueryCacheDigests(sql2)

        assertFalse(digests1.isEmpty())
        assertFalse(digests2.isEmpty())
        assertEquals(digests1[0], digests2[0])
    }

    def assertDigestOverlap = { String sql1, String sql2 ->
        def digests1 = getQueryCacheDigests(sql1)
        def digests2 = getQueryCacheDigests(sql2)

        assertFalse(digests1.isEmpty())
        assertFalse(digests2.isEmpty())
        assertTrue(digests1.any { digests2.contains(it) })
    }

    def assertDigestNotEqual = { String sql1, String sql2 ->
        def digests1 = getQueryCacheDigests(sql1)
        def digests2 = getQueryCacheDigests(sql2)

        assertFalse(digests1.isEmpty())
        assertFalse(digests2.isEmpty())
        assertTrue(digests1[0] != digests2[0])
    }

    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    sql """ DROP TABLE IF EXISTS ${baseTable} """
    sql """
        CREATE TABLE ${baseTable} (
            id int,
            region int,
            value int
        ) ENGINE=OLAP
        DUPLICATE KEY(id, region)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """ DROP TABLE IF EXISTS ${partTable} """
    sql """
        CREATE TABLE ${partTable} (
            dt date,
            id int,
            value int
        ) ENGINE=OLAP
        DUPLICATE KEY(dt, id)
        PARTITION BY RANGE(dt) (
            PARTITION p20251001 VALUES [('2025-10-01'), ('2025-10-02')),
            PARTITION p20251002 VALUES [('2025-10-02'), ('2025-10-03')),
            PARTITION p20251003 VALUES [('2025-10-03'), ('2025-10-04'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ${baseTable} VALUES
        (1, 10, 1),
        (1, 10, 2),
        (2, 20, 3),
        (2, 20, 4),
        (3, 30, 5),
        (4, 40, 6)
    """

    sql """
        INSERT INTO ${partTable} VALUES
        ('2025-10-01', 1, 1),
        ('2025-10-01', 2, 2),
        ('2025-10-02', 1, 3),
        ('2025-10-02', 2, 4),
        ('2025-10-03', 1, 5)
    """

    sql "sync"
    setSessionVariables()

    def equivalentSql1 = """
        select id, sum(value) as total_value
        from ${baseTable}
        where id between 1 and 3 and region > 0
        group by id
    """

    def equivalentSql2 = """
        select id, sum(value) as total_value
        from ${baseTable}
        where region > 0 and id >= 1 and id <= 3
        group by id
    """

    def differentSql = """
        select id, sum(value) as total_value
        from ${baseTable}
        where region > 10 and id >= 1 and id <= 3
        group by id
    """

    def plainAggSql = """
        select id, count(value) as cnt
        from ${baseTable}
        where id between 1 and 3
        group by id
    """

    // Wrapper query: add Sort above the same Agg->Scan subtree.
    // Sort has a single child, so doComputeCachePoint can recursively pass through it and find the bottom Agg->Scan.
    // Therefore, it shares the same DIGEST with noWrapSql and can reuse cache.
    def wrappedSql = """
        select id, cnt
        from (
            select id, count(value) as cnt
            from ${baseTable}
            where id between 1 and 3
            group by id
        ) agg
        order by cnt desc
    """

    def partitionAllSql = """
        select id, sum(value) as total_value
        from ${partTable}
        where value > 0
        group by id
    """

    def partitionRangeSql1 = """
        select id, sum(value) as total_value
        from ${partTable}
        where dt between '2025-10-01' and '2025-10-02' and value > 0
        group by id
    """

    def partitionRangeSql2 = """
        select id, sum(value) as total_value
        from ${partTable}
        where dt >= '2025-10-01' and dt <= '2025-10-02' and value > 0
        group by id
    """

    def partitionDifferentNonPartSql = """
        select id, sum(value) as total_value
        from ${partTable}
        where dt between '2025-10-01' and '2025-10-02' and value > 1
        group by id
    """

    def havingSql1 = """
        select id, sum(value) as total_value
        from ${baseTable}
        group by id
        having sum(value) > 2
    """

    def havingSql2 = """
        select sum(value) as total_value, id
        from ${baseTable}
        group by id
        having sum(value) > 2
    """

    def havingSql3 = """
        select id, sum(value) as total_value
        from ${baseTable}
        group by id
        having sum(value) > 3
    """

    def nonAggregateSql = """
        select id, value
        from ${baseTable}
        where id = 1
    """

    assertDigestEqual(equivalentSql1, equivalentSql2)
    assertDigestNotEqual(equivalentSql1, differentSql)
    assertDigestOverlap(plainAggSql, wrappedSql)
    assertDigestEqual(partitionAllSql, partitionRangeSql1)
    assertDigestEqual(partitionRangeSql1, partitionRangeSql2)
    assertDigestNotEqual(partitionRangeSql1, partitionDifferentNonPartSql)
    assertDigestEqual(havingSql1, havingSql2)
    assertDigestNotEqual(havingSql1, havingSql3)

    assertMissAndInsertCache(equivalentSql1)
    assertHasCache(equivalentSql2)

    assertMissAndInsertCache(plainAggSql)
    assertHasCache(plainAggSql)
    assertContainsCacheHit(wrappedSql)

    assertMissAndInsertCache(partitionRangeSql1)
    assertHasCache(partitionRangeSql2)

    assertMissAndInsertCache(havingSql1)
    assertHasCache(havingSql2)

    noQueryCache(nonAggregateSql)
        })
    }
}
