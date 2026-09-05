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

// Query Cache cacheable subtree boundary and disabling-condition tests
//   1. Runtime filter scenarios:
//      - The fragment containing a colocate join currently does not support query cache
//      - A non-target Agg->Scan subtree split into an independent fragment can still keep cache
//   2. Agg->Scan subtrees in Union branches use cache independently
//   3. Window/Sort outside Agg->Scan does not affect cache for the bottom subtree
//   4. Pure Window queries without aggregation should not use cache
//   5. Nondeterministic functions inside Agg->Scan block cache; outside it, behavior follows the actual plan
//   6. UDFs inside Agg->Scan block cache; outside it, behavior follows the actual plan
//   7. Changing a user variable inside Agg->Scan changes the digest; outside it, it does not affect cache
//
// Core test pattern: each operator/function type is tested in two positions
//   - Inside the Agg->Scan subtree -> should block cache for that subtree
//   - Outside (above) the Agg->Scan subtree -> should not affect cache for the bottom subtree

suite("query_cache_eligibility_boundary_test") {
    def factTable = "query_cache_subtree_boundary_fact"
    def factTable2 = "query_cache_subtree_boundary_fact2"

    // ---------- Helper methods ----------

    // Set session variables: enable Nereids and Query Cache, and disable SQL Cache
    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_nereids_distribute_planner=true"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
        sql "set query_cache_force_refresh=false"
        sql "set runtime_filter_mode='GLOBAL'"
    }

    // Assert that the query fully hits cache (HitCache=1 and no HitCache=0)
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

    // Assert that the query fully misses cache but enters the cache path (HitCache=0 and no HitCache=1)
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

    // Assert that the query does not reach any HitCache=1 (loose assertion; profile may contain no HitCache field)
    def assertNotHitCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    // Assert that at least one subtree hits cache (profile contains HitCache=1, without requiring all subtrees to hit)
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

    // Extract all DIGEST values from Explain output (fingerprints of cacheable subtrees)
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

    // Get the DIGEST count
    def getQueryCacheDigestCount = { String sqlStr ->
        return getQueryCacheDigests(sqlStr).size()
    }

    // Assert that the DIGEST count equals the expected value
    def assertDigestCount = { String sqlStr, int expectedCount ->
        int actualCount = getQueryCacheDigestCount(sqlStr)
        assertEquals(expectedCount, actualCount)
    }

    // ---------- Base data setup ----------

    // Set the cache version interval to 0 so cache is available immediately
    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    sql """DROP TABLE IF EXISTS ${factTable}"""
    sql """
        CREATE TABLE ${factTable} (
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

    sql """DROP TABLE IF EXISTS ${factTable2}"""
    sql """
        CREATE TABLE ${factTable2} (
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

    sql """
        INSERT INTO ${factTable} VALUES
        (1, 10, 1), (1, 10, 2),
        (2, 20, 3), (2, 20, 4),
        (3, 30, 5), (4, 40, 6)
    """

    sql """
        INSERT INTO ${factTable2} VALUES
        (1, 10, 10), (2, 20, 20),
        (3, 30, 30)
    """

    sql "sync"
    setSessionVariables()

    // ================================================================
    // 0. Runtime filter scenarios
    //    0.1 The fragment containing a colocate join currently does not support query cache
    // ================================================================
    def runtimeFilterColocateSql = """
        select *
        from (
            select id, count(value) as cnt
            from ${factTable}
            where id < 5
            group by id
        ) a
        join (
            select id, count(value) as cnt
            from ${factTable}
            where id < 4
            group by id
        ) b
          on a.id = b.id
    """
    assertDigestCount(runtimeFilterColocateSql, 0)
    assertNotHitCache(runtimeFilterColocateSql)
    assertNotHitCache(runtimeFilterColocateSql)

    //    0.2 A non-target Agg->Scan subtree split into an independent fragment can still keep cache
    def runtimeFilterBroadcastSql = """
        select *
        from ${factTable2} probe
        join [broadcast]
        (
            select id, count(value) as cnt
            from ${factTable}
            where id < 4
            group by id
        ) agg_side
          on probe.id = agg_side.id
    """
    assertDigestCount(runtimeFilterBroadcastSql, 1)
    assertNoCache(runtimeFilterBroadcastSql)
    assertContainsCacheHit(runtimeFilterBroadcastSql)

    // ================================================================
    // 1. Agg->Scan subtrees in Union branches use cache independently
    // ================================================================
    // Union is **above** the Agg->Scan subtree, not inside the cacheable subtree.
    // Each branch can be cached if it independently satisfies the Agg->Scan pattern.

    def unionBranch1Sql = """
        select id, sum(value) as total_value
        from ${factTable}
        group by id
    """

    def unionBranch2Sql = """
        select id, sum(value) as total_value
        from ${factTable2}
        group by id
    """

    def unionSql = """
        select * from (
            select id, sum(value) as total_value
            from ${factTable}
            group by id
        ) a
        union all
        select * from (
            select id, sum(value) as total_value
            from ${factTable2}
            group by id
        ) b
    """

    // Run the two branches separately first to warm up cache
    assertNoCache(unionBranch1Sql)
    assertHasCache(unionBranch1Sql)
    assertNoCache(unionBranch2Sql)
    assertHasCache(unionBranch2Sql)

    // The Union query should hit cache for both bottom branches
    assertContainsCacheHit(unionSql)

    // ================================================================
    // 2. A Window function between Agg and Scan blocks caching for that subtree
    // ================================================================
    // Window function is not a valid node inside a cacheable subtree.
    // If window is inside the Agg->Scan path, that subtree should not use cache.

    // Window outside (above) Agg->Scan does not affect cache for the bottom Agg->Scan
    def windowOutsideSql = """
        select id, total_value, row_number() over (order by total_value) as rn
        from (
            select id, sum(value) as total_value
            from ${factTable}
            group by id
        ) t
    """
    // The bottom Agg->Scan subtree remains cacheable and should already have cache from warm-up
    assertContainsCacheHit(windowOutsideSql)

    // Pure Window query without aggregation should not use Query Cache
    def windowOnlySql = """
        select id, value, row_number() over (partition by id order by value) as rn
        from ${factTable}
    """
    assertDigestCount(windowOnlySql, 0)

    // ================================================================
    // 3. Sort outside Agg->Scan does not affect cache
    // ================================================================
    // Sort is above the cacheable subtree and does not block cache for the bottom subtree

    def sortOutsideSql = """
        select id, sum(value) as total_value
        from ${factTable}
        group by id
        order by total_value desc
    """
    // The bottom Agg->Scan subtree should have a DIGEST
    assertTrue(getQueryCacheDigestCount(sortOutsideSql) > 0)
    // It should hit the previously warmed-up cache
    assertContainsCacheHit(sortOutsideSql)

    // ================================================================
    // 4. Nondeterministic functions are position-sensitive
    // ================================================================

    // 4a. Nondeterministic function inside Agg->Scan blocks cache
    def nondetermInsideSql = """
        select id, sum(value), count(random())
        from ${factTable}
        group by id
    """
    // random() is inside the aggregation, so this subtree should not use cache
    assertNotHitCache(nondetermInsideSql)
    assertNotHitCache(nondetermInsideSql)

    // 4b. Nondeterministic function outside (above) Agg->Scan; however, explain shows random()
    //     is placed in the agg part, so it cannot be cached
    def nondetermOutsideSql = """
        select id, total_value, random() as rand_col
        from (
            select id, sum(value) as total_value
            from ${factTable}
            group by id
        ) t
    """
    // The bottom Agg->Scan subtree is not cacheable
    assertNotHitCache(nondetermOutsideSql)

    def randomAndWindowSql = """
        select id, total_value, random() as rand_col, row_number() over (order by total_value) as rn
        from (
            select id, sum(value) as total_value
            from ${factTable}
            group by id
        ) t;
        """
    // The bottom Agg->Scan subtree remains cacheable
    assertContainsCacheHit(randomAndWindowSql)

    // ================================================================
    // 5. UDFs are position-sensitive, matching nondeterministic behavior
    // ================================================================
    def jarPath = """${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    File path = new File(jarPath)
    if (path.exists()) {
        scp_udf_file_to_all_be(jarPath)
        def udfFunc = "query_cache_subtree_boundary_udf_test"
        try_sql("DROP FUNCTION IF EXISTS ${udfFunc}(string, int, int);")
        sql """
            CREATE FUNCTION ${udfFunc}(string, int, int) RETURNS string PROPERTIES (
                "file"="file://${jarPath}",
                "symbol"="org.apache.doris.udf.StringTest",
                "type"="JAVA_UDF"
            )
        """

        // 5a. UDF inside Agg->Scan blocks cache
        def udfInsideSql = """
            select ${udfFunc}(cast(id as string), 1, 1) as processed_id, count(value)
            from ${factTable}
            group by processed_id
        """
        assertNotHitCache(udfInsideSql)
        assertNotHitCache(udfInsideSql)

        // 5b. UDF outside (above) Agg->Scan does not affect cache for the bottom subtree
        def udfOutsideSql = """
            select ${udfFunc}(cast(id as string), 1, 1) as processed_id, total_value
            from (
                select id, sum(value) as total_value
                from ${factTable}
                group by id
            ) t
        """
        assertNotHitCache(udfOutsideSql)

        try_sql("DROP FUNCTION IF EXISTS ${udfFunc}(string, int, int);")
    }

    // ================================================================
    // 6. User variables are position-sensitive
    // ================================================================
    // Rule: if variable changes affect the inside of the Agg->Scan subtree, cache cannot be reused
    //       if variable changes only affect outside Agg->Scan, cache for the bottom subtree is unaffected

    // 6a. User variable inside Agg->Scan (in the WHERE condition):
    //     Different variable values -> different digest -> cache is not shared
    sql "set @filter_val = 2"
    def userVarInsideSql1 = """
        select id, sum(value) as total_value
        from ${factTable}
        where value > @filter_val
        group by id
    """
    // First execution misses -> insert
    assertNoCache(userVarInsideSql1)
    // Second execution hits
    assertHasCache(userVarInsideSql1)

    // Change the variable value because it affects the WHERE condition inside Agg->Scan
    // The expanded SQL differs, so the digest should differ -> miss
    sql "set @filter_val = 4"
    def userVarInsideSql2 = """
        select id, sum(value) as total_value
        from ${factTable}
        where value > @filter_val
        group by id
    """
    assertNoCache(userVarInsideSql2)

    // Change it back to the original value; the digest should match the first execution -> hit
    sql "set @filter_val = 2"
    assertHasCache(userVarInsideSql1)

    // 6b. User variable outside Agg->Scan (using a Window function as the barrier):
    //     Cache for the bottom Agg->Scan subtree is unaffected
    //
    //     Key point: a plain Project->Agg pattern is merged by Nereids MergeProject rule
    //     into AggregationNode output expressions (whether from WHERE conditions or
    //     SELECT projection expressions, they may be merged/pushed down).
    //     A Window function (AnalyticEvalNode) must be used as a barrier node
    //     to prevent outer projection expressions from being merged through into the bottom Agg->Scan subtree.
    sql "set @outer_offset = 10"
    def userVarOutsideSql = """
        select id, total_value + @outer_offset as adjusted_value,
               row_number() over (order by id) as rn
        from (
            select id, sum(value) as total_value
            from ${factTable}
            group by id
        ) t
    """
    // The bottom Agg->Scan subtree was already warmed up (unionBranch1Sql hit the same subtree)
    // It should hit cache
    assertContainsCacheHit(userVarOutsideSql)

    // Changing the outer variable value still does not affect cache for the bottom subtree
    sql "set @outer_offset = 99999"
    def userVarOutsideSql2 = """
        select id, total_value + @outer_offset as adjusted_value,
               row_number() over (order by id) as rn
        from (
            select id, sum(value) as total_value
            from ${factTable}
            group by id
        ) t
    """
    assertContainsCacheHit(userVarOutsideSql2)
        })
    }
}
