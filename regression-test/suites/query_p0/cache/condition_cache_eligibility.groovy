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

// Isolate the global lookup metric from other condition-cache queries.
suite("condition_cache_eligibility", "nonConcurrent") {
    // Internal statistics queries use new sessions and would otherwise pollute the global metric.
    setGlobalVarTemporary([enable_condition_cache: false]) {
        sql "DROP TABLE IF EXISTS condition_cache_eligibility"
        sql """
            CREATE TABLE condition_cache_eligibility (
                k BIGINT,
                a ARRAY<INT>,
                dt1 DATETIME,
                dt2 DATETIME
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO condition_cache_eligibility
            SELECT number, [1,2,3,4,5,6,7], '2026-09-30 12:00:00', '2026-09-30 12:00:00'
            FROM numbers("number" = "100000")
        """

        sql "set enable_parallel_scan = false"
        sql "set enable_sql_cache = false"
        sql "set enable_query_cache = false"
        sql "set enable_condition_cache = true"

        def backendIdToIp = [:]
        def backendIdToHttpPort = [:]
        getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
        def cacheLookups = {
            long total = 0
            backendIdToIp.each { backendId, ip ->
                httpTest {
                    endpoint "${ip}:${backendIdToHttpPort[backendId]}"
                    uri "/metrics?type=json"
                    op "get"
                    printResponse false
                    check { code, body ->
                        assertEquals(200, code)
                        def metric = parseJson(body).find {
                            it.tags.metric == "condition_cache" && it.tags.type == "condition_cache_search"
                        }
                        assertNotNull(metric, "Missing condition-cache lookup metric")
                        total += metric.value as long
                    }
                }
            }
            return total
        }

        // Seeded shuffle is repeatable within a block, but its count changes with block boundaries.
        // Check that volatile scans never look up the cache instead of recording their random counts.
        for (int batchSize : [1, 1024]) {
            sql "set batch_size = ${batchSize}"
            for (String predicate : [
                    "k < 0 OR rand(1) < 0.0001",
                    "k < 0 OR rand() < 0.0001",
                    "array_shuffle(a) = [1,2,3,4,5,6,7]",
                    "array_shuffle(a, 1) = [1,2,3,4,5,6,7]",
                    "shuffle(a, 1) = [1,2,3,4,5,6,7]",
                    "CAST(TIMEDIFF(dt1, dt2) AS DATE) = CURRENT_DATE()",
                    "CAST(TIMEDIFF(dt1, dt2) AS DATETIME) = CAST(CURRENT_DATE() AS DATETIME)",
                    "CAST(ARRAY(TIMEDIFF(dt1, dt2)) AS ARRAY<DATETIME>)[1] = CAST(CURRENT_DATE() AS DATETIME)",
                    "CAST(MAP(1, TIMEDIFF(dt1, dt2)) AS MAP<INT, DATETIME>)[1] = CAST(CURRENT_DATE() AS DATETIME)",
                    "STRUCT_ELEMENT(CAST(STRUCT(k, TIMEDIFF(dt1, dt2)) AS STRUCT<id:BIGINT, d:DATETIME>), 2)"
                            + " = CAST(CURRENT_DATE() AS DATETIME)"]) {
                long before = cacheLookups()
                2.times {
                    // Keep one-row batches inexpensive under ASAN while retaining the volatile scan predicate.
                    sql "SELECT count(*) FROM condition_cache_eligibility WHERE k < 1000 AND (${predicate})"
                }
                assertEquals(before, cacheLookups(),
                        "Volatile scan looked up condition cache: batch_size=${batchSize}, ${predicate}")
            }
        }

        sql "DROP TABLE IF EXISTS condition_cache_rf_probe"
        sql """
            CREATE TABLE condition_cache_rf_probe (k BIGINT, s STRING NOT NULL)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO condition_cache_rf_probe
            SELECT number, '00:00:00' FROM numbers("number" = "10000")
        """
        sql "DROP TABLE IF EXISTS condition_cache_rf_build"
        sql """
            CREATE TABLE condition_cache_rf_build (d DATETIME NOT NULL)
            DUPLICATE KEY(d) DISTRIBUTED BY HASH(d) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        // Keep the expected join count stable even if these executions cross midnight.
        sql """
            INSERT INTO condition_cache_rf_build VALUES
                (CURRENT_DATE() - INTERVAL 1 DAY), (CURRENT_DATE()), (CURRENT_DATE() + INTERVAL 1 DAY)
        """
        sql "set enable_runtime_filter_prune = false"
        sql "set runtime_filter_wait_infinitely = true"
        sql "set disable_join_reorder = true"
        def rfQuery = """
            SELECT count(*) FROM condition_cache_rf_probe p
            JOIN [broadcast] condition_cache_rf_build b
            ON CAST(CAST(p.s AS TIME) AS DATETIME) = b.d
        """
        explain {
            sql rfQuery
            contains "RF"
            contains "CAST(CAST"
        }
        long beforeRuntimeFilter = cacheLookups()
        order_qt_volatile_rf_first rfQuery
        order_qt_volatile_rf_second rfQuery
        assertEquals(beforeRuntimeFilter, cacheLookups(), "Volatile runtime-filter probe looked up condition cache")

        sql "set enable_profile = false"
        def token = UUID.randomUUID().toString()
        def deterministic = """
            SELECT '${token}', count(*) FROM condition_cache_eligibility WHERE k % 10000 = 0
        """
        long before = cacheLookups()
        // Execute the same SQL twice so both scans have the same digest; omit the profile token from output.
        quickRunTest("deterministic_first", deterministic, true, { row -> [row[1]] })
        // Only the second execution records a profile, so the token cannot select the warm-up query.
        sql "set enable_profile = true"
        sql "set profile_level = 2"
        quickRunTest("deterministic_second", deterministic, true, { row -> [row[1]] })
        assertTrue(cacheLookups() > before, "Deterministic scans did not look up condition cache")
        def profile = new ProfileAction(context).getProfileBySql(token, ["ConditionCacheHit"])
        def hits = (profile =~ /ConditionCacheHit: (\d+)/)
        assertTrue(hits.findAll().any { (it[1] as long) > 0 },
                "Second deterministic scan did not hit condition cache: ${profile}")

        order_qt_null_expressions """
            SELECT count(*) FROM condition_cache_eligibility
            WHERE CAST(CAST(NULL AS TIME) AS DATETIME) IS NULL
                AND array_shuffle(CAST(NULL AS ARRAY<INT>), 1) IS NULL
                AND shuffle(CAST(NULL AS ARRAY<INT>)) IS NULL
                AND array_shuffle(a, NULL) IS NULL
        """
    }
}
