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

suite("ann_index_result_cache", "nonConcurrent") {
    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"

    String[][] backends = sql """ show backends """
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equalsIgnoreCase("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
        }
    }
    assertTrue(backendIdToBackendIP.size() > 0)

    def clearAnnTopnResultCache = {
        for (def backendId : backendIdToBackendIP.keySet()) {
            def url = backendIdToBackendIP.get(backendId) + ":" +
                    backendIdToBackendHttpPort.get(backendId) + "/api/clear_cache/AnnIndexResultCache"
            httpTest {
                endpoint ""
                uri url
                op "get"
                body ""
                check { respCode, body ->
                    assertEquals("${respCode}".toString(), "200")
                    assertTrue("${body}".toString().contains("prune win"))
                }
            }
        }
    }

    def checkAnnCacheHit = { String debugPoint, Map debugParams, String query ->
        def params = debugParams + [execute: 1]
        GetDebugPoint().enableDebugPointForAllBEs(debugPoint, params)
        sql query
    }

    def checkTopnCacheHit = { Map debugParams, String query ->
        checkAnnCacheHit("olap_scanner.ann_topn_cache_hits", debugParams, query)
    }

    def checkRangeCacheHit = { Map debugParams, String query ->
        checkAnnCacheHit("olap_scanner.ann_range_cache_hits", debugParams, query)
    }

    def timeout = 60000
    def deltaTime = 1000
    def wait_for_latest_op_on_table_finish = { tableName, opTimeout ->
        int useTime = 0
        for (int t = deltaTime; t <= opTimeout; t += deltaTime) {
            def alterRes = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            def alterResStr = alterRes.toString()
            if (alterResStr.contains("FINISHED")) {
                sleep(3000)
                break
            }
            useTime = t
            sleep(deltaTime)
        }
        assertTrue(useTime <= opTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    // ======================== Setup ========================
    sql "drop table if exists ann_result_cache_test"
    sql """
        CREATE TABLE ann_result_cache_test (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO ann_result_cache_test VALUES
        (1, [1.0, 2.0, 3.0]),
        (2, [1.1, 2.1, 3.1]),
        (3, [1.2, 2.2, 3.2]),
        (4, [1.3, 2.3, 3.3]),
        (5, [1.4, 2.4, 3.4]),
        (6, [1.5, 2.5, 3.5]),
        (7, [5.0, 5.0, 5.0]),
        (8, [8.0, 8.0, 8.0]),
        (9, [9.0, 9.0, 9.0]);
    """

    clearAnnTopnResultCache()

    // ======================== Test 1: TopN cache without prefilter ========================
    sql """
        select id
        from ann_result_cache_test
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    checkTopnCacheHit([min_hits: 1], """
        select id
        from ann_result_cache_test
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)

    // ======================== Test 2: TopN cache WITH prefilter (bitmap hashing) ========================
    clearAnnTopnResultCache()

    sql """
        select id
        from ann_result_cache_test
        where id < 8
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    checkTopnCacheHit([min_hits: 1], """
        select id
        from ann_result_cache_test
        where id < 8
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)

    // ======================== Test 3: Different prefilter => cache miss ========================
    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_result_cache_test
        where id < 6
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)

    // ======================== Test 4: Range search cache without prefilter ========================
    clearAnnTopnResultCache()

    sql """
        select id
        from ann_result_cache_test
        where l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 1.0
        order by id;
    """

    checkRangeCacheHit([min_hits: 1], """
        select id
        from ann_result_cache_test
        where l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 1.0
        order by id;
    """)

    // ======================== Test 5: Range search cache with prefilter ========================
    clearAnnTopnResultCache()

    sql """
        select id
        from ann_result_cache_test
        where id < 8 and l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 1.0
        order by id;
    """

    checkRangeCacheHit([min_hits: 1], """
        select id
        from ann_result_cache_test
        where id < 8 and l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 1.0
        order by id;
    """)

    // ======================== Test 6: Range search different radius => cache miss ========================
    checkRangeCacheHit([expected_hits: 0], """
        select id
        from ann_result_cache_test
        where l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 2.0
        order by id;
    """)

    // ======================== Test 7: Cache clear API works for range search too ========================
    clearAnnTopnResultCache()

    checkRangeCacheHit([expected_hits: 0], """
        select id
        from ann_result_cache_test
        where l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 1.0
        order by id;
    """)

    // ======================== Test 8: Drop index => fallback, cache stays 0 ========================
    sql "drop index idx_emb on ann_result_cache_test"

    wait_for_latest_op_on_table_finish("ann_result_cache_test", timeout)

    sql """
        select id
        from ann_result_cache_test
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_result_cache_test
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)

    checkRangeCacheHit([expected_hits: 0], """
        select id
        from ann_result_cache_test
        where l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 1.0
        order by id;
    """)
    GetDebugPoint().clearDebugPointsForAllBEs()
}
