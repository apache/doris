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

suite("ann_index_topn_cache", "nonConcurrent") {
    sql "unset variable all;"
    sql "set enable_segment_limit_pushdown=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"

    String[][] backends = sql """ show backends """
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    for (String[] backend in backends) {
        // backend[9] is Alive in current SHOW BACKENDS format
        if (backend[9].equalsIgnoreCase("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
        }
    }
    assertTrue(backendIdToBackendIP.size() > 0)

    sql "drop table if exists ann_index_topn_cache"
    sql """
        CREATE TABLE ann_index_topn_cache (
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
        INSERT INTO ann_index_topn_cache VALUES
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

    def timeout = 60000
    def deltaTime = 1000
    def wait_for_latest_op_on_table_finish = { tableName, opTimeout ->
        int useTime = 0
        for (int t = deltaTime; t <= opTimeout; t += deltaTime) {
            def alterRes = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            def alterResStr = alterRes.toString()
            if (alterResStr.contains("FINISHED")) {
                sleep(3000)
                logger.info("${tableName} latest alter job finished, detail: ${alterResStr}")
                break
            }
            useTime = t
            sleep(deltaTime)
        }
        assertTrue(useTime <= opTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

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

    def checkTopnCacheHit = { Map debugParams, String query ->
        def debugPoint = "olap_scanner.ann_topn_cache_hits"
        def params = debugParams + [execute: 1]
        GetDebugPoint().enableDebugPointForAllBEs(debugPoint, params)
        sql query
    }

    // 1) No prefilter: second identical ANN TopN query should hit cache.
    sql """
        select id
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    checkTopnCacheHit([min_hits: 1], """
        select id
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)

    // 1.1) Manual clear cache API: after clear, next same query should be cache miss.
    clearAnnTopnResultCache()
    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)

    // 1.2) Session variable can disable ANN result cache.
    clearAnnTopnResultCache()
    sql "set enable_ann_index_result_cache=false;"
    sql """
        select id
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """
    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)
    sql "set enable_ann_index_result_cache=true;"

    // 2) With prefilter (id < 8): ANN TopN result cache should hit on second query
    //    (prefilter bitmap is hashed into the cache key).
    clearAnnTopnResultCache()
    sql """
        select id
        from ann_index_topn_cache
        where id < 8
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 2;
    """

    checkTopnCacheHit([min_hits: 1], """
        select id
        from ann_index_topn_cache
        where id < 8
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 2;
    """)

    // 3) After dropping ANN index, query should still run (fallback path), and TopN cache hit must stay 0.
    sql "drop index idx_emb on ann_index_topn_cache"
    wait_for_latest_op_on_table_finish("ann_index_topn_cache", timeout)

    sql """
        select id
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """

    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_topn_cache
        order by l2_distance_approximate(embedding, [1.0,2.0,3.0])
        limit 3;
    """)
    GetDebugPoint().clearDebugPointsForAllBEs()
}
