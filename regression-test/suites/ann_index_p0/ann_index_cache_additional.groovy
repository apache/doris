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

suite("ann_index_cache_additional", "nonConcurrent") {
    sql "unset variable all;"
    sql "set enable_segment_limit_pushdown=true;"
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

    def clearAnnResultCache = {
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

    sql "drop table if exists ann_index_cache_ip"
    sql """
        CREATE TABLE ann_index_cache_ip (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="inner_product",
                "dim"="4"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO ann_index_cache_ip VALUES
        (1, [0.1, 0.2, 0.3, 0.4]),
        (2, [0.5, 0.6, 0.7, 0.8]),
        (3, [1.0, 1.0, 1.0, 1.0]),
        (4, [0.0, 0.1, 0.0, 0.1]);
    """

    clearAnnResultCache()

    sql """
        select id
        from ann_index_cache_ip
        order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) desc
        limit 2;
    """
    checkTopnCacheHit([min_hits: 1], """
        select id
        from ann_index_cache_ip
        order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) desc
        limit 2;
    """)

    clearAnnResultCache()

    sql """
        select id
        from ann_index_cache_ip
        order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) asc
        limit 2;
    """
    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_cache_ip
        order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) asc
        limit 2;
    """)

    sql "drop table if exists ann_index_cache_metric_mismatch"
    sql """
        CREATE TABLE ann_index_cache_metric_mismatch (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="4"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql "insert into ann_index_cache_metric_mismatch select * from ann_index_cache_ip"

    clearAnnResultCache()

    sql """
        select id
        from ann_index_cache_metric_mismatch
        order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) desc
        limit 2;
    """
    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_cache_metric_mismatch
        order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) desc
        limit 2;
    """)

    clearAnnResultCache()

    sql """
        select id
        from ann_index_cache_metric_mismatch
        order by l2_distance_approximate(embedding, [0.1,0.2,0.3,0.4]) desc
        limit 2;
    """
    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_cache_metric_mismatch
        order by l2_distance_approximate(embedding, [0.1,0.2,0.3,0.4]) desc
        limit 2;
    """)

    clearAnnResultCache()

    sql """
        select id
        from ann_index_cache_ip
        where inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) >= 0.6
        order by id;
    """
    checkRangeCacheHit([min_hits: 1], """
        select id
        from ann_index_cache_ip
        where inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) >= 0.6
        order by id;
    """)

    clearAnnResultCache()

    sql """
        select id
        from ann_index_cache_metric_mismatch
        where inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) >= 0.6
        order by id;
    """
    checkRangeCacheHit([expected_hits: 0], """
        select id
        from ann_index_cache_metric_mismatch
        where inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) >= 0.6
        order by id;
    """)

    sql "drop table if exists ann_index_cache_two_ann_cols"
    sql """
        CREATE TABLE ann_index_cache_two_ann_cols (
            id INT NOT NULL,
            emb_a ARRAY<FLOAT> NOT NULL,
            emb_b ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb_a (`emb_a`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            ),
            INDEX idx_emb_b (`emb_b`) USING ANN PROPERTIES(
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
        INSERT INTO ann_index_cache_two_ann_cols VALUES
        (1, [1.0, 2.0, 3.0], [9.0, 9.0, 9.0]),
        (2, [1.1, 2.1, 3.1], [8.0, 8.0, 8.0]),
        (3, [8.0, 8.0, 8.0], [1.0, 2.0, 3.0]),
        (4, [9.0, 9.0, 9.0], [1.1, 2.1, 3.1]);
    """

    clearAnnResultCache()

    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_cache_two_ann_cols
        order by l2_distance_approximate(emb_a, [1.0,2.0,3.0])
        limit 2;
    """)

    checkTopnCacheHit([expected_hits: 0], """
        select id
        from ann_index_cache_two_ann_cols
        order by l2_distance_approximate(emb_b, [1.0,2.0,3.0])
        limit 2;
    """)

    checkTopnCacheHit([min_hits: 1], """
        select id
        from ann_index_cache_two_ann_cols
        order by l2_distance_approximate(emb_b, [1.0,2.0,3.0])
        limit 2;
    """)
    GetDebugPoint().clearDebugPointsForAllBEs()
}
