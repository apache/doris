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

// This suite changes BE-wide writer settings and clears shared index caches.
// Constant and unindexed distance checks run in test_array_distance_functions.
suite("ann_empty_array_queries", "nonConcurrent") {
    def reloadIndexes = {
        def hosts = [:]
        def ports = [:]
        getBackendIpHttpPort(hosts, ports)
        for (def backendId : hosts.keySet()) {
            for (def cache : ['AnnIndexResultCache', 'SegmentCache', 'AnnIndexIVFListCache']) {
                httpTest {
                    endpoint "${hosts[backendId]}:${ports[backendId]}"
                    uri "/api/clear_cache/${cache}"
                    op 'get'
                    check { code, body ->
                        assertEquals(200, code)
                        assertTrue(body.contains('prune win'), body)
                    }
                }
            }
        }
    }
    setBeConfigTemporary([enable_ann_index_empty_array: true, ann_index_build_min_segment_rows: 0]) {
        for (def indexType : ['hnsw', 'ivf', 'ivf_on_disk']) {
            for (def metric : ['l2_distance', 'inner_product']) {
                logger.info("ANN missing-vector queries: index=${indexType}, metric=${metric}")
                sql "DROP TABLE IF EXISTS ann_empty_array_queries"
                // Connect the at-most-96-vector fixture fully so recall does not obscure row-ID checks.
                def indexProperties = indexType == 'hnsw' ? '"max_degree"="128"' : '"nlist"="1"'
                sql """
                    CREATE TABLE ann_empty_array_queries (
                        id INT NOT NULL,
                        vec ARRAY<FLOAT> NOT NULL,
                        INDEX ann_vec (vec) USING ANN PROPERTIES
                            ("index_type"="${indexType}", "metric_type"="${metric}",
                             "dim"="3", ${indexProperties})
                    ) DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")
                """
                sql """INSERT INTO ann_empty_array_queries
                    SELECT number,
                           IF(number % 3 = 0, CAST([] AS ARRAY<FLOAT>),
                              array(CAST(number AS FLOAT), CAST(0 AS FLOAT), CAST(0 AS FLOAT)))
                    FROM numbers("number"="96")"""
                // A separate all-empty segment exercises the untrained-IVF short circuit.
                sql "INSERT INTO ann_empty_array_queries VALUES (100, []), (101, [])"
                def fn = "${metric}_approximate"
                def query = metric == 'l2_distance' ? '[0,0,0]' : '[1,0,0]'
                def queries = [
                    "SELECT id, ${fn}(vec, ${query}) FROM ann_empty_array_queries ORDER BY id",
                    "SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${query}) < 20.5 ORDER BY id",
                    "SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${query}) > 20.5 ORDER BY id",
                    "SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${query}) IS NULL ORDER BY id"
                ]
                for (def missing : ['CAST([] AS ARRAY<FLOAT>)', 'CAST(NULL AS ARRAY<FLOAT>)']) {
                    queries.add("SELECT id, ${fn}(vec, ${missing}) FROM ann_empty_array_queries ORDER BY id")
                    queries.add("SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${missing}) < 20.5 ORDER BY id")
                    queries.add("SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${missing}) > 20.5 ORDER BY id")
                    queries.add("SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${missing}) IS NULL ORDER BY id")
                    for (def direction : ['ASC', 'DESC']) {
                        for (def nullOrder : ['FIRST', 'LAST']) {
                            for (def limit : [1, 10, 64, 65, 1000]) {
                                queries.add("SELECT ${fn}(vec, ${missing}) FROM ann_empty_array_queries "
                                    + "ORDER BY ${fn}(vec, ${missing}) ${direction} NULLS ${nullOrder} LIMIT ${limit}")
                            }
                        }
                    }
                }
                for (def direction : ['ASC', 'DESC']) {
                    for (def nullOrder : ['FIRST', 'LAST']) {
                        for (def limit : [1, 10, 64, 65, 1000]) {
                            queries.add("SELECT ${fn}(vec, ${query}) FROM ann_empty_array_queries "
                                + "ORDER BY ${fn}(vec, ${query}) ${direction} NULLS ${nullOrder} LIMIT ${limit}")
                        }
                    }
                }
                // Exact threshold 20 hits a stored vector; 64 vectors are nonempty.
                for (def op : ['<', '<=', '>', '>=']) {
                    queries.add("SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${query}) ${op} 20 ORDER BY id")
                    queries.add("SELECT id FROM ann_empty_array_queries WHERE id > 10 AND ${fn}(vec, ${query}) ${op} 20 ORDER BY id")
                    for (def missing : ['NULL', 'CAST([] AS ARRAY<FLOAT>)']) {
                        queries.add("SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${missing}) ${op} 20 ORDER BY id")
                    }
                }
                queries.add("SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${query}) IS NOT NULL ORDER BY id")
                queries.add("SELECT id, ${fn}(vec, IF(1 = 1, CAST(NULL AS ARRAY<FLOAT>), ${query})) FROM ann_empty_array_queries ORDER BY id")
                queries.add("SELECT id, ${fn}(vec, IF(1 = 1, CAST([] AS ARRAY<FLOAT>), ${query})) FROM ann_empty_array_queries ORDER BY id")
                try {
                    sql "SET hnsw_ef_search = 256"
                    sql "SET ann_index_candidate_rows_percent_threshold = 0"
                    sql "SET ann_index_candidate_rows_threshold = 1000000"
                    sql "SET enable_sql_cache = false"
                    sql "SET enable_condition_cache = false"
                    sql "SET enable_ann_index_result_cache = false"
                    // Invalid query parameters must fail both scalar fallback and indexed execution.
                    for (def threshold : [1000000, 0]) {
                        sql "SET ann_index_candidate_rows_threshold = ${threshold}"
                        for (def invalid : [
                                ['CAST([NULL, 1, 3] AS ARRAY<FLOAT>)', 'cannot', 'cannot'],
                                ['CAST([1, NULL, 3] AS ARRAY<FLOAT>)', 'cannot', 'cannot'],
                                ['CAST(array(1, NULL, 3) AS ARRAY<FLOAT>)', 'cannot', 'cannot'],
                                ['CAST([1, 2, NULL] AS ARRAY<FLOAT>)', 'cannot', 'cannot'],
                                ['CAST([NULL, NULL, NULL] AS ARRAY<FLOAT>)', 'cannot', 'cannot'],
                                ['CAST([1, 2, 3, 4] AS ARRAY<FLOAT>)', 'input element sizes', 'dimension'],
                                ['CAST([1] AS ARRAY<FLOAT>)', 'input element sizes', 'dimension']]) {
                            def invalidQuery = invalid[0]
                            test {
                                sql "SELECT ${fn}(vec, ${invalidQuery}) FROM ann_empty_array_queries ORDER BY id"
                                exception invalid[1]
                            }
                            test {
                                sql "SELECT id FROM ann_empty_array_queries WHERE ${fn}(vec, ${invalidQuery}) > 0"
                                exception invalid[2]
                            }
                            test {
                                sql "SELECT id FROM ann_empty_array_queries ORDER BY ${fn}(vec, ${invalidQuery}) ${metric == 'l2_distance' ? 'ASC' : 'DESC'} LIMIT 1"
                                exception invalid[2]
                            }
                        }
                    }
                    sql "SET ann_index_candidate_rows_threshold = 1000000"
                    // Record every execution path in .out so a shared scalar/index regression
                    // cannot pass merely because both paths return the same wrong result.
                    def checkQueries = { phase, exact = false ->
                        queries.eachWithIndex { statement, position ->
                            quickTest("${indexType}_${metric}_${phase}_${position}",
                                      exact ? statement.replace(fn, metric) : statement)
                        }
                    }
                    checkQueries('exact', true)
                    checkQueries('scalar_fallback')
                    sql "SET ann_index_candidate_rows_threshold = 0"
                    sql "SET enable_ann_index_result_cache = true"
                    // Repeat to cover cached range results as well as the initial search.
                    for (def iteration : 0..1) {
                        checkQueries("cached_${iteration}")
                    }
                    qt_missing_count """SELECT count(*), count(${fn}(vec, ${query})),
                        count(${fn}(vec, CAST(NULL AS ARRAY<FLOAT>))),
                        count(${fn}(vec, CAST([] AS ARRAY<FLOAT>))) FROM ann_empty_array_queries"""
                    for (def op : ['<', '<=', '>', '>=']) {
                        qt_range_boundary """SELECT id FROM ann_empty_array_queries
                            WHERE ${fn}(vec, ${query}) ${op} 20 ORDER BY id"""
                    }
                    // Compare results after reloading indexes without reusing cached ANN results.
                    sql "SET enable_ann_index_result_cache = false"
                    def direction = metric == 'l2_distance' ? 'ASC' : 'DESC'
                    reloadIndexes()
                    checkQueries('reload')
                    trigger_and_wait_compaction('ann_empty_array_queries', 'full')
                    checkQueries('compacted')
                    setBeConfigTemporary([enable_ann_index_empty_array: false]) {
                        checkQueries('write_disabled')
                    }
                    // No nonempty row exists anywhere: IVF must persist/reload without training.
                    sql "TRUNCATE TABLE ann_empty_array_queries"
                    sql "INSERT INTO ann_empty_array_queries VALUES (1, []), (2, [])"
                    sql "INSERT INTO ann_empty_array_queries VALUES (3, [])"
                    reloadIndexes()
                    for (def phase : ['before_compaction', 'after_compaction']) {
                        if (phase == 'after_compaction') {
                            trigger_and_wait_compaction('ann_empty_array_queries', 'full')
                        }
                        qt_all_empty "SELECT id, ${fn}(vec, ${query}) FROM ann_empty_array_queries ORDER BY id"
                        qt_all_empty_topn """SELECT ${fn}(vec, ${query}) FROM ann_empty_array_queries
                            ORDER BY ${fn}(vec, ${query}) ${direction} LIMIT 10"""
                        for (def op : ['<', '<=', '>', '>=']) {
                            qt_all_empty_range """SELECT id FROM ann_empty_array_queries
                                WHERE ${fn}(vec, ${query}) ${op} 0 ORDER BY id"""
                        }
                    }
                    // Check TopN results when every row has a valid vector.
                    sql "TRUNCATE TABLE ann_empty_array_queries"
                    sql """INSERT INTO ann_empty_array_queries
                        SELECT number, array(CAST(number AS FLOAT), CAST(0 AS FLOAT), CAST(0 AS FLOAT))
                        FROM numbers("number"="96")"""
                    qt_dense_topn """SELECT id, ${fn}(vec, ${query}) AS d FROM ann_empty_array_queries
                        ORDER BY d ${direction} LIMIT 10"""
                    // A deliberately unindexed small segment coexists with a trained dense segment.
                    setBeConfigTemporary([ann_index_build_min_segment_rows: 1000]) {
                        sql "INSERT INTO ann_empty_array_queries VALUES (200, [200,0,0]), (201, [])"
                    }
                    qt_mixed_segments """SELECT id FROM ann_empty_array_queries
                        WHERE ${fn}(vec, ${query}) >= 90 ORDER BY id"""
                    qt_mixed_segments_exact """SELECT id FROM ann_empty_array_queries
                        WHERE ${metric}(vec, ${query}) >= 90 ORDER BY id"""
                } finally {
                    sql "SET enable_sql_cache = DEFAULT"
                    sql "SET enable_condition_cache = DEFAULT"
                    sql "SET enable_ann_index_result_cache = DEFAULT"
                    sql "SET hnsw_ef_search = DEFAULT"
                    sql "SET ann_index_candidate_rows_percent_threshold = DEFAULT"
                    sql "SET ann_index_candidate_rows_threshold = DEFAULT"
                }
            }
        }
    }
}
