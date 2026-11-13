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

suite("ann_empty_array_queries") {
    setBeConfigTemporary([enable_ann_index_empty_array: true, ann_index_build_min_segment_rows: 0]) {
        for (def indexType : ['hnsw', 'ivf', 'ivf_on_disk']) {
            for (def metric : ['l2_distance', 'inner_product']) {
                sql "DROP TABLE IF EXISTS ann_empty_array_queries"
                def indexProperties = indexType == 'hnsw' ? '"max_degree"="16"' : '"nlist"="1"'
                sql """
                    CREATE TABLE ann_empty_array_queries (
                        id INT NOT NULL,
                        vec ARRAY<FLOAT> NOT NULL,
                        INDEX ann_vec (vec) USING ANN PROPERTIES
                            ("index_type"="${indexType}", "metric_type"="${metric}",
                             "dim"="2", ${indexProperties})
                    ) DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ("replication_num"="1")
                """
                sql """INSERT INTO ann_empty_array_queries
                    SELECT number,
                           IF(number % 3 = 0, CAST([] AS ARRAY<FLOAT>),
                              [CAST(number AS FLOAT), CAST(1 AS FLOAT)])
                    FROM numbers("number"="96")"""
                // A separate all-empty segment exercises the untrained-IVF short circuit.
                sql "INSERT INTO ann_empty_array_queries VALUES (100, []), (101, [])"
                def fn = "${metric}_approximate"
                def query = metric == 'l2_distance' ? '[0,0]' : '[1,0]'
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
                            for (def limit : [1, 1000]) {
                                queries.add("SELECT ${fn}(vec, ${missing}) FROM ann_empty_array_queries "
                                    + "ORDER BY ${fn}(vec, ${missing}) ${direction} NULLS ${nullOrder} LIMIT ${limit}")
                            }
                        }
                    }
                }
                for (def direction : ['ASC', 'DESC']) {
                    for (def nullOrder : ['FIRST', 'LAST']) {
                        for (def limit : [1, 1000]) {
                            queries.add("SELECT ${fn}(vec, ${query}) FROM ann_empty_array_queries "
                                + "ORDER BY ${fn}(vec, ${query}) ${direction} NULLS ${nullOrder} LIMIT ${limit}")
                        }
                    }
                }
                try {
                    sql "SET hnsw_ef_search = 256"
                    sql "SET ann_index_candidate_rows_percent_threshold = 0"
                    sql "SET ann_index_candidate_rows_threshold = 1000000"
                    // Invalid query parameters must fail both scalar fallback and indexed execution.
                    for (def threshold : [1000000, 0]) {
                        sql "SET ann_index_candidate_rows_threshold = ${threshold}"
                        for (def invalid : [
                                ['CAST([NULL, 1] AS ARRAY<FLOAT>)', 'cannot', 'cannot'],
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
                                sql "SELECT id FROM ann_empty_array_queries ORDER BY ${fn}(vec, ${invalidQuery}) LIMIT 1"
                                exception invalid[2]
                            }
                        }
                    }
                    sql "SET ann_index_candidate_rows_threshold = 1000000"
                    def scalarResults = queries.collect { sql(it) }
                    sql "SET ann_index_candidate_rows_threshold = 0"
                    // Repeat to cover cached range results as well as the initial search.
                    for (def iteration : 0..1) {
                        queries.eachWithIndex { statement, position ->
                            assertEquals(scalarResults[position], sql(statement))
                        }
                    }
                    setBeConfigTemporary([enable_ann_index_empty_array: false]) {
                        queries.eachWithIndex { statement, position ->
                            assertEquals(scalarResults[position], sql(statement))
                        }
                    }
                } finally {
                    sql "SET hnsw_ef_search = DEFAULT"
                    sql "SET ann_index_candidate_rows_percent_threshold = DEFAULT"
                    sql "SET ann_index_candidate_rows_threshold = DEFAULT"
                }
            }
        }
    }
}
