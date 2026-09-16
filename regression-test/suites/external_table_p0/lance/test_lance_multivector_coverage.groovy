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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

suite("test_lance_multivector_coverage", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) return
    String catalog = "test_lance_multivector_coverage"
    String endpoint = "http://${context.config.otherConfigs.get('externalEnvIp')}:${context.config.otherConfigs.get('iceberg_minio_port')}"
    def saved = [:]
    for (String variable : ["batch_size", "topn_lazy_materialization_threshold", "enable_file_scanner_v2",
                            "enable_sql_cache", "enable_query_cache"]) {
        saved[variable] = (sql "SHOW VARIABLES LIKE '${variable}'")[0][1]
    }
    def parseVectors = { value -> new JsonSlurper().parseText(value.toString()) }
    def assertVectors = { expected, actual ->
        assertEquals(expected.size(), actual.size())
        expected.eachWithIndex { vector, i ->
            assertEquals(vector.size(), actual[i].size())
            vector.eachWithIndex { value, j ->
                assertTrue(Math.abs((value as double) - (actual[i][j] as double)) < 1e-6)
            }
        }
    }
    // Compute the oracle directly from original values, independently of Lance or another SQL search.
    def score = { queries, vectors, String metric ->
        queries.collect { q ->
            // Ignore undefined cosine pairs when the same row also has valid matches.
            vectors.collect { v ->
                double dot = 0, qNorm = 0, vNorm = 0, l2 = 0
                q.eachWithIndex { value, j ->
                    double a = value as double, b = v[j] as double
                    dot += a * b; qNorm += a * a; vNorm += b * b; l2 += (a - b) * (a - b)
                }
                metric == "l2" ? l2 : metric == "dot" ? 1d - dot : 1d - dot / Math.sqrt(qNorm * vNorm)
            }.findAll { Double.isFinite(it as double) }.min()
        }.sum() as double
    }
    def dimensionVectors = { int id, int dim ->
        (0..<(1 + id % 4)).collect { sub ->
            (0..<dim).collect { j -> ((id * 3 + sub * 5 + j * 7) % 23 - 11) / 16d }
        }
    }
    def representativeVector = { int id, int sub ->
        (0..<128).collect { j -> ((id * 17 + sub * 29 + j * 13 + j * j * 7 + id * j * 3) % 1009 - 504) / 512d }
    }
    def source = { String table, String column, queries, String metric, boolean indexed,
                   int k, int offset, String filter ->
        String predicate = filter == null ? "" : ', "filter"="' + filter + '"'
        """vector_search("table"="${catalog}.`default`.${table}", "column"="${column}",
            "query_vector"='${JsonOutput.toJson(queries)}', "metric"="${metric}",
            "use_index"="${indexed}", "nprobes"="4", "refine_factor"="64",
            "top_k"="${k}", "offset"="${offset}"${predicate})"""
    }
    try {
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type"="lance", "lance.catalog.type"="filesystem", "warehouse"="s3://warehouse/lance",
            "s3.endpoint"="${endpoint}", "s3.access_key"="admin", "s3.secret_key"="password",
            "s3.region"="us-east-1", "use_path_style"="true")"""
        sql "SET enable_file_scanner_v2 = true"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        // Odd dimensions exercise the scalar tail; 128 dimensions exercise vectorized kernels.
        for (int bits : [16, 32, 64]) {
            for (int dim : [1, 3, 8, 128]) {
                String column = "v${bits}_d${dim}"
                String table = "${catalog}.`default`.multivector_dimensions"
                def stored = sql "SELECT row_id, CAST(${column} AS STRING) FROM ${table} ORDER BY row_id"
                assertEquals(6, stored.size())
                (1..4).each { id -> assertVectors(dimensionVectors(id, dim), parseVectors(stored[id - 1][1])) }
                assertEquals([], parseVectors(stored[4][1]))
                assertEquals(null, stored[5][1])
                def q = dimensionVectors(1, dim).take(2)
                for (String metric : ["l2", "cosine", "dot"]) {
                    def expected = (1..4).collect { id -> [id, score(q, dimensionVectors(id, dim), metric)] }
                            .sort { a, b -> a[1] <=> b[1] ?: a[0] <=> b[0] }
                    String tvf = source("multivector_dimensions", column, q, metric, false, 10, 0, null)
                    // Both normal and row-ID materialization must return the original nested payload.
                    for (int threshold : [-1, 1024]) {
                        sql "SET topn_lazy_materialization_threshold = ${threshold}"
                        def rows = sql "SELECT row_id, CAST(${column} AS STRING), _distance FROM ${tvf} ORDER BY _distance, row_id"
                        assertEquals(4, rows.size())
                        rows.eachWithIndex { row, i ->
                            int id = (row[0] as Number).intValue()
                            assertEquals(expected[i][0], id)
                            assertVectors(dimensionVectors(id, dim), parseVectors(row[1]))
                            double tolerance = (bits == 16 ? 3e-3 : 2e-5) * Math.max(1d, Math.abs(expected[i][1] as double))
                            assertTrue(Math.abs((row[2] as double) - (expected[i][1] as double)) < tolerance)
                        }
                    }
                }
                // Repeated queries count twice; this distinguishes sum from mean or set semantics.
                def repeated = [q[0], q[0]]
                def doubled = sql "SELECT row_id, _distance FROM ${source('multivector_dimensions', column, repeated, 'l2', false, 10, 0, null)} ORDER BY row_id"
                doubled.each { row ->
                    double expected = 2d * score([q[0]], dimensionVectors((row[0] as Number).intValue(), dim), "l2")
                    assertTrue(Math.abs((row[1] as double) - expected) < 2e-5 * Math.max(1d, Math.abs(expected)))
                }
            }
        }
        def query = [representativeVector(7, 0), representativeVector(11, 1)]
        def expectedVectors = (1..768).collectEntries { id ->
            [(id): (0..<(1 + id % 4)).collect { sub -> representativeVector(id, sub) }]
        }
        def ranked = { boolean filtered ->
            expectedVectors.findAll { id, value -> !filtered || id % 5 == 0 }.collect { id, value ->
                [id, score(query, value, "cosine")]
            }.sort { a, b -> a[1] <=> b[1] ?: a[0] <=> b[0] }
        }
        // Reuse the independent oracle; test variants must not repeatedly score the same payload.
        ranked = ranked.memoize()
        // The expected window must need all three physical fragments, not only one local winner.
        assertEquals([0, 1, 2], ranked(false).take(20).collect { (it[0] - 1) % 3 }.unique().sort())
        for (String kind : ["flat", "pq"]) {
            String table = "multivector_ivf_${kind}"
            assertEquals(768L, (sql "SELECT COUNT(*) FROM ${catalog}.`default`.${table}")[0][0] as long)
            for (boolean indexed : [false, true]) {
                for (boolean filtered : [false, true]) {
                    for (int offset : [0, 7]) {
                        String tvf = source(table, "vectors", query, "cosine", indexed, 13, offset,
                                filtered ? "row_id % 5 = 0" : null)
                        def expected = ranked(filtered).drop(offset).take(13)
                        for (int threshold : [-1, 1024]) {
                            sql "SET topn_lazy_materialization_threshold = ${threshold}"
                            String statement = "SELECT row_id, CAST(vectors AS STRING), label, note, _distance FROM ${tvf} ORDER BY _distance, row_id"
                            explain {
                                sql "verbose ${statement}"
                                contains "lanceSearchFragments=3"
                                contains "inputSplitNum=3"
                                contains "lanceSearchIndexSegments=${indexed ? 2 : 0}"
                                contains "lanceSearchUnindexedFragments=${indexed ? 1 : 3}"
                                if (indexed) contains "lanceSearchIndexFragments=2"
                                if (threshold > 0) {
                                    contains "VMaterializeNode"
                                    contains "__DORIS_GLOBAL_ROWID_COL__vector_search"
                                } else {
                                    notContains "VMaterializeNode"
                                }
                            }
                            // Probe all partitions and overfetch this frozen small fixture before exact
                            // refinement. This equality is not a general ANN recall guarantee.
                            def rows = sql statement
                            assertEquals(expected.collect { it[0] }, rows.collect { (it[0] as Number).intValue() })
                            assertEquals(13, rows.collect { it[0] }.unique().size())
                            rows.eachWithIndex { row, i ->
                                int id = (row[0] as Number).intValue()
                                assertVectors(expectedVectors[id], parseVectors(row[1]))
                                assertEquals("document-${id}".toString(), row[2])
                                assertEquals(id % 11 == 0 ? null : "note-${id}".toString(), row[3])
                                assertTrue(Math.abs((row[4] as double) - (expected[i][1] as double)) < 2e-5)
                            }
                        }
                    }
                }
                // Keep the logical window at the coordinator even when it falls beyond all rows.
                for (int offset : [767, 768, 800]) {
                    def rows = sql "SELECT row_id FROM ${source(table, 'vectors', query, 'cosine', indexed, 3, offset, null)} ORDER BY _distance, row_id"
                    assertEquals(ranked(false).drop(offset).take(3).collect { it[0] }, rows.collect { (it[0] as Number).intValue() })
                }
                assertEquals([], sql("SELECT row_id FROM ${source(table, 'vectors', query, 'cosine', indexed, 3, 0, 'row_id < 0')}"))
            }
        }
        // A TVF prefilter can find the next eligible row; an outer WHERE cannot refill TopK.
        int nearest = ranked(false)[0][0]
        String full = source("multivector_ivf_flat", "vectors", query, "cosine", false, 1, 0, null)
        assertEquals([], sql("SELECT row_id FROM ${full} WHERE row_id != ${nearest}"))
        String prefiltered = source("multivector_ivf_flat", "vectors", query, "cosine", false, 1, 0, "row_id != ${nearest}")
        assertEquals(ranked(false)[1][0], (sql "SELECT row_id FROM ${prefiltered}")[0][0] as int)
        for (String column : ["integer_vectors", "nullable_vectors"]) {
            test {
                sql "SELECT _distance FROM ${source('multivector_dimensions', column, [[1, 2, 3]], 'l2', false, 1, 0, null)}"
                exception (column == "integer_vectors" ? "must be fixed_size_list" : "require non-nullable subvectors")
            }
        }
        for (int bits : [16, 32, 64]) {
            for (String prefix : ["null", "nan", "inf"]) {
                test {
                    sql "SELECT _distance FROM ${source('multivector_invalid_types', prefix + bits, [[1, 0, 1]], 'l2', false, 1, 0, null)}"
                    exception "finite, non-null elements"
                }
            }
            for (def invalid : [[[1, null, 2]], [[1, true, 2]], [[1, "invalid", 2]], [[1, 2]]]) {
                test {
                    sql "SELECT _distance FROM ${source('multivector_dimensions', 'v' + bits + '_d3', invalid, 'l2', false, 1, 0, null)}"
                    exception (invalid[0].size() == 3 ? "must be a number" : "Each query subvector")
                }
            }
        }
        test {
            sql "SELECT _distance FROM ${source('multivector_dimensions', 'v16_d1', [[65520]], 'l2', false, 1, 0, null)}"
            exception "not representable"
        }
    } finally {
        saved.each { variable, value -> sql "SET ${variable} = ${value}" }
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
