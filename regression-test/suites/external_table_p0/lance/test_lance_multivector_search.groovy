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

suite("test_lance_multivector_search", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String catalogName = "test_lance_multivector_search"
    String endpoint = "http://${context.config.otherConfigs.get('externalEnvIp')}:${context.config.otherConfigs.get('iceberg_minio_port')}"
    String tableName = "${catalogName}.`default`.multivector"
    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """CREATE CATALOG `${catalogName}` PROPERTIES (
        "type"="lance", "lance.catalog.type"="filesystem", "warehouse"="s3://warehouse/lance",
        "s3.endpoint"="${endpoint}", "s3.access_key"="admin", "s3.secret_key"="password",
        "s3.region"="us-east-1", "use_path_style"="true")"""
    sql "SET enable_file_scanner_v2 = true"
    // The fixture has unequal subvector counts, empty/null outer rows, two fragments,
    // and cosine IVF_FLAT indexes built before appending the second fragment.
    def vectors = [1L: [[1d, 0d], [0d, 1d]], 2L: [[2d, 0d]],
                   3L: [[3d, 0d], [0d, 3d]], 6L: [[1d, 1d]]]
    def query = [[1d, 0d], [0d, 1d]]
    def distance = { q, v, String metric ->
        if (metric == "l2") return (q[0] - v[0]) ** 2 + (q[1] - v[1]) ** 2
        double dot = q[0] * v[0] + q[1] * v[1]
        if (metric == "dot") return 1d - dot
        return 1d - dot / Math.sqrt((q[0] ** 2 + q[1] ** 2) * (v[0] ** 2 + v[1] ** 2))
    }
    def search = { String column, String json, String metric, boolean indexed, int k, int offset, String filter ->
        String where = filter == null ? "" : ', "filter"="' + filter + '"'
        return """vector_search("table"="${tableName}", "column"="${column}",
            "query_vector"="${json}", "metric"="${metric}", "use_index"="${indexed}",
            "nprobes"="1", "top_k"="${k}", "offset"="${offset}"${where})"""
    }
    try {
        assertEquals(6, (sql "SELECT row_id FROM ${tableName}").size())
        for (String column : ["vectors16", "vectors32", "vectors64"]) {
            // Check outer nulls, empty arrays and both nested offsets during materialization.
            def sizes = sql "SELECT COALESCE(size(${column}), -1) FROM ${tableName} ORDER BY row_id"
            assertEquals([2, 1, 2, 0, -1, 1], sizes.collect { (it[0] as Number).intValue() })
            def payload = sql "SELECT ${column}[1][1], ${column}[2][2] FROM ${tableName} WHERE row_id = 1"
            assertEquals([1d, 1d], payload[0].collect { (it as Number).doubleValue() })
            for (String metric : ["l2", "cosine", "dot"]) {
                for (int count : [1, 2]) {
                    def queries = query.take(count)
                    String json = count == 1 ? "[[1,0]]" : "[[1,0],[0,1]]"
                    def expected = vectors.collect { id, row ->
                        [id, queries.collect { q -> row.collect { v -> distance(q, v, metric) }.min() }.sum()]
                    }.sort { a, b -> a[1] <=> b[1] ?: a[0] <=> b[0] }
                    String source = search(column, json, metric, false, 10, 0, null)
                    def actual = sql "SELECT row_id, _distance FROM ${source} ORDER BY _distance, row_id"
                    assertEquals(expected.size(), actual.size())
                    actual.eachWithIndex { row, i ->
                        assertEquals(expected[i][0], (row[0] as Number).longValue())
                        assertTrue(Math.abs((row[1] as Number).doubleValue() - expected[i][1]) < 1e-5)
                    }
                    // Omitting _distance must preserve the same row-level candidate set.
                    assertEquals([1L, 2L, 3L, 6L], (sql "SELECT row_id FROM ${source} ORDER BY row_id")
                            .collect { (it[0] as Number).longValue() })
                }
            }
        }
        for (String indexedColumn : ["vectors16", "vectors32", "vectors64"]) {
            for (boolean indexed : [false, true]) {
                String source = search(indexedColumn, "[[1,0],[0,1]]", "cosine", indexed, 10, 0, null)
                if (indexed) {
                    explain {
                        sql "SELECT row_id, _distance FROM ${source}"
                        contains "lanceSearchIndexSegments=1"
                        contains "lanceSearchIndexFragments=1"
                        contains "lanceSearchUnindexedFragments=1"
                    }
                }
                def rows = sql "SELECT row_id, _distance FROM ${source} ORDER BY _distance, row_id"
                assertEquals([1L, 3L, 6L, 2L], rows.collect { (it[0] as Number).longValue() })
                [0d, 0d, 2d - Math.sqrt(2d), 1d].eachWithIndex { value, i ->
                    assertTrue(Math.abs((rows[i][1] as Number).doubleValue() - value) < 1e-5)
                }
                source = search(indexedColumn, "[[1,0],[0,1]]", "cosine", indexed, 2, 1, "row_id >= 2")
                rows = sql "SELECT row_id FROM ${source} ORDER BY _distance, row_id"
                assertEquals([6L, 2L], rows.collect { (it[0] as Number).longValue() })
                source = search(indexedColumn, "[[1,0],[0,1]]", "cosine", indexed, 3, 0, "row_id > 100")
                assertEquals(0, (sql "SELECT row_id FROM ${source}").size())
                source = search(indexedColumn, "[[1,0],[0,1]]", "cosine", indexed, 1, 0, "row_id >= 2")
                assertEquals(3L, ((sql "SELECT row_id FROM ${source}")[0][0] as Number).longValue())
            }
        }
        def invalidQueries = ["[]": "must be non-empty", "[1,0]": "Each query subvector",
                              "[[1]]": "Each query subvector", "[[1,0],[0]]": "Each query subvector",
                              "[null]": "Each query subvector", "[[1,null]]": "must be a number",
                              "[[1,1e999]]": "not representable"]
        invalidQueries.each { invalid, message ->
            test {
                sql "SELECT row_id FROM ${search('vectors32', invalid, 'l2', false, 2, 0, null)}"
                exception message
            }
        }
        test {
            sql "SELECT row_id FROM ${search('vectors32', '[[1,0]]', 'hamming', false, 2, 0, null)}"
            exception "supports l2, cosine, and dot"
        }
    } finally {
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
