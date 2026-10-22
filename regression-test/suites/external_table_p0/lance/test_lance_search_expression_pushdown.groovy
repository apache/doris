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

suite("test_lance_search_expression_pushdown", "p0,external") {
    if (context.config.otherConfigs.get("enableIcebergTest") != "true") {
        logger.info("Lance fixtures are disabled")
        return
    }
    def catalog = "test_lance_search_expression_pushdown"
    def endpoint = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("iceberg_minio_port")
    def previousFeDebug = sql("SHOW VARIABLES LIKE 'fe_debug'")[0][1]
    sql "DROP CATALOG IF EXISTS ${catalog}"
    try {
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type"="lance", "lance.catalog.type"="filesystem", "warehouse"="s3://warehouse/lance",
            "s3.endpoint"="http://${endpoint}:${port}", "s3.access_key"="admin",
            "s3.secret_key"="password", "s3.region"="us-east-1", "use_path_style"="true")"""
        sql "SET enable_file_scanner_v2 = true"
        // Validate that vector rewrites preserve the original functions' NOT NULL contract.
        sql "SET fe_debug = true"
        def vector = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
        def table = "${catalog}.doris.vs_ivf_flat_f32"
        def autoVector = """SELECT row_id, l2_distance_approximate(embedding, ${vector}) AS distance
            FROM ${table} WHERE category = 'odd' ORDER BY distance LIMIT 5 OFFSET 1"""
        explain {
            sql autoVector
            contains "externalSearchType=VECTOR"
            contains "lanceMetric=l2"
        }
        // Compare with the explicit TVF, including metric conversion and a global OFFSET.
        def explicitVector = """SELECT row_id, CAST(sqrt(_distance) AS FLOAT) AS distance
            FROM vector_search("table"="${table}", "column"="embedding", "query_vector"="${vector}",
                "top_k"="5", "offset"="1", "metric"="l2", "filter"="category = 'odd'", "prefilter"="true")
            ORDER BY distance"""
        assertEquals(sql(explicitVector), sql(autoVector))
        // The scalar function returns Euclidean distance, while Lance returns squared L2.
        for (row in sql(autoVector)) {
            def actual = sql "SELECT l2_distance(embedding, ${vector}) FROM ${table} WHERE row_id = ${row[0]}"
            assertTrue(Math.abs((row[1] as double) - (actual[0][0] as double)) < 0.0001)
        }
        def dotTable = "${catalog}.doris.vs_ivf_pq_f32_dot"
        def autoDot = """SELECT row_id, inner_product_approximate(embedding, ${vector}) AS similarity
            FROM ${dotTable} ORDER BY similarity DESC LIMIT 5"""
        explain {
            sql autoDot
            contains "externalSearchType=VECTOR"
            contains "lanceMetric=dot"
        }
        def explicitDot = """SELECT row_id, 1.0 - _distance AS similarity
            FROM vector_search("table"="${dotTable}", "column"="embedding", "query_vector"="${vector}",
                "top_k"="5", "metric"="dot") ORDER BY similarity DESC"""
        def dotActual = sql(autoDot)
        def dotExpected = sql(explicitDot)
        assertEquals(dotExpected.collect { it[0] }, dotActual.collect { it[0] })
        for (int i = 0; i < dotActual.size(); ++i) {
            assertTrue(Math.abs((dotExpected[i][1] as double) - (dotActual[i][1] as double)) < 0.001)
        }
        def cosineTable = "${catalog}.doris.vs_ivf_flat_f32_cosine"
        for (entry in [["cosine_distance_approximate", "ASC", "_distance"],
                ["cosine_similarity_approximate", "DESC", "CAST(1 AS FLOAT) - _distance"]]) {
            def automatic = """SELECT row_id, ${entry[0]}(embedding, ${vector}) AS value
                FROM ${cosineTable} ORDER BY value ${entry[1]} LIMIT 5"""
            explain {
                sql automatic
                contains "externalSearchType=VECTOR"
                contains "lanceMetric=cosine"
            }
            def explicit = """SELECT row_id, ${entry[2]} AS value FROM vector_search(
                "table"="${cosineTable}", "column"="embedding", "query_vector"="${vector}",
                "top_k"="5", "metric"="cosine") ORDER BY value ${entry[1]}"""
            def actual = sql(automatic)
            def expected = sql(explicit)
            assertEquals(expected.collect { it[0] }, actual.collect { it[0] })
            for (int i = 0; i < actual.size(); ++i) {
                assertTrue(Math.abs((expected[i][1] as double) - (actual[i][1] as double)) < 0.0001)
                def exact = entry[0].replace("_approximate", "")
                def scalar = sql """SELECT ${exact}(embedding, ${vector}) FROM ${cosineTable}
                    WHERE row_id = ${actual[i][0]}"""
                assertTrue(Math.abs((scalar[0][0] as double) - (actual[i][1] as double)) < 0.0001)
            }
        }
        def binaryTable = "${catalog}.doris.vs_ivf_flat_u8"
        def binaryVector = "[" + (0..<128).collect { "0" }.join(",") + "]"
        def automaticHamming = """SELECT row_id,
            hamming_distance_approximate(embedding, ${binaryVector}) AS distance
            FROM ${binaryTable} ORDER BY distance LIMIT 5"""
        explain {
            sql automaticHamming
            contains "externalSearchType=VECTOR"
            contains "lanceMetric=hamming"
        }
        def explicitHamming = """SELECT row_id, _distance FROM vector_search(
            "table"="${binaryTable}", "column"="embedding", "query_vector"="${binaryVector}",
            "top_k"="5", "metric"="hamming") ORDER BY _distance"""
        def hammingRows = sql(automaticHamming)
        assertEquals(sql(explicitHamming), hammingRows)
        for (row in hammingRows) {
            def scalar = sql """SELECT hamming_distance(embedding, ${binaryVector})
                FROM ${binaryTable} WHERE row_id = ${row[0]}"""
            assertEquals(scalar[0][0], row[1])
        }
        for (name in ["cosine_distance", "cosine_similarity"]) {
            explain {
                sql "SELECT row_id FROM ${cosineTable} ORDER BY ${name}(embedding, ${vector}) LIMIT 5"
                notContains "externalSearchType=VECTOR"
            }
        }
        def zeroVector = "[" + (0..<16).collect { "0" }.join(",") + "]"
        explain {
            sql """SELECT row_id FROM ${cosineTable}
                ORDER BY cosine_distance_approximate(embedding, ${zeroVector}) LIMIT 5"""
            notContains "externalSearchType=VECTOR"
        }
        explain {
            sql """SELECT row_id FROM ${binaryTable}
                ORDER BY hamming_distance(embedding, ${binaryVector}) LIMIT 5"""
            notContains "externalSearchType=VECTOR"
        }
        sql "SET fe_debug = ${previousFeDebug}"
        def textTable = "${catalog}.doris.full_text_search"
        for (entry in [["MATCH_ANY", "match", "or"], ["MATCH_ALL", "match", "and"],
                ["MATCH_PHRASE", "phrase", null]]) {
            def autoText = """SELECT row_id, score() AS relevance FROM ${textTable}
                WHERE body ${entry[0]} 'lance search' AND row_id >= 2 AND score() > CAST(0 AS FLOAT)
                ORDER BY relevance DESC LIMIT 100"""
            explain {
                sql autoText
                contains "externalSearchType=FULL_TEXT"
                contains "lanceFullTextColumn=body"
            }
            def operator = entry[2] == null ? "" : ', "operator"="' + entry[2] + '"'
            def explicitText = """SELECT row_id, _score FROM full_text_search(
                "table"="${textTable}", "column"="body", "query"="lance search", "top_k"="100",
                "query_type"="${entry[1]}", "filter"="row_id >= 2", "prefilter"="true"${operator})
                WHERE _score > 0 ORDER BY _score DESC"""
            // Equal-score rows need not have the same physical order.
            def actual = sql(autoText).sort { a, b -> a[0] <=> b[0] }
            def expected = sql(explicitText).sort { a, b -> a[0] <=> b[0] }
            assertEquals(expected, actual)
        }
        def empty = sql """SELECT row_id, score() AS relevance FROM ${textTable}
            WHERE body MATCH_ANY 'lance' AND row_id < 0 ORDER BY relevance DESC LIMIT 10"""
        assertEquals([], empty)
        explain {
            sql "SELECT row_id FROM ${table} ORDER BY l2_distance(embedding, ${vector}) LIMIT 5"
            notContains "externalSearchType=VECTOR"
        }
        explain {
            sql """SELECT row_id FROM ${table}
                WHERE md5(category) = 'unsupported'
                ORDER BY l2_distance_approximate(embedding, ${vector}) LIMIT 5"""
            notContains "externalSearchType=VECTOR"
        }
        test {
            sql """SELECT row_id, score() AS relevance FROM ${textTable}
                WHERE body MATCH_ANY 'lance' AND md5(category) = 'unsupported'
                ORDER BY relevance DESC LIMIT 5"""
            exception "WHERE conditions must be fully pushable before Top-N"
        }
    } finally {
        sql "SET fe_debug = ${previousFeDebug}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
