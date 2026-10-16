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

suite("test_search_score_raw_index", "p0") {
    // SEARCH needs index execution even when CI fuzzes these session defaults.
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"

    // Score TopN pushdown accepts one ordering key. Sort ties outside the bounded query.
    def scoredQuery = { hint, table, predicate, direction, limit ->
        """
            SELECT id, ROUND(bm25_score, 6), bm25_score > 0 AND bm25_score < 1000000
            FROM (
                SELECT ${hint} id, score() AS bm25_score FROM ${table}
                WHERE ${predicate}
                ORDER BY bm25_score ${direction} LIMIT ${limit}
            ) hits
            ORDER BY bm25_score ${direction}, id
        """
    }
    for (def storageFormat : ["V2", "V3", "SNII"]) {
        sql "DROP TABLE IF EXISTS test_search_score_raw_index"
        sql """
            CREATE TABLE test_search_score_raw_index (
                id INT NOT NULL,
                raw_index_content STRING NULL,
                INDEX idx_raw_content(raw_index_content) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default:1",
                "inverted_index_storage_format" = "${storageFormat}"
            )
        """
        // Separate inserts exercise statistics across multiple segments.
        sql """INSERT INTO test_search_score_raw_index VALUES
            (1, 'apple'), (2, 'banana'), (3, 'apple'), (4, 'orange'),
            (5, 'apple banana'), (6, 'Apple'), (7, NULL), (8, '')
        """
        sql "sync"
        sql """INSERT INTO test_search_score_raw_index VALUES
            (9, 'apple'), (10, 'apple banana'), (11, 'banana')
        """
        sql "sync"

        def term = "search('raw_index_content:apple', '{\"mode\":\"lucene\"}')"
        def phrase = "search('raw_index_content:\"apple banana\"', '{\"mode\":\"lucene\"}')"
        def exact = "search('raw_index_content:EXACT(apple banana)', '{\"mode\":\"standard\"}')"
        def rankedOr = "search('raw_index_content:apple OR raw_index_content:\"apple banana\"', '{\"mode\":\"lucene\"}')"
        def uniqueTop = "search('raw_index_content:apple OR raw_index_content:Apple', '{\"mode\":\"lucene\"}')"
        def caseSensitive = "search('raw_index_content:Apple', '{\"mode\":\"lucene\"}')"
        def missing = "search('raw_index_content:pear', '{\"mode\":\"lucene\"}')"
        quickTest("${storageFormat}_unscored", """
            SELECT id FROM test_search_score_raw_index WHERE ${term} ORDER BY id
        """)
        for (def wand : [true, false]) {
            def hint = "/*+SET_VAR(enable_inverted_index_wand_query=${wand}) */"
            for (def entry : [
                    ["term", term, "DESC", 100],
                    ["phrase", phrase, "DESC", 100],
                    ["exact", exact, "DESC", 100],
                    ["or_top_two", rankedOr, "DESC", 2],
                    ["top_one", uniqueTop, "DESC", 1],
                    ["ascending", term, "ASC", 100],
                    ["case", caseSensitive, "DESC", 100],
                    ["missing", missing, "DESC", 100]]) {
                quickTest("${storageFormat}_${wand}_${entry[0]}",
                          scoredQuery(hint, "test_search_score_raw_index", entry[1],
                                      entry[2], entry[3]))
            }
        }
        sql "DROP TABLE IF EXISTS test_search_score_dual_index"
        sql """
            CREATE TABLE test_search_score_dual_index (
                id INT NOT NULL,
                content STRING NULL,
                INDEX idx_raw(content) USING INVERTED,
                INDEX idx_analyzed(content) USING INVERTED
                    PROPERTIES("parser"="standard", "support_phrase"="true")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation"="tag.location.default:1",
                "inverted_index_storage_format"="${storageFormat}"
            )
        """
        sql """INSERT INTO test_search_score_dual_index VALUES
            (1, 'apple banana'), (2, 'apple'), (3, 'apple banana orange')
        """
        sql "sync"
        quickTest("${storageFormat}_dual_exact",
                  scoredQuery("", "test_search_score_dual_index",
                              "search('content:EXACT(apple banana)', '{\"mode\":\"standard\"}')",
                              "DESC", 10))
    }
}
