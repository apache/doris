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

suite("test_search_score_keyword_filters", "p0") {
    // SEARCH needs index execution even when CI fuzzes these session defaults.
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"

    for (def storageFormat : ["V2", "V3", "SNII"]) {
        for (def explicitNone : [false, true]) {
            String variant = explicitNone ? "explicit_none_" : ""
            String keywordProperties = explicitNone ? 'PROPERTIES("parser" = "none")' : ""
            sql "DROP TABLE IF EXISTS test_search_score_keyword_filters"
            sql """
                CREATE TABLE test_search_score_keyword_filters (
                    id INT NOT NULL,
                    body STRING NULL,
                    tag STRING NULL,
                    INDEX idx_body(body) USING INVERTED
                        PROPERTIES("parser" = "english", "support_phrase" = "true"),
                    INDEX idx_tag(tag) USING INVERTED ${keywordProperties}
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_allocation" = "tag.location.default:1",
                    "inverted_index_storage_format" = "${storageFormat}"
                )
            """
            sql """INSERT INTO test_search_score_keyword_filters VALUES
                (1, 'apple apple', 'x'), (2, 'apple pear', 'y'),
                (3, 'pear', 'x'), (4, 'apple', 'x'),
                (5, 'apple', 'z'), (6, 'apple', NULL)
            """
            sql "sync"

            for (def wand : [false, true]) {
                // Inside SEARCH both fields contribute scores; keyword terms have unit frequency.
                quickTest("${variant}${storageFormat}_${wand}_internal_and", """
                    SELECT id, ROUND(bm25_score, 6), bm25_score > 0 AND bm25_score < 1000000
                    FROM (
                        SELECT /*+SET_VAR(enable_inverted_index_wand_query=${wand}) */
                               id, score() AS bm25_score
                        FROM test_search_score_keyword_filters
                        WHERE SEARCH('body:apple AND tag:x', '{"mode":"standard"}')
                        ORDER BY bm25_score DESC LIMIT 10
                    ) hits
                    ORDER BY id
                """)
                for (def filter : [
                        ["baseline", "id IN (1, 4)"],
                        ["equality", "tag = 'x'"],
                        ["keyword_match", "tag MATCH_ANY 'x'"],
                        ["keyword_match_all", "tag MATCH_ALL 'x'"]]) {
                    // The keyword predicate filters rows; only the SEARCH leaf contributes a score.
                    quickTest("${variant}${storageFormat}_${wand}_${filter[0]}", """
                        SELECT id, ROUND(bm25_score, 6), bm25_score > 0 AND bm25_score < 1000000
                        FROM (
                            SELECT /*+SET_VAR(enable_inverted_index_wand_query=${wand}) */
                                   id, score() AS bm25_score
                            FROM test_search_score_keyword_filters
                            WHERE SEARCH('body:apple') AND ${filter[1]}
                            ORDER BY bm25_score DESC LIMIT 10
                        ) hits
                        ORDER BY id
                    """)
                }
            }
        }
    }
}
