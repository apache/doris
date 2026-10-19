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

suite("test_search_score_regexp_wildcard", "p0") {
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"

    def scoredQuery = { predicate, direction, limit ->
        """
            SELECT id, ROUND(bm25_score, 6)
            FROM (
                SELECT id, score() AS bm25_score
                FROM test_search_score_regexp_wildcard
                WHERE ${predicate}
                ORDER BY bm25_score ${direction} LIMIT ${limit}
            ) hits
            ORDER BY bm25_score ${direction}, id
        """
    }
    for (String storageFormat : ["V2", "V3"]) {
        for (String parser : ["standard", "none"]) {
            sql "DROP TABLE IF EXISTS test_search_score_regexp_wildcard"
            sql """
                CREATE TABLE test_search_score_regexp_wildcard (
                    id INT NOT NULL,
                    content STRING NULL,
                    INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "${parser}")
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_allocation" = "tag.location.default:1",
                    "inverted_index_storage_format" = "${storageFormat}"
                )
            """
            sql """
                INSERT INTO test_search_score_regexp_wildcard VALUES
                    (1, 'apple'), (2, 'apple apple apple'), (3, 'application'),
                    (4, 'apple application'), (5, 'banana'), (6, NULL),
                    (7, 'apple application apple'), (8, 'apply apple')
            """
            sql "sync"
            String regexp = "search('content:/app.*/', '{\"mode\":\"lucene\"}')"
            String wildcard = "search('content:*pp*', '{\"mode\":\"lucene\"}')"
            String terms = parser == "standard"
                    ? "search('content:apple OR content:application OR content:apply', '{\"mode\":\"lucene\"}')"
                    : "search('content:EXACT(apple) OR content:EXACT(apple apple apple) OR content:EXACT(application) OR content:EXACT(apple application) OR content:EXACT(apple application apple) OR content:EXACT(apply apple)', '{\"mode\":\"standard\"}')"
            String label = "${storageFormat}_${parser}"
            quickTest("${label}_unscored_regexp", """
                SELECT id FROM test_search_score_regexp_wildcard WHERE ${regexp} ORDER BY id
            """)
            quickTest("${label}_unscored_wildcard", """
                SELECT id FROM test_search_score_regexp_wildcard WHERE ${wildcard} ORDER BY id
            """)
            for (boolean wand : [false, true]) {
                sql "set enable_inverted_index_wand_query = ${wand}"
                quickTest("${label}_${wand}_regexp", scoredQuery(regexp, "DESC", 100))
                quickTest("${label}_${wand}_wildcard", scoredQuery(wildcard, "DESC", 100))
                quickTest("${label}_${wand}_terms", scoredQuery(terms, "DESC", 100))
                quickTest("${label}_${wand}_ascending", scoredQuery(regexp, "ASC", 100))
                quickTest("${label}_${wand}_missing", scoredQuery(
                        "search('content:/missing.*/', '{\"mode\":\"lucene\"}')", "DESC", 100))
            }
        }
    }
}
