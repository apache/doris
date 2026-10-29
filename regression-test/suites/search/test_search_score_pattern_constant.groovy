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

// score() of pattern queries follows Elasticsearch's default rewrite: a regexp, wildcard or
// prefix in SEARCH and a MATCH_REGEXP score a constant 1 per matching row, whatever segment the
// row is in, and add exactly 1 next to a scored clause. An equality predicate adds nothing.
suite("test_search_score_pattern_constant", "p0") {
    // SEARCH and MATCH need index execution even when CI fuzzes these session defaults.
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"

    for (def storageFormat : ["V2", "V3", "SNII"]) {
        sql "DROP TABLE IF EXISTS test_search_score_pattern_constant"
        sql """
            CREATE TABLE test_search_score_pattern_constant (
                id BIGINT NOT NULL,
                content STRING NULL,
                tag STRING NULL,
                INDEX idx_content(content) USING INVERTED
                    PROPERTIES("parser" = "english", "support_phrase" = "true"),
                INDEX idx_tag(tag) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default:1",
                "inverted_index_storage_format" = "${storageFormat}",
                "disable_auto_compaction" = "true"
            )
        """
        // Two inserts make two segments with different term statistics; a constant score must not
        // depend on them.
        sql """INSERT INTO test_search_score_pattern_constant VALUES
            (1, 'Apache Doris is a high performance analytical database', 'doris'),
            (2, 'Inverted indexes are suitable for full text search', 'index'),
            (3, 'The parser splits text into multiple terms', 'parser'),
            (4, 'Doris supports fast real time data analysis', 'doris'),
            (5, 'Users can quickly search product names', 'search')
        """
        sql """INSERT INTO test_search_score_pattern_constant VALUES
            (6, 'text text text', 'text'), (7, 'plain words only', 'textual')
        """
        sql "sync"

        for (def query : [
                ["search_regexp", "SEARCH('content:/.*ext/')"],
                ["search_wildcard", "SEARCH('content:*ext')"],
                ["search_prefix", "SEARCH('content:te*')"],
                ["search_keyword_regexp", "SEARCH('tag:/text.*/')"],
                ["match_regexp", "content MATCH_REGEXP 'ext'"],
                ["match_regexp_keyword", "tag MATCH_REGEXP 'text'"],
                // A pattern next to a scored clause adds exactly 1, as an Elasticsearch bool must
                // with a regexp clause does.
                ["match_and_match_regexp", "content MATCH 'search' AND content MATCH_REGEXP 'ext'"],
                ["search_and_regexp", "SEARCH('content:search AND content:/.*ext/')"],
                ["match_only", "content MATCH 'search'"],
                ["match_and_equality", "content MATCH 'search' AND tag = 'index'"]]) {
            quickTest("${storageFormat}_${query[0]}", """
                SELECT id, ROUND(s, 6) FROM (
                    SELECT id, score() AS s FROM test_search_score_pattern_constant
                    WHERE ${query[1]} ORDER BY s DESC LIMIT 10
                ) hits ORDER BY id
            """, true)
        }
    }
}
