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

// score() over MATCH on a keyword (untokenized) inverted index. Like an Elasticsearch match or term
// query on a keyword field, it scores BM25 with unit frequency and length, so every matching row
// scores the idf of the whole value, the same as SEARCH on that field; it used to score 0. An
// equality predicate stays a pure filter.
suite("test_match_keyword_score", "p0") {
    // MATCH and SEARCH need index execution even when CI fuzzes these session defaults.
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"
    sql "set enable_match_without_inverted_index = true"

    for (def storageFormat : ["V2", "V3"]) {
        sql "DROP TABLE IF EXISTS test_match_keyword_score"
        sql """
            CREATE TABLE test_match_keyword_score (
                id INT NOT NULL,
                body STRING NULL,
                tag STRING NULL,
                tag_none STRING NULL,
                n INT NULL,
                INDEX idx_body(body) USING INVERTED
                    PROPERTIES("parser" = "english", "support_phrase" = "true"),
                INDEX idx_tag(tag) USING INVERTED PROPERTIES("ignore_above" = "1024"),
                INDEX idx_tag_none(tag_none) USING INVERTED
                    PROPERTIES("parser" = "none", "support_phrase" = "true"),
                INDEX idx_n(n) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default:1",
                "inverted_index_storage_format" = "${storageFormat}"
            )
        """
        // The first row alone is the reported case: one document scores idf = ln(4/3) = 0.287682.
        sql """INSERT INTO test_match_keyword_score VALUES (1, 'apple apple', 'BP', 'Hello World', 1)"""
        sql "sync"
        quickTest("${storageFormat}_single_row_match", """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_match_keyword_score
                WHERE tag MATCH 'BP' ORDER BY s DESC LIMIT 10
            ) hits ORDER BY id
        """, true)

        sql """INSERT INTO test_match_keyword_score VALUES
            (2, 'apple pear', 'BP', 'hello world', 2), (3, 'pear', 'other', 'Hello World', 1),
            (4, 'apple', 'x', 'Hello', 1), (5, 'apple', 'BP', NULL, NULL),
            (6, 'apple', NULL, 'Hello World', 2)
        """
        sql "sync"

        // The whole value is one term: MATCH, MATCH_ALL and MATCH_PHRASE score as SEARCH does.
        for (def query : [
                ["match", "tag MATCH 'BP'"],
                ["match_all", "tag MATCH_ALL 'BP'"],
                ["match_phrase", "tag MATCH_PHRASE 'BP'"],
                ["search", "SEARCH('tag:BP')"],
                // Case and spaces are kept, and phrase support inherited by parser=none is ignored.
                ["none_match", "tag_none MATCH 'Hello World'"],
                ["none_match_phrase", "tag_none MATCH_PHRASE 'Hello World'"],
                ["none_search", "SEARCH('tag_none:\"Hello World\"')"],
                // Both leaves score: idf(apple) from body plus idf(BP) from tag.
                ["analyzed_and_keyword", "body MATCH 'apple' AND tag MATCH 'BP'"],
                ["search_and", "SEARCH('body:apple AND tag:BP')"],
                // An equality is a filter: only the body leaf scores.
                ["analyzed_and_equality", "body MATCH 'apple' AND tag = 'BP'"],
                // A MATCH on a number cast to text is a filter over the BKD index of n: only the
                // body leaf scores. Opening that BKD index for statistics used to abort the BE.
                ["analyzed_and_numeric_cast", "body MATCH 'apple' AND CAST(n AS STRING) MATCH '1'"]]) {
            quickTest("${storageFormat}_${query[0]}", """
                SELECT id, ROUND(s, 6) FROM (
                    SELECT id, score() AS s FROM test_match_keyword_score
                    WHERE ${query[1]} ORDER BY s DESC LIMIT 10
                ) hits ORDER BY id
            """, true)
        }

        // A VARIANT path is typed per segment: n holds numbers, indexed as BKD, in the first
        // insert and text in the second. The plain VARIANT index makes every path a keyword
        // index, so CAST(v['n'] AS STRING) MATCH collects statistics for n; the BKD segment adds
        // none instead of aborting the BE, and its rows match without the index with score 0. p
        // exists only in the first insert: the other segment has no index for it and adds nothing.
        sql "set default_variant_enable_typed_paths_to_sparse = false"
        sql "set default_variant_enable_doc_mode = false"
        sql "DROP TABLE IF EXISTS test_match_keyword_score_variant"
        sql """
            CREATE TABLE test_match_keyword_score_variant (
                id INT NOT NULL,
                v VARIANT NULL,
                INDEX idx_v(v) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default:1",
                "inverted_index_storage_format" = "${storageFormat}",
                "disable_auto_compaction" = "true"
            )
        """
        sql """INSERT INTO test_match_keyword_score_variant VALUES
            (1, '{"n": 1, "s": "abc", "p": "abc"}'), (2, '{"n": 2, "s": "abd", "p": "abd"}'),
            (3, '{"n": 1, "s": "xyz", "p": "xyz"}')
        """
        sql """INSERT INTO test_match_keyword_score_variant VALUES (4, '{"n": "1", "s": "abc"}')"""
        sql "sync"
        for (def query : [
                ["variant_numeric_path", "CAST(v['n'] AS STRING) MATCH '1'"],
                ["variant_text_path", "CAST(v['s'] AS STRING) MATCH 'abc'"],
                ["variant_sparse_path", "CAST(v['p'] AS STRING) MATCH 'abc'"]]) {
            quickTest("${storageFormat}_${query[0]}", """
                SELECT id, ROUND(s, 6) FROM (
                    SELECT id, score() AS s FROM test_match_keyword_score_variant
                    WHERE ${query[1]} ORDER BY s DESC LIMIT 10
                ) hits ORDER BY id
            """, true)
        }
        // A numeric predicate leaves the BKD searcher of n in the searcher cache, where collecting
        // statistics finds it next: it used to take it for a text searcher and abort the BE.
        sql "SELECT COUNT(*) FROM test_match_keyword_score_variant WHERE CAST(v['n'] AS BIGINT) = 1"
        quickTest("${storageFormat}_variant_numeric_path_cached_bkd", """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM test_match_keyword_score_variant
                WHERE CAST(v['n'] AS STRING) MATCH '1' ORDER BY s DESC LIMIT 10
            ) hits ORDER BY id
        """, true)
    }
}
