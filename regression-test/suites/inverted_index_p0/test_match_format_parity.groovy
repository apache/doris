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

// MATCH on the V2 (CLucene) and SNII formats. Each case records the V2 answer and checks
// that SNII gives the same one; a case the formats answer differently records both.
suite("test_match_format_parity") {
    // The pinyin tokenizer puts a text's first letters at the position of its first syllable.
    def analyzer = "test_match_format_parity_pinyin"
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzer}
        PROPERTIES ("tokenizer" = "pinyin")
    """
    // A new analyzer reaches the backends asynchronously.
    boolean analyzerReady = false
    for (int i = 0; i < 60 && !analyzerReady; i++) {
        try {
            sql """ select tokenize("probe", '"analyzer"="${analyzer}"') """
            analyzerReady = true
        } catch (Exception e) {
            logger.info("analyzer ${analyzer} not ready yet: ${e.message}")
            sleep(1000)
        }
    }
    assertTrue(analyzerReady, "analyzer ${analyzer} did not reach the backends")

    def formats = ["V2", "SNII"]
    def tables = [:]
    formats.each { fmt ->
        def table = "test_match_format_parity_${fmt.toLowerCase()}"
        tables[fmt] = table
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE ${table} (
                id INT,
                body TEXT,
                tag VARCHAR(64),
                stacked TEXT,
                INDEX idx_body (body) USING INVERTED PROPERTIES(
                    "parser" = "unicode", "support_phrase" = "true"),
                INDEX idx_tag (tag) USING INVERTED,
                INDEX idx_stacked (stacked) USING INVERTED PROPERTIES(
                    "analyzer" = "${analyzer}", "support_phrase" = "true")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "inverted_index_storage_format" = "${fmt}"
            )
        """
        sql """
            INSERT INTO ${table} VALUES
            (1, 'quick brown fox', 'alpha', '合作高峰论坛'),
            (2, 'quick fox', 'alphabet', '合作伙伴关系'),
            (3, 'the quick fox jumps', 'beta', '高峰论坛会议'),
            (4, 'lazy dog', 'ALPHA', NULL),
            (5, NULL, NULL, NULL),
            (6, 'fox quick', 'alpha', NULL),
            (7, 'quick the fox', 'alp', NULL),
            (8, 'quickly foxes', '', NULL)
        """
    }
    sql "sync"
    sql "set enable_inverted_index_query_cache = false"

    def ids = { String fmt, String predicate ->
        sql("SELECT id FROM ${tables[fmt]} WHERE ${predicate} ORDER BY id").collect { it[0] }
    }
    // Records the V2 answer and checks that SNII gives the same one.
    def same = { String tag, String predicate ->
        "order_qt_${tag}"("SELECT id FROM ${tables['V2']} WHERE ${predicate}")
        assertEquals(ids("V2", predicate), ids("SNII", predicate), "SNII differs from V2: ${tag}")
    }
    // Records each format's answer where they differ.
    def each_format = { String tag, String predicate ->
        formats.each { fmt ->
            "order_qt_${tag}_${fmt.toLowerCase()}"("SELECT id FROM ${tables[fmt]} WHERE ${predicate}")
        }
    }

    same("any", "body match_any 'fox dog'")
    same("all", "body match_all 'quick fox'")
    same("phrase", "body match_phrase 'quick fox'")
    // unicode drops "the" without leaving a gap, in the index and in the query.
    same("phrase_stopword", "body match_phrase 'quick the fox'")
    same("phrase_slop", "body match_phrase 'quick fox ~1'")
    same("phrase_slop_reversed", "body match_phrase 'fox quick ~2'")
    same("phrase_ordered", "body match_phrase 'quick fox ~1+'")
    same("phrase_prefix", "body match_phrase_prefix 'quick fo'")
    same("phrase_prefix_one", "body match_phrase_prefix 'qui'")
    // MATCH_REGEXP matches anywhere inside a term.
    same("regexp", "body match_regexp 'uic'")
    same("phrase_edge", "body match_phrase_edge 'ick fo'")
    same("phrase_edge_one", "body match_phrase_edge 'uic'")
    // A value that analyzes to nothing matches nothing.
    same("no_tokens", "body match_any '...'")

    same("keyword_any", "tag match_any 'alpha'")
    same("keyword_equal", "tag = 'alpha'")
    same("keyword_in", "tag in ('alpha', 'beta')")
    same("keyword_empty", "tag = ''")
    same("keyword_prefix", "tag match_phrase_prefix 'alp'")
    same("keyword_regexp", "tag match_regexp 'lph'")
    // V2 keeps "~1" in a keyword term; SNII strips it as a slop.
    each_format("keyword_phrase_slop", "tag match_phrase 'alp ~1'")

    // Tokens that share a position: both formats place a phrase's tokens by their order.
    same("stacked_any", "stacked match_any '合作'")
    same("stacked_phrase", "stacked match_phrase '合作高峰'")
    same("stacked_phrase_inner", "stacked match_phrase '高峰论坛'")

    // BM25 ranks the one row that holds both terms first on both formats.
    def best = { String fmt ->
        sql("""
            SELECT id FROM ${tables[fmt]} WHERE body match_any 'jumps quick'
            ORDER BY score() DESC LIMIT 1
        """).collect { it[0] }
    }
    assertEquals([3], best("V2"), "V2: the row with both terms ranks first")
    assertEquals([3], best("SNII"), "SNII: a MATCH_ANY must rank like V2")
}
