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

// SEARCH lowers every clause to one logical query before the index format sees
// it, so the V2 (CLucene) and SNII formats must answer the same DSL the same way.
suite("test_search_format_parity") {
    // Folds accents and case; SEARCH normalizes prefixes and wildcards with these filters.
    def analyzer = "test_search_format_parity_folding"
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzer}
        PROPERTIES ("tokenizer" = "standard", "token_filter" = "asciifolding, lowercase")
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
        def table = "test_search_format_parity_${fmt.toLowerCase()}"
        tables[fmt] = table
        sql "DROP TABLE IF EXISTS ${table}"
        sql """
            CREATE TABLE ${table} (
                id INT,
                body TEXT,
                tag VARCHAR(64),
                note TEXT,
                folded TEXT,
                INDEX idx_body (body) USING INVERTED PROPERTIES(
                    "parser" = "english", "lower_case" = "true", "support_phrase" = "true"),
                INDEX idx_tag (tag) USING INVERTED,
                INDEX idx_note (note) USING INVERTED PROPERTIES("parser" = "unicode"),
                INDEX idx_folded (folded) USING INVERTED PROPERTIES("analyzer" = "${analyzer}")
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
            (1, 'quick brown fox', 'alpha', 'the theme', 'Café au lait'),
            (2, 'quick fox', 'alphabet', 'Theory', 'cafeteria'),
            (3, 'quick fox 2', 'beta', 'other', 'CAFÉS'),
            (4, 'lazy dog', 'ALPHA', 'the', 'coffee'),
            (5, NULL, NULL, NULL, NULL),
            (6, 'quick', 'alpha', 'thesis', 'Kaffee'),
            (7, '...', 'alp', 'athens', 'café-au-lait')
        """
    }
    sql "sync"
    Thread.sleep(3000)
    sql "set enable_inverted_index_query_cache = false"

    def ids = { String fmt, String predicate ->
        sql("SELECT id FROM ${tables[fmt]} WHERE ${predicate} ORDER BY id").collect { it[0] }
    }
    // Both formats must give the V2 answer.
    def same = { String label, String predicate, List expected ->
        def v2 = ids("V2", predicate)
        assertEquals(expected, v2, "V2: ${label}")
        assertEquals(v2, ids("SNII", predicate), "SNII differs from V2: ${label}")
    }

    // The DSL has no slop syntax: a trailing "~2" is ordinary text, so only the
    // document that contains the token "2" after "quick fox" matches.
    same("phrase with a literal ~2", "search('body:\"quick fox ~2\"')", [3])
    same("exact phrase", "search('body:\"quick fox\"')", [2, 3])

    // A value that analyzes to no token matches nothing and raises no error.
    same("phrase with no tokens", "search('body:\"...\"')", [])

    // As in Elasticsearch's query_string, a PREFIX or WILDCARD value is normalized the
    // way the index normalizes its terms but never analyzed: it is not split, and a
    // stopword stays a prefix. A keyword field keeps the value as written.
    same("prefix on an analyzed field", "search('body:qui*')", [1, 2, 3, 6])
    same("prefix is lowercased like the index", "search('body:QUI*')", [1, 2, 3, 6])
    same("prefix is never split", "search('body:quick-fo*')", [])
    same("prefix of a stopword", "search('note:the*')", [1, 2, 6])
    same("prefix through the analyzer's filters", "search('folded:Café*')", [1, 2, 3, 7])
    same("wildcard through the analyzer's filters", "search('folded:CAF?')", [1, 7])
    same("prefix on a keyword field", "search('tag:alp*')", [1, 2, 6, 7])

    // A prefix matches with a constant score, so score() works on it on every format.
    def prefixScores = { String fmt ->
        sql("""
            SELECT id, score() FROM ${tables[fmt]} WHERE search('body:qui*')
            ORDER BY score() DESC LIMIT 10
        """).collect { [it[0], it[1]] }.sort { it[0] }
    }
    def v2PrefixScores = prefixScores("V2")
    assertEquals([1, 2, 3, 6], v2PrefixScores.collect { it[0] }, "V2: prefix rows with score()")
    assertEquals(v2PrefixScores, prefixScores("SNII"), "SNII: a PREFIX clause must score like V2")

    // REGEXP matches whole terms on every format: "alphabet" contains "alpha" but
    // is not it.
    same("regexp on a keyword field", "search('tag:/alpha/')", [1, 6])

    // A multi-token TERM value follows default_operator on every format.
    same("multi-token term, or",
         "search('quick dog', '{\"default_field\":\"body\",\"default_operator\":\"or\"}')",
         [1, 2, 3, 4, 6])
    same("multi-token term, and",
         "search('quick fox', '{\"default_field\":\"body\",\"default_operator\":\"and\"}')",
         [1, 2, 3])

    // minimum_should_match over several DSL tokens is applied by the compound the parser
    // builds, on every format.
    same("several tokens with minimum_should_match",
         "search('quick fox brown', '{\"default_field\":\"body\",\"minimum_should_match\":2}')",
         [1, 2, 3])
    // One DSL token that analyzes to several terms carries the threshold on the leaf; it
    // is counted above the field on every format, and a single term never carries one.
    same("leaf-level minimum_should_match",
         "search('body:quick/fox', '{\"minimum_should_match\":2}')", [1, 2, 3])
    same("single token with minimum_should_match",
         "search('fox', '{\"default_field\":\"body\",\"minimum_should_match\":2}')", [1, 2, 3])

    // A TERM clause scores on SNII the way it does on V2: the shortest document
    // that contains the term ranks first.
    def best = { String fmt ->
        sql("""
            SELECT id FROM ${tables[fmt]} WHERE search('body:fox')
            ORDER BY score() DESC LIMIT 1
        """).collect { it[0] }
    }
    assertEquals([2], best("V2"), "V2: the shortest matching document ranks first")
    assertEquals([2], best("SNII"), "SNII: a TERM clause must rank like V2")

    formats.each { fmt -> sql "DROP TABLE IF EXISTS ${tables[fmt]}" }
}
