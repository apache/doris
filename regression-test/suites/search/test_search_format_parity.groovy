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
                INDEX idx_body (body) USING INVERTED PROPERTIES(
                    "parser" = "english", "lower_case" = "true", "support_phrase" = "true"),
                INDEX idx_tag (tag) USING INVERTED
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
            (1, 'quick brown fox', 'alpha'),
            (2, 'quick fox', 'alphabet'),
            (3, 'quick fox 2', 'beta'),
            (4, 'lazy dog', 'ALPHA'),
            (5, NULL, NULL),
            (6, 'quick', 'alpha'),
            (7, '...', 'alp')
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

    // PREFIX analyzes the stem on an analyzed field and is one case-sensitive
    // wildcard on a keyword field.
    same("prefix on an analyzed field", "search('body:qui*')", [1, 2, 3, 6])
    same("prefix on a keyword field", "search('tag:alp*')", [1, 2, 6, 7])

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
