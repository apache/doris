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

// Target contract: SEARCH is a two-valued document predicate. A NULL source
// value does not match a leaf; NOT complements the matching document set.
// Ordinary SQL comparison and MATCH predicates retain SQL three-valued logic.
// These are target assertions: do not accept either the old or new result.
suite("test_search_document_boolean_contract", "p0") {
    def allIds = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    def variableNames = [
        "enable_inverted_index_query",
        "enable_inverted_index_query_cache",
        "enable_common_expr_pushdown",
        "experimental_enable_virtual_slot_for_cse"
    ]
    def savedVariables = [:]
    variableNames.each { name ->
        def row = sql("SHOW VARIABLES LIKE '${name}'").find {
            it[0].toString().equalsIgnoreCase(name)
        }
        assertTrue(row != null, "Required session variable is missing: ${name}")
        savedVariables[name] = row[1].toString()
    }

    // Queries only contain fixed test strings. Still escape SQL literals at
    // one boundary so edits cannot silently change the DSL through SQL quoting.
    def sqlLiteral = { String value ->
        "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
    }
    def searchPredicate = { String dsl, String mode ->
        def options = '{"mode":"' + mode + '","default_operator":"OR"}'
        "SEARCH(${sqlLiteral(dsl)}, ${sqlLiteral(options)})"
    }
    def queryIds = { String statement ->
        sql(statement).collect { ((Number) it[0]).intValue() }.sort()
    }
    def assertIds = { List<Integer> expected, String statement, String label ->
        def actual = queryIds(statement)
        assertEquals(expected, actual, "${label}\nSQL: ${statement}")
    }

    try {
        sql "SET enable_inverted_index_query = true"
        // Disable SQL CSE here to isolate SEARCH's document Boolean contract.
        sql "SET experimental_enable_virtual_slot_for_cse = false"
        sql "DROP TABLE IF EXISTS test_search_document_boolean_contract"
        sql """
            CREATE TABLE test_search_document_boolean_contract (
                id INT,
                a TEXT,
                b TEXT,
                c TEXT,
                INDEX idx_a(a) USING INVERTED PROPERTIES("parser"="english"),
                INDEX idx_b(b) USING INVERTED PROPERTIES("parser"="english"),
                INDEX idx_c(c) USING INVERTED PROPERTIES("parser"="english")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES(
                "replication_num"="1",
                "inverted_index_storage_format"="V2"
            )
        """
        // a/b exhaust all nine hit/miss/NULL combinations. c supplies the
        // crossed TRUE/NULL exclusions from DORIS-28633: row 1 is excluded by
        // b despite c=NULL, and row 3 is excluded by c despite b=NULL.
        sql """
            INSERT INTO test_search_document_boolean_contract VALUES
                (1, 'hit',  'hit',  NULL),
                (2, 'hit',  'miss', 'miss'),
                (3, 'hit',  NULL,   'hit'),
                (4, 'miss', 'hit',  'miss'),
                (5, 'miss', 'miss', 'miss'),
                (6, 'miss', NULL,   NULL),
                (7, NULL,   'hit',  'miss'),
                (8, NULL,   'miss', 'hit'),
                (9, NULL,   NULL,   NULL)
        """
        sql "SYNC"
        assertEquals(allIds, queryIds("SELECT id FROM test_search_document_boolean_contract"))

        // Both modes use explicit operators and grouping. In particular,
        // Lucene A OR NOT A is NOT the ordinary Boolean OR of two predicates:
        // NOT is a MUST_NOT modifier at that level. A OR (NOT A) makes the
        // complement a grouped child, giving equivalent Boolean trees here.
        def cases = [
            [name: "leaf", dsl: "a:hit", ids: [1, 2, 3]],
            [name: "and", dsl: "a:hit AND b:hit", ids: [1]],
            [name: "or", dsl: "a:hit OR b:hit", ids: [1, 2, 3, 4, 7]],
            [name: "not_and", dsl: "NOT (a:hit AND b:hit)", ids: [2, 3, 4, 5, 6, 7, 8, 9]],
            [name: "not_or", dsl: "NOT (a:hit OR b:hit)", ids: [5, 6, 8, 9]],
            [name: "not_leaf", dsl: "NOT a:hit", ids: [4, 5, 6, 7, 8, 9]],
            [name: "and_not", dsl: "a:hit AND NOT b:hit", ids: [2, 3]],
            [name: "multi_must_not", dsl: "a:hit AND NOT b:hit AND NOT c:hit", ids: [2]],
            [name: "not_multi_must_not", dsl: "NOT (a:hit AND NOT b:hit AND NOT c:hit)", ids: [1, 3, 4, 5, 6, 7, 8, 9]],
            [name: "tautology", dsl: "a:hit OR (NOT a:hit)", ids: allIds],
            [name: "contradiction", dsl: "a:hit AND NOT a:hit", ids: []],
            [name: "double_not", dsl: "NOT (NOT a:hit)", ids: [1, 2, 3]],
            [name: "empty_hit_stream", dsl: "a:unmatchabletoken", ids: []],
            [name: "not_empty_hit_stream", dsl: "NOT a:unmatchabletoken", ids: allIds]
        ]

        // DORIS-28632 formerly expected a NULL field to remain UNKNOWN; under
        // this selected contract, a:hit AND b:hit is FALSE on row 3, and its
        // complement includes row 3. DORIS-28631 must return that same set
        // with and without score(). Do not preserve the old standard 3VL
        // result by accepting a second list of expected IDs.
        def configurations = [
            [cache: false, segmentLimit: true],
            [cache: true, segmentLimit: true]
        ]
        // SEARCH remains in its known supported scan path for this first
        // contract suite. An enable_common_expr_pushdown=false comparison
        // belongs in a separate capability test after its accepted plan shape
        // is verified; never override such a comparison with a SET_VAR hint.
        configurations.each { configuration ->
            sql "SET enable_inverted_index_query_cache = ${configuration.cache}"
            sql "SET enable_common_expr_pushdown = ${configuration.segmentLimit}"
            ["standard", "lucene"].each { mode ->
                cases.each { tc ->
                    def predicate = searchPredicate(tc.dsl, mode)
                    def complement = allIds.findAll { !tc.ids.contains(it) }
                    // A repeat covers warmed execution without pretending
                    // SQL results alone prove that the DSL cache was hit.
                    // FunctionSearchTest verifies actual cache hits and entry ownership.
                    (1..(configuration.cache ? 2 : 1)).each { repetition ->
                        def label = "${mode}/${tc.name}/${configuration}/run${repetition}"
                        assertIds(tc.ids,
                                "SELECT id FROM test_search_document_boolean_contract WHERE ${predicate} ORDER BY id",
                                label)
                        assertIds(complement,
                                "SELECT id FROM test_search_document_boolean_contract WHERE NOT (${predicate}) ORDER BY id",
                                label + "/sql_not")
                    }
                }
            }
        }

        // Keep the score usage in an existing supported shape: one root
        // SEARCH filter, score() in SELECT/ORDER BY, and a LIMIT. There is no
        // SEARCH projection, and no claim that a score-ranked result has id
        // order. LIMIT exceeds the entire fixture, so compare exact ID sets.
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET enable_common_expr_pushdown = true"
        def scoreCases = cases.findAll {
            it.name in ["and", "or", "not_and", "not_or", "and_not",
                        "multi_must_not", "not_multi_must_not", "field_exists",
                        "field_missing", "empty_hit_stream", "not_empty_hit_stream"]
        }
        ["standard", "lucene"].each { mode ->
            scoreCases.each { tc ->
                def predicate = searchPredicate(tc.dsl, mode)
                ["ASC", "DESC"].each { direction ->
                    def statement = """
                        SELECT id, score() AS s FROM test_search_document_boolean_contract
                        WHERE ${predicate}
                        ORDER BY s ${direction} LIMIT 20
                    """
                    assertIds(tc.ids, statement, "${mode}/${tc.name}/score_${direction}")
                }
            }
        }

        // Protect the outer SQL boundary. NULLs from b='hit' continue to
        // participate in SQL 3VL even though SEARCH(a:hit) itself is two-valued.
        ["standard", "lucene"].each { mode ->
            def predicate = searchPredicate("a:hit", mode)
            assertIds([2, 4, 5, 6, 7, 8, 9],
                    "SELECT id FROM test_search_document_boolean_contract WHERE NOT (${predicate} AND b='hit') ORDER BY id",
                    "${mode}/mixed_sql_three_valued_boundary")
        }
        assertIds([2, 4, 5, 6, 8],
                "SELECT id FROM test_search_document_boolean_contract WHERE NOT (a='hit' AND b='hit') ORDER BY id",
                "ordinary_sql_comparisons_remain_three_valued")
        assertIds([2, 4, 5, 6, 8],
                "SELECT id FROM test_search_document_boolean_contract WHERE NOT ((a MATCH_ANY 'hit') AND (b MATCH_ANY 'hit')) ORDER BY id",
                "ordinary_match_remains_three_valued")
    } finally {
        savedVariables.each { name, value ->
            sql "SET ${name} = ${sqlLiteral(value)}"
        }
    }
}
