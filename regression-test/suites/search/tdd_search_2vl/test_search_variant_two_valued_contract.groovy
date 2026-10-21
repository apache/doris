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

// Target: all SEARCH modes are two-valued document predicates, including
// a Variant path PROVEN absent in the segment. Index unavailability is a
// different state and is covered by the BE unavailable-binding tests.
// This fixture only fixes Boolean/NULL semantics. It does not choose the
// ignore_above contract or promise Elasticsearch indexed-value exists parity.
suite("test_search_variant_two_valued_contract", "p0") {
    // V1 INSERT parses JSON with the table's properties; parse_to_variant returns Variant(0),
    // which branch-4.2 cannot cast to the table's Variant(2048). V2 needs explicit JSON parsing.
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    def allIds = [1, 2, 3, 4]
    def variableNames = [
        "enable_inverted_index_query",
        "enable_inverted_index_query_cache",
        "enable_segment_limit_pushdown",
        "experimental_enable_virtual_slot_for_cse"
    ]
    def savedVariables = [:]
    variableNames.each { name ->
        def row = sql("SHOW VARIABLES LIKE '${name}'").find {
            it[0].toString().equalsIgnoreCase(name)
        }
        assertTrue(row != null)
        savedVariables[name] = row[1].toString()
    }
    def sqlLiteral = { String value ->
        "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
    }
    def searchPredicate = { String dsl, String mode ->
        def options = '{"mode":"' + mode + '","default_operator":"OR"}'
        "SEARCH(${sqlLiteral(dsl)}, ${sqlLiteral(options)})"
    }
    def assertIds = { String label, String predicate, List<Integer> expected ->
        def statement = "SELECT id FROM test_search_variant_two_valued_contract WHERE ${predicate} ORDER BY id"
        def actual = sql(statement).collect { it[0].toString().toInteger() }
        logger.info("${label}: expected=${expected}, actual=${actual}, sql=${statement}")
        assertEquals(expected, actual)
    }

    try {
        sql "SET enable_inverted_index_query = true"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET enable_segment_limit_pushdown = true"
        sql "SET experimental_enable_virtual_slot_for_cse = false"
        sql "DROP TABLE IF EXISTS test_search_variant_two_valued_contract"
        sql """
            CREATE TABLE test_search_variant_two_valued_contract (
                id INT,
                s VARCHAR(16),
                n INT,
                v VARIANT<PROPERTIES(
                    "variant_enable_doc_mode"="false"
                )>,
                INDEX idx_s(s) USING INVERTED PROPERTIES("parser"="none"),
                INDEX idx_n(n) USING INVERTED,
                INDEX idx_v(v) USING INVERTED PROPERTIES("parser"="none")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES(
                "replication_num"="1",
                "inverted_index_storage_format"="V2",
                "disable_auto_compaction"="true"
            )
        """
        // Doc mode is disabled, and the single present path fits the default
        // materialization budget. This is a known-absent-path fixture rather
        // than a sparse/doc fallback fixture.
        // One small INSERT avoids old/new index generations and compaction.
        // Values are short ASCII, with no analyzer or ignore_above ambiguity.
        sql """
            INSERT INTO test_search_variant_two_valued_contract VALUES
                (1, 'hit',  NULL, ${variantV2Function}('{"present":"hit"}')),
                (2, 'miss', 1,    ${variantV2Function}('{"present":"miss"}')),
                (3, NULL,   1,    ${variantV2Function}('{}')),
                (4, 'hit',  2,    NULL)
        """

        sql "SYNC"

        // Source-value checks make the fixture independently reviewable.
        // They do not claim to prove a per-segment binding state; the BE
        // tests separately check binding failures and complete missing-field AST evaluation.
        assertIds("fixture/all_rows", "TRUE", allIds)
        assertIds("fixture/scalar_null", "s IS NULL", [3])
        assertIds("fixture/absent_path", "v['absent'] IS NULL", allIds)
        assertIds("fixture/present_path_null", "v['present'] IS NULL", [3, 4])
        assertIds("fixture/present_path_hit", "CAST(v['present'] AS STRING) = 'hit'", [1])

        def cases = [
            // A wholly empty iterator bundle must still evaluate the AST.
            [name: "known_missing_leaf", dsl: "v.absent:hit", ids: []],
            [name: "known_missing_not", dsl: "NOT v.absent:hit", ids: allIds],
            [name: "known_missing_double_not", dsl: "NOT (NOT v.absent:hit)", ids: []],
            [name: "all_missing_nested_ast",
                dsl: "NOT (v.absent:hit OR v.also_absent:hit)", ids: allIds],

            // A known-missing child is FALSE even when another child has a
            // usable reader. It must not become a full UNKNOWN bitmap.
            [name: "bound_and_missing", dsl: "s:hit AND v.absent:hit", ids: []],
            [name: "not_bound_and_missing", dsl: "NOT (s:hit AND v.absent:hit)", ids: allIds],
            [name: "bound_or_missing", dsl: "s:hit OR v.absent:hit", ids: [1, 4]],
            [name: "not_bound_or_missing", dsl: "NOT (s:hit OR v.absent:hit)", ids: [2, 3]],
            [name: "bound_leaf", dsl: "s:hit", ids: [1, 4]],
            [name: "bound_not_leaf", dsl: "NOT s:hit", ids: [2, 3]],

            // Numeric terms use the same two-valued Boolean contract.
            [name: "numeric_leaf", dsl: "n:1", ids: [2, 3]],
            [name: "numeric_not_leaf", dsl: "NOT n:1", ids: [1, 4]]
        ]

        ["standard", "lucene"].each { mode ->
            cases.each { tc ->
                def predicate = searchPredicate(tc.dsl, mode)
                assertIds("${mode}/${tc.name}", predicate, tc.ids)
                // SEARCH itself is non-nullable, so SQL NOT must yield the
                // exact complement of the complete SEARCH result as well.
                def complement = allIds.findAll { !tc.ids.contains(it) }
                assertIds("${mode}/${tc.name}/sql_not", "NOT (${predicate})", complement)
            }

            def predicate = searchPredicate("s:hit", mode)
            // Row 1 remains UNKNOWN because n is NULL in an OUTER SQL
            // comparison. The SEARCH leaf being two-valued does not change
            // SQL AND/NOT semantics outside SEARCH.
            assertIds("${mode}/outer_sql_three_valued_boundary",
                    "NOT (n = 1 AND ${predicate})", [2, 3, 4])

            // These WHERE-only assertions also protect FE nullability and
            // constant folding. They may fold before reaching BE, so they
            // are not a substitute for direct scorer/binding unit tests.
            assertIds("${mode}/search_is_never_null", "(${predicate}) IS NULL", [])
            assertIds("${mode}/search_is_always_not_null", "(${predicate}) IS NOT NULL", allIds)
        }
    } finally {
        savedVariables.each { name, value ->
            sql "SET ${name} = ${sqlLiteral(value)}"
        }
    }
}
