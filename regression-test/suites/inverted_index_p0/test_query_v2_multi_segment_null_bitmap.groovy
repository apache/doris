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

suite("test_query_v2_multi_segment_null_bitmap", "nonConcurrent") {
    def tableName = "test_query_v2_multi_segment_null_bitmap"
    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    def originalMaxBufferedDocs = [:]
    boolean beConfigUpdated = false

    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)

    try {
        backendIdToIp.each { backendId, ip ->
            def (code, out, err) = show_be_config(ip, backendIdToHttpPort.get(backendId))
            assertEquals(code, 0)
            def configList = parseJson(out.trim())
            for (Object entry in (List) configList) {
                if (((List<String>) entry)[0] == "inverted_index_max_buffered_docs") {
                    originalMaxBufferedDocs[backendId] = ((List<String>) entry)[2]
                }
            }
        }
        assertEquals(originalMaxBufferedDocs.size(), backendIdToIp.size())

        backendIdToIp.each { backendId, ip ->
            def (code, out, err) = update_be_config(
                    ip, backendIdToHttpPort.get(backendId), "inverted_index_max_buffered_docs", "2")
            assertEquals(code, 0)
        }
        beConfigUpdated = true
        backendIdToIp.each { backendId, ip ->
            def (code, out, err) = show_be_config(ip, backendIdToHttpPort.get(backendId))
            assertEquals(code, 0)
            def configList = parseJson(out.trim())
            def maxBufferedDocs = ((List) configList).find {
                ((List<String>) it)[0] == "inverted_index_max_buffered_docs"
            }
            assertEquals(((List<String>) maxBufferedDocs)[2], "2")
        }

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                body TEXT NULL,
                aux TEXT NULL,
                score INT NULL,
                INDEX body_idx (body) USING INVERTED PROPERTIES(
                    "parser" = "english",
                    "support_phrase" = "true"
                ),
                INDEX aux_idx (aux) USING INVERTED PROPERTIES(
                    "parser" = "english",
                    "support_phrase" = "true"
                ),
                INDEX score_idx (score) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES(
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            )
        """

        // Each batch exceeds max_buffered_docs=2, forcing the IndexWriter to flush
        // multiple CLucene segments within a single rowset inverted index.
        sql """
            INSERT INTO ${tableName} VALUES
                (1, 'alpha beta gamma', 'side', 10),
                (2, 'alpha beta', NULL, 20),
                (3, NULL, 'alpha', NULL)
        """
        sql """
            INSERT INTO ${tableName} VALUES
                (4, 'alphabet soup', 'side', 40),
                (5, 'gamma delta', 'aux phrase', 50),
                (6, 'alpha gamma', NULL, 60),
                (7, NULL, NULL, NULL),
                (8, 'edge alpha beta', 'side beta', 80)
        """

        // These indexes were declared with the table, so INSERT writes and closes their files
        // before the rowset is published. There is no asynchronous BUILD INDEX job to wait for.
        sql "SET enable_inverted_index_query = true"
        sql "SET enable_segment_limit_pushdown = true"

        def assertQueryIds = { expected, statement ->
            assert sql(statement) == expected
        }

        // IS NULL and IS NOT NULL use the regular inverted-index NULL bitmap path rather than
        // query-v2. The NULL rows are in different CLucene sub segments.
        assertQueryIds([[3], [7]], """ SELECT id FROM ${tableName} WHERE body IS NULL ORDER BY id """)
        assertQueryIds([[1], [2], [4], [5], [6], [8]], """ SELECT id FROM ${tableName} WHERE body IS NOT NULL ORDER BY id """)

        // Combine the regular NULL predicate with SEARCH to cover outer three-valued logic.
        assertQueryIds([[1], [2], [3], [6], [7], [8]], """ SELECT id FROM ${tableName} WHERE body IS NULL OR SEARCH('body:alpha') ORDER BY id """)
        assertQueryIds([[1], [2], [6], [8]], """ SELECT id FROM ${tableName} WHERE body IS NOT NULL AND SEARCH('body:alpha') ORDER BY id """)

        // SEARCH leaf queries use the user-facing DSL syntax parsed by FE.
        assertQueryIds([[1], [2], [6], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:alpha') ORDER BY id """)
        assertQueryIds([[1], [2], [6], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:EXACT(alpha)') ORDER BY id """)
        assertQueryIds([[1], [2], [4], [6], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:al*') ORDER BY id """)
        assertQueryIds([[1], [2], [6], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:a?pha') ORDER BY id """)
        assertQueryIds([[1], [2], [4], [6], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:/alpha.*/') ORDER BY id """)
        assertQueryIds([[1], [2], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:"alpha beta"') ORDER BY id """)
        assertQueryIds([[1], [2], [5], [6], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:ANY("alpha gamma")') ORDER BY id """)
        assertQueryIds([[1], [6]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:ALL("alpha gamma")') ORDER BY id """)
        assertQueryIds([], """ SELECT id FROM ${tableName} WHERE SEARCH('score:[10 TO 50]') ORDER BY id """)
        assertQueryIds([], """ SELECT id FROM ${tableName} WHERE SEARCH('score:IN(10 20)') ORDER BY id """)
        assertQueryIds([[1], [2], [3], [4], [5], [6], [7], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:*') ORDER BY id """)

        // Cross-field boolean paths must preserve global document semantics when one operand is NULL.
        assertQueryIds([[1]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:ALL("alpha gamma") AND aux:ANY(side)') ORDER BY id """)
        assertQueryIds([[1], [3], [6]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:ALL("alpha gamma") OR aux:ANY(alpha)') ORDER BY id """)
        assertQueryIds([[1]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:ALL("alpha gamma") AND aux:ANY(side)', '{"mode":"standard"}') ORDER BY id """)
        assertQueryIds([[1], [3], [6]], """ SELECT id FROM ${tableName} WHERE SEARCH('body:ALL("alpha gamma") OR aux:ANY(alpha)', '{"mode":"standard"}') ORDER BY id """)

        // body is NULL for ids 3 and 7. SQL NOT(NULL) is UNKNOWN, so neither row may be returned.
        // The first query verifies that
        // FunctionSearch propagates its NULL bitmap to an outer SQL NOT predicate.
        assertQueryIds([[4], [5]], """ SELECT id FROM ${tableName} WHERE NOT SEARCH('body:ANY(alpha)', '{"mode":"standard"}') ORDER BY id """)
        assertQueryIds([[4], [5]], """ SELECT id FROM ${tableName} WHERE SEARCH('NOT body:ANY(alpha)', '{"mode":"standard"}') ORDER BY id """)

        // A AND B is UNKNOWN for ids 6 and 7. Both outer and DSL-internal NOT must
        // exclude those rows, while retaining rows where the inner expression is FALSE.
        assertQueryIds([[2], [3], [4], [5], [8]], """ SELECT id FROM ${tableName} WHERE NOT SEARCH('body:ALL("alpha gamma") AND aux:ANY(side)', '{"mode":"standard"}') ORDER BY id """)
        assertQueryIds([[2], [3], [4], [5], [8]], """ SELECT id FROM ${tableName} WHERE SEARCH('NOT (body:ALL("alpha gamma") AND aux:ANY(side))', '{"mode":"standard"}') ORDER BY id """)

        // A OR B is UNKNOWN for ids 2, 3, and 7; only id 5 is FALSE and may pass NOT.
        assertQueryIds([[5]], """ SELECT id FROM ${tableName} WHERE SEARCH('NOT (body:ALL("alpha gamma") OR aux:ANY(side))', '{"mode":"standard"}') ORDER BY id """)

        // SQL MATCH and multi_match cover the externally exposed match query types with NULL values.
        assertQueryIds([[1], [2], [6], [8]], """ SELECT id FROM ${tableName} WHERE body MATCH 'alpha' ORDER BY id """)
        assertQueryIds([[1], [2], [5], [6], [8]], """ SELECT id FROM ${tableName} WHERE body MATCH_ANY 'alpha gamma' ORDER BY id """)
        assertQueryIds([[1], [6]], """ SELECT id FROM ${tableName} WHERE body MATCH_ALL 'alpha gamma' ORDER BY id """)
        assertQueryIds([[1], [2], [8]], """ SELECT id FROM ${tableName} WHERE body MATCH_PHRASE 'alpha beta' ORDER BY id """)
        assertQueryIds([[8]], """ SELECT id FROM ${tableName} WHERE body MATCH_PHRASE_PREFIX 'alpha be' ORDER BY id """)
        assertQueryIds([[1], [2], [4], [6], [8]], """ SELECT id FROM ${tableName} WHERE body MATCH_REGEXP 'alpha.*' ORDER BY id """)
        assertQueryIds([[1], [2], [8]], """ SELECT id FROM ${tableName} WHERE body MATCH_PHRASE_EDGE 'alpha b' ORDER BY id """)
        assertQueryIds([[1], [4], [8]], """ SELECT id FROM ${tableName} WHERE multi_match(body, aux, 'any', 'side') ORDER BY id """)
        assertQueryIds([[1], [2], [3], [6], [8]], """ SELECT id FROM ${tableName} WHERE multi_match(body, aux, 'all', 'alpha') ORDER BY id """)
        assertQueryIds([[1], [2], [8]], """ SELECT id FROM ${tableName} WHERE multi_match(body, aux, 'phrase', 'alpha beta') ORDER BY id """)
        assertQueryIds([[8]], """ SELECT id FROM ${tableName} WHERE multi_match(body, aux, 'phrase_prefix', 'alpha be') ORDER BY id """)
    } finally {
        if (beConfigUpdated) {
            backendIdToIp.each { backendId, ip ->
                def (code, out, err) = update_be_config(
                        ip, backendIdToHttpPort.get(backendId), "inverted_index_max_buffered_docs",
                        originalMaxBufferedDocs[backendId])
                assertEquals(code, 0)
            }
        }
    }
}
