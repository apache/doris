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

// A NULL ARRAY produced from real columns may retain nested values and index postings.
// SEARCH must filter those postings before Boolean NOT, while SQL MATCH stays nullable.
suite("test_search_null_array_payload_contract", "p0") {
    sql "DROP TABLE IF EXISTS test_search_null_array_payload_source"
    sql "DROP TABLE IF EXISTS test_search_null_array_payload_target"
    sql """
        CREATE TABLE test_search_null_array_payload_source (
            id INT, lhs ARRAY<STRING>, rhs ARRAY<STRING>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """
    sql """
        CREATE TABLE test_search_null_array_payload_target (
            id INT, a ARRAY<STRING>,
            INDEX idx_a(a) USING INVERTED
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1", "inverted_index_storage_format"="V2")
    """
    sql """
        INSERT INTO test_search_null_array_payload_source VALUES
            (1, ['hit'], NULL),
            (2, ['hit'], ['tail']),
            (3, ['miss'], [])
    """
    sql """
        INSERT INTO test_search_null_array_payload_target
        SELECT id, array_concat(lhs, rhs) FROM test_search_null_array_payload_source
    """
    sql "SYNC"
    def ids = { String query -> sql(query).collect { ((Number)it[0]).intValue() }.sort() }
    assertEquals([1], ids("SELECT id FROM test_search_null_array_payload_target WHERE a IS NULL ORDER BY id"))
    def cases = [
        [dsl: 'a:hit', matches: [2], complement: [1,3]],
        [dsl: 'a:"hit"', matches: [2], complement: [1,3]],
        [dsl: 'a:h*', matches: [2], complement: [1,3]],
        [dsl: 'a:*', matches: [2,3], complement: [1]]
    ]
    def variable = "enable_inverted_index_query_cache"
    def saved = sql("SHOW VARIABLES LIKE '${variable}'")[0][1]
    try {
        ["standard", "lucene"].each { mode ->
            [false, true].each { cache ->
                sql "SET ${variable} = ${cache}"
                cases.each { tc ->
                    def options = '{"mode":"' + mode + '"}'
                    def positive = "SEARCH('${tc.dsl}', '${options}')"
                    def negative = "SEARCH('NOT (${tc.dsl})', '${options}')"
                    (0..<2).each { repetition ->
                        assertEquals(tc.matches, ids("SELECT id FROM test_search_null_array_payload_target WHERE ${positive} ORDER BY id"), "${mode}/${cache}/${tc.dsl}/${repetition}")
                        assertEquals(tc.complement, ids("SELECT id FROM test_search_null_array_payload_target WHERE ${negative} ORDER BY id"))
                        assertEquals(tc.complement, ids("SELECT id FROM test_search_null_array_payload_target WHERE NOT (${positive}) ORDER BY id"))
                    }
                    ["ASC", "DESC"].each { direction ->
                        assertEquals(tc.matches, ids("SELECT id, score() AS s FROM test_search_null_array_payload_target WHERE ${positive} ORDER BY s ${direction} LIMIT 20"))
                        // A single matching row must survive even when K is smaller
                        // than the segment and the untokenized score is NaN.
                        if (tc.matches == [2]) {
                            assertEquals([2], ids("SELECT id, score() AS s FROM test_search_null_array_payload_target WHERE ${positive} ORDER BY s ${direction} LIMIT 1"))
                        }
                        assertEquals(tc.complement, ids("SELECT id, score() AS s FROM test_search_null_array_payload_target WHERE ${negative} ORDER BY s ${direction} LIMIT 20"))
                    }
                }
            }
        }
        assertEquals([2], ids("SELECT id FROM test_search_null_array_payload_target WHERE a MATCH_ANY 'hit' ORDER BY id"))
        assertEquals([3], ids("SELECT id FROM test_search_null_array_payload_target WHERE NOT (a MATCH_ANY 'hit') ORDER BY id"))
    } finally {
        sql "SET ${variable} = ${saved}"
    }
}
