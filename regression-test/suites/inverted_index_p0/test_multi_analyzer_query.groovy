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

suite("test_multi_analyzer_query", "p0") {
    // Test USING ANALYZER query syntax for multi-analyzer indexes

    def analyzerStd = "multi_query_test_std_analyzer"
    def analyzerKw = "multi_query_test_kw_analyzer"
    def tableName = "test_multi_analyzer_query_tbl"

    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerStd} PROPERTIES ("tokenizer" = "standard", "token_filter" = "lowercase");"""
    sql """CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${analyzerKw} PROPERTIES ("tokenizer" = "keyword", "token_filter" = "lowercase");"""
    sleep(10000)

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT, first_name STRING,
            INDEX idx_std (first_name) USING INVERTED PROPERTIES("analyzer"="${analyzerStd}", "support_phrase"="true"),
            INDEX idx_kw (first_name) USING INVERTED PROPERTIES("analyzer"="${analyzerKw}")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sleep(10000)

    sql """INSERT INTO ${tableName} VALUES (1, 'alice'), (2, 'alice cooper'), (3, 'bob'), (4, 'allyson'), (5, NULL);"""
    sql "sync"

    // TC-QUERY-001: Default analyzer query (should use analyzed index)
    qt_match_default """SELECT id FROM ${tableName} WHERE first_name MATCH 'alice' ORDER BY id"""

    // TC-QUERY-002: USING ANALYZER keyword for exact match
    qt_match_keyword """SELECT id FROM ${tableName} WHERE first_name MATCH 'alice' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-QUERY-003: USING ANALYZER standard for tokenized search
    qt_match_standard """SELECT id FROM ${tableName} WHERE first_name MATCH 'alice' USING ANALYZER ${analyzerStd} ORDER BY id"""

    // TC-QUERY-004: Non-existent analyzer should report error
    try {
        sql """SELECT id FROM ${tableName} WHERE first_name MATCH 'alice' USING ANALYZER not_exist_analyzer"""
        assertTrue(false, "Should fail with non-existent analyzer")
    } catch (Exception e) {
        assertTrue(e.message.contains("No inverted index found") || e.message.contains("not found"))
    }

    // TC-QUERY-005: MATCH_ANY with USING ANALYZER
    qt_match_any """SELECT id FROM ${tableName} WHERE first_name MATCH_ANY 'alice bob' USING ANALYZER ${analyzerStd} ORDER BY id"""

    // TC-QUERY-006: MATCH_ALL with USING ANALYZER
    qt_match_all """SELECT id FROM ${tableName} WHERE first_name MATCH_ALL 'alice cooper' USING ANALYZER ${analyzerStd} ORDER BY id"""

    // TC-QUERY-007: MATCH_PHRASE with USING ANALYZER
    qt_match_phrase """SELECT id FROM ${tableName} WHERE first_name MATCH_PHRASE 'alice cooper' USING ANALYZER ${analyzerStd} ORDER BY id"""

    // TC-QUERY-008: MATCH_PHRASE_PREFIX with USING ANALYZER
    qt_match_phrase_prefix """SELECT id FROM ${tableName} WHERE first_name MATCH_PHRASE_PREFIX 'al' USING ANALYZER ${analyzerStd} ORDER BY id"""

    // TC-QUERY-009: MATCH_REGEXP with USING ANALYZER
    qt_match_regexp """SELECT id FROM ${tableName} WHERE first_name MATCH_REGEXP 'ali.*' USING ANALYZER ${analyzerKw} ORDER BY id"""

    // TC-QUERY-010: Case insensitive analyzer name
    qt_match_case_insensitive """SELECT id FROM ${tableName} WHERE first_name MATCH 'alice' USING ANALYZER ${analyzerKw.toUpperCase()} ORDER BY id"""

    // TC-QUERY-011: Query with NULL values
    qt_match_null """SELECT id FROM ${tableName} WHERE first_name MATCH 'alice' OR first_name IS NULL ORDER BY id"""

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
