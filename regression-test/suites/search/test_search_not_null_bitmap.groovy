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

suite("test_search_not_null_bitmap", "p0") {
    // SEARCH uses two-valued matching; SQL comparisons and MATCH retain three-valued logic.
    // Regression test for DORIS-24681:
    // A NULL field is non-matching inside SEARCH. Internal NOT and SQL
    // NOT SEARCH complement that two-valued result and include NULL rows.
    // NOT MATCH still excludes UNKNOWN and is covered separately below.

    def tableName = "search_not_null_bitmap"

    sql """ set enable_segment_limit_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id INT,
            msg TEXT,
            INDEX idx_msg (msg) USING INVERTED PROPERTIES("parser" = "unicode")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, NULL),
        (2, 'omega alpha'),
        (3, 'hello world'),
        (4, 'alpha beta')
    """

    Thread.sleep(5000)

    // ---------------------------------------------------------------
    // Internal and external NOT SEARCH include NULL rows
    // ---------------------------------------------------------------

    // NOT in the DSL keeps the NULL row.
    qt_not_internal_ids """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${tableName}
        WHERE search('NOT msg:omega')
        ORDER BY id
    """

    // SQL NOT consumes SEARCH's non-nullable match result
    qt_not_external_ids """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${tableName}
        WHERE NOT search('msg:omega')
        ORDER BY id
    """

    qt_not_internal_count """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('NOT msg:omega')
    """

    qt_not_external_count """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ count(*) FROM ${tableName}
        WHERE NOT search('msg:omega')
    """

    qt_not_match_ids """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${tableName}
        WHERE NOT (msg MATCH_ANY 'omega')
        ORDER BY id
    """

    // ---------------------------------------------------------------
    // Test with all rows NULL for the searched field
    // ---------------------------------------------------------------

    def allNullTable = "search_not_all_null"

    sql "DROP TABLE IF EXISTS ${allNullTable}"

    sql """
        CREATE TABLE ${allNullTable} (
            id INT,
            msg TEXT,
            INDEX idx_msg (msg) USING INVERTED PROPERTIES("parser" = "unicode")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${allNullTable} VALUES
        (1, NULL),
        (2, NULL),
        (3, NULL)
    """

    Thread.sleep(5000)

    // All NULL rows are non-matching leaves, so NOT SEARCH includes every row
    qt_all_null_internal """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ count(*) FROM ${allNullTable}
        WHERE search('NOT msg:anything')
    """

    qt_all_null_external """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ count(*) FROM ${allNullTable}
        WHERE NOT search('msg:anything')
    """

    qt_all_null_not_match_count """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ count(*) FROM ${allNullTable}
        WHERE NOT (msg MATCH_ANY 'anything')
    """

    // ---------------------------------------------------------------
    // Test with mixed NULL and matching values (multi-field)
    // ---------------------------------------------------------------

    def mixedTable = "search_not_mixed_null"

    sql "DROP TABLE IF EXISTS ${mixedTable}"

    // Use unicode parser to avoid stemming ambiguity in test expectations
    sql """
        CREATE TABLE ${mixedTable} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "unicode"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "unicode")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${mixedTable} VALUES
        (1, 'hello world', 'good morning'),
        (2, NULL, 'good afternoon'),
        (3, 'hello earth', NULL),
        (4, NULL, NULL),
        (5, 'goodbye world', 'good evening'),
        (6, 'test title', 'good night')
    """

    Thread.sleep(5000)

    // NOT on title includes the NULL title rows (id=2,4)
    qt_mixed_not_title_search """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE search('NOT title:hello')
        ORDER BY id
    """

    qt_mixed_not_title_external """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE NOT search('title:hello')
        ORDER BY id
    """

    // NOT on content includes the NULL content rows (id=3,4)
    qt_mixed_not_content_search """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE search('NOT content:morning')
        ORDER BY id
    """

    qt_mixed_not_content_external """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE NOT search('content:morning')
        ORDER BY id
    """

    // Complex: (title:hello OR content:good) AND NOT title:goodbye
    qt_mixed_complex_search """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE search('(title:hello OR content:good) AND NOT title:goodbye')
        ORDER BY id
    """

    qt_mixed_complex_external """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE (search('title:hello') OR search('content:good'))
          AND NOT search('title:goodbye')
        ORDER BY id
    """

    // ---------------------------------------------------------------
    // Test with multiple MUST_NOT clauses
    // ---------------------------------------------------------------

    qt_multi_must_not_search """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE search('NOT title:hello AND NOT title:goodbye')
        ORDER BY id
    """

    qt_multi_must_not_external """
        SELECT /*+SET_VAR(enable_segment_limit_pushdown=true) */ id FROM ${mixedTable}
        WHERE NOT search('title:hello')
          AND NOT search('title:goodbye')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${allNullTable}"
    sql "DROP TABLE IF EXISTS ${mixedTable}"
}
