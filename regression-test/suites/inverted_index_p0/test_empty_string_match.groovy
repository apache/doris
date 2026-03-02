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

suite("test_empty_string_match", "p0") {
    def tableName = "test_empty_string_match"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            keyword_col TEXT DEFAULT '',
            english_col TEXT DEFAULT '',
            INDEX keyword_idx(keyword_col) USING INVERTED COMMENT 'keyword index',
            INDEX english_idx(english_col) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'english parser'
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation" = "tag.location.default: 1");
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, '', 'hello world'),
        (2, 'test', ''),
        (3, '', ''),
        (4, 'data', 'some text');
    """

    sql "SET enable_common_expr_pushdown = true"

    // Test 1: Empty string match on keyword index (index path)
    // Should match rows where keyword_col is empty string (rows 1 and 3)
    sql "SET enable_inverted_index_query = true"
    qt_keyword_index_path """SELECT id FROM ${tableName} WHERE keyword_col match '' ORDER BY id"""

    // Test 2: Empty string match on keyword index (slow path)
    // Should also match rows where keyword_col is empty string
    sql "SET enable_inverted_index_query = false"
    sql "SET enable_match_without_inverted_index = true"
    qt_keyword_slow_path """SELECT id FROM ${tableName} WHERE keyword_col match '' ORDER BY id"""

    // Test 3: Empty string match on tokenized index (index path)
    // Should return no rows because empty string tokenizes to nothing
    sql "SET enable_inverted_index_query = true"
    qt_english_index_path """SELECT count() FROM ${tableName} WHERE english_col match ''"""

    // Test 4: Empty string match on tokenized index (slow path)
    // Should also return no rows
    sql "SET enable_inverted_index_query = false"
    qt_english_slow_path """SELECT count() FROM ${tableName} WHERE english_col match ''"""

    // Test 5: Non-empty string match on keyword index should work as before
    sql "SET enable_inverted_index_query = true"
    qt_keyword_nonempty """SELECT id FROM ${tableName} WHERE keyword_col match 'test' ORDER BY id"""

    // Test 6: Verify match_any with empty string on keyword index
    sql "SET enable_inverted_index_query = false"
    qt_match_any_empty """SELECT id FROM ${tableName} WHERE keyword_col match_any '' ORDER BY id"""

    // Test 7: Verify match_all with empty string on keyword index
    qt_match_all_empty """SELECT id FROM ${tableName} WHERE keyword_col match_all '' ORDER BY id"""

    sql "DROP TABLE IF EXISTS ${tableName}"
}
