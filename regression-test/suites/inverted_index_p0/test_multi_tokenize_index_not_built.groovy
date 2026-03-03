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

suite("test_multi_tokenize_index_not_built", "p0") {
    // Test case for https://github.com/apache/doris/issues/XXXXX
    // When a column has multiple tokenize indexes and one of them is not built,
    // queries using that analyzer should NOT fallback to other indexes.
    // Instead, they should use non-index matching to return correct results.

    def tableName = "test_multi_tokenize_index_not_built"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with one inverted index using standard analyzer (tokenizes by whitespace)
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(512) NOT NULL,
            INDEX idx_title_standard (title) USING INVERTED PROPERTIES("built_in_analyzer" = "standard", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    // Insert test data
    // Difference: standard analyzer tokenizes "hello world" -> ["hello", "world"]
    //            keyword analyzer keeps "hello world" as single token
    sql """
        INSERT INTO ${tableName} VALUES
            (1, 'hello world'),
            (2, 'hello'),
            (3, 'world'),
            (4, 'hello world test'),
            (5, 'foo bar')
    """

    sql "sync"

    // Query using standard analyzer - should use idx_title_standard
    // Standard tokenizes text, so 'hello' matches rows containing 'hello' token
    // Should match rows 1, 2, 4
    qt_standard_before """
        SELECT id FROM ${tableName}
        WHERE title MATCH 'hello' USING ANALYZER standard
        ORDER BY id
    """

    // Add new index with keyword analyzer (no tokenization) but DO NOT build it
    sql """
        ALTER TABLE ${tableName} ADD INDEX idx_title_keyword (title)
        USING INVERTED PROPERTIES("built_in_analyzer" = "none")
    """

    // Wait for schema change to complete
    def waitSchemaChange = { tbl ->
        int maxRetry = 60
        for (int i = 0; i < maxRetry; i++) {
            def result = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY CreateTime DESC LIMIT 1"
            if (result.size() > 0 && result[0][9] == "FINISHED") {
                return true
            }
            sleep(1000)
        }
        return false
    }
    assertTrue(waitSchemaChange(tableName), "Schema change did not finish in time")

    // Query using keyword/none analyzer - idx_title_keyword is NOT built
    // This should NOT use idx_title_standard (which would give wrong results)
    // Instead, it should use non-index matching path (downgrade to slow path)
    // With none analyzer: 'hello' should only match exact 'hello' (row 2)
    // If it wrongly uses standard index, it would match rows 1, 2, 4
    qt_keyword_not_built """
        SELECT id FROM ${tableName}
        WHERE title MATCH 'hello' USING ANALYZER none
        ORDER BY id
    """

    // Query using standard analyzer - should still work with idx_title_standard
    qt_standard_after_add """
        SELECT id FROM ${tableName}
        WHERE title MATCH 'hello' USING ANALYZER standard
        ORDER BY id
    """

    // Now build the keyword index
    // Cloud mode: BUILD INDEX ON table (builds all indexes)
    // Non-cloud mode: BUILD INDEX idx_name ON table (builds specific index)
    if (isCloudMode()) {
        sql "BUILD INDEX ON ${tableName}"
    } else {
        sql "BUILD INDEX idx_title_keyword ON ${tableName}"
    }

    // Wait for build index to complete
    def waitBuildIndex = { tbl ->
        int maxRetry = 60
        for (int i = 0; i < maxRetry; i++) {
            def result = sql "SHOW BUILD INDEX WHERE TableName='${tbl}' ORDER BY CreateTime DESC LIMIT 1"
            if (result.size() > 0 && result[0][7] == "FINISHED") {
                return true
            }
            sleep(1000)
        }
        return false
    }
    assertTrue(waitBuildIndex(tableName), "Build index did not finish in time")

    // Query using keyword analyzer after build - should use idx_title_keyword
    // 'hello' exact match only matches row 2
    qt_keyword_after_build """
        SELECT id FROM ${tableName}
        WHERE title MATCH 'hello' USING ANALYZER none
        ORDER BY id
    """

    // Additional test: search for full string with keyword analyzer
    qt_full_string_keyword """
        SELECT id FROM ${tableName}
        WHERE title MATCH 'hello world' USING ANALYZER none
        ORDER BY id
    """

    // Same search with standard analyzer (tokenizes, so matches 'hello' AND 'world')
    qt_full_string_standard """
        SELECT id FROM ${tableName}
        WHERE title MATCH 'hello world' USING ANALYZER standard
        ORDER BY id
    """

    // Clean up
    sql "DROP TABLE IF EXISTS ${tableName}"
}
