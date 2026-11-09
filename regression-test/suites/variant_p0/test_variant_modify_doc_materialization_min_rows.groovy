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

suite("test_variant_modify_doc_materialization_min_rows", "p0") {
    def variantV2Function = "parse_to_variant"
    def tableName = "test_variant_modify_doc_min_rows"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    // Step 1: Create table with variant_subcolumn_extraction_min_rows=10
    sql """
        CREATE TABLE ${tableName} (
            k INT,
            v variant<properties("variant_enable_doc_mode"="true","variant_subcolumn_extraction_min_rows"="10", "variant_sparse_shard_count" = "32")>
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """

    // Verify SHOW CREATE TABLE shows min_rows=5
    def createResult = sql """ SHOW CREATE TABLE ${tableName} """
    def createStmt = createResult[0][1]
    assertTrue(createStmt.contains('"variant_subcolumn_extraction_min_rows" = "10"'),
        "SHOW CREATE TABLE should show variant_subcolumn_extraction_min_rows=10, got: ${createStmt}")

    // Verify tablet meta shows min_rows=5
    // Insert 3 rows with different paths (3 < 5, so no sub-columns should be materialized)
    sql """ INSERT INTO ${tableName} VALUES
        (1, ${variantV2Function}('{"path_a": 100, "path_b": "hello", "path_c": 1.5}')),
        (2, ${variantV2Function}('{"path_a": 200, "path_b": "world", "path_c": 2.5}')),
        (3, ${variantV2Function}('{"path_a": 300, "path_b": "doris", "path_c": 3.5}'))
    """

    // Sync rowsets
    sql "SELECT * FROM ${tableName} LIMIT 1"

    // Enable describe_extend_variant_column and check DESC
    sql """set describe_extend_variant_column = true"""
    // Expect: only k and v columns, NO sub-columns extracted (3 rows < min_rows=5)
    qt_desc_step1 """desc ${tableName}"""

    // Step 2: MODIFY COLUMN cannot change the row threshold
    test {
        sql """
            ALTER TABLE ${tableName} MODIFY COLUMN v variant<properties("variant_enable_doc_mode"="true","variant_subcolumn_extraction_min_rows"="2", "variant_sparse_shard_count" = "32")>;
        """
        exception "Can not change variant subcolumn extraction min rows"
    }

    // The threshold stays 10, so a compaction of the 3 rows extracts nothing either
    trigger_and_wait_compaction(tableName, "full")
    sql "SELECT * FROM ${tableName} LIMIT 1"
    qt_desc_step2 """desc ${tableName}"""

    def count = sql """ SELECT COUNT(*) FROM ${tableName} """
    assertEquals(3, count[0][0])

    // Cleanup
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
