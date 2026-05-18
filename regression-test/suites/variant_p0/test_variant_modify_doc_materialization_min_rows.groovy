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
    def tableName = "test_variant_modify_doc_min_rows"

    sql """ set default_variant_max_subcolumns_count = 0 """
    sql """ DROP TABLE IF EXISTS ${tableName} """

    // Step 1: Create table with variant_doc_materialization_min_rows=10
    sql """
        CREATE TABLE ${tableName} (
            k INT,
            v variant<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="10", "variant_doc_hash_shard_count" = "32")>
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
    assertTrue(createStmt.contains('"variant_doc_materialization_min_rows" = "10"'),
        "SHOW CREATE TABLE should show variant_doc_materialization_min_rows=10, got: ${createStmt}")

    // Verify tablet meta shows min_rows=5
    // Insert 3 rows with different paths (3 < 5, so no sub-columns should be materialized)
    sql """ INSERT INTO ${tableName} VALUES
        (1, '{"path_a": 100, "path_b": "hello", "path_c": 1.5}'),
        (2, '{"path_a": 200, "path_b": "world", "path_c": 2.5}'),
        (3, '{"path_a": 300, "path_b": "doris", "path_c": 3.5}')
    """

    // Sync rowsets
    sql "SELECT * FROM ${tableName} LIMIT 1"

    // Enable describe_extend_variant_column and check DESC
    sql """set describe_extend_variant_column = true"""
    // Expect: only k and v columns, NO sub-columns extracted (3 rows < min_rows=5)
    qt_desc_step1 """desc ${tableName}"""

    // Step 2: ALTER TABLE MODIFY COLUMN to change variant_doc_materialization_min_rows to 2
    sql """
        ALTER TABLE ${tableName} MODIFY COLUMN v variant<properties("variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="2", "variant_doc_hash_shard_count" = "32")>;
    """

    // Verify SHOW CREATE TABLE shows min_rows=2
    createResult = sql """ SHOW CREATE TABLE ${tableName} """
    createStmt = createResult[0][1]
    assertTrue(createStmt.contains('"variant_doc_materialization_min_rows" = "2"'),
        "SHOW CREATE TABLE should show variant_doc_materialization_min_rows=2 after ALTER, got: ${createStmt}")

    // Insert 3 more rows with different paths (3 >= 2, so sub-columns should be materialized for new data)
    sql """ INSERT INTO ${tableName} VALUES
        (4, '{"path_d": 400, "path_e": "alpha", "path_f": 4.5}'),
        (5, '{"path_d": 500, "path_e": "beta", "path_f": 5.5}'),
        (6, '{"path_d": 600, "path_e": "gamma", "path_f": 6.5}')
    """

    // Sync rowsets
    sql "SELECT * FROM ${tableName} LIMIT 1"

    // Expect: 3 sub-columns from new data (path_d, path_e, path_f)
    qt_desc_step2 """desc ${tableName}"""

    // Step 3: Trigger compaction - old data (path_a, path_b, path_c) should also get materialized
    trigger_and_wait_compaction(tableName, "full")

    // Sync rowsets
    sql "SELECT * FROM ${tableName} LIMIT 1"

    // Expect: 6 sub-columns total (path_a through path_f all materialized after compaction with min_rows=2)
    qt_desc_step3 """desc ${tableName}"""

    // Verify data integrity
    def count = sql """ SELECT COUNT(*) FROM ${tableName} """
    assertEquals(6, count[0][0])

    // Cleanup
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
