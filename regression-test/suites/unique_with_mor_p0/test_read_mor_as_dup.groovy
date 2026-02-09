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

suite("test_read_mor_as_dup") {
    def tableName = "test_read_mor_as_dup_tbl"
    def tableName2 = "test_read_mor_as_dup_tbl2"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableName2}"

    // Create a MOR (Merge-On-Read) UNIQUE table
    sql """
        CREATE TABLE ${tableName} (
            `k` int NOT NULL,
            `v1` int NULL,
            `v2` varchar(100) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        );
    """

    // Create a second MOR table for per-table control test
    sql """
        CREATE TABLE ${tableName2} (
            `k` int NOT NULL,
            `v1` int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        );
    """

    // Insert multiple versions of the same key
    sql "INSERT INTO ${tableName} VALUES (1, 10, 'first');"
    sql "INSERT INTO ${tableName} VALUES (1, 20, 'second');"
    sql "INSERT INTO ${tableName} VALUES (1, 30, 'third');"
    sql "INSERT INTO ${tableName} VALUES (2, 100, 'only_version');"

    // Insert a row and then delete it
    sql "INSERT INTO ${tableName} VALUES (3, 50, 'to_be_deleted');"
    sql "DELETE FROM ${tableName} WHERE k = 3;"

    // Insert into second table
    sql "INSERT INTO ${tableName2} VALUES (1, 10);"
    sql "INSERT INTO ${tableName2} VALUES (1, 20);"

    // === Test 1: Normal query returns merged result ===
    sql "SET read_mor_as_dup_tables = '';"
    def normalResult = sql "SELECT * FROM ${tableName} ORDER BY k;"
    // Should see only latest version per key: (1,30,'third'), (2,100,'only_version')
    // Key 3 was deleted, should not appear
    assertTrue(normalResult.size() == 2, "Normal query should return 2 rows (merged), got ${normalResult.size()}")

    // === Test 2: Wildcard — read all MOR tables as DUP ===
    sql "SET read_mor_as_dup_tables = '*';"
    def dupResult = sql "SELECT * FROM ${tableName} ORDER BY k, v1;"
    // Should see all row versions for key 1 (3 versions) + key 2 (1 version)
    // Key 3: delete predicates are still applied, so deleted row should still be filtered
    // But the delete sign filter is skipped, so we may see it depending on how delete was done.
    // For MOR tables, DELETE adds a delete predicate in rowsets, which IS still honored.
    assertTrue(dupResult.size() >= 4, "DUP mode should return at least 4 rows (all versions), got ${dupResult.size()}")

    // Verify key 1 has multiple versions
    def key1Rows = dupResult.findAll { it[0] == 1 }
    assertTrue(key1Rows.size() == 3, "Key 1 should have 3 versions in DUP mode, got ${key1Rows.size()}")

    // === Test 3: Per-table control with db.table format ===
    def dbName = sql "SELECT DATABASE();"
    def currentDb = dbName[0][0]

    sql "SET read_mor_as_dup_tables = '${currentDb}.${tableName}';"

    // tableName should be in DUP mode
    def perTableResult = sql "SELECT * FROM ${tableName} ORDER BY k, v1;"
    assertTrue(perTableResult.size() >= 4, "Per-table DUP mode should return all versions, got ${perTableResult.size()}")

    // tableName2 should still be in normal (merged) mode
    def table2Result = sql "SELECT * FROM ${tableName2} ORDER BY k;"
    assertTrue(table2Result.size() == 1, "Table2 should still be merged (1 row), got ${table2Result.size()}")

    // === Test 4: Per-table control with just table name ===
    sql "SET read_mor_as_dup_tables = '${tableName2}';"

    // tableName should now be in normal mode
    def revertedResult = sql "SELECT * FROM ${tableName} ORDER BY k;"
    assertTrue(revertedResult.size() == 2, "Table1 should be merged again (2 rows), got ${revertedResult.size()}")

    // tableName2 should be in DUP mode
    def table2DupResult = sql "SELECT * FROM ${tableName2} ORDER BY k, v1;"
    assertTrue(table2DupResult.size() == 2, "Table2 in DUP mode should return 2 rows, got ${table2DupResult.size()}")

    // === Cleanup ===
    sql "SET read_mor_as_dup_tables = '';"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableName2}"
}
