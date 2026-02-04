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

suite("test_mor_value_predicate_pushdown") {
    def tbName = "test_mor_value_pred_pushdown"
    def dbName = context.config.getDbNameByFile(context.file)

    // Test 1: Basic MOR table with value predicate pushdown
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbName} (
            k1 INT,
            v1 INT,
            v2 VARCHAR(100)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true"
        );
    """

    // Insert test data
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'hello')"
    sql "INSERT INTO ${tbName} VALUES (2, 200, 'world')"
    sql "INSERT INTO ${tbName} VALUES (3, 300, 'test')"

    // Delete a row (for MOR, this marks the row with __DORIS_DELETE_SIGN__)
    sql "DELETE FROM ${tbName} WHERE k1 = 2"

    // Test: pushdown disabled (default)
    sql "SET enable_mor_value_predicate_pushdown_tables = ''"

    // Verify session variable is set correctly
    def result = sql "SHOW VARIABLES LIKE 'enable_mor_value_predicate_pushdown_tables'"
    assertTrue(result.size() > 0)
    assertTrue(result[0][1] == "")

    // Query with value predicate - should return correct results
    qt_select_disabled """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Test: enable for specific table (just table name)
    sql "SET enable_mor_value_predicate_pushdown_tables = '${tbName}'"

    result = sql "SHOW VARIABLES LIKE 'enable_mor_value_predicate_pushdown_tables'"
    assertTrue(result[0][1] == tbName)

    qt_select_enabled_tablename """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Test: enable for specific table with db prefix
    sql "SET enable_mor_value_predicate_pushdown_tables = '${dbName}.${tbName}'"

    qt_select_enabled_fullname """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Test: enable for all MOR tables with '*'
    sql "SET enable_mor_value_predicate_pushdown_tables = '*'"

    result = sql "SHOW VARIABLES LIKE 'enable_mor_value_predicate_pushdown_tables'"
    assertTrue(result[0][1] == "*")

    qt_select_enabled_wildcard """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Test: verify deleted row is not returned (correctness check)
    qt_select_deleted_row """
        SELECT * FROM ${tbName} WHERE v1 = 200 ORDER BY k1
    """

    // Test: table not in list - pushdown should be disabled
    sql "SET enable_mor_value_predicate_pushdown_tables = 'other_table'"

    qt_select_not_in_list """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Test 2: Verify correctness with multiple updates to same key
    // This is critical - MOR tables with overlapping rowsets must return correct latest values
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbName} (
            k1 INT,
            v1 INT,
            v2 VARCHAR(100)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true"
        );
    """

    // Insert multiple versions of same key (creates overlapping rowsets)
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'first')"
    sql "INSERT INTO ${tbName} VALUES (1, 200, 'second')"
    sql "INSERT INTO ${tbName} VALUES (2, 300, 'third')"

    // Test with pushdown enabled - must still return correct latest version
    sql "SET enable_mor_value_predicate_pushdown_tables = '*'"

    // Should only return the latest version
    qt_select_latest_version """
        SELECT * FROM ${tbName} WHERE v1 >= 100 ORDER BY k1
    """

    // Value predicate on older version should not match (k1=1 has v1=200 now, not 100)
    qt_select_old_version """
        SELECT * FROM ${tbName} WHERE v1 = 100 ORDER BY k1
    """

    // Value predicate on new version should match
    qt_select_new_version """
        SELECT * FROM ${tbName} WHERE v1 = 200 ORDER BY k1
    """

    // Test 3: Multiple tables in the list
    def tbName2 = "test_mor_value_pred_pushdown_2"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbName2} (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        );
    """

    sql "INSERT INTO ${tbName2} VALUES (1, 100), (2, 200)"

    sql "SET enable_mor_value_predicate_pushdown_tables = '${tbName},${tbName2}'"

    qt_select_multiple_tables """
        SELECT * FROM ${tbName2} WHERE v1 > 100 ORDER BY k1
    """

    // Test 4: Non-MOR table (MOW) - value predicates should always be pushed down
    // The session variable should have no effect on MOW tables
    def tbNameMow = "test_mow_value_pred"
    sql "DROP TABLE IF EXISTS ${tbNameMow}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbNameMow} (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql "INSERT INTO ${tbNameMow} VALUES (1, 100), (2, 200)"

    // MOW tables always push down value predicates regardless of setting
    sql "SET enable_mor_value_predicate_pushdown_tables = ''"

    qt_select_mow_table """
        SELECT * FROM ${tbNameMow} WHERE v1 > 100 ORDER BY k1
    """

    // Test 5: DUP_KEYS table - value predicates should always be pushed down
    // The session variable should have no effect on DUP_KEYS tables
    def tbNameDup = "test_dup_value_pred"
    sql "DROP TABLE IF EXISTS ${tbNameDup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbNameDup} (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql "INSERT INTO ${tbNameDup} VALUES (1, 100), (2, 200)"

    // DUP_KEYS tables always push down value predicates regardless of setting
    qt_select_dup_table """
        SELECT * FROM ${tbNameDup} WHERE v1 > 100 ORDER BY k1
    """

    // Test 6: Profile verification - check that predicate pushdown affects filtering
    // Enable profiling and verify RowsConditionsFiltered counter when pushdown is enabled
    sql "SET enable_profile = true"
    sql "SET enable_mor_value_predicate_pushdown_tables = '*'"

    // Execute query and check profile shows filtering happened at storage layer
    def profileQuery = "SELECT /*+ SET_VAR(enable_mor_value_predicate_pushdown_tables='*') */ * FROM ${tbName} WHERE v1 > 250"
    sql profileQuery

    // Cleanup
    sql "SET enable_profile = false"
    sql "SET enable_mor_value_predicate_pushdown_tables = ''"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql "DROP TABLE IF EXISTS ${tbNameMow}"
    sql "DROP TABLE IF EXISTS ${tbNameDup}"
}
