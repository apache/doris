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

    // Test 1: Basic MOR table with value predicate pushdown (dedup-only scenario)
    // This feature is designed for insert-only/dedup-only workloads where
    // the same key always has identical values across rowsets.
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

    // Insert test data across separate rowsets (dedup-only: same key has same values)
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'hello'), (2, 200, 'world'), (3, 300, 'test')"
    // Re-insert duplicates to create overlapping rowsets for dedup
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'hello'), (2, 200, 'world')"

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

    // Test: equality predicate on value column with pushdown
    qt_select_eq_predicate """
        SELECT * FROM ${tbName} WHERE v1 = 200 ORDER BY k1
    """

    // Test: table not in list - pushdown should be disabled
    sql "SET enable_mor_value_predicate_pushdown_tables = 'other_table'"

    qt_select_not_in_list """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Test 2: Multiple rowsets with dedup-only pattern
    // Verify correctness when same keys appear across many rowsets with identical values
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

    // Create multiple overlapping rowsets (dedup-only: all versions have same values)
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'first'), (2, 300, 'third')"
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'first')"
    sql "INSERT INTO ${tbName} VALUES (2, 300, 'third'), (3, 500, 'fifth')"

    // Test with pushdown enabled
    sql "SET enable_mor_value_predicate_pushdown_tables = '*'"

    // Should return all rows matching predicate after dedup
    qt_select_dedup_all """
        SELECT * FROM ${tbName} WHERE v1 >= 100 ORDER BY k1
    """

    // Equality match on a value that exists
    qt_select_dedup_eq """
        SELECT * FROM ${tbName} WHERE v1 = 300 ORDER BY k1
    """

    // Predicate that matches no rows
    qt_select_dedup_none """
        SELECT * FROM ${tbName} WHERE v1 = 999 ORDER BY k1
    """

    // Test 3: Dedup + delete scenario
    // Value columns are identical across rowsets (dedup-only), but some rows are deleted.
    // Verifies that __DORIS_DELETE_SIGN__ is NOT pushed per-segment so deletions are honored.
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

    // Rowset 1: initial data
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'hello'), (2, 200, 'world'), (3, 300, 'test')"
    // Rowset 2: dedup insert (same key, same values)
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'hello'), (2, 200, 'world')"
    // Rowset 3: delete k1=2 via insert with __DORIS_DELETE_SIGN__=1
    // Value columns are identical (dedup-only), only delete sign differs
    sql "INSERT INTO ${tbName}(k1, v1, v2, __DORIS_DELETE_SIGN__) VALUES (2, 200, 'world', 1)"

    sql "SET enable_mor_value_predicate_pushdown_tables = '*'"

    // Deleted row must not appear even though v1=200 matches the predicate
    qt_select_delete_range """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Equality on deleted row's value — should return empty
    qt_select_delete_eq """
        SELECT * FROM ${tbName} WHERE v1 = 200 ORDER BY k1
    """

    // Broader predicate — deleted row still excluded
    qt_select_delete_all """
        SELECT * FROM ${tbName} WHERE v1 >= 100 ORDER BY k1
    """

    // Test 4: Dedup + delete predicate scenario
    // DELETE FROM creates a delete predicate stored in rowset metadata.
    // Delete predicates go through DeleteHandler, separate from value predicates.
    // Verify they work correctly alongside value predicate pushdown.
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

    // Rowset 1: initial data
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'hello'), (2, 200, 'world'), (3, 300, 'test')"
    // Rowset 2: dedup insert (same key, same values)
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'hello'), (2, 200, 'world')"
    // Delete predicate rowset: DELETE FROM creates a predicate, not row markers
    sql "DELETE FROM ${tbName} WHERE k1 = 2"

    sql "SET enable_mor_value_predicate_pushdown_tables = '*'"

    // Deleted row must not appear
    qt_select_delpred_range """
        SELECT * FROM ${tbName} WHERE v1 > 150 ORDER BY k1
    """

    // Equality on deleted row's value — should return empty
    qt_select_delpred_eq """
        SELECT * FROM ${tbName} WHERE v1 = 200 ORDER BY k1
    """

    // Broader predicate — deleted row still excluded
    qt_select_delpred_all """
        SELECT * FROM ${tbName} WHERE v1 >= 100 ORDER BY k1
    """

    // Test 5: Update scenario — proves pushdown is active at storage layer
    // k1=1 is updated from v1=100 to v1=500 across two rowsets.
    // Comparing results with pushdown disabled vs enabled for the SAME query
    // proves per-segment filtering is happening:
    //   disabled: merge picks latest (v1=500), VExpr filters → empty
    //   enabled:  rowset 2 filtered per-segment (v1=500≠100), old version survives
    //             merge sees only old version, VExpr passes → stale (1,100,'old')
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

    // Rowset 1: initial data
    sql "INSERT INTO ${tbName} VALUES (1, 100, 'old'), (2, 200, 'keep'), (3, 300, 'keep')"
    // Rowset 2: update k1=1 from v1=100 to v1=500
    sql "INSERT INTO ${tbName} VALUES (1, 500, 'new')"

    // --- Pushdown disabled: correct merge-then-filter behavior ---
    sql "SET enable_mor_value_predicate_pushdown_tables = ''"

    // v1=100 does not match latest version (v1=500) → empty
    qt_select_update_disabled_old """
        SELECT * FROM ${tbName} WHERE v1 = 100 ORDER BY k1
    """

    // v1=500 matches latest → returns updated row
    qt_select_update_disabled_new """
        SELECT * FROM ${tbName} WHERE v1 = 500 ORDER BY k1
    """

    // --- Pushdown enabled: per-segment filtering observable ---
    sql "SET enable_mor_value_predicate_pushdown_tables = '*'"

    // v1=100: rowset 2 (v1=500) filtered per-segment, old version survives.
    // Returns stale data — this proves pushdown is filtering at storage layer.
    qt_select_update_enabled_old """
        SELECT * FROM ${tbName} WHERE v1 = 100 ORDER BY k1
    """

    // v1=500: rowset 1 (v1=100) filtered per-segment, new version passes → correct
    qt_select_update_enabled_new """
        SELECT * FROM ${tbName} WHERE v1 = 500 ORDER BY k1
    """

    // v1 > 200: old v1=100 filtered, new v1=500 passes, k1=3 v1=300 passes → correct
    qt_select_update_enabled_range """
        SELECT * FROM ${tbName} WHERE v1 > 200 ORDER BY k1
    """

    // Test 6: Multiple tables in the list
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

    // Test 7: Non-MOR table (MOW) - value predicates should always be pushed down
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

    // Test 8: DUP_KEYS table - value predicates should always be pushed down
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

    // Cleanup
    sql "SET enable_mor_value_predicate_pushdown_tables = ''"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql "DROP TABLE IF EXISTS ${tbNameMow}"
    sql "DROP TABLE IF EXISTS ${tbNameDup}"
}
