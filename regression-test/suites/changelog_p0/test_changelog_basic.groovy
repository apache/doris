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

suite("test_changelog_basic", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_dup_with_changelog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_with_changelog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_with_before_changelog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_reinsert_with_before_changelog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_reinsert_changelog FORCE"
    sql "DROP TABLE IF EXISTS test_mow_seq_with_changelog FORCE"
    sql "DROP TABLE IF EXISTS test_empty_rowset FORCE"

    sql """
        CREATE TABLE test_dup_with_changelog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING,
            jv VARIANT NULL
        )
        DUPLICATE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_with_changelog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING,
            jv VARIANT NULL
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_with_before_changelog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING,
            jv VARIANT NULL
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_reinsert_with_before_changelog (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_reinsert_changelog (
            k1 INT,
            v1 INT,
            v2 STRING DEFAULT "default"
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true"
        )
    """

    sql """
        CREATE TABLE test_mow_seq_with_changelog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING,
            jv VARIANT NULL
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """

    sql """ALTER TABLE test_mow_seq_with_changelog
             ENABLE FEATURE "SEQUENCE_LOAD"
             WITH PROPERTIES ("function_column.sequence_type" = "int")"""

    sql """
        CREATE TABLE test_empty_rowset (
            k1 INT,
            k2 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "changelog.enable" = "true"
        )
    """

    // Cover changelog empty-rowset close.
    sql "INSERT INTO test_empty_rowset VALUES (1, 1, 10, '10')"

    qt_empty_rowset_raw """
        SELECT k1, k2, v1, v2
        FROM test_empty_rowset
        ORDER BY k1, k2
    """

    qt_empty_rowset_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               k2,
               v1,
               v2
        FROM changelog("table" = "test_empty_rowset")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql """
        INSERT INTO test_dup_with_changelog VALUES
            (1, 1, 1, 10, '10', parse_to_variant('{"k": 1, "d": {"row": "1 1 1 10 10"}}')),
            (2, 2, 2, 20, '20', parse_to_variant('{"k": 2, "d": {"row": "2 2 2 20 20"}}'))
    """
    sql """
        INSERT INTO test_dup_with_changelog VALUES
            (1, 1, 1, 11, '11', parse_to_variant('{"k": 1, "d": {"row": "1 1 1 11 11"}}')),
            (3, 3, 3, 30, '30', parse_to_variant('{"k": 3, "d": {"row": "3 3 3 30 30"}}'))
    """

    order_qt_dup_raw """
        SELECT k1, k2, k3, v1, v2, jv, jv['d']['row']
        FROM test_dup_with_changelog
    """

    qt_dup_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               jv,
               jv['d']['row']
        FROM changelog("table" = "test_dup_with_changelog")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    def dupRawLsnRows = sql """
        SELECT k1, k2, k3, v1, v2, __DORIS_ROW_LSN_COL__
        FROM test_dup_with_changelog
        ORDER BY k1, k2, k3, v1, v2
    """
    def dupChangelogLsnRows = sql """
        SELECT k1, k2, k3, v1, v2, __DORIS_CHANGELOG_LSN__
        FROM changelog("table" = "test_dup_with_changelog")
        ORDER BY k1, k2, k3, v1, v2
    """
    assertEquals(dupRawLsnRows.size(), dupChangelogLsnRows.size())
    def dupRowLsns = dupRawLsnRows.collect { it[5] as long }
    dupRowLsns.each { lsn -> assertTrue(lsn > 0, "row lsn should be positive but got ${lsn}") }
    assertEquals(dupRowLsns.size(), dupRowLsns.toSet().size())
    for (int i = 0; i < dupRawLsnRows.size(); i++) {
        assertEquals(dupRawLsnRows[i][0..4], dupChangelogLsnRows[i][0..4])
        assertEquals(dupRawLsnRows[i][5], dupChangelogLsnRows[i][5])
    }

    sql """
        INSERT INTO test_mow_with_changelog VALUES
            (1, 1, 1, 10, '10', parse_to_variant('{"k": 1, "d": {"row": "1 1 1 10 10"}}')),
            (2, 2, 2, 20, '20', parse_to_variant('{"k": 2, "d": {"row": "2 2 2 20 20"}}'))
    """
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_mow_with_changelog(k1, k2, k3, v2) VALUES (2, 2, 2, '200')"
    sql "INSERT INTO test_mow_with_changelog(k1, k2, k3, v1) VALUES (2, 2, 2, 2000)"
    sql "INSERT INTO test_mow_with_changelog(k1, k2, k3, v2) VALUES (2, 2, 2, '201')"
    sql "SET enable_unique_key_partial_update = false"
    sql "DELETE FROM test_mow_with_changelog WHERE k1 = 1 AND k2 = 1 AND k3 = 1"

    order_qt_mow_raw """
        SELECT k1, k2, k3, v1, v2, jv, jv['d']['row']
        FROM test_mow_with_changelog
    """

    qt_mow_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               jv,
               jv['d']['row']
        FROM changelog("table" = "test_mow_with_changelog")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    test {
        sql """
            SELECT __DORIS_ROW_LSN_COL__
            FROM test_mow_with_changelog
        """
        exception "Unknown column"
    }

    sql """
        INSERT INTO test_mow_with_before_changelog VALUES
            (1, 1, 1, 10, '10', parse_to_variant('{"k": 1, "d": {"row": "1 1 1 10 10"}}')),
            (2, 2, 2, 20, '20', parse_to_variant('{"k": 2, "d": {"row": "2 2 2 20 20"}}'))
    """
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_mow_with_before_changelog(k1, k2, k3, v2) VALUES (2, 2, 2, '200')"
    sql "INSERT INTO test_mow_with_before_changelog(k1, k2, k3, v1) VALUES (2, 2, 2, 2000)"
    sql "INSERT INTO test_mow_with_before_changelog(k1, k2, k3, v2) VALUES (2, 2, 2, '201')"
    sql "SET enable_unique_key_partial_update = false"
    sql "DELETE FROM test_mow_with_before_changelog WHERE k1 = 1 AND k2 = 1 AND k3 = 1"

    order_qt_mow_before_raw """
        SELECT k1, k2, k3, v1, v2, jv, jv['d']['row']
        FROM test_mow_with_before_changelog
    """

    qt_mow_before_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               __DORIS_CHANGELOG_BEFORE_v1__,
               __DORIS_CHANGELOG_BEFORE_v2__,
               jv,
               __DORIS_CHANGELOG_BEFORE_jv__,
               jv['d']['row'],
               __DORIS_CHANGELOG_BEFORE_jv__['d']['row']
        FROM changelog("table" = "test_mow_with_before_changelog")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql "INSERT INTO test_mow_reinsert_with_before_changelog VALUES (1, 10)"
    sql "DELETE FROM test_mow_reinsert_with_before_changelog WHERE k1 = 1"
    sql "INSERT INTO test_mow_reinsert_with_before_changelog VALUES (1, 10)"

    qt_mow_reinsert_before_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               v1,
               __DORIS_CHANGELOG_BEFORE_v1__
        FROM changelog("table" = "test_mow_reinsert_with_before_changelog")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql "INSERT INTO test_mow_reinsert_changelog VALUES (2, 20, 'partial')"
    sql "DELETE FROM test_mow_reinsert_changelog WHERE k1 = 2"
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_mow_reinsert_changelog(k1, v1) VALUES (2, 20)"
    sql "SET enable_unique_key_partial_update = false"

    qt_mow_reinsert_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               v1,
               v2
        FROM changelog("table" = "test_mow_reinsert_changelog")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql """
        INSERT INTO test_mow_seq_with_changelog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__, jv)
        VALUES (1, 1, 1, 100, '100', 2, parse_to_variant('{"k": 1, "d": {"row": "1 1 1 100 100 2"}}'))
    """
    sql """
        INSERT INTO test_mow_seq_with_changelog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__, jv)
        VALUES (1, 1, 1, 200, '200', 1, parse_to_variant('{"k": 1, "d": {"row": "1 1 1 200 200 1"}}'))
    """
    sql """
        INSERT INTO test_mow_seq_with_changelog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__, jv)
        VALUES (1, 1, 1, 300, '300', 3, parse_to_variant('{"k": 1, "d": {"row": "1 1 1 300 300 3"}}'))
    """
    sql "SET require_sequence_in_insert = false"
    sql "SET enable_unique_key_partial_update = true"
    sql """
        INSERT INTO test_mow_seq_with_changelog(k1, k2, k3, v2)
        VALUES (1, 1, 1, '350')
    """
    sql """
        INSERT INTO test_mow_seq_with_changelog(k1, k2, k3, v1, __DORIS_SEQUENCE_COL__)
        VALUES (1, 1, 1, 400, 4)
    """
    sql """
        INSERT INTO test_mow_seq_with_changelog(k1, k2, k3, v2, __DORIS_SEQUENCE_COL__)
        VALUES (1, 1, 1, '360', 2)
    """
    sql "SET enable_unique_key_partial_update = false"
    sql "SET require_sequence_in_insert = true"

    sql "DELETE FROM test_mow_seq_with_changelog WHERE k1 = 1 AND k2 = 1 AND k3 = 1"

    order_qt_mow_seq_raw """
        SELECT k1, k2, k3, v1, v2, jv, jv['d']['row']
        FROM test_mow_seq_with_changelog
    """

    sql "SET skip_delete_bitmap = false"

    qt_mow_seq_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               __DORIS_CHANGELOG_BEFORE_v1__,
               __DORIS_CHANGELOG_BEFORE_v2__,
               jv,
               __DORIS_CHANGELOG_BEFORE_jv__,
               jv['d']['row'],
               __DORIS_CHANGELOG_BEFORE_jv__['d']['row']
        FROM changelog("table" = "test_mow_seq_with_changelog")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql "SET skip_delete_bitmap = true"

    qt_mow_seq_changelog_skip_delete_bitmap """
        SELECT __DORIS_CHANGELOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               __DORIS_CHANGELOG_BEFORE_v1__,
               __DORIS_CHANGELOG_BEFORE_v2__,
               jv,
               __DORIS_CHANGELOG_BEFORE_jv__,
               jv['d']['row'],
               __DORIS_CHANGELOG_BEFORE_jv__['d']['row']
        FROM changelog("table" = "test_mow_seq_with_changelog")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    test {
        sql """
            SELECT __DORIS_SEQUENCE_COL__
            FROM changelog("table" = "test_mow_seq_with_changelog")
        """
        exception "Unknown column"
    }
    
    sql "SET skip_delete_bitmap = false"
}
