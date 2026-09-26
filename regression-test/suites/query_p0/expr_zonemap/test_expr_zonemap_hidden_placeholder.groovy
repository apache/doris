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

// #67995: on a single-version unique-MoW rowset, __DORIS_VERSION_COL__ stores 0 on disk and is
// replaced with the rowset version at read time. Zone-map pruning must use that effective value,
// not the on-disk [0, 0] summary, or `WHERE __DORIS_VERSION_COL__ = <real version>` prunes the
// segment and drops every matching row. This test fails (returns 0) without the fix.
suite("test_expr_zonemap_hidden_placeholder") {
    sql " set show_hidden_columns = true "

    sql " DROP TABLE IF EXISTS test_zonemap_version_col "
    sql """
        CREATE TABLE test_zonemap_version_col (
            k INT,
            v INT
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """ INSERT INTO test_zonemap_version_col SELECT number, number FROM numbers("number" = "2048") """
    sql " sync "

    // The rows sit in one single-version rowset. Learn the version they carry with a query that has
    // no equality predicate on the column, so nothing is pruned. It is a real, non-zero version.
    def versionRows = sql " SELECT DISTINCT __DORIS_VERSION_COL__ FROM test_zonemap_version_col "
    assertEquals(1, versionRows.size())
    long realVersion = versionRows[0][0] as long
    assertTrue(realVersion > 0)

    // The regression: `= realVersion` must return every row. Against the on-disk [0, 0] summary the
    // segment would be pruned to 0; the synthesized [realVersion, realVersion] keeps it.
    order_qt_version_col_match """
        SELECT COUNT(*) FROM test_zonemap_version_col WHERE __DORIS_VERSION_COL__ = ${realVersion}
    """
    // The negative side still prunes correctly: no row carries a different version.
    order_qt_version_col_miss """
        SELECT COUNT(*) FROM test_zonemap_version_col WHERE __DORIS_VERSION_COL__ = ${realVersion + 1000}
    """
    // A pushed-down aggregate must not be answered from the placeholder summary either.
    order_qt_version_col_pushed_agg """
        SELECT MIN(k), MAX(k), COUNT(*) FROM test_zonemap_version_col
        WHERE __DORIS_VERSION_COL__ = ${realVersion}
    """

    // MoR (no merge-on-write) reaches the version column through the same read-time substitution.
    sql " DROP TABLE IF EXISTS test_zonemap_version_col_mor "
    sql """
        CREATE TABLE test_zonemap_version_col_mor (
            k INT,
            v INT
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """ INSERT INTO test_zonemap_version_col_mor SELECT number, number FROM numbers("number" = "2048") """
    sql " sync "
    def morVersionRows = sql " SELECT DISTINCT __DORIS_VERSION_COL__ FROM test_zonemap_version_col_mor "
    assertEquals(1, morVersionRows.size())
    long morVersion = morVersionRows[0][0] as long
    assertTrue(morVersion > 0)
    order_qt_version_col_mor_match """
        SELECT COUNT(*) FROM test_zonemap_version_col_mor WHERE __DORIS_VERSION_COL__ = ${morVersion}
    """

    // An inverted index and a bloom filter are both accepted on the hidden version column. Each
    // stores the on-disk placeholder (0), so physical index pruning runs before the read-time
    // substitution and would reduce `= <real version>` to an empty result. Placeholder columns must
    // be skipped by inverted-index and bloom-filter pruning; this keeps every row.
    sql " DROP TABLE IF EXISTS test_zonemap_version_col_indexed "
    sql """
        CREATE TABLE test_zonemap_version_col_indexed (
            k INT,
            v INT
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    def indexTimeout = 60000
    sql """ ALTER TABLE test_zonemap_version_col_indexed ADD INDEX idx_ver (__DORIS_VERSION_COL__) USING INVERTED """
    wait_for_last_schema_change_finish("test_zonemap_version_col_indexed", indexTimeout)
    sql """ ALTER TABLE test_zonemap_version_col_indexed SET ("bloom_filter_columns" = "__DORIS_VERSION_COL__") """
    wait_for_last_schema_change_finish("test_zonemap_version_col_indexed", indexTimeout)
    // Index the user column too, so a compound `version OR v` predicate is a candidate for the
    // common-expression inverted-index path (_apply_index_expr), not just per-column predicates.
    sql """ ALTER TABLE test_zonemap_version_col_indexed ADD INDEX idx_v (v) USING INVERTED """
    wait_for_last_schema_change_finish("test_zonemap_version_col_indexed", indexTimeout)
    sql """ INSERT INTO test_zonemap_version_col_indexed SELECT number, number FROM numbers("number" = "2048") """
    sql " sync "
    def indexedVersionRows = sql " SELECT DISTINCT __DORIS_VERSION_COL__ FROM test_zonemap_version_col_indexed "
    assertEquals(1, indexedVersionRows.size())
    long indexedVersion = indexedVersionRows[0][0] as long
    assertTrue(indexedVersion > 0)
    order_qt_version_col_indexed_match """
        SELECT COUNT(*) FROM test_zonemap_version_col_indexed WHERE __DORIS_VERSION_COL__ = ${indexedVersion}
    """
    // A compound `OR` stays a common expression (VCompoundPred) rather than splitting into per-column
    // predicates, so it reaches the common-expression inverted-index path. `v = -1` matches no row
    // and every row carries the version, so this must return all rows; the version column's index
    // must not prune it against the physical placeholder first.
    order_qt_version_col_indexed_compound """
        SELECT COUNT(*) FROM test_zonemap_version_col_indexed
        WHERE __DORIS_VERSION_COL__ = ${indexedVersion} OR v = -1
    """

    // With CSE virtual-slot pushdown on (experimental, default off), a repeated
    // `__DORIS_VERSION_COL__ + 1` subexpression is materialized once as a virtual column pushed into
    // the scan (PushDownVirtualColumnsIntoOlapScan; EXPLAIN VERBOSE shows
    // `virtualColumn=(__DORIS_VERSION_COL__ + 1)`). That virtual column must be computed from the
    // read-time-substituted version, not the on-disk placeholder (0); otherwise the row would read
    // `1` instead of `version + 1`. Every row carries the version, so the distinct value is
    // `${indexedVersion + 1}`.
    sql " set enable_virtual_slot_for_cse = true "
    order_qt_version_col_indexed_cse """
        SELECT DISTINCT (__DORIS_VERSION_COL__ + 1) AS vp1, (__DORIS_VERSION_COL__ + 1) AS vp2
        FROM test_zonemap_version_col_indexed
        WHERE (__DORIS_VERSION_COL__ + 1) > 0
    """
    sql " set enable_virtual_slot_for_cse = false "
}
