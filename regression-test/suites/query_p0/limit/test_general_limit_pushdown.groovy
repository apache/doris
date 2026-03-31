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

// Test general limit pushdown to storage layer for DUP_KEYS and UNIQUE_KEYS (MOW).
// This exercises the non-topn limit path where VCollectIterator enforces
// general_read_limit with filter_block_conjuncts applied before counting.

suite("test_general_limit_pushdown") {

    // ---- DUP_KEYS table ----
    sql "DROP TABLE IF EXISTS dup_limit_pushdown"
    sql """
        CREATE TABLE dup_limit_pushdown (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL
        )
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    // Insert 50 rows: k1 in [1..50], k2 = 100 - k1, v1 = 'val_<k1>'
    StringBuilder sb = new StringBuilder()
    sb.append("INSERT INTO dup_limit_pushdown VALUES ")
    for (int i = 1; i <= 50; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${100 - i}, 'val_${i}')")
    }
    sql sb.toString()

    // Basic LIMIT without ORDER BY — exercises general limit pushdown path.
    // Use order_qt_ to ensure deterministic output ordering.
    order_qt_dup_basic_limit """
        SELECT k1, k2 FROM dup_limit_pushdown LIMIT 10
    """

    // LIMIT with WHERE clause — filter_block_conjuncts must be applied before
    // limit counting, otherwise we may get fewer rows than requested.
    // k1 > 10 matches 40 rows, LIMIT 15 should return exactly 15.
    order_qt_dup_filter_limit """
        SELECT k1, k2 FROM dup_limit_pushdown WHERE k1 > 10 LIMIT 15
    """

    // LIMIT larger than matching rows — should return all matching rows.
    // k1 > 45 matches 5 rows, LIMIT 20 should return all 5.
    order_qt_dup_filter_over_limit """
        SELECT k1, k2 FROM dup_limit_pushdown WHERE k1 > 45 LIMIT 20
    """

    // LIMIT with complex predicate (function-based, may not push into storage predicates).
    // This exercises the filter_block_conjuncts path for predicates that remain as conjuncts.
    order_qt_dup_complex_filter_limit """
        SELECT k1, k2 FROM dup_limit_pushdown WHERE abs(k1 - 25) < 10 LIMIT 8
    """

    // ---- UNIQUE_KEYS with MOW table ----
    sql "DROP TABLE IF EXISTS mow_limit_pushdown"
    sql """
        CREATE TABLE mow_limit_pushdown (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sb = new StringBuilder()
    sb.append("INSERT INTO mow_limit_pushdown VALUES ")
    for (int i = 1; i <= 50; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${100 - i}, 'val_${i}')")
    }
    sql sb.toString()

    // Basic LIMIT without ORDER BY on MOW table.
    order_qt_mow_basic_limit """
        SELECT k1, k2 FROM mow_limit_pushdown LIMIT 10
    """

    // LIMIT with WHERE on MOW table.
    order_qt_mow_filter_limit """
        SELECT k1, k2 FROM mow_limit_pushdown WHERE k1 > 10 LIMIT 15
    """

    // LIMIT with complex predicate on MOW table.
    order_qt_mow_complex_filter_limit """
        SELECT k1, k2 FROM mow_limit_pushdown WHERE abs(k1 - 25) < 10 LIMIT 8
    """

    // ---- Verify row count correctness with COUNT ----
    // These verify the LIMIT returns the expected number of rows,
    // protecting against the pre-filter counting bug.

    def dup_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1 FROM dup_limit_pushdown WHERE k1 > 10 LIMIT 15
        ) t
    """
    assert dup_count[0][0] == 15 : "DUP_KEYS: expected 15 rows with WHERE k1>10 LIMIT 15, got ${dup_count[0][0]}"

    def mow_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1 FROM mow_limit_pushdown WHERE k1 > 10 LIMIT 15
        ) t
    """
    assert mow_count[0][0] == 15 : "MOW: expected 15 rows with WHERE k1>10 LIMIT 15, got ${mow_count[0][0]}"

    // With complex predicate (abs(k1-25) < 10 => k1 in 16..34, 19 rows; LIMIT 8 should return 8)
    def dup_complex_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1 FROM dup_limit_pushdown WHERE abs(k1 - 25) < 10 LIMIT 8
        ) t
    """
    assert dup_complex_count[0][0] == 8 : "DUP_KEYS complex filter: expected 8 rows, got ${dup_complex_count[0][0]}"

    // ---- AGG_KEYS table (negative test: optimization must NOT apply) ----
    sql "DROP TABLE IF EXISTS agg_limit_pushdown"
    sql """
        CREATE TABLE agg_limit_pushdown (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 INT SUM NOT NULL
        )
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    // Insert duplicate keys so aggregation is required
    sql "INSERT INTO agg_limit_pushdown VALUES (1, 1, 10), (1, 1, 20), (2, 2, 30), (2, 2, 40), (3, 3, 50)"
    // After aggregation: (1,1,30), (2,2,70), (3,3,50)
    def agg_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2, v1 FROM agg_limit_pushdown LIMIT 2
        ) t
    """
    assert agg_count[0][0] == 2 : "AGG_KEYS: expected 2 rows, got ${agg_count[0][0]}"
    order_qt_agg_basic_limit """
        SELECT k1, k2, v1 FROM agg_limit_pushdown LIMIT 2
    """

    // ---- MOR UNIQUE_KEYS table (negative test: optimization must NOT apply) ----
    sql "DROP TABLE IF EXISTS mor_limit_pushdown"
    sql """
        CREATE TABLE mor_limit_pushdown (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    sb = new StringBuilder()
    sb.append("INSERT INTO mor_limit_pushdown VALUES ")
    for (int i = 1; i <= 30; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${100 - i}, 'val_${i}')")
    }
    sql sb.toString()
    def mor_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM mor_limit_pushdown WHERE k1 > 10 LIMIT 10
        ) t
    """
    assert mor_count[0][0] == 10 : "MOR UNIQUE_KEYS: expected 10 rows, got ${mor_count[0][0]}"
    order_qt_mor_filter_limit """
        SELECT k1, k2 FROM mor_limit_pushdown WHERE k1 > 10 LIMIT 10
    """

    // ---- MOW with DELETEs ----
    // Verify __DORIS_DELETE_SIGN__ predicate (in _conjuncts) is correctly
    // handled after being moved to filter_block_conjuncts.
    sql "DROP TABLE IF EXISTS mow_delete_limit_pushdown"
    sql """
        CREATE TABLE mow_delete_limit_pushdown (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sb = new StringBuilder()
    sb.append("INSERT INTO mow_delete_limit_pushdown VALUES ")
    for (int i = 1; i <= 50; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${100 - i}, 'val_${i}')")
    }
    sql sb.toString()
    // Delete rows where k1 <= 20, leaving 30 rows (k1 in 21..50)
    sql "DELETE FROM mow_delete_limit_pushdown WHERE k1 <= 20"
    def mow_del_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM mow_delete_limit_pushdown LIMIT 15
        ) t
    """
    assert mow_del_count[0][0] == 15 : "MOW with deletes: expected 15 rows, got ${mow_del_count[0][0]}"
    order_qt_mow_delete_limit """
        SELECT k1, k2 FROM mow_delete_limit_pushdown LIMIT 15
    """
    // With filter + delete: k1 > 30 matches 20 rows (31..50), LIMIT 10
    def mow_del_filter_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM mow_delete_limit_pushdown WHERE k1 > 30 LIMIT 10
        ) t
    """
    assert mow_del_filter_count[0][0] == 10 : "MOW delete+filter: expected 10 rows, got ${mow_del_filter_count[0][0]}"

    // ---- Multiple buckets/tablets ----
    // Exercises per-scanner limit vs global limit coordination with multiple scanners.
    sql "DROP TABLE IF EXISTS dup_multi_bucket_limit"
    sql """
        CREATE TABLE dup_multi_bucket_limit (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL
        )
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES ("replication_num" = "1");
    """
    sb = new StringBuilder()
    sb.append("INSERT INTO dup_multi_bucket_limit VALUES ")
    for (int i = 1; i <= 200; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${1000 - i}, 'val_${i}')")
    }
    sql sb.toString()
    def multi_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_multi_bucket_limit LIMIT 20
        ) t
    """
    assert multi_count[0][0] == 20 : "Multi-bucket: expected 20 rows, got ${multi_count[0][0]}"
    def multi_filter_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_multi_bucket_limit WHERE k1 > 100 LIMIT 15
        ) t
    """
    assert multi_filter_count[0][0] == 15 : "Multi-bucket filter: expected 15 rows, got ${multi_filter_count[0][0]}"

    // ---- LIMIT + OFFSET ----
    // Verify OFFSET interacts correctly with general limit pushdown.
    def offset_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_limit_pushdown LIMIT 10 OFFSET 5
        ) t
    """
    assert offset_count[0][0] == 10 : "LIMIT+OFFSET: expected 10 rows, got ${offset_count[0][0]}"
    order_qt_dup_limit_offset """
        SELECT k1, k2 FROM dup_limit_pushdown LIMIT 10 OFFSET 5
    """
    // OFFSET beyond matching rows: k1 > 45 matches 5 rows, OFFSET 3 LIMIT 10 should return 2
    def offset_over_count = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_limit_pushdown WHERE k1 > 45 LIMIT 10 OFFSET 3
        ) t
    """
    assert offset_over_count[0][0] == 2 : "LIMIT+OFFSET over: expected 2 rows, got ${offset_over_count[0][0]}"
}
