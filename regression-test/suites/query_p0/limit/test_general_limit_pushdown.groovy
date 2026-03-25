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
}
