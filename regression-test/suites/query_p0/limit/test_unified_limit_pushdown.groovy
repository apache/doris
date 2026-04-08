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

// Test unified limit pushdown to SegmentIterator.
// This exercises the code path where _can_opt_limit_reads() returns true
// because all column predicates are evaluated by inverted index, allowing
// the segment iterator to tighten nrows_read_limit using topn_limit and
// shared_scan_limit.

suite("test_unified_limit_pushdown") {

    // ---- DUP_KEYS table with inverted indexes ----
    // Inverted indexes on all queried columns allow _can_opt_limit_reads()
    // to return true, enabling the SegmentIterator limit optimization.
    sql "DROP TABLE IF EXISTS dup_inv_limit"
    sql """
        CREATE TABLE dup_inv_limit (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL,
            INDEX idx_k1 (k1) USING INVERTED,
            INDEX idx_k2 (k2) USING INVERTED,
            INDEX idx_v1 (v1) USING INVERTED
        )
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    // Insert 200 rows for sufficient data volume
    StringBuilder sb = new StringBuilder()
    sb.append("INSERT INTO dup_inv_limit VALUES ")
    for (int i = 1; i <= 200; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${1000 - i}, 'val_${i}')")
    }
    sql sb.toString()

    // Basic LIMIT — general_read_limit path with inverted index optimization.
    def count1 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_inv_limit LIMIT 10
        ) t
    """
    assert count1[0][0] == 10 : "DUP inverted: basic LIMIT 10, got ${count1[0][0]}"

    // LIMIT with WHERE predicate covered by inverted index.
    // k1 > 100 matches 100 rows, LIMIT 15 should return exactly 15.
    def count2 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_inv_limit WHERE k1 > 100 LIMIT 15
        ) t
    """
    assert count2[0][0] == 15 : "DUP inverted: WHERE k1>100 LIMIT 15, got ${count2[0][0]}"

    // LIMIT with multiple predicates on indexed columns.
    // k1 > 50 AND k2 < 980 → k1 in [51..200] AND k2 < 980 → k2=1000-k1 < 980 → k1 > 20
    // intersection: k1 in [51..200], all 150 rows match. LIMIT 30.
    def count3 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2, v1 FROM dup_inv_limit WHERE k1 > 50 AND k2 < 980 LIMIT 30
        ) t
    """
    assert count3[0][0] == 30 : "DUP inverted: multi-pred LIMIT 30, got ${count3[0][0]}"

    // LIMIT larger than matching rows — should return all matching rows.
    // k1 > 195 matches 5 rows, LIMIT 20 should return all 5.
    def count4 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_inv_limit WHERE k1 > 195 LIMIT 20
        ) t
    """
    assert count4[0][0] == 5 : "DUP inverted: LIMIT > matching, got ${count4[0][0]}"

    // ---- MOW UNIQUE_KEYS with inverted indexes ----
    sql "DROP TABLE IF EXISTS mow_inv_limit"
    sql """
        CREATE TABLE mow_inv_limit (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL,
            INDEX idx_k1 (k1) USING INVERTED,
            INDEX idx_k2 (k2) USING INVERTED
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sb = new StringBuilder()
    sb.append("INSERT INTO mow_inv_limit VALUES ")
    for (int i = 1; i <= 200; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${1000 - i}, 'val_${i}')")
    }
    sql sb.toString()

    def count5 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM mow_inv_limit WHERE k1 > 100 LIMIT 20
        ) t
    """
    assert count5[0][0] == 20 : "MOW inverted: WHERE k1>100 LIMIT 20, got ${count5[0][0]}"

    // MOW with DELETEs + inverted index
    sql "DELETE FROM mow_inv_limit WHERE k1 <= 50"
    def count6 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM mow_inv_limit LIMIT 25
        ) t
    """
    assert count6[0][0] == 25 : "MOW inverted after delete: LIMIT 25, got ${count6[0][0]}"

    def count7 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM mow_inv_limit WHERE k1 > 150 LIMIT 10
        ) t
    """
    assert count7[0][0] == 10 : "MOW inverted delete+filter: LIMIT 10, got ${count7[0][0]}"

    // ---- Multi-bucket table with inverted indexes ----
    // Tests shared_scan_limit coordination across multiple scanners.
    sql "DROP TABLE IF EXISTS dup_multi_inv_limit"
    sql """
        CREATE TABLE dup_multi_inv_limit (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 VARCHAR(100) NULL,
            INDEX idx_k1 (k1) USING INVERTED,
            INDEX idx_k2 (k2) USING INVERTED
        )
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 8
        PROPERTIES ("replication_num" = "1");
    """
    sb = new StringBuilder()
    sb.append("INSERT INTO dup_multi_inv_limit VALUES ")
    for (int i = 1; i <= 500; i++) {
        if (i > 1) sb.append(",")
        sb.append("(${i}, ${10000 - i}, 'val_${i}')")
    }
    sql sb.toString()

    // With 8 buckets, up to 8 scanners may run. shared_scan_limit coordinates
    // to return exactly LIMIT rows.
    def count8 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_multi_inv_limit LIMIT 20
        ) t
    """
    assert count8[0][0] == 20 : "Multi-bucket inverted: LIMIT 20, got ${count8[0][0]}"

    def count9 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_multi_inv_limit WHERE k1 > 200 LIMIT 50
        ) t
    """
    assert count9[0][0] == 50 : "Multi-bucket inverted filter: LIMIT 50, got ${count9[0][0]}"

    // Small limit with many buckets — tests that each scanner correctly
    // respects the shared limit even when some scanners have no data.
    def count10 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_multi_inv_limit LIMIT 3
        ) t
    """
    assert count10[0][0] == 3 : "Multi-bucket inverted: LIMIT 3, got ${count10[0][0]}"

    // ---- ORDER BY key LIMIT with inverted index (topn path) ----
    // Tests that the topn_limit path also benefits from inverted index
    // optimization in SegmentIterator. Use order_qt_* to validate the
    // actual ordered rows, not just cardinality, so the optimization
    // cannot silently return a wrong subset of the right size.
    order_qt_topn_asc """
        SELECT k1, k2 FROM dup_inv_limit ORDER BY k1 LIMIT 10
    """

    order_qt_topn_desc_filter """
        SELECT k1, k2 FROM dup_inv_limit WHERE k1 > 100 ORDER BY k1 DESC LIMIT 15
    """

    // Also assert ORDER BY content for the multi-bucket case so that the
    // shared_scan_limit path is row-validated, not just count-validated.
    order_qt_topn_multi_bucket """
        SELECT k1, k2 FROM dup_multi_inv_limit ORDER BY k1 LIMIT 12
    """

    def count11 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_inv_limit ORDER BY k1 LIMIT 10
        ) t
    """
    assert count11[0][0] == 10 : "DUP inverted ORDER BY LIMIT: got ${count11[0][0]}"

    def count12 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_inv_limit WHERE k1 > 100 ORDER BY k1 DESC LIMIT 15
        ) t
    """
    assert count12[0][0] == 15 : "DUP inverted ORDER BY DESC with filter: got ${count12[0][0]}"

    // ---- LIMIT + OFFSET with inverted index ----
    def count13 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_inv_limit LIMIT 10 OFFSET 5
        ) t
    """
    assert count13[0][0] == 10 : "DUP inverted LIMIT+OFFSET: got ${count13[0][0]}"

    // OFFSET near end: k1 > 195 matches 5 rows, OFFSET 2 LIMIT 10 should return 3
    def count14 = sql """
        SELECT COUNT(*) FROM (
            SELECT k1, k2 FROM dup_inv_limit WHERE k1 > 195 LIMIT 10 OFFSET 2
        ) t
    """
    assert count14[0][0] == 3 : "DUP inverted LIMIT+OFFSET near end: got ${count14[0][0]}"
}
