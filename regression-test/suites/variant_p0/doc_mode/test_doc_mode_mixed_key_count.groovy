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

// Test doc mode variant behavior with different key counts relative to
// variant_max_subcolumns_count threshold.
// Uses a small threshold (count=5) to exercise all scenarios:
//   A) keys < count  → downgraded to subcolumn mode
//   B) keys > count  → stays in doc-value mode
//   C) keys = count  → boundary case
//   D) mixed: first batch < count, second batch > count
//   E) mixed: first batch > count, second batch < count
//   F) all above after compaction

suite("test_doc_mode_mixed_key_count", "p0") {
    // Enable doc mode with a small subcolumns count threshold
    sql """ set default_variant_enable_doc_mode = true; """
    sql """ set default_variant_max_subcolumns_count = 5 """

    def create_table = { tableName ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "true");
        """
    }

    // Helper: generate JSON with N keys: {"k0":val, "k1":val+1, ..., "k(N-1)":val+N-1}
    def genJson = { int numKeys, int baseVal ->
        def parts = []
        for (int i = 0; i < numKeys; i++) {
            parts.add("\"k${i}\":${baseVal + i}")
        }
        return "{${parts.join(',')}}"
    }

    // Helper: insert rows with specified number of keys
    def insertRows = { tableName, int startK, int numRows, int numKeys ->
        for (int i = 0; i < numRows; i++) {
            int k = startK + i
            def json = genJson(numKeys, k * 100)
            sql "insert into ${tableName} values (${k}, '${json}')"
        }
    }

    // Helper: verify row count
    def verifyRowCount = { tableName, int expected ->
        def result = sql "select count(*) from ${tableName}"
        assertEquals(expected.toLong(), result[0][0])
    }

    // Helper: verify a specific key exists with correct value for a given row
    def verifyKeyValue = { tableName, int k, String keyName, int expectedVal ->
        def result = sql "select cast(v['${keyName}'] as int) from ${tableName} where k = ${k}"
        assertEquals(1, result.size(), "Row k=${k} not found")
        assertEquals(expectedVal, result[0][0])
    }

    // ============================================================
    // Case A: keys < count (3 keys, threshold=5) → subcolumn mode
    // ============================================================
    def tableA = "doc_mode_keys_lt_count"
    create_table(tableA)
    insertRows(tableA, 1, 5, 3)  // 5 rows, each with 3 keys

    verifyRowCount(tableA, 5)
    // Verify first and last row
    verifyKeyValue(tableA, 1, "k0", 100)
    verifyKeyValue(tableA, 1, "k2", 102)
    verifyKeyValue(tableA, 5, "k0", 500)
    verifyKeyValue(tableA, 5, "k2", 502)

    // Verify SELECT * returns correct data
    qt_case_a """ SELECT k, cast(v['k0'] as int), cast(v['k1'] as int), cast(v['k2'] as int)
                  FROM ${tableA} ORDER BY k """

    // ============================================================
    // Case B: keys > count (8 keys, threshold=5) → doc-value mode
    // ============================================================
    def tableB = "doc_mode_keys_gt_count"
    create_table(tableB)
    insertRows(tableB, 1, 5, 8)  // 5 rows, each with 8 keys

    verifyRowCount(tableB, 5)
    verifyKeyValue(tableB, 1, "k0", 100)
    verifyKeyValue(tableB, 1, "k7", 107)
    verifyKeyValue(tableB, 5, "k0", 500)
    verifyKeyValue(tableB, 5, "k7", 507)

    qt_case_b """ SELECT k, cast(v['k0'] as int), cast(v['k7'] as int)
                  FROM ${tableB} ORDER BY k """

    // ============================================================
    // Case C: keys = count (5 keys, threshold=5) → boundary
    // ============================================================
    def tableC = "doc_mode_keys_eq_count"
    create_table(tableC)
    insertRows(tableC, 1, 5, 5)  // 5 rows, each with 5 keys

    verifyRowCount(tableC, 5)
    verifyKeyValue(tableC, 1, "k0", 100)
    verifyKeyValue(tableC, 1, "k4", 104)
    verifyKeyValue(tableC, 5, "k0", 500)
    verifyKeyValue(tableC, 5, "k4", 504)

    qt_case_c """ SELECT k, cast(v['k0'] as int), cast(v['k4'] as int)
                  FROM ${tableC} ORDER BY k """

    // ============================================================
    // Case D: mixed — first batch < count, second batch > count
    // ============================================================
    def tableD = "doc_mode_mixed_lt_then_gt"
    create_table(tableD)
    insertRows(tableD, 1, 3, 3)   // batch1: 3 rows × 3 keys (< 5)
    insertRows(tableD, 10, 3, 8)  // batch2: 3 rows × 8 keys (> 5)

    verifyRowCount(tableD, 6)
    // batch1 rows: k0..k2
    verifyKeyValue(tableD, 1, "k0", 100)
    verifyKeyValue(tableD, 1, "k2", 102)
    // batch2 rows: k0..k7
    verifyKeyValue(tableD, 10, "k0", 1000)
    verifyKeyValue(tableD, 10, "k7", 1007)
    // batch1 rows should NOT have k7
    def check_d1 = sql "select cast(v['k7'] as int) from ${tableD} where k = 1"
    assertTrue(check_d1[0][0] == null, "batch1 row should not have k7")

    qt_case_d """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int), cast(v['k7'] as int)
                  FROM ${tableD} ORDER BY k """

    // ============================================================
    // Case E: mixed — first batch > count, second batch < count
    // ============================================================
    def tableE = "doc_mode_mixed_gt_then_lt"
    create_table(tableE)
    insertRows(tableE, 1, 3, 8)   // batch1: 3 rows × 8 keys (> 5)
    insertRows(tableE, 10, 3, 3)  // batch2: 3 rows × 3 keys (< 5)

    verifyRowCount(tableE, 6)
    // batch1 rows: k0..k7
    verifyKeyValue(tableE, 1, "k0", 100)
    verifyKeyValue(tableE, 1, "k7", 107)
    // batch2 rows: k0..k2
    verifyKeyValue(tableE, 10, "k0", 1000)
    verifyKeyValue(tableE, 10, "k2", 1002)
    // batch2 rows should NOT have k7
    def check_e1 = sql "select cast(v['k7'] as int) from ${tableE} where k = 10"
    assertTrue(check_e1[0][0] == null, "batch2 row should not have k7")

    qt_case_e """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int), cast(v['k7'] as int)
                  FROM ${tableE} ORDER BY k """

    // ============================================================
    // Case F: Compaction tests — compact each mixed table and re-verify
    // ============================================================
    def compactAndVerify = { tableName, int expectedRows ->
        // Trigger compaction
        trigger_and_wait_compaction(tableName, "cumulative")

        // Re-verify row count after compaction
        verifyRowCount(tableName, expectedRows)
    }

    // Compact Case A (all subcolumn)
    compactAndVerify(tableA, 5)
    qt_case_a_compact """ SELECT k, cast(v['k0'] as int), cast(v['k1'] as int), cast(v['k2'] as int)
                          FROM ${tableA} ORDER BY k """

    // Compact Case B (all doc-value)
    compactAndVerify(tableB, 5)
    qt_case_b_compact """ SELECT k, cast(v['k0'] as int), cast(v['k7'] as int)
                          FROM ${tableB} ORDER BY k """

    // Compact Case C (boundary)
    compactAndVerify(tableC, 5)
    qt_case_c_compact """ SELECT k, cast(v['k0'] as int), cast(v['k4'] as int)
                          FROM ${tableC} ORDER BY k """

    // Compact Case D (mixed lt→gt)
    compactAndVerify(tableD, 6)
    qt_case_d_compact """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int), cast(v['k7'] as int)
                          FROM ${tableD} ORDER BY k """

    // Compact Case E (mixed gt→lt)
    compactAndVerify(tableE, 6)
    qt_case_e_compact """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int), cast(v['k7'] as int)
                          FROM ${tableE} ORDER BY k """

    // ============================================================
    // Case G: Multiple inserts with same key count → cumulative compaction
    // ============================================================
    def tableG = "doc_mode_multi_insert_compact"
    create_table(tableG)
    // 4 separate inserts with 3 keys each (< threshold)
    insertRows(tableG, 1, 2, 3)
    insertRows(tableG, 10, 2, 3)
    insertRows(tableG, 20, 2, 3)
    insertRows(tableG, 30, 2, 3)

    verifyRowCount(tableG, 8)
    qt_case_g_before """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int)
                         FROM ${tableG} ORDER BY k """

    compactAndVerify(tableG, 8)
    qt_case_g_after """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int)
                        FROM ${tableG} ORDER BY k """

    // ============================================================
    // Case H: Alternating above/below threshold across many batches
    // ============================================================
    def tableH = "doc_mode_alternating_batches"
    create_table(tableH)
    insertRows(tableH, 1, 2, 3)    // batch1: 3 keys (< 5)
    insertRows(tableH, 10, 2, 8)   // batch2: 8 keys (> 5)
    insertRows(tableH, 20, 2, 3)   // batch3: 3 keys (< 5)
    insertRows(tableH, 30, 2, 8)   // batch4: 8 keys (> 5)

    verifyRowCount(tableH, 8)
    qt_case_h_before """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int), cast(v['k7'] as int)
                         FROM ${tableH} ORDER BY k """

    compactAndVerify(tableH, 8)
    qt_case_h_after """ SELECT k, cast(v['k0'] as int), cast(v['k2'] as int), cast(v['k7'] as int)
                        FROM ${tableH} ORDER BY k """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableA}"
    sql "DROP TABLE IF EXISTS ${tableB}"
    sql "DROP TABLE IF EXISTS ${tableC}"
    sql "DROP TABLE IF EXISTS ${tableD}"
    sql "DROP TABLE IF EXISTS ${tableE}"
    sql "DROP TABLE IF EXISTS ${tableG}"
    sql "DROP TABLE IF EXISTS ${tableH}"
}
