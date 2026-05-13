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

// Test doc mode with sparse overflow scenarios:
//   Case 1: Basic disjoint INSERT INTO SELECT (scenario ④)
//   Case 2: Compaction after INSERT INTO
//   Case 3: Cross-rowset SELECT *
//   Case 4: Large key count (2000+2000)
//   Case 5: INSERT src(S+sp) into dst(D) — scenario ⑥
//   Case 6: INSERT src(D) into dst(S+sp) — scenario ⑦
//   Case 7: Mixed types in sparse overflow
//   Case 8: Multiple rounds of INSERT INTO
//   Case 9: JSON completeness — cast(v as text) verification

suite("test_doc_mode_downgrade_insert_into", "p0") {
    // ─────────────────────────────────────────────────────
    // Setup: doc mode ON, small threshold for fast testing
    // ─────────────────────────────────────────────────────
    sql """ set default_variant_enable_doc_mode = true; """
    sql """ set default_variant_max_subcolumns_count = 5 """

    def genJson = { int numKeys, int keyOffset, int baseVal ->
        def parts = []
        for (int i = 0; i < numKeys; i++) {
            parts.add("\"k${keyOffset + i}\":${baseVal + i}")
        }
        return "{${parts.join(',')}}"
    }

    def insertRows = { tableName, int startK, int numRows, int numKeys, int keyOffset ->
        for (int i = 0; i < numRows; i++) {
            int k = startK + i
            def json = genJson(numKeys, keyOffset, k * 100)
            sql "insert into ${tableName} values (${k}, '${json}')"
        }
    }

    def createTable = { tableName ->
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

    // ─────────────────────────────────────────────────────
    // Case 1: Basic disjoint INSERT INTO SELECT (scenario ④)
    //   src has 2 rowsets with disjoint keys, combined > threshold
    // ─────────────────────────────────────────────────────
    def t_src = "doc_downgrade_src"
    def t_dst = "doc_downgrade_dst"
    createTable(t_src)
    createTable(t_dst)

    // rowset1: 3 rows × 4 keys (k0..k3)
    insertRows(t_src, 1, 3, 4, 0)
    // rowset2: 3 rows × 4 keys (k4..k7), combined=8 > threshold(5)
    insertRows(t_src, 10, 3, 4, 4)

    qt_src_rs1 """ SELECT k, cast(v['k0'] as int), cast(v['k3'] as int)
                   FROM ${t_src} WHERE k < 10 ORDER BY k """
    qt_src_rs2 """ SELECT k, cast(v['k4'] as int), cast(v['k7'] as int)
                   FROM ${t_src} WHERE k >= 10 ORDER BY k """

    sql "INSERT INTO ${t_dst} SELECT * FROM ${t_src}"

    qt_dst_all """ SELECT k,
                          cast(v['k0'] as int), cast(v['k1'] as int),
                          cast(v['k2'] as int), cast(v['k3'] as int),
                          cast(v['k4'] as int), cast(v['k5'] as int),
                          cast(v['k6'] as int), cast(v['k7'] as int)
                   FROM ${t_dst} ORDER BY k """

    qt_dst_rs1_detail """ SELECT k, cast(v['k0'] as int), cast(v['k3'] as int),
                                   cast(v['k4'] as int), cast(v['k7'] as int)
                          FROM ${t_dst} WHERE k < 10 ORDER BY k """

    qt_dst_rs2_detail """ SELECT k, cast(v['k0'] as int), cast(v['k3'] as int),
                                   cast(v['k4'] as int), cast(v['k7'] as int)
                          FROM ${t_dst} WHERE k >= 10 ORDER BY k """

    // ─────────────────────────────────────────────────────
    // Case 2: Compaction on dst after INSERT INTO
    // ─────────────────────────────────────────────────────
    sql "INSERT INTO ${t_dst} SELECT * FROM ${t_src}"
    trigger_and_wait_compaction(t_dst, "cumulative")

    qt_dst_after_compact """ SELECT k,
                                    cast(v['k0'] as int), cast(v['k1'] as int),
                                    cast(v['k2'] as int), cast(v['k3'] as int),
                                    cast(v['k4'] as int), cast(v['k5'] as int),
                                    cast(v['k6'] as int), cast(v['k7'] as int)
                             FROM ${t_dst} ORDER BY k """

    // ─────────────────────────────────────────────────────
    // Case 3: Direct SELECT * FROM source
    // ─────────────────────────────────────────────────────
    qt_src_select_star """ SELECT k,
                                  cast(v['k0'] as int), cast(v['k3'] as int),
                                  cast(v['k4'] as int), cast(v['k7'] as int)
                           FROM ${t_src} ORDER BY k """

    // ─────────────────────────────────────────────────────
    // Case 4: Large key count — 2000+2000
    // ─────────────────────────────────────────────────────
    sql """ set default_variant_max_subcolumns_count = 2048 """

    def t_large_src = "doc_downgrade_large_src"
    def t_large_dst = "doc_downgrade_large_dst"
    createTable(t_large_src)
    createTable(t_large_dst)

    for (int row = 0; row < 2; row++) {
        def parts = []
        for (int i = 0; i < 2000; i++) {
            parts.add("\"k${i}\":${row * 10000 + i}")
        }
        def json = "{${parts.join(',')}}"
        sql "insert into ${t_large_src} values (${row}, '${json}')"
    }
    for (int row = 0; row < 2; row++) {
        def parts = []
        for (int i = 0; i < 2000; i++) {
            parts.add("\"k${2000 + i}\":${(row + 10) * 10000 + i}")
        }
        def json = "{${parts.join(',')}}"
        sql "insert into ${t_large_src} values (${row + 10}, '${json}')"
    }

    sql "INSERT INTO ${t_large_dst} SELECT * FROM ${t_large_src}"

    def dstK0 = sql "select cast(v['k0'] as int) from ${t_large_dst} where k = 0"
    assertEquals(0, dstK0[0][0])
    def dstK1999 = sql "select cast(v['k1999'] as int) from ${t_large_dst} where k = 0"
    assertEquals(1999, dstK1999[0][0])
    def dstK2000 = sql "select cast(v['k2000'] as int) from ${t_large_dst} where k = 10"
    assertEquals(100000, dstK2000[0][0])
    def dstK3999 = sql "select cast(v['k3999'] as int) from ${t_large_dst} where k = 10"
    assertEquals(101999, dstK3999[0][0])
    def dstCheckNull1 = sql "select cast(v['k2000'] as int) from ${t_large_dst} where k = 0"
    assertTrue(dstCheckNull1[0][0] == null, "rs1 row in dst should not have k2000")
    def dstCheckNull2 = sql "select cast(v['k0'] as int) from ${t_large_dst} where k = 10"
    assertTrue(dstCheckNull2[0][0] == null, "rs2 row in dst should not have k0")

    // ─────────────────────────────────────────────────────
    // Case 5: Scenario ⑥ — INSERT src(S+sparse) into dst(D)
    //   Step 1: Build dst with doc_value data (many keys → doc mode)
    //   Step 2: Build src with 2 rowsets of disjoint keys → read
    //           produces S+sparse in memory
    //   Step 3: INSERT INTO dst SELECT * FROM src
    //           → insert_range_from: src=S+sp, dst=D
    // ─────────────────────────────────────────────────────
    sql """ set default_variant_max_subcolumns_count = 5 """

    def t5_dst = "doc_downgrade_case5_dst"
    def t5_src = "doc_downgrade_case5_src"
    createTable(t5_dst)
    createTable(t5_src)

    // Build dst with 6 keys (> threshold) → becomes doc_value mode
    for (int i = 0; i < 3; i++) {
        def json = genJson(6, 0, i * 100)  // k0..k5
        sql "insert into ${t5_dst} values (${i}, '${json}')"
    }

    // Build src with 2 rowsets of disjoint keys → will produce S+sparse on read
    insertRows(t5_src, 100, 3, 4, 10)  // rowset1: k10..k13
    insertRows(t5_src, 200, 3, 4, 14)  // rowset2: k14..k17, combined=8 > threshold

    // INSERT INTO dst (which has doc_value) SELECT FROM src (which has S+sparse)
    sql "INSERT INTO ${t5_dst} SELECT * FROM ${t5_src}"

    // Verify original dst rows preserved
    qt_case5_original """ SELECT k, cast(v['k0'] as int), cast(v['k5'] as int)
                          FROM ${t5_dst} WHERE k < 100 ORDER BY k """

    // Verify inserted rows from src
    qt_case5_inserted """ SELECT k, cast(v['k10'] as int), cast(v['k13'] as int),
                                   cast(v['k14'] as int), cast(v['k17'] as int)
                          FROM ${t5_dst} WHERE k >= 100 ORDER BY k """

    // Verify disjoint: original rows should not have k10, inserted rows should not have k0
    def c5_check1 = sql "select cast(v['k10'] as int) from ${t5_dst} where k = 0"
    assertTrue(c5_check1[0][0] == null, "case5: original row should not have k10")
    def c5_check2 = sql "select cast(v['k0'] as int) from ${t5_dst} where k = 100"
    assertTrue(c5_check2[0][0] == null, "case5: inserted row should not have k0")

    // ─────────────────────────────────────────────────────
    // Case 6: Scenario ⑦ — INSERT src(D) into dst(S+sparse)
    //   Step 1: Build dst from 2 disjoint-key rowsets → read produces S+sparse
    //   Step 2: Build src with many keys → doc_value mode
    //   Step 3: INSERT INTO dst SELECT * FROM src
    //           But dst is rewritten per row, so we need a trick:
    //           First INSERT INTO an intermediate table to produce S+sparse dst,
    //           then INSERT src(D) into that table.
    // ─────────────────────────────────────────────────────
    def t6_src_a = "doc_downgrade_case6_src_a"
    def t6_src_b = "doc_downgrade_case6_src_b"
    def t6_dst = "doc_downgrade_case6_dst"
    createTable(t6_src_a)
    createTable(t6_src_b)
    createTable(t6_dst)

    // src_a: 2 rowsets disjoint keys
    insertRows(t6_src_a, 1, 3, 4, 0)   // k0..k3
    insertRows(t6_src_a, 10, 3, 4, 4)  // k4..k7

    // First INSERT: dst gets data from src_a (disjoint → will have subcolumns after write)
    sql "INSERT INTO ${t6_dst} SELECT * FROM ${t6_src_a}"

    // src_b: many keys → doc mode (6 keys > threshold=5)
    for (int i = 0; i < 3; i++) {
        def json = genJson(6, 20, i * 100)  // k20..k25
        sql "insert into ${t6_src_b} values (${i + 50}, '${json}')"
    }

    // Second INSERT: dst already has subcolumns from first INSERT,
    // now inserting src_b (which has doc_value data from many keys)
    // → insert_range_from: src=D, dst has sub (may have sparse from future compaction)
    sql "INSERT INTO ${t6_dst} SELECT * FROM ${t6_src_b}"

    // Verify first batch preserved
    qt_case6_batch1 """ SELECT k, cast(v['k0'] as int), cast(v['k3'] as int),
                                  cast(v['k4'] as int), cast(v['k7'] as int)
                        FROM ${t6_dst} WHERE k < 50 ORDER BY k """

    // Verify second batch
    qt_case6_batch2 """ SELECT k, cast(v['k20'] as int), cast(v['k25'] as int)
                        FROM ${t6_dst} WHERE k >= 50 ORDER BY k """

    // Verify disjoint across batches
    def c6_check1 = sql "select cast(v['k20'] as int) from ${t6_dst} where k = 1"
    assertTrue(c6_check1[0][0] == null, "case6: batch1 row should not have k20")
    def c6_check2 = sql "select cast(v['k0'] as int) from ${t6_dst} where k = 50"
    assertTrue(c6_check2[0][0] == null, "case6: batch2 row should not have k0")

    // ─────────────────────────────────────────────────────
    // Case 7: Mixed types in sparse overflow
    //   Sparse columns have different types: int, string, array, bool
    // ─────────────────────────────────────────────────────
    def t7_src = "doc_downgrade_case7_src"
    def t7_dst = "doc_downgrade_case7_dst"
    createTable(t7_src)
    createTable(t7_dst)

    // rowset1: 4 keys with int values
    for (int i = 0; i < 3; i++) {
        sql """insert into ${t7_src} values (${i},
            '{"a":${i}, "b":${i+10}, "c":${i+20}, "d":${i+30}}')"""
    }
    // rowset2: 4 different keys with mixed types (string, bool, array, float)
    for (int i = 0; i < 3; i++) {
        sql """insert into ${t7_src} values (${i+10},
            '{"e":"str_${i}", "f":${i % 2 == 0}, "g":[${i},${i+1}], "h":${i}.5}')"""
    }

    sql "INSERT INTO ${t7_dst} SELECT * FROM ${t7_src}"

    // Verify int values from rowset1
    qt_case7_int """ SELECT k, cast(v['a'] as int), cast(v['b'] as int),
                            cast(v['c'] as int), cast(v['d'] as int)
                     FROM ${t7_dst} WHERE k < 10 ORDER BY k """

    // Verify mixed types from rowset2
    qt_case7_mixed """ SELECT k, cast(v['e'] as text), cast(v['f'] as text),
                              cast(v['g'] as text), cast(v['h'] as text)
                       FROM ${t7_dst} WHERE k >= 10 ORDER BY k """

    // Verify disjoint
    def c7_check1 = sql "select cast(v['e'] as text) from ${t7_dst} where k = 0"
    assertTrue(c7_check1[0][0] == null, "case7: int row should not have key 'e'")
    def c7_check2 = sql "select cast(v['a'] as int) from ${t7_dst} where k = 10"
    assertTrue(c7_check2[0][0] == null, "case7: mixed row should not have key 'a'")

    // ─────────────────────────────────────────────────────
    // Case 8: Multiple rounds of INSERT INTO
    //   3 rounds of INSERT from sources with different key sets
    //   Each round would produce sparse overflow
    // ─────────────────────────────────────────────────────
    def t8_dst = "doc_downgrade_case8_dst"
    createTable(t8_dst)

    // Round 1: keys k0..k3
    def t8_r1 = "doc_downgrade_case8_r1"
    createTable(t8_r1)
    insertRows(t8_r1, 1, 2, 4, 0)

    // Round 2: keys k4..k7
    def t8_r2 = "doc_downgrade_case8_r2"
    createTable(t8_r2)
    insertRows(t8_r2, 10, 2, 4, 4)

    // Round 3: keys k8..k11
    def t8_r3 = "doc_downgrade_case8_r3"
    createTable(t8_r3)
    insertRows(t8_r3, 20, 2, 4, 8)

    sql "INSERT INTO ${t8_dst} SELECT * FROM ${t8_r1}"
    sql "INSERT INTO ${t8_dst} SELECT * FROM ${t8_r2}"
    sql "INSERT INTO ${t8_dst} SELECT * FROM ${t8_r3}"

    assertEquals(6L, sql("select count(*) from ${t8_dst}")[0][0])

    // Verify each round's data
    qt_case8_all """ SELECT k,
                            cast(v['k0'] as int), cast(v['k3'] as int),
                            cast(v['k4'] as int), cast(v['k7'] as int),
                            cast(v['k8'] as int), cast(v['k11'] as int)
                     FROM ${t8_dst} ORDER BY k """

    // Compaction after 3 rounds
    trigger_and_wait_compaction(t8_dst, "cumulative")

    qt_case8_after_compact """ SELECT k,
                                      cast(v['k0'] as int), cast(v['k3'] as int),
                                      cast(v['k4'] as int), cast(v['k7'] as int),
                                      cast(v['k8'] as int), cast(v['k11'] as int)
                               FROM ${t8_dst} ORDER BY k """

    // ─────────────────────────────────────────────────────
    // Case 9: JSON completeness — cast(v as text)
    //   Verify the full JSON output is correct after
    //   sparse → doc_value conversion (path ordering matters)
    // ─────────────────────────────────────────────────────
    def t9_src = "doc_downgrade_case9_src"
    def t9_dst = "doc_downgrade_case9_dst"
    createTable(t9_src)
    createTable(t9_dst)

    // rowset1: keys a, b, c, d (alphabetical, < threshold)
    sql """insert into ${t9_src} values (1, '{"a":1, "b":2, "c":3, "d":4}')"""
    // rowset2: keys e, f, g, h (alphabetical, disjoint, combined=8 > threshold)
    sql """insert into ${t9_src} values (2, '{"e":5, "f":6, "g":7, "h":8}')"""

    sql "INSERT INTO ${t9_dst} SELECT * FROM ${t9_src}"

    // cast(v as text) should produce valid, path-sorted JSON
    qt_case9_json """ SELECT k, cast(v as text) FROM ${t9_dst} ORDER BY k """

    // Compaction then verify JSON again
    trigger_and_wait_compaction(t9_dst, "cumulative")
    qt_case9_json_after_compact """ SELECT k, cast(v as text) FROM ${t9_dst} ORDER BY k """

    // ─────────────────────────────────────────────────────
    // Cleanup
    // ─────────────────────────────────────────────────────
    sql "DROP TABLE IF EXISTS ${t_src}"
    sql "DROP TABLE IF EXISTS ${t_dst}"
    sql "DROP TABLE IF EXISTS ${t_large_src}"
    sql "DROP TABLE IF EXISTS ${t_large_dst}"
    sql "DROP TABLE IF EXISTS ${t5_dst}"
    sql "DROP TABLE IF EXISTS ${t5_src}"
    sql "DROP TABLE IF EXISTS ${t6_src_a}"
    sql "DROP TABLE IF EXISTS ${t6_src_b}"
    sql "DROP TABLE IF EXISTS ${t6_dst}"
    sql "DROP TABLE IF EXISTS ${t7_src}"
    sql "DROP TABLE IF EXISTS ${t7_dst}"
    sql "DROP TABLE IF EXISTS ${t8_dst}"
    sql "DROP TABLE IF EXISTS ${t8_r1}"
    sql "DROP TABLE IF EXISTS ${t8_r2}"
    sql "DROP TABLE IF EXISTS ${t8_r3}"
    sql "DROP TABLE IF EXISTS ${t9_src}"
    sql "DROP TABLE IF EXISTS ${t9_dst}"
}
