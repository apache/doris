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

suite("test_spm_group_concat", "spm") {

    // group_concat freeze coverage:
    //  - MULTI_DISTINCT_GROUP_CONCAT(v [ORDER BY k]) is the execution shape of
    //    GROUP_CONCAT(DISTINCT ...): the class name carries the dedup contract while
    //    isDistinct() is false, so the frozen SQL must restore DISTINCT (otherwise the
    //    replay returns "1,1,2" instead of "1,2");
    //  - GROUP_CONCAT(DISTINCT v ORDER BY k) used to make the decompiler return null and
    //    fall back to the user plan text; it must now be frozen by SPM (the frozen text
    //    carries _spm_const_var placeholders) and keep its ORDER BY;
    //  - a plain group_concat keeps its duplicates: no DISTINCT may be invented.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_gc_t"""
    sql """
        CREATE TABLE spm_gc_t (k1 INT, k2 INT)
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // k1 = 1 appears twice
    sql """INSERT INTO spm_gc_t VALUES (1, 1), (1, 1), (2, 1)"""

    // cleanup: drop this suite's leftover baselines (ids are dynamic)
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_gc_t") }
    }
    ownBaselines().each { row ->
        sql """DROP BASELINE PLAN ${row[0]}"""
    }
    assertEquals(0, ownBaselines().size(), "no spm_gc_t baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql("""CREATE GLOBAL BASELINE PLAN '${text}' WITH '${text}'""")[0][0] as Long)
    }
    def frozenSql = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0].toString()
    }

    // ==================== dedup contract survives the freeze ====================
    String dedupSql = "select multi_distinct_group_concat(k1 order by k1) as s" +
            " from spm_gc_t where k2 = 1"
    long dedupId = createBaseline(dedupSql)
    assertTrue(explainOf(dedupSql).contains("SPM baseline hit: id=${dedupId}"),
            "the dedup query must replay from its baseline: " + explainOf(dedupSql))
    String dedupFrozen = frozenSql(dedupId)
    assertTrue(dedupFrozen.contains("_spm_const_var"),
            "the frozen SQL must come from the SPM decompiler: " + dedupFrozen)
    assertTrue(dedupFrozen.toUpperCase().contains("DISTINCT"),
            "multi_distinct_group_concat must freeze as GROUP_CONCAT(DISTINCT ...): " + dedupFrozen)
    assertTrue(dedupFrozen.contains("ORDER BY"),
            "the ORDER BY of the dedup aggregate must survive the freeze: " + dedupFrozen)
    assertEquals("1,2", sql(dedupSql)[0][0].toString(),
            "the replay must keep the dedup (GROUP_CONCAT(k1) would return 1,1,2)")

    // ==================== distinct + order is decompiled, not fallen back ====================
    String distinctOrderSql = "select group_concat(distinct k1 order by k1) as s" +
            " from spm_gc_t where k2 = 1"
    long distinctOrderId = createBaseline(distinctOrderSql)
    assertTrue(explainOf(distinctOrderSql).contains("SPM baseline hit: id=${distinctOrderId}"),
            "the distinct+order query must replay from its baseline: " + explainOf(distinctOrderSql))
    String distinctOrderFrozen = frozenSql(distinctOrderId)
    assertTrue(distinctOrderFrozen.contains("_spm_const_var"),
            "the frozen SQL must come from the SPM decompiler (before the fix this shape"
                    + " forced a fallback to the raw plan text): " + distinctOrderFrozen)
    assertTrue(distinctOrderFrozen.toUpperCase().contains("GROUP_CONCAT(DISTINCT"),
            "the frozen SQL must carry the DISTINCT form: " + distinctOrderFrozen)
    order_qt_distinct_order_replay """select group_concat(distinct k1 order by k1) as s
        from spm_gc_t where k2 = 1"""

    // ==================== a plain group_concat keeps its duplicates ====================
    String plainSql = "select group_concat(k1 order by k1) as s from spm_gc_t where k2 = 1"
    long plainId = createBaseline(plainSql)
    String plainFrozen = frozenSql(plainId)
    assertTrue(plainFrozen.contains("_spm_const_var"),
            "the plain group_concat must also be decompiled: " + plainFrozen)
    assertFalse(plainFrozen.toUpperCase().contains("DISTINCT"),
            "no DISTINCT may be invented for a plain group_concat: " + plainFrozen)
    assertEquals("1,1,2", sql(plainSql)[0][0].toString(),
            "the plain group_concat must keep its duplicates")

    // leave no baselines behind for other runs
    ownBaselines().each { row ->
        sql """DROP BASELINE PLAN ${row[0]}"""
    }
    assertEquals(0, ownBaselines().size(), "all spm_gc_t baselines must be dropped")
}
