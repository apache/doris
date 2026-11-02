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

suite("test_spm_scan_modifiers", "spm") {

    // Scan-modifier freeze coverage: a scan that reads a strict partition subset or
    // carries TABLESAMPLE used to fail the decompile, so CREATE kept the raw user plan
    // text (no SPM placeholder) and the rewrite degraded to the parameterized-tree
    // path. The decompiler must now render the modifiers itself:
    //  - PARTITION(p1, p2) pins the partitions the captured plan read (a replayed
    //    catalog.db.table without the clause would read every partition);
    //  - TABLESAMPLE(n PERCENT) [REPEATABLE seed] keeps the sampling of the captured
    //    plan (a replayed plan without it would return the full table);
    //  - partition lists are sets: the same selection in another order must keep
    //    hitting (the decompiler emits the selected partitions in partition-id order);
    //  - a predicate-derived partition subset is NOT frozen (replay re-derives it),
    //    and a different user selection must never replay the captured partitions.

    if (isCloudMode()) {
        return
    }
    sql "use regression_test_spm"

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_sm_part"""
    sql """
        CREATE TABLE spm_sm_part (k1 INT)
        PARTITION BY RANGE(k1)
        (
            PARTITION p1 VALUES [("-2147483648"), ("0")),
            PARTITION p2 VALUES [("0"), ("100")),
            PARTITION p3 VALUES [("100"), ("2147483647"))
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // every row lives in p2, so a wrong partition replay is visible in the result
    sql """INSERT INTO spm_sm_part VALUES (5), (6)"""
    sql """DROP TABLE IF EXISTS spm_sm_dup"""
    sql """
        CREATE TABLE spm_sm_dup (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_sm_dup VALUES (1, 10), (2, 20), (3, 30)"""

    // cleanup: drop this suite's leftover baselines (ids are dynamic)
    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_sm_") }
    }
    ownBaselines().each { row ->
        sql """DROP BASELINE PLAN ${row[0]}"""
    }
    assertEquals(0, ownBaselines().size(), "no spm_sm_ baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql("""CREATE GLOBAL BASELINE PLAN '${text}' WITH '${text}'""")[0][0] as Long)
    }
    def frozenSql = { long id ->
        sql("""SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}""")[0][0].toString()
    }

    // ==================== PARTITION(...) is decompiled, not fallen back ====================
    String pinSql = "select * from spm_sm_part partition(p1, p2) where k1 = 5"
    long pinId = createBaseline(pinSql)
    assertTrue(explainOf(pinSql).contains("SPM baseline hit: id=${pinId}"),
            "the partition-pinned query must replay from its baseline: " + explainOf(pinSql))
    String pinFrozen = frozenSql(pinId)
    assertTrue(pinFrozen.contains("_spm_const_var"),
            "the frozen SQL must come from the SPM decompiler: " + pinFrozen)
    assertTrue(pinFrozen.contains("PARTITION(p1, p2)"),
            "the frozen SQL must pin the partitions the captured plan read: " + pinFrozen)
    order_qt_partition_pin_replay """select * from spm_sm_part partition(p1, p2) where k1 = 5"""

    // the same selection in another order keeps hitting: a partition list is a set
    String reordered = "select * from spm_sm_part partition(p2, p1) where k1 = 5"
    assertTrue(explainOf(reordered).contains("SPM baseline hit: id=${pinId}"),
            "the reordered partition selection must keep hitting the baseline: " + explainOf(reordered))
    order_qt_partition_pin_reordered """select * from spm_sm_part partition(p2, p1) where k1 = 5"""

    // a smaller selection must NOT replay the captured partitions (p1 is empty, so a
    // wrong replay of PARTITION(p1, p2) would return the row 5)
    String smaller = "select * from spm_sm_part partition(p1) where k1 = 5"
    assertFalse(explainOf(smaller).contains("SPM baseline hit"),
            "a different partition selection must not replay the captured partitions: " + explainOf(smaller))
    order_qt_partition_pin_miss """select * from spm_sm_part partition(p1) where k1 = 5"""

    // a query WITHOUT the pin re-derives its partition subset at replay time: the
    // predicate-derived subset is not frozen into the text, and the query hits its own
    // baseline with the full result
    String unpinnedSql = "select * from spm_sm_part where k1 = 5"
    long unpinnedId = createBaseline(unpinnedSql)
    assertTrue(explainOf(unpinnedSql).contains("SPM baseline hit: id=${unpinnedId}"),
            "the unpinned query must replay from its baseline: " + explainOf(unpinnedSql))
    String unpinnedFrozen = frozenSql(unpinnedId)
    assertTrue(unpinnedFrozen.contains("_spm_const_var"),
            "the frozen SQL must come from the SPM decompiler: " + unpinnedFrozen)
    assertFalse(unpinnedFrozen.contains("PARTITION("),
            "a predicate-derived subset must not be frozen as a partition pin: " + unpinnedFrozen)
    order_qt_partition_unpinned_replay """select * from spm_sm_part where k1 = 5"""

    // ==================== TABLESAMPLE is decompiled, not fallen back ====================
    String sampleSql = "select * from spm_sm_dup tablesample(100 percent) where k = 2"
    long sampleId = createBaseline(sampleSql)
    assertTrue(explainOf(sampleSql).contains("SPM baseline hit: id=${sampleId}"),
            "the sampled query must replay from its baseline: " + explainOf(sampleSql))
    String sampleFrozen = frozenSql(sampleId)
    assertTrue(sampleFrozen.contains("_spm_const_var"),
            "the frozen SQL must come from the SPM decompiler: " + sampleFrozen)
    assertTrue(sampleFrozen.contains("TABLESAMPLE(100 PERCENT)"),
            "the frozen SQL must carry the sampling clause: " + sampleFrozen)
    order_qt_tablesample_replay """select * from spm_sm_dup tablesample(100 percent) where k = 2"""

    String repeatSql = "select * from spm_sm_dup tablesample(100 percent) repeatable 7 where k = 3"
    long repeatId = createBaseline(repeatSql)
    assertTrue(explainOf(repeatSql).contains("SPM baseline hit: id=${repeatId}"),
            "the REPEATABLE sample must replay from its baseline: " + explainOf(repeatSql))
    String repeatFrozen = frozenSql(repeatId)
    assertTrue(repeatFrozen.contains("TABLESAMPLE(100 PERCENT) REPEATABLE 7"),
            "the REPEATABLE seed must survive the freeze: " + repeatFrozen)
    order_qt_tablesample_repeatable_replay """select * from spm_sm_dup tablesample(100 percent) repeatable 7 where k = 3"""

    // an unsampled query must not replay a sampled baseline (the captured sample would
    // restrict the rows read)
    String unsampled = "select * from spm_sm_dup where k = 2"
    assertFalse(explainOf(unsampled).contains("SPM baseline hit"),
            "a query without the sample must not replay a sampled baseline: " + explainOf(unsampled))
    order_qt_tablesample_unsampled_miss """select * from spm_sm_dup where k = 2"""

    // leave no baselines behind for other runs
    ownBaselines().each { row ->
        sql """DROP BASELINE PLAN ${row[0]}"""
    }
    assertEquals(0, ownBaselines().size(), "all spm_sm_ baselines must be dropped")
}
