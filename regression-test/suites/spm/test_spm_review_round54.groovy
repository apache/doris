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
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_spm_review_round54", "spm") {

    // Round-54 #2: an explicit `FROM t INDEX mv` pin is a SEMANTIC choice of the user, not
    // an optimizer decision - on an aggregate-key table the pinned (coarser) rollup
    // returns one aggregated row per rollup key while the base table returns one row per
    // record. The frozen text cannot carry it: the decompiler only sees the physical
    // scan's SELECTED index id (which every optimizer-side rollup / MV choice sets as
    // well), and the matching key DOES carry the pin (the bind digest renders
    // "INDEX <name>"), so a frozen "FROM t" would be replayed for a caller whose pinned
    // query matched the baseline and would silently read the BASE table. Freezing is
    // therefore declined for such a statement: the stored planSql stays the user text
    // (plan_frozen = 0) and the rewrite replays the parameterized tree, whose own
    // analysis re-applies the pin. This suite pins BOTH halves: the stored provenance and
    // the replayed result on a rollup whose rows really differ from the base table's.

    // #1 (late pending reservations), #3 (mutation-window visibility) and #5 (open mutation
    // marker) are two-master orderings of the capture checkpoint / the mutation clock: they
    // have no SQL surface a single sandbox cluster can exercise and are covered by
    // PlanCaptureCycleHandoffTest and BaselineManagerMutationClockTest.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""
    sql """set enable_nereids_planner = true"""
    sql """set enable_fallback_to_original_planner = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r54_agg") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String bind, String plan ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + bind + '" WITH "' + plan + '"')[0][0] as Long)
    }

    // ==================== setup: an aggregate-key table with a COARSER rollup ====================
    sql """DROP TABLE IF EXISTS spm_r54_agg"""
    sql """
        CREATE TABLE spm_r54_agg (
            k1 INT,
            k2 INT,
            v INT SUM
        )
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r54_agg VALUES (1, 1, 10), (1, 2, 20), (2, 1, 5)"""
    sql """ALTER TABLE spm_r54_agg ADD ROLLUP mv(k1, v)"""
    String rollupState = "NOT_FINISHED"
    Awaitility.await().atMost(60, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).until(() -> {
        rollupState = sql("""SHOW ALTER TABLE ROLLUP WHERE TableName = 'spm_r54_agg'
                ORDER BY CreateTime DESC LIMIT 1""")[0][8]
        return rollupState == "FINISHED" || rollupState == "CANCELLED"
    })
    assertEquals("FINISHED", rollupState, "the rollup must be built before it is queried")

    String pinned = "SELECT k1, v FROM spm_r54_agg INDEX mv"
    String unpinned = "SELECT k1, v FROM spm_r54_agg"
    // precondition: the rollup really is COARSER - the forced index scan aggregates k2 away
    assertEquals([[1, 30], [2, 5]], sql(pinned).sort(),
            "the forced INDEX scan must read the coarser rollup")
    assertEquals([[1, 10], [1, 20], [2, 5]], sql(unpinned).sort(),
            "without the pin the base table keeps one row per (k1, k2)")

    // ==================== #2: the INDEX pin is never frozen away ====================
    long pinnedId = createBaseline(pinned, pinned)
    def stored = sql("""SELECT plan_frozen, plan_sql FROM __internal_schema.spm_baselines
            WHERE id = ${pinnedId}""")
    assertEquals("false", stored[0][0].toString(),
            "a statement pinning an INDEX must keep the user planSql: the frozen text"
                    + " cannot carry the pin: " + stored)
    assertEquals(pinned, stored[0][1].toString(),
            "the stored planSql must stay the user's own text (pin included)")

    // the replay keeps the rollup's granularity: the pin is applied by the parameterized
    // tree's own analysis, which the stored planSql drives
    assertTrue(explainOf(pinned).contains("SPM baseline hit: id=${pinnedId}"),
            "the pinned caller must hit its own baseline: " + explainOf(pinned))
    assertEquals([[1, 30], [2, 5]], sql(pinned).sort(),
            "the rewritten pinned query must preserve the rollup's rows - a frozen"
                    + " FROM spm_r54_agg would have returned the base table's three rows")

    // a caller WITHOUT the pin never matches the pinned baseline (the digest carries INDEX)
    assertTrue(!explainOf(unpinned).contains("SPM baseline hit: id=${pinnedId}"),
            "an unpinned caller must not reuse a pinned baseline: " + explainOf(unpinned))
    assertEquals([[1, 10], [1, 20], [2, 5]], sql(unpinned).sort(),
            "the unpinned caller keeps reading the base table")

    // ... and the guard is specific to the pin: a statement WITHOUT an INDEX clause on the
    // same table is still frozen (the normal decompile path is untouched)
    String control = "SELECT k1 FROM spm_r54_agg"
    long controlId = createBaseline(control, control)
    def controlStored = sql("""SELECT plan_frozen FROM __internal_schema.spm_baselines
            WHERE id = ${controlId}""")
    assertEquals("true", controlStored[0][0].toString(),
            "a statement without an INDEX pin must keep the normal frozen replay")
    assertTrue(explainOf(control).contains("SPM baseline hit: id=${controlId}"),
            "the control baseline must be hit: " + explainOf(control))
}
