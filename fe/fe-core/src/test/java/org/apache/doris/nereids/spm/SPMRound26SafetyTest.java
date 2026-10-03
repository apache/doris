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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-26 review fixes without their own regression suite:
 *
 * - #2: restoring the forwarded SESSION baselines must be skipped when the statement
 *   has rewrite disabled - the import runs BEFORE StmtExecutor checks
 *   enable_spm_rewrite, so unconditionally re-parsing every enabled row added unbounded
 *   work to each forwarded statement of a connection with many session baselines.
 * - #6: a manual plan may not choose its own SCAN SELECTION (partition / tablet /
 *   sample / snapshot / index): the bind text is the matching key, so a divergent
 *   selection silently changes which rows the replayed baseline reads (the fingerprint
 *   hashes table identity and base columns only). The reviewer's example - bind
 *   'SELECT k FROM t' WITH 'SELECT k FROM t PARTITION(p1)' - must be rejected at
 *   CREATE, because after ADD PARTITION p2 the unpinned caller still matches and would
 *   silently lose every row of p2.
 */
public class SPMRound26SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    // ==================== #6: plan-side scan selections are rejected ====================

    /** The reviewer's exact pair: an unpinned bind may not be paired with a pinned plan. */
    @Test
    public void testPlanOnlyPartitionPinIsRejected() {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT k FROM t", "SELECT k FROM t PARTITION(p1)"));
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("scan selectors"),
                failure.getMessage());
    }

    /** The mirror case widens the result silently and is rejected the same way. */
    @Test
    public void testBindOnlyPartitionPinIsRejected() {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT k FROM t PARTITION(p1)", "SELECT k FROM t"));
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("scan selectors"),
                failure.getMessage());
    }

    /** A plan-only sample / tablet pin is the same class of divergence. */
    @Test
    public void testPlanOnlySamplePinIsRejected() {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT k FROM t", "SELECT k FROM t TABLESAMPLE(10 PERCENT)"));
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("scan selectors"),
                failure.getMessage());
    }

    /** MATCHING selections on both sides stay accepted: the pair is symmetric. */
    @Test
    public void testSymmetricSelectionIsAccepted() throws Exception {
        BaselinePlan plan = new SPMPlanner().buildBaseline(
                "SELECT k FROM t PARTITION(p1)", "SELECT k FROM t PARTITION(p1)");
        Assertions.assertEquals("SELECT k FROM t PARTITION(p1)", plan.getBindSql());
        // ... and the same holds for an explicit empty selection on both sides
        BaselinePlan unpinned = new SPMPlanner().buildBaseline(
                "SELECT k FROM t", "SELECT k FROM t");
        Assertions.assertEquals("SELECT k FROM t", unpinned.getBindSql());
    }

    /**
     * A manual plan over its OWN table set stays accepted: only a table BOTH statements
     * read can carry a divergent selection, so a bind-side table the plan never touches
     * (the privilege guard's secret-table bind planned over a public table) is no
     * mismatch - the plan-side tables are covered by the schema fingerprint.
     */
    @Test
    public void testTablesOnlyInTheBindAreNotCompared() throws Exception {
        BaselinePlan plan = new SPMPlanner().buildBaseline(
                "SELECT k FROM secret_table WHERE k = 1",
                "SELECT k FROM public_table WHERE k = 1");
        Assertions.assertEquals("SELECT k FROM secret_table WHERE k = 1", plan.getBindSql());
    }

    /**
     * The comparison is per-table and IN STATEMENT ORDER: the two occurrences of a self
     * joined table are compared one by one, so a swap of the pin between occurrences is
     * rejected as ambiguous (round-27: the multiset was equal, but after a partition
     * change the bind pair and the replayed pairing diverge), while pinning only ONE of
     * them is rejected as a mismatch.
     */
    @Test
    public void testSelectorComparisonIsAMultiset() throws Exception {
        // both occurrences pinned on both sides -> accepted
        BaselinePlan selfJoin = new SPMPlanner().buildBaseline(
                "SELECT a.k FROM t PARTITION(p1) a JOIN t PARTITION(p1) b ON a.k = b.k",
                "SELECT a.k FROM t PARTITION(p1) a JOIN t PARTITION(p1) b ON a.k = b.k");
        Assertions.assertNotNull(selfJoin.getBindSql());
        // the pinned occurrence swapped -> rejected: the multiset is equal but the pin is
        // not attached to the same occurrence
        RuntimeException swapped = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT a.k FROM t PARTITION(p1) a JOIN t b ON a.k = b.k",
                        "SELECT a.k FROM t a JOIN t PARTITION(p1) b ON a.k = b.k"));
        Assertions.assertTrue(swapped.getMessage().contains("DIFFERENT occurrences"),
                swapped.getMessage());
        // only ONE occurrence pinned on the plan side -> rejected
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(
                        "SELECT a.k FROM t PARTITION(p1) a JOIN t PARTITION(p1) b ON a.k = b.k",
                        "SELECT a.k FROM t PARTITION(p1) a JOIN t b ON a.k = b.k"));
        Assertions.assertTrue(failure.getMessage().contains("scan selectors"),
                failure.getMessage());
    }

    // ==================== #2: the forwarded import follows enable_spm_rewrite ====================

    /**
     * With rewrite disabled the master never consults a baseline, so the import must not
     * rebuild the carried rows (it runs before the rewrite gate). With rewrite enabled
     * the rows are restored exactly as before.
     */
    @Test
    public void testForwardedImportSkipsWhenRewriteIsDisabled() {
        ConnectContext ctx = new ConnectContext();
        try {
            SessionBaselineStore store = ctx.getSessionBaselineStore();
            Assertions.assertNotNull(store, "the context must carry a session store");
            store.importBaseline(sessionBaseline());
            String payload = SPMForwardedSession.serialize(store);
            Assertions.assertFalse(payload.isEmpty(), "the fixture row must serialize");

            ctx.getSessionVariable().setEnableSpmRewrite(false);
            store.clear();
            SPMForwardedSession.importInto(ctx, payload);
            Assertions.assertTrue(store.getAllBaselines().isEmpty(),
                    "rewrite disabled: nothing may be rebuilt into the master context");
            Assertions.assertTrue(store.isEmpty(),
                    "the store must stay empty / clear");

            ctx.getSessionVariable().setEnableSpmRewrite(true);
            SPMForwardedSession.importInto(ctx, payload);
            Assertions.assertEquals(1, store.getAllBaselines().size(),
                    "rewrite enabled: the carried rows are restored");
        } finally {
            ctx.getSessionVariable().setEnableSpmRewrite(false);
        }
    }

    /** A rebuildable SESSION row (bind == plan: only the parameterized bind tree is needed). */
    private static BaselinePlan sessionBaseline() {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(7L);
        plan.setBindSql("SELECT k FROM t WHERE k = 1");
        plan.setPlanSql("SELECT k FROM t WHERE k = 1");
        plan.setBindSqlDigest("digest-7");
        plan.setBindSqlHash(7);
        plan.setCost(0.0);
        plan.setSource(BaselineSource.USER);
        plan.setStatus(BaselineStatus.ENABLED);
        return plan;
    }
}
