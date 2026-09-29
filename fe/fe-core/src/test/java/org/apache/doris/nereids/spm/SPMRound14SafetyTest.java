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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Fourteenth review round: the frozen-baseline metadata guard and the replan state.
 *
 * - The CREATE fingerprint must hash the metadata snapshot the OPTIMIZED PLAN was
 *   built with (its own catalog relations) and must include tables referenced only by
 *   the stored PLAN side; the replay revalidates the SAME computation against the
 *   plan it actually planned, so an ALTER committing between the pre-match guard and
 *   the replay planning (or a dropped plan-side table) no longer slips through.
 * - A failed SPM replay falls back to the ORIGINAL query, which is planned by the
 *   normal pipeline; it must start from fresh planner state (the plan-side
 *   PREAGGOPEN hint and the resolved-table cache must not leak into that replan).
 */
public class SPMRound14SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** A resolvable-looking table identity for the fingerprint entries. */
    private static TableIf table(String name, long id, String column) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getBaseSchema()).thenReturn(
                List.of(new Column(column, Type.INT)));
        return table;
    }

    /** A physical catalog relation carrying its OWN metadata snapshot. */
    private static Plan planOver(TableIf table) {
        org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation relation =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation.class);
        Mockito.when(relation.getTable()).thenReturn(table);
        return relation;
    }

    /** A session whose statement context resolves the bind-side relation to {@code bind}. */
    private static ConnectContext contextResolvingTo(TableIf bind) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getAndCacheTable(
                        Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenReturn(bind);
        ctx.setStatementContext(statementContext);
        return ctx;
    }

    /**
     * The CREATE fingerprint must be the UNION of the bind side and the OPTIMIZED
     * plan's own relations: a table used ONLY by the stored plan (bind over t, plan
     * over u) was missing entirely, so after u was dropped a matching t query still
     * passed the guard, rewrote to frozen SQL over the missing u and failed although
     * the original query was valid.
     */
    @Test
    public void testCreateFingerprintIncludesPlanSideTables() {
        TableIf bindTable = table("t_bind", 7L, "k");
        TableIf planTable = table("t_plan", 8L, "v");
        ConnectContext ctx = contextResolvingTo(bindTable);
        LogicalPlan bindPlan = parse("SELECT k FROM internal.spm_db.t_bind");

        try {
            String fingerprint = SPMPlanTreeSupport.schemaFingerprintForCreate(
                    ctx, bindPlan, planOver(planTable));
            Assertions.assertTrue(fingerprint.contains("t_bind|7|"),
                    "the bind-side table must stay in the fingerprint: " + fingerprint);
            Assertions.assertTrue(fingerprint.contains("t_plan|8|"),
                    "a table used only by the stored PLAN must be part of the"
                            + " fingerprint, otherwise a dropped u replays a stale"
                            + " baseline: " + fingerprint);
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The replay revalidation recomputes the fingerprint from the metadata the
     * REPLAYED plan was actually planned with: equal snapshot -> the baseline stays
     * valid; an ALTER committing between the pre-match guard and planning (different
     * id / schema of a plan-side table) -> the recomputation DIFFERS from the stored
     * fingerprint and the stale baseline is skipped; a plan-side table that vanished
     * (planned tree without it) differs as well.
     */
    @Test
    public void testReplayFingerprintPinsThePlannedSnapshot() {
        TableIf bindTable = table("t_bind", 7L, "k");
        TableIf planTable = table("t_plan", 8L, "v");
        ConnectContext ctx = contextResolvingTo(bindTable);
        LogicalPlan bindPlan = parse("SELECT k FROM internal.spm_db.t_bind");

        try {
            String stored = SPMPlanTreeSupport.schemaFingerprintForCreate(
                    ctx, bindPlan, planOver(planTable));
            Assertions.assertEquals(stored, SPMPlanTreeSupport.schemaFingerprintForReplay(
                            ctx, bindPlan, planOver(planTable)),
                    "the same metadata snapshot must reproduce the stored fingerprint");

            // id changed (DROP + CREATE of the plan-side table) between validation and
            // the replay planning
            String recreated = SPMPlanTreeSupport.schemaFingerprintForReplay(
                    ctx, bindPlan, planOver(table("t_plan", 9L, "v")));
            Assertions.assertNotEquals(stored, recreated,
                    "a plan-side table recreated under a NEW id must fail the"
                            + " revalidation");

            // the plan-side table vanished from the replayed tree entirely (it was
            // dropped before planning: the stale baseline must not replay)
            String withoutPlanTable = SPMPlanTreeSupport.schemaFingerprintForReplay(
                    ctx, bindPlan, null);
            Assertions.assertNotEquals(stored, withoutPlanTable,
                    "a replayed plan without the captured plan-side table must fail"
                            + " the revalidation");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The pre-match guard runs BEFORE the query is planned, so it cannot recompute the
     * PLAN-side part of the stored fingerprint: strict equality would skip every
     * baseline whose frozen plan references a table the bind query does not (an
     * admin-authored baseline binding on a secret table and planning on a public one).
     * Membership keeps the guard fail-closed for the bind side; the full comparison,
     * plan side included, runs after planning (verifyReplayMetadata).
     */
    @Test
    public void testPreMatchGuardOnlyChecksTheBindSide() {
        TableIf bindTable = table("t_bind", 7L, "k");
        TableIf planTable = table("t_plan", 8L, "v");
        ConnectContext ctx = contextResolvingTo(bindTable);
        LogicalPlan bindPlan = parse("SELECT k FROM internal.spm_db.t_bind");

        try {
            String stored = SPMPlanTreeSupport.schemaFingerprintForCreate(
                    ctx, bindPlan, planOver(planTable));
            String currentBind = SPMPlanTreeSupport.schemaFingerprint(ctx, bindPlan);
            Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                            stored, currentBind),
                    "a plan-side-only table must not invalidate the baseline before"
                            + " planning: " + stored + " / " + currentBind);

            // bind-side drift (DROP + CREATE of the bind table: new id) still fails
            Assertions.assertFalse(SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                            "t_bind|7|h;t_plan|8|v", "t_bind|9|h"),
                    "a replaced bind-side entry must fail the containment guard");
            // a query without a resolvable relation has nothing to invalidate
            Assertions.assertTrue(SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                    "t_plan|8|v", ""));
            // a stored fingerprint missing the current entry is not containment
            Assertions.assertFalse(SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                    "t_bind|9|h", "t_bind|7|h"));
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * After a failed SPM replay the ORIGINAL query is planned by the normal pipeline
     * with the SAME statement context: without a reset it inherited the plan-side
     * PREAGGOPEN hint (enabling force pre-aggregation on an unrelated query) and the
     * resolved-table entries of the frozen plan (stale TableIf objects survive a
     * DROP + CREATE in the same statement context).
     */
    @Test
    public void testResetPlannerStateForReplanClearsLeakedState() {
        StatementContext statementContext = new StatementContext();
        TableIf staleTable = table("t_leak", 11L, "k");
        statementContext.setHintForcePreAggOn(true);
        statementContext.getTables().put(List.of("internal", "db", "t_leak"), staleTable);
        statementContext.getOneLevelTables().put(List.of("internal", "db", "t_leak"), staleTable);
        statementContext.getMtmvRelatedTables().put(List.of("internal", "db", "t_mv"), staleTable);
        statementContext.getInsertTargetTables().put(List.of("internal", "db", "t_ins"), staleTable);

        statementContext.resetPlannerStateForReplan();

        Assertions.assertFalse(statementContext.isHintForcePreAggOn(),
                "the plan-side PREAGGOPEN hint must not leak into the replanned query");
        Assertions.assertTrue(statementContext.getTables().isEmpty(),
                "stale resolved tables must be re-resolved by the replan");
        Assertions.assertTrue(statementContext.getOneLevelTables().isEmpty());
        Assertions.assertTrue(statementContext.getMtmvRelatedTables().isEmpty());
        Assertions.assertTrue(statementContext.getInsertTargetTables().isEmpty());
    }
}
