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
 * The hidden expression sources of the metadata guard.
 *
 * - The ASOF USING join keeps its MATCH_CONDITION outside getExpressions(): an
 *   alias UDF used only as the temporal boundary must be fingerprinted like any other
 *   call, otherwise redefining it changes which right row a direct ASOF query picks
 *   while the frozen boundary still replays.
 * - The bind-side table entry must hash the PLANNED (under-lock) TableIf snapshot, not a
 *   later re-resolution: an ALTER committing between the optimization and the fingerprint
 *   would otherwise bind the fingerprint to a schema the frozen output slots were not
 *   built from.
 */
public class SPMRound19SafetyTest {

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

    /** A session whose statement context resolves the bind-side relation to bind. */
    private static ConnectContext contextResolvingTo(TableIf bind) {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(statementContext.getAndCacheTable(
                        Mockito.anyList(), Mockito.any(), Mockito.any()))
                .thenReturn(bind);
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenReturn(bind);
        ctx.setStatementContext(statementContext);
        return ctx;
    }

    /**
     * An alias UDF used ONLY as the ASOF boundary must be part of the fingerprint: the
     * MATCH_CONDITION is not in LogicalUsingJoin.getExpressions(), so the walk saw only
     * the USING keys - redefining the UDF (analysis INLINES the body) changes which right
     * row the query picks while the stored fingerprint kept matching.
     */
    @Test
    public void testAsofMatchConditionFunctionsJoinTheFingerprint() {
        ConnectContext ctx = contextResolvingTo(table("t_any", 7L, "k"));
        try {
            String withoutFunction = SPMPlanTreeSupport.schemaFingerprintForCreate(ctx,
                    parse("SELECT l.k FROM t1 l ASOF JOIN t2 r MATCH_CONDITION(l.d >= r.d)"
                            + " USING(k)"), null, null);
            String withFunction = SPMPlanTreeSupport.schemaFingerprintForCreate(ctx,
                    parse("SELECT l.k FROM t1 l ASOF JOIN t2 r"
                            + " MATCH_CONDITION(l.d >= spm_r19_bound(r.d)) USING(k)"),
                    null, null);
            Assertions.assertFalse(withoutFunction.contains("spm_r19_bound"),
                    "control: the boundary UDF is not referenced here: " + withoutFunction);
            Assertions.assertTrue(withFunction.contains("spm_r19_bound"),
                    "the ASOF boundary function must be fingerprinted: " + withFunction);
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The bind-side table entry must hash the planned plan's OWN TableIf (the under-lock
     * snapshot the frozen slots came from), not a re-resolution that an intervening
     * ALTER could have moved to a NEWER schema. If the replay plans against the newer
     * table, the recomputed fingerprint differs and the stale replay is rejected.
     */
    @Test
    public void testBindFingerprintPinsThePlannedTableSnapshot() {
        TableIf lockedTable = table("t_pin", 11L, "old_col");
        TableIf reResolved = table("t_pin", 99L, "new_col");
        ConnectContext ctx = contextResolvingTo(reResolved);
        LogicalPlan bindPlan = parse("SELECT * FROM internal.spm_db.t_pin");
        try {
            String stored = SPMPlanTreeSupport.schemaFingerprintForCreate(ctx, bindPlan,
                    planOver(lockedTable), "SELECT old_col FROM internal.spm_db.t_pin");
            Assertions.assertTrue(stored.contains("t_pin|11|"),
                    "the bind side must hash the PLANNED (under-lock) snapshot: " + stored);
            Assertions.assertFalse(stored.contains("t_pin|99|"),
                    "a later re-resolution must not leak into the fingerprint: " + stored);

            String current = SPMPlanTreeSupport.schemaFingerprintForReplay(ctx, bindPlan,
                    planOver(reResolved), "SELECT old_col FROM internal.spm_db.t_pin");
            Assertions.assertNotEquals(stored, current,
                    "planning against the NEWER schema must fail the revalidation instead of"
                            + " silently omitting the new column");
        } finally {
            ConnectContext.remove();
        }
    }
}
