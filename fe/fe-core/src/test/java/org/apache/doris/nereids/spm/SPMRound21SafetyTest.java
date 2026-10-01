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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Twenty-first review round: session-free stored-text parsing, cache-free fingerprint
 * resolution and the EXPLAIN forwarding policy.
 *
 * - A stored bindSQL carrying a JOIN distribute hint must parse WITHOUT a session
 *   context: the hint branch dereferenced ConnectContext.get(), so refresh skipped the
 *   GLOBAL row and the forwarded SESSION import omitted the row on the master;
 * - the bind-side fingerprint fallback must resolve WITHOUT populating the planner's
 *   resolved-table cache (a cached pre-lock TableIf survives a concurrent DROP /
 *   CREATE t and the later lock pass reuses it);
 * - an EXPLAIN of a query must follow the query's forwarding policy, otherwise an
 *   observer reports "no hit" for a baseline the forwarded SELECT uses on the master.
 */
public class SPMRound21SafetyTest {

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

    /**
     * A stored bindSQL with JOIN [shuffle] is re-parsed by the refresh / forwarded-import
     * paths where NO ConnectContext is installed. The builder's hint branch used to
     * dereference ConnectContext.get() (NPE), so the valid row was skipped on refresh and
     * the forwarded SESSION row was omitted on the master.
     */
    @Test
    public void testJoinDistributeHintParsesWithoutASession() {
        try {
            ConnectContext.remove();
            Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                    "SELECT * FROM t1 JOIN [shuffle] t2 ON t1.k = t2.k WHERE t1.k = 1",
                    null, SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_DEFAULT);
            Assertions.assertNotNull(trees.first,
                    "the stored text must parse without a session context");
            final boolean[] hinted = {false};
            SPMPlanTreeSupport.<RuntimeException>walkPlans(trees.first, (Plan node) -> {
                if (node instanceof LogicalJoin
                        && ((LogicalJoin<?, ?>) node).getDistributeHint().distributeType
                                != DistributeType.NONE) {
                    hinted[0] = true;
                }
            });
            Assertions.assertTrue(hinted[0],
                    "the JOIN hint itself must survive the session-free parse");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * The bind-side fingerprint fallback (a relation the planned tree does not carry)
     * must resolve WITHOUT populating StatementContext's resolved-table cache: the
     * fingerprint runs before collectAndLockTable, and a cached pre-lock TableIf would be
     * reused by CollectRelation / BindExpression - a concurrent DROP / CREATE t would be
     * invisible and the replay could read stale metadata.
     */
    @Test
    public void testFingerprintLookupDoesNotPopulateTheResolverCache() {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        TableIf resolved = table("t", 7L, "k");
        Mockito.when(statementContext.resolveTableWithoutCache(Mockito.anyList(), Mockito.any()))
                .thenReturn(resolved);
        ctx.setStatementContext(statementContext);
        try {
            String fingerprint = SPMPlanTreeSupport.schemaFingerprint(ctx,
                    parse("SELECT k FROM cat.db.t"));
            Assertions.assertTrue(fingerprint.contains("t|7|"),
                    "the fallback must still contribute its entry: " + fingerprint);
            Mockito.verify(statementContext, Mockito.never())
                    .getAndCacheTable(Mockito.anyList(), Mockito.any(), Mockito.any());
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * EXPLAIN is a Command (NoForward); the forced-forward policy must still route an
     * EXPLAIN of a QUERY like the query itself. Otherwise an observer whose baseline
     * cache predates a baseline just created on the master reports "no hit" although the
     * immediately following SELECT is forwarded and uses that baseline.
     */
    @Test
    public void testExplainOfAQueryFollowsTheQueryRouting() {
        ExplainCommand explainSelect = new ExplainCommand(
                ExplainCommand.ExplainLevel.NORMAL, parse("SELECT k FROM t"), false);
        Assertions.assertTrue(explainSelect.isQueryExplain(),
                "EXPLAIN SELECT must participate in the QUERY forwarding policy");
        ExplainCommand explainCommand = new ExplainCommand(
                ExplainCommand.ExplainLevel.NORMAL, Mockito.mock(Command.class), false);
        Assertions.assertFalse(explainCommand.isQueryExplain(),
                "EXPLAIN of a command stays local");
    }
}
