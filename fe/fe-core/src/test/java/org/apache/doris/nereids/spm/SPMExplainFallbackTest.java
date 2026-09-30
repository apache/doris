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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Round 16: the EXPLAIN fallback must replan the ORIGINAL tree from FRESH planner state.
 *
 * The retry used to reuse the StatementContext of the abandoned rewritten plan and
 * clear only privChecked. The first pass can leave hintForcePreAggOn (a plan-side
 * PREAGGOPEN hint) behind - a spurious PREAGGOPEN failure on a valid
 * t&#64;incr(...) EXPLAIN - and its resolved TableIf objects survive an intervening DDL
 * (DROP + CREATE), binding the retry to the OLD table. StmtExecutor calls
 * resetPlannerStateForReplan() in its equivalent fallback; ExplainCommand must do the
 * same.
 */
public class SPMExplainFallbackTest {

    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        manager = BaselineManager.getInstance();
        manager.clearForTest();
    }

    @AfterEach
    public void tearDown() {
        manager.clearForTest();
        ConnectContext.remove();
    }

    @Test
    public void testFallbackResetsPlannerStateForTheOriginalExplain() throws Exception {
        ConnectContext ctx = new ConnectContext();
        SessionVariable session = new SessionVariable();
        session.setEnableSpmRewrite(true);
        session.setEnableSpmFallback(true);
        ctx.setSessionVariable(session);
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext(ctx,
                new OriginStatement("EXPLAIN SELECT * FROM t1 WHERE a > 42", 0)));
        StatementContext statementContext = ctx.getStatementContext();

        // A matched frozen baseline whose REPLAY cannot be planned: the frozen text
        // references a table that does not exist, so the first pass fails and the
        // fallback must replan the ORIGINAL tree.
        manager.createBaseline(frozenBaseline(
                "SELECT * FROM t1 WHERE a > 100",
                "SELECT * FROM t_missing WHERE (a > CAST(_spm_const_var(1) AS INT))"));

        // Leak the first-pass planner state exactly like a plan-side PREAGGOPEN hint and
        // a resolved-table cache would (the fix must clear both before the retry).
        statementContext.setHintForcePreAggOn(true);
        statementContext.getTables().put(List.of("internal", "db", "t_leak"),
                Mockito.mock(TableIf.class));

        ExplainCommand command = new ExplainCommand(ExplainLevel.NORMAL,
                parse("SELECT * FROM t1 WHERE a > 42"), false);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        Throwable thrown = null;
        try {
            command.run(ctx, executor);
        } catch (Throwable t) {
            thrown = t;
        }

        Assertions.assertNotNull(thrown,
                "the retry plans the ORIGINAL tree; without a catalog its own analysis"
                        + " failure must surface");
        Assertions.assertFalse(String.valueOf(thrown.getMessage()).contains("t_missing"),
                "the surfaced failure must come from the ORIGINAL tree, not the frozen"
                        + " one: " + thrown.getMessage());
        Assertions.assertFalse(statementContext.isSpmBaselineApplied(),
                "the abandoned rewrite must not stay marked as applied");
        Assertions.assertFalse(statementContext.isHintForcePreAggOn(),
                "the plan-side PREAGGOPEN hint must not leak into the retried EXPLAIN");
        Assertions.assertTrue(statementContext.getTables().isEmpty(),
                "stale resolved tables must be re-resolved for the retried EXPLAIN");
    }

    /**
     * Hand-builds a baseline whose planSql is a frozen (placeholder-carrying) plan text -
     * the equivalent of what buildBaselineFromSql stores after a successful decompile.
     */
    private static BaselinePlan frozenBaseline(String bindSql, String frozenPlanSql) {
        LogicalPlan bindPlan = parse(bindSql);
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = SPMPlanTreeSupport.transform(
                bindPlan, expr -> expr.accept(builder, null));

        BaselinePlan baseline = new BaselinePlan();
        baseline.setBindSql(bindSql);
        baseline.setBindSqlDigest(bindPlan.toSpmDigest());
        baseline.setBindSqlHash(SPMUtils.hashOf(bindPlan.toSpmDigest()));
        baseline.setPlanSql(frozenPlanSql);
        baseline.setParameterizedBindPlan(parameterizedBind);
        baseline.setCost(0.0);
        return baseline;
    }

    /** Parses a single SELECT SQL into an unbound logical plan. */
    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }
}
