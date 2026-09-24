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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * M3 milestone test: rewrite replays the FROZEN optimal plan (SR-aligned).
 *
 * After CREATE BASELINE (buildBaselineFromSql) the baseline's planSql is the frozen
 * optimal plan - decompiled from the SPM-optimized physical plan and carrying the
 * placeholder ids (_spm_const_var(id) / _spm_const_list(id)), the join
 * distribution hints ([BROADCAST] / [SHUFFLE]) and the pushed-down structure. On a
 * rewrite hit SPMPlanner re-parses that frozen text and substitutes the user values by
 * placeholder id (SPMFrozenTreeReplacer), so the rewritten tree starts from the
 * frozen optimal structure instead of the user's raw SQL structure.
 *
 * These tests hand-build a BaselinePlan with a frozen (placeholder-carrying) planSql -
 * the equivalent of what buildBaselineFromSql stores after a successful decompile - and
 * verify the rewrite path: scalar / CAST-wrapped placeholders, IN-list placeholders,
 * join distribution hint preservation, and the rejection of plan-only placeholders.
 */
public class SPMFrozenTreeReplayTest {

    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        manager = BaselineManager.getInstance();
        manager.clearForTest();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    // ==================== frozen-tree rewrite (re-parse + id substitution) ====================

    /**
     * A scalar placeholder wrapped by the optimizer's CAST survives the replay: the
     * frozen text a > CAST(_spm_const_var(1) AS INT) is re-parsed, the placeholder
     * is replaced with the user value inside the CAST and no placeholder call remains.
     */
    @Test
    public void testFrozenScalarCastRewrite() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a > 100";
        // frozen planSql as produced by the decompiler (id-only placeholder, CAST kept)
        String frozenPlanSql =
                "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT))";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "structurally identical user query must hit the frozen-text baseline");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");
        String exprSqls = allExprSqls(rewritten);
        // the user value is substituted inside the preserved CAST wrapper
        Assertions.assertTrue(exprSqls.contains("CAST(42 AS INT)")
                        || exprSqls.contains("42"),
                "user value must be substituted into the frozen tree: " + exprSqls);
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain in the rewritten tree");
        Assertions.assertFalse(exprSqls.contains("_spm_const_var"),
                "no placeholder may appear in the rewritten SQL: " + exprSqls);
    }

    /**
     * The frozen JOIN plan keeps its join distribution hint ([BROADCAST]) and its
     * subquery-nesting structure after the replay: the rewritten tree must carry the
     * hint so the later full optimization starts from the frozen join structure.
     */
    @Test
    public void testFrozenJoinHintPreserved() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.x WHERE t1.a > 100";
        // frozen planSql: filter pushed into a nested subquery + [BROADCAST] join hint
        String frozenPlanSql = "SELECT * FROM (SELECT * FROM t1 "
                + "WHERE (a > CAST(_spm_const_var(1) AS INT))) t_5 "
                + "INNER JOIN [BROADCAST] t2 ON (t1.a = t2.x)";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 JOIN t2 ON t1.a = t2.x WHERE t1.a > 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "frozen JOIN plan must be replayed on a hit");
        String tree = rewritten.treeString();
        Assertions.assertTrue(tree.contains("hint=[broadcast]"),
                "frozen join distribution hint must be preserved in the replay: " + tree);
        Assertions.assertTrue(allExprSqls(rewritten).contains("42"),
                "user value must be substituted: " + allExprSqls(rewritten));
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain: " + tree);
    }

    /**
     * An IN-list placeholder of the frozen text is replaced with the user's actual IN
     * list. The frozen text carries the type-coercion CAST wrapper around the list call
     * (exactly what the decompiler emits: b IN (CAST(_spm_const_list(1) AS INT))),
     * so the replacer must detect the list call THROUGH the CAST.
     */
    @Test
    public void testFrozenInListRewrite() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE b IN (1, 2, 3)";
        String frozenPlanSql =
                "SELECT * FROM t1 WHERE (b IN (CAST(_spm_const_list(1) AS INT)))";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE b IN (10, 20)");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "IN-list query must be rewritten from the frozen text");
        String exprSqls = allExprSqls(rewritten);
        Assertions.assertTrue(exprSqls.contains("10"), "user IN value 10 missing: " + exprSqls);
        Assertions.assertTrue(exprSqls.contains("20"), "user IN value 20 missing: " + exprSqls);
        Assertions.assertFalse(exprSqls.contains("_spm_const_list"),
                "no list placeholder may remain: " + exprSqls);
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain in the rewritten tree: " + exprSqls);
    }

    /**
     * A frozen placeholder with no user-extracted value (a plan-only placeholder) must
     * never reach the analyzer: the rewrite is rejected (falls back and returns null when
     * no other rewrite source exists).
     */
    @Test
    public void testFrozenPlanOnlyPlaceholderRejected() throws Exception {
        installConnectContext();
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a > 100";
        // frozen text carries an extra placeholder (id 2) with no bind-side counterpart
        String frozenPlanSql = "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT)) "
                + "AND (b < CAST(_spm_const_var(2) AS INT))";
        manager.createBaseline(frozenBaseline(bindSql, frozenPlanSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNull(rewritten,
                "a frozen plan-only placeholder (no user value) must reject the rewrite");
    }

    // ==================== helpers ====================

    /**
     * Installs a minimal ConnectContext on this thread (join-hint / statement-context
     * parsing of the frozen text needs it, exactly like the real StmtExecutor path).
     */
    private static void installConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext(ctx, new OriginStatement("SELECT 1", 0)));
    }

    /**
     * Hand-builds a baseline whose planSql is a frozen (placeholder-carrying) plan text -
     * the equivalent of what buildBaselineFromSql stores after a successful decompile.
     * The parameterized bind tree is produced by the same whole-tree parameterization the
     * real CREATE path runs, so the placeholder ids align with the frozen text.
     */
    private static BaselinePlan frozenBaseline(String bindSql, String frozenPlanSql)
            throws Exception {
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

    /** Concatenates the SQL text of every expression, recursing into subquery plans. */
    private static String allExprSqls(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        collectExprSqls(plan, sb);
        return sb.toString();
    }

    private static void collectExprSqls(Plan plan, StringBuilder sb) {
        for (Expression expr : plan.getExpressions()) {
            sb.append(expr.toSql()).append('\n');
        }
        for (Plan child : plan.children()) {
            collectExprSqls(child, sb);
        }
    }
}
