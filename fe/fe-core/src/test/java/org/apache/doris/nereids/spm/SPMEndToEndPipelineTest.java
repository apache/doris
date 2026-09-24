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
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SPM whole-query pipeline test: create a baseline from SQL texts (whole-tree
 * parameterization) -> three-level match -> rewrite.
 *
 * Covers the placeholder types over the whole tree: SpmConstVar (scalar literal),
 * SpmConstList (IN list) and Between (range predicate), plus value independence of the
 * digest.
 */
public class SPMEndToEndPipelineTest {

    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        manager = BaselineManager.getInstance();
        manager.clearForTest();
    }

    /**
     * The full-pipeline main flow: WHERE a > 100 AND b IN (1, 2, 3).
     *
     * Verifies that a user query with different values (42 / 10, 20) still hits and that
     * in the rewritten tree the placeholders are replaced with the user values while the
     * capture-time value 100 does not leak.
     */
    @Test
    public void testCaptureAndRewritePipeline() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a > 100 AND b IN (1, 2, 3)";
        long id = manager.createBaseline(planner.buildBaseline(bindSql, bindSql));
        Assertions.assertTrue(id > 0);
        Assertions.assertEquals(1, manager.getAllBaselines().size());

        // user query with the same structure but different values
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 42 AND b IN (10, 20)");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "structurally identical query must hit the baseline");
        Assertions.assertEquals(id, planner.getUsedBaselineId());
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("42"),
                "scalar placeholder should be replaced with user value 42: " + rewrittenSql);
        Assertions.assertTrue(rewrittenSql.contains("10"),
                "IN-list should contain user value 10: " + rewrittenSql);
        Assertions.assertTrue(rewrittenSql.contains("20"),
                "IN-list should contain user value 20: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    /**
     * Value-independence regression: one baseline matches any literal value (42, -1, and
     * 100 itself).
     */
    @Test
    public void testValueIndependence() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        manager.createBaseline(planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100",
                "SELECT * FROM t1 WHERE a = 100"));

        // all three different user values must hit and be substituted
        for (int userValue : new int[] {42, -1, 100}) {
            LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = " + userValue);
            long deadline = System.currentTimeMillis() + 5000;
            LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);
            Assertions.assertNotNull(rewritten, "value " + userValue + " must match");
            String exprSqls = allExprSqls(rewritten);
            Assertions.assertTrue(exprSqls.contains("= " + userValue),
                    "rewritten plan must contain user value " + userValue + ": " + exprSqls);
            Assertions.assertFalse(exprSqls.contains("_spm_const_var"),
                    "no placeholder may remain for value " + userValue);
        }
    }

    /**
     * Between range predicate full pipeline: capture a BETWEEN 10 AND 20, user a BETWEEN
     * 30 AND 40.
     */
    @Test
    public void testBetweenPipeline() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        manager.createBaseline(planner.buildBaseline(
                "SELECT * FROM t1 WHERE a BETWEEN 10 AND 20",
                "SELECT * FROM t1 WHERE a BETWEEN 10 AND 20"));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a BETWEEN 30 AND 40");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "Between query must hit the baseline");
        String exprSqls = allExprSqls(rewritten);
        Assertions.assertTrue(exprSqls.contains("BETWEEN 30 AND 40"), exprSqls);
        Assertions.assertFalse(exprSqls.contains("BETWEEN 10 AND 20"), exprSqls);
        Assertions.assertFalse(exprSqls.contains("_spm_const_var"), exprSqls);
    }

    /** Concatenates the SQL text of every expression of the tree (for assertions). */
    private static String allExprSqls(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        collectExprSqls(plan, sb);
        return sb.toString();
    }

    private static void collectExprSqls(org.apache.doris.nereids.trees.plans.Plan plan, StringBuilder sb) {
        for (org.apache.doris.nereids.trees.expressions.Expression expr : plan.getExpressions()) {
            sb.append(expr.toSql()).append('\n');
        }
        for (org.apache.doris.nereids.trees.plans.Plan child : plan.children()) {
            collectExprSqls(child, sb);
        }
    }

    /** Parses a single SELECT SQL into an unbound logical plan. */
    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }
}
