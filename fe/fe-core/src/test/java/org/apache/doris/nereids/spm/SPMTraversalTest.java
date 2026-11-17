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
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SPMSubquerySupport;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests traversal scopes, expression identity and output labels in SPM helpers. */
public class SPMTraversalTest {
    @Test
    public void testRightSemiUsingDoesNotExpandDiscardedSideLabels() {
        assertAlignedLabels("SELECT * FROM"
                + " (SELECT * FROM t CROSS JOIN (SELECT k FROM u) s) l"
                + " RIGHT SEMI JOIN (SELECT k, r FROM v) q USING(k)",
                "SELECT 1 AS captured_k, 2 AS captured_r FROM t", List.of("k", "r"));
    }

    @Test
    public void testOneRowDerivedStarUsesCallerLabel() {
        assertAlignedLabels("SELECT * FROM (SELECT 2) s",
                "SELECT 2 AS captured FROM t", List.of("2"));
    }

    private static void assertAlignedLabels(String callerSql, String replaySql, List<String> expected) {
        NereidsParser parser = new NereidsParser();
        LogicalPlan caller = (LogicalPlan) parser.parseSingle(callerSql);
        LogicalPlan replay = (LogicalPlan) parser.parseSingle(replaySql);
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(replay, caller);
        List<List<String>> labels = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(aligned, node -> {
            if (node instanceof LogicalProject) {
                List<String> projectLabels = new ArrayList<>();
                for (Expression item : ((LogicalProject<?>) node).getProjects()) {
                    projectLabels.add(SpmPlanTreeSupportTestAccess.outputLabel(item));
                }
                labels.add(projectLabels);
            }
        });
        Assertions.assertEquals(expected, labels.get(0));
    }

    @Test
    public void testExpressionListKeepsIndependentScalarAndListValues() {
        NereidsParser parser = new NereidsParser();
        for (String expression : List.of("a = 1", "a IN (1, 2)")) {
            List<Expression> bind = List.of(parser.parseExpression(expression),
                    parser.parseExpression(expression));
            SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
            List<Expression> parameterized = builder.parameterizeExpressions(bind);
            Assertions.assertEquals(2, builder.getPlaceholderExprs().size());
            Assertions.assertEquals(0L, builder.getPlaceholderExprs().get(0).getRootOrdinal());
            Assertions.assertEquals(1L, builder.getPlaceholderExprs().get(1).getRootOrdinal());

            String first = expression.contains("IN") ? "a IN (3)" : "a = 3";
            String second = expression.contains("IN") ? "a IN (4, 5, 6)" : "a = 4";
            Map<Long, Expression> values = new HashMap<>();
            SPMAstCheckVisitor matcher = new SPMAstCheckVisitor();
            Assertions.assertTrue(matcher.checkExpression(parameterized.get(0),
                    parser.parseExpression(first), values));
            Assertions.assertTrue(matcher.checkExpression(parameterized.get(1),
                    parser.parseExpression(second), values));
            Assertions.assertEquals(2, values.size());
            Assertions.assertEquals(parameterized, builder.parameterizeExpressions(bind));
            Assertions.assertEquals(2, builder.getPlaceholderExprs().size(),
                    "corresponding lists must still reuse the same marker ids");
        }
    }

    @Test
    public void testSubqueryHavingReusesNestedGroupByExpression() {
        NereidsParser parser = new NereidsParser();
        Expression subquery = parser.parseExpression("(SELECT substr(s, 1, 2) FROM t"
                + " GROUP BY substr(s, 1, 2) HAVING length(substr(s, 1, 2)) = 2)");
        SubqueryExpr parameterized = (SubqueryExpr) new SPMPlaceholderBuilder()
                .parameterizeExpressions(List.of(subquery)).get(0);
        List<Expression> groupKeys = new ArrayList<>();
        List<Expression> havingKeys = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(parameterized.getQueryPlan(), node -> {
            if (node instanceof LogicalAggregate) {
                groupKeys.addAll(((LogicalAggregate<?>) node).getGroupByExpressions());
            }
            if (node instanceof LogicalHaving) {
                havingKeys.add(((LogicalHaving<?>) node).getPredicate().child(0).child(0));
            }
        });
        Assertions.assertEquals(1, groupKeys.size());
        Assertions.assertEquals(1, havingKeys.size());
        Assertions.assertSame(groupKeys.get(0), havingKeys.get(0),
                "nested HAVING copies must reuse GROUP BY marker ids in subqueries too");
    }

    @Test
    public void testCollectsWhereBelowHaving() {
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = (LogicalPlan) parser.parseSingle(
                "SELECT a, count(*) FROM t WHERE b = 1 GROUP BY a HAVING count(*) > 2");
        Assertions.assertEquals(List.of(parser.parseExpression("b = 1"),
                parser.parseExpression("count(*) > 2")),
                SPMSubquerySupport.collectFilterPredicates(plan));
    }

    @Test
    public void testCollectsFiltersBelowDerivedTable() {
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = (LogicalPlan) parser.parseSingle(
                "SELECT a FROM (SELECT a FROM t WHERE a > 1) s WHERE a < 9");
        Assertions.assertEquals(List.of(parser.parseExpression("a > 1"),
                parser.parseExpression("a < 9")),
                SPMSubquerySupport.collectFilterPredicates(plan));
        Assertions.assertTrue(SPMSubquerySupport.collectFilterPredicates(
                (LogicalPlan) parser.parseSingle("SELECT 1")).isEmpty());
    }

    @Test
    public void testExpressionDigestIncludesInnerWhere() {
        NereidsParser parser = new NereidsParser();
        SubqueryExpr first = (SubqueryExpr) parser.parseExpression(
                "EXISTS (SELECT a FROM t WHERE b = 1 GROUP BY a HAVING count(*) > 2)");
        SubqueryExpr second = (SubqueryExpr) parser.parseExpression(
                "EXISTS (SELECT a FROM t WHERE b = 9 GROUP BY a HAVING count(*) > 2)");
        Assertions.assertNotEquals(SPMUtils.digest(first), SPMUtils.digest(second));
    }
}
