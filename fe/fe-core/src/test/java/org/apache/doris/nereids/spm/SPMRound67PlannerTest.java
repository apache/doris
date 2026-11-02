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
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Round-67 review fixes on the parameterization / alignment side:
 *
 * - #3 the GROUP BY mirror records must END at the query-block boundary: a derived table's
 *   aggregate leaves mirrors for ITS OWN block's select list, but the enclosing block's
 *   projection used to consume them too - an outer item spelled like the inner group-by
 *   expression reused the inner expression's placeholder id, so changing only the outer
 *   literal kept the digest but extracted conflicting values for that shared id and a
 *   valid variant never used its baseline;
 * - #5 an EXCEPT name is resolved with the analyzer's identifier-case rules
 *   (Scope.findSlotIgnoreCase): `EXCEPT(k)` removes the column aliased `K`; the
 *   case-sensitive comparison kept it and the lenient-tail tolerance suppressed the
 *   mismatch, so the frozen Project(v) was renamed to Project(v AS K).
 */
public class SPMRound67PlannerTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static String labelOf(NamedExpression expr) {
        try {
            return expr.getName();
        } catch (org.apache.doris.nereids.exceptions.UnboundException unbound) {
            return expr.toSql();
        }
    }

    /** The labels of the first Project under the root. */
    private static List<String> rootLabels(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        List<String> labels = new ArrayList<>();
        for (NamedExpression expr : ((LogicalProject<?>) node).getProjects()) {
            labels.add(labelOf(expr));
        }
        return labels;
    }

    private static Plan firstSubQueryAlias(Plan node) {
        if (node instanceof LogicalSubQueryAlias) {
            return node;
        }
        for (Plan child : node.children()) {
            Plan found = firstSubQueryAlias(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** The placeholder id of the first SpmConstVar inside one expression. */
    private static Long placeholderIdOf(Expression expression) {
        if (expression instanceof SpmConstVar) {
            return ((SpmConstVar) expression).getId();
        }
        for (Expression child : expression.children()) {
            Long found = placeholderIdOf(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static void collectPlaceholderIds(Plan node, Set<Long> ids) {
        for (Expression expression : node.getExpressions()) {
            collectExpressionIds(expression, ids);
        }
        for (Plan child : node.children()) {
            collectPlaceholderIds(child, ids);
        }
    }

    private static void collectExpressionIds(Expression expression, Set<Long> ids) {
        if (expression instanceof SpmConstVar) {
            ids.add(((SpmConstVar) expression).getId());
        }
        for (Expression child : expression.children()) {
            collectExpressionIds(child, ids);
        }
    }

    // ==================== #3: GROUP BY mirror scope ====================

    /**
     * `SELECT k + 1 AS c FROM (SELECT k + 1 AS k FROM t GROUP BY k + 1) s`: the inner
     * block's two copies of `k + 1` legitimately share the group-by mirror id (the
     * analyzer requires the select expression to reappear in the GROUP BY), but the OUTER
     * item is in ANOTHER query block and must allocate its own id. Reusing the inner id
     * made the outer literal share a placeholder with a DIFFERENT value, so a variant
     * changing only the outer literal was rejected by recordPlaceholderValue.
     */
    @Test
    public void testGroupByMirrorDoesNotLeakAcrossTheQueryBlockBoundary() {
        LogicalPlan bind = parse(
                "SELECT k + 1 AS c FROM (SELECT k + 1 AS k FROM t GROUP BY k + 1) s");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(bind, builder);

        Plan alias = firstSubQueryAlias(parameterized);
        Assertions.assertNotNull(alias, "the derived table must keep its subquery alias");
        Set<Long> innerIds = new HashSet<>();
        collectPlaceholderIds(alias, innerIds);
        Assertions.assertEquals(1, innerIds.size(),
                "the inner block's select item must reuse its OWN group-by mirror id: "
                        + innerIds);

        Plan outerProject = parameterized;
        while (!(outerProject instanceof LogicalProject)) {
            outerProject = outerProject.child(0);
        }
        Long outerId = placeholderIdOf(
                ((LogicalProject<?>) outerProject).getProjects().get(0));
        Assertions.assertNotNull(outerId, "the outer item must be parameterized");
        Assertions.assertFalse(innerIds.contains(outerId),
                "the enclosing block's projection must NOT reuse the inner block's"
                        + " group-by placeholder id (outer=" + outerId + ", inner="
                        + innerIds + ")");
        // sanity: the inner aggregate really is a GROUP BY block
        Plan probe = alias;
        while (probe != null && !(probe instanceof LogicalAggregate)) {
            probe = probe.children().isEmpty() ? null : probe.child(0);
        }
        Assertions.assertNotNull(probe, "the derived table must contain the aggregate");
    }

    // ==================== #5: EXCEPT with identifier-case rules ====================

    /**
     * `SELECT * EXCEPT(k) FROM (SELECT k AS K FROM t) s CROSS JOIN u` with u(v): the
     * analyzer resolves `k` to the column aliased `K` and exposes only `v`. The star
     * expansion here yields the known prefix [K] plus an unknown tail; the case-sensitive
     * comparison kept K, the lenient prefix path suppressed the mismatch, and the frozen
     * Project(v) was renamed to Project(v AS K).
     */
    @Test
    public void testExceptMatchesNamesWithIdentifierCaseRules() {
        LogicalPlan userPlan = parse(
                "SELECT * EXCEPT(k) FROM (SELECT k AS K FROM t) s CROSS JOIN u");
        LogicalPlan frozenReplay = parse(
                "SELECT v FROM (SELECT k AS K FROM t) s CROSS JOIN u");

        Assertions.assertEquals(List.of("v"), rootLabels(frozenReplay),
                "precondition: the frozen tree exposes the surviving column");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("v"), rootLabels(aligned),
                "EXCEPT(k) removes the column aliased K under the analyzer's case rules:"
                        + " the frozen v header must not be renamed");
    }
}
