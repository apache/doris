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
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Round-73 review fixes on the planner side.
 *
 * <ul>
 *   <li>#1 an analyzer-inserted integral -&gt; LARGEINT WIDENING is only part of the frozen
 *       SEMANTICS inside an aggregate's argument list (the AVG lowering's accumulator). As an
 *       operand of ordinary arithmetic it is plain coercion, and keeping it pins the captured
 *       operand type: `k + 9223372036854775808` froze `CAST(k AS LARGEINT) + ...`, so the
 *       `+ 0.25` variant re-analyzed as DECIMAL(38,0) and truncated 1.25 to 1.</li>
 *   <li>#3 a qualified star's derived-alias / relation lookup follows the catalog's identifier
 *       case policy (ExpressionAnalyzer#sameTableName): with lower_case_table_names = 1 a
 *       case-sensitive comparison rejected `d.*` over a subquery aliased `D`, so even the
 *       identical replay skipped the baseline.</li>
 *   <li>#7 the producer/consumer marker defect was repaired for CASE only: an all-NULL IF
 *       consumed by OR is replaced by the analyzer the same way, deleting its placeholders.</li>
 * </ul>
 */
public class SPMRound73PlannerTest {

    // ==================== #1: ordinary arithmetic widening is stripped ====================

    /**
     * The reviewer's capture: BIGINT `k` plus a LARGEINT literal freezes as
     * `Cast(k AS LARGEINT) + _spm_const_var(1)`. Keeping that cast made the `+ 0.25` variant
     * derive DECIMAL(38,0) - the substituted value was truncated to 1 instead of 1.25.
     */
    @Test
    public void testOrdinaryArithmeticWideningIsStripped() {
        SlotReference k = new SlotReference("k", BigIntType.INSTANCE);
        Cast widening = new Cast(k, LargeIntType.INSTANCE);
        Add arithmetic = new Add(widening,
                new SpmConstVar(1L, new DecimalLiteral(new java.math.BigDecimal("0.25"))));

        String printed = new SPMExprSqlBuilder().print(arithmetic, new SQLRelation());
        Assertions.assertFalse(printed.contains("CAST"),
                "an ordinary arithmetic widening is an execution detail and must not pin the"
                        + " captured operand type: " + printed);
        Assertions.assertTrue(printed.contains("_spm_const_var(1)"),
                "the placeholder still renders its id-only form: " + printed);
    }

    /** Inside an aggregate the SAME shape is the AVG lowering's accumulator and survives. */
    @Test
    public void testAggregateArgumentWideningIsPreserved() {
        SlotReference k = new SlotReference("k", BigIntType.INSTANCE);
        Sum sum = new Sum(true, false, false, new Cast(k, LargeIntType.INSTANCE));

        String printed = new SPMExprSqlBuilder().print(sum, new SQLRelation());
        Assertions.assertTrue(printed.startsWith("sum(CAST(")
                        && printed.endsWith(" AS LARGEINT))"),
                "the LARGEINT accumulator of the AVG lowering must survive: " + printed);
    }

    // ==================== #3: the catalog's alias case policy ====================

    /**
     * `lower_case_table_names = 1`, and the parser keeps the alias as written (`D`) while the
     * caller qualifies the star with `d`. The analyzer resolves that pair case-insensitively,
     * so the star expansion must too - it threw before, and both replay paths skipped the
     * candidate (the query could not use even its identical baseline).
     */
    @Test
    public void testQualifiedStarOverCaseDifferingAliasResolves() {
        int previous = GlobalVariable.lowerCaseTableNames;
        GlobalVariable.lowerCaseTableNames = 1;
        ConnectContext.remove();
        try {
            LogicalPlan userPlan = parse("SELECT d.* FROM (SELECT k + 1 AS x FROM t) D");
            LogicalPlan frozenReplay = parse("SELECT d.x FROM (SELECT k + 1 AS x FROM t) D");

            LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay,
                    userPlan);
            Assertions.assertEquals(List.of("x"), rootOutputLabels(aligned),
                    "the star resolves and the replay exposes the caller's column");
        } finally {
            GlobalVariable.lowerCaseTableNames = previous;
        }
    }

    /** The precise policy still applies: with lower_case_table_names = 0 the case matters. */
    @Test
    public void testQualifiedStarKeepsCaseSensitivityWhenPolicyIsZero() {
        int previous = GlobalVariable.lowerCaseTableNames;
        GlobalVariable.lowerCaseTableNames = 0;
        ConnectContext.remove();
        try {
            LogicalPlan userPlan = parse("SELECT d.* FROM (SELECT k + 1 AS x FROM t) D");
            LogicalPlan frozenReplay = parse("SELECT d.x FROM (SELECT k + 1 AS x FROM t) D");

            Assertions.assertThrows(
                    SPMPlanTreeSupport.UnalignableOutputLabelsException.class,
                    () -> SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan),
                    "with the case-sensitive policy `d` does not address the alias `D`");
        } finally {
            GlobalVariable.lowerCaseTableNames = previous;
        }
    }

    // ==================== #7: an all-NULL IF in a Boolean context ====================

    /**
     * `k > 0 OR IF(k = 0, NULL, NULL)`: the IF's result type is NULL, so the analyzer replaces
     * the WHOLE operand with a typed constant NULL - both branch markers disappeared and an
     * ELSE TRUE variant had nothing to substitute (the replay returned NULL).
     */
    @Test
    public void testAllNullIfOperandParameterizesBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR IF(k = 0, NULL, NULL)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        List<PlaceholderExpr> markers = builder.getPlaceholderExprs();
        Assertions.assertEquals(4, markers.size(),
                "both comparison literals and BOTH IF branch NULLs must stay markers: "
                        + markers);
        int nullMarkers = 0;
        for (PlaceholderExpr marker : markers) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                nullMarkers++;
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof BooleanType,
                        "a branch NULL of a Boolean IF operand is parameterized BOOLEAN, got: "
                                + ((SpmConstVar) placeholder).getDataType());
            }
        }
        Assertions.assertEquals(2, nullMarkers,
                "the all-NULL IF carries two branch markers: " + markers);
    }

    /**
     * The producer/consumer shape: the all-NULL IF is PROJECTED and only the outer query block
     * consumes it as a Boolean operand, so the markers must already be BOOLEAN at the producer.
     */
    @Test
    public void testProjectedAllNullIfKeepsMarkersForAnOuterBooleanConsumer() {
        LogicalPlan plan = parse("SELECT IF(k = 0, NULL, NULL) AS n FROM t");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(plan, builder);

        Expression item = firstProjectItem(parameterized);
        Assertions.assertTrue(item instanceof NamedExpression,
                "expected the parameterized projection item, got: " + item);
        boolean sawNullMarker = false;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                sawNullMarker = true;
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof BooleanType,
                        "the projected IF's NULL results are BOOLEAN so the derived slot (and"
                                + " them) survive an outer OR");
            }
        }
        Assertions.assertTrue(sawNullMarker, "the branch NULLs are still parameterized");
    }

    /** A CASE's WHEN operand keeps its NULL type: only RESULTS may be typed BOOLEAN. */
    @Test
    public void testConditionalOperandNullIsNotTypedBoolean() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR IF(k = NULL, NULL, NULL)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (!(placeholder instanceof SpmConstVar)
                    || !(((SpmConstVar) placeholder).getValue() instanceof NullLiteral)) {
                continue;
            }
            Expression parent = marker.getParentExpr();
            if (parent instanceof org.apache.doris.nereids.trees.expressions.EqualTo) {
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof NullType,
                        "the comparison operand keeps NULL: `k = CAST(NULL AS BOOLEAN)` would"
                                + " not bind in the replayed plan");
            }
        }
    }

    // ==================== helpers ====================

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static Expression firstProjectItem(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        return ((LogicalProject<?>) node).getProjects().get(0);
    }

    /** The output labels of the first Project under the root (unbound trees). */
    private static List<String> rootOutputLabels(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        List<String> labels = new java.util.ArrayList<>();
        for (NamedExpression expr : ((LogicalProject<?>) node).getProjects()) {
            String rendered;
            try {
                rendered = expr.getName();
            } catch (org.apache.doris.nereids.exceptions.UnboundException unbound) {
                rendered = expr.toSql();
            }
            int cut = rendered.lastIndexOf(" AS ");
            labels.add(cut < 0 ? rendered : rendered.substring(cut + 4));
        }
        return labels;
    }
}
