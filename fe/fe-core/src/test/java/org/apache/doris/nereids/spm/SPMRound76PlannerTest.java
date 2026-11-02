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

import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.LargeIntType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

/**
 * Round-76 review fixes on the capture / replay side.
 *
 * <ul>
 *   <li>#1 an all-NULL {@code IFNULL} / {@code NVL} call has NullType exactly like an all-NULL
 *       CASE, so a Boolean consumer replaced the WHOLE call with a typed constant NULL: both
 *       markers disappeared and an {@code ifnull(NULL, TRUE)} variant had nothing to
 *       substitute;</li>
 *   <li>#3 {@code UnboundFunction#computeToSql()} omits the database qualifier, so a manual plan
 *       calling {@code db1.f(k)} passed every comparison against a bind text calling
 *       {@code db2.f(k)} and the replay ran the wrong function with the caller's values;</li>
 *   <li>#4 the hint peel rebuilt only PLAN children, so a SET_VAR inside an expression-owned
 *       scalar subquery survived and the replay applied the CREATOR's session variable to a
 *       caller that carried no hint at all;</li>
 *   <li>#6 the ACCUMULATOR widening is preserved only for the ROOT of an aggregate argument: a
 *       cast nested inside that argument's arithmetic pinned the captured operand type and a
 *       substituted fractional literal was truncated.</li>
 * </ul>
 */
public class SPMRound76PlannerTest {

    // ==================== #1: an all-NULL IFNULL / NVL in a Boolean context ==============

    /**
     * `k > 0 OR ifnull(NULL, NULL)`: the call's result type is NULL, so the analyzer replaces
     * the WHOLE operand with a typed constant NULL - both argument markers disappeared and the
     * `ifnull(NULL, TRUE)` variant had nothing to substitute (the replay returned NULL).
     */
    @Test
    public void testAllNullIfNullOperandParameterizesBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR ifnull(NULL, NULL)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        List<PlaceholderExpr> markers = builder.getPlaceholderExprs();
        int nullMarkers = 0;
        for (PlaceholderExpr marker : markers) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                nullMarkers++;
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof BooleanType,
                        "an argument NULL of a Boolean IFNULL operand is parameterized BOOLEAN,"
                                + " got: " + ((SpmConstVar) placeholder).getDataType());
            }
        }
        Assertions.assertEquals(2, nullMarkers,
                "the all-NULL IFNULL carries two argument markers: " + markers);
    }

    /** `nvl` is the same call under another spelling and must behave identically. */
    @Test
    public void testAllNullNvlOperandParameterizesBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR nvl(NULL, NULL)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int nullMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                nullMarkers++;
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof BooleanType,
                        "the NVL spelling is the same conditional family");
            }
        }
        Assertions.assertEquals(2, nullMarkers,
                "the all-NULL NVL carries two argument markers: "
                        + builder.getPlaceholderExprs());
    }

    /**
     * A projected all-NULL IFNULL behaves exactly like a projected all-NULL IF (round-73 #7):
     * the item's type is NULL and an OUTER Boolean consumer may replace the whole item, so the
     * markers must already be BOOLEAN at the producer. IFNULL belongs to the same family.
     */
    @Test
    public void testProjectedAllNullIfNullKeepsMarkersForAnOuterBooleanConsumer() {
        LogicalPlan plan = parse("SELECT ifnull(NULL, NULL) AS x FROM t");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int nullMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                nullMarkers++;
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof BooleanType,
                        "a projected all-NULL IFNULL types its markers BOOLEAN, like the IF");
            }
        }
        Assertions.assertEquals(2, nullMarkers,
                "both arguments stay markers: " + builder.getPlaceholderExprs());
    }

    // ==================== #3: the function's qualified identity ==========================

    /**
     * `db1.f(k)` and `db2.f(k)` render the SAME text (`f(k)`) through
     * {@code UnboundFunction#computeToSql()}, so the divergent manual plan used to be accepted
     * and the caller's values were executed by another database's function.
     */
    @Test
    public void testManualPlanWithAnotherDatabaseFunctionIsRejected() {
        LogicalPlan bind = parse("SELECT db1.f(k) FROM t");
        LogicalPlan manual = parse("SELECT db2.f(k) FROM t");

        org.apache.doris.nereids.exceptions.AnalysisException error =
                Assertions.assertThrows(
                        org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> SPMPlanTreeSupport.rejectManualPlanDivergence(bind, manual,
                                "SELECT db1.f(k) FROM t"),
                        "another database's same-named function is a different call target");
        Assertions.assertTrue(error.getMessage().contains("f"),
                "the message names the diverging expression: " + error.getMessage());
    }

    /** The SAME qualified function on both sides is accepted (the identity matches). */
    @Test
    public void testManualPlanWithTheSameDatabaseFunctionIsAccepted() {
        LogicalPlan bind = parse("SELECT db1.f(k) FROM t");
        LogicalPlan manual = parse("SELECT db1.f(k) AS x FROM t");

        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(bind, manual,
                        "SELECT db1.f(k) FROM t"));
    }

    // ==================== #4: SET_VAR inside an expression-owned subquery ================

    /**
     * The SET_VAR hint lives in a scalar subquery that a PROJECTION owns - its query plan is not
     * a plan child, so the plan-level peel never reached it and the ordinary replay analysis
     * applied the creator's `time_zone` to a caller that carried no hint.
     */
    @Test
    public void testStripReachesSetVarInsideProjectedScalarSubquery() {
        LogicalPlan plan = parse("SELECT (SELECT /*+ SET_VAR(time_zone = '+08:00') */ 1"
                + " FROM u) AS x FROM t");
        Assertions.assertTrue(hasSetVarHint(plan),
                "precondition: the parsed tree carries the nested SET_VAR");

        LogicalPlan stripped = SPMPlanTreeSupport.stripSelectHints(plan);
        Assertions.assertFalse(hasSetVarHint(stripped),
                "the peel must reach an expression-owned scalar subquery, got: "
                        + describeSetVars(stripped));
    }

    /** The same hazard behind a FILTER predicate (a HAVING / WHERE subquery). */
    @Test
    public void testStripReachesSetVarInsideFilterScalarSubquery() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k = (SELECT /*+ SET_VAR(time_zone ="
                + " '+08:00') */ max(k) FROM u)");
        Assertions.assertTrue(hasSetVarHint(plan),
                "precondition: the parsed tree carries the nested SET_VAR");

        LogicalPlan stripped = SPMPlanTreeSupport.stripSelectHints(plan);
        Assertions.assertFalse(hasSetVarHint(stripped),
                "the peel must reach a predicate-owned subquery, got: "
                        + describeSetVars(stripped));
    }

    /** A non-SET_VAR hint (the frozen structure's point) survives the peel. */
    @Test
    public void testStripKeepsStructuralHints() {
        LogicalPlan plan = parse("SELECT /*+ SET_VAR(time_zone = '+08:00') */ k FROM t");
        Assertions.assertTrue(hasSetVarHint(plan), "precondition: the tree carries a SET_VAR");

        LogicalPlan stripped = SPMPlanTreeSupport.stripSelectHints(plan);
        Assertions.assertFalse(hasSetVarHint(stripped), "the SET_VAR payload is peeled");
        Assertions.assertEquals(describeSetVars(plan), "[SET_VAR:SelectHintSetVar]",
                "precondition: the only hint is the SET_VAR");
        Assertions.assertFalse(hasHintWrapper(stripped),
                "dropping the only hint removes the wrapper, not the query: "
                        + describeSetVars(stripped));
        Assertions.assertTrue(stripped instanceof UnboundResultSink,
                "the query itself is untouched: " + stripped.getClass().getName());
    }

    // ==================== #6: the accumulator widening is a ROOT-only rule ===============

    /**
     * `sum(CAST(k AS LARGEINT) + ph)`: the cast is an OPERAND of the argument's arithmetic, not
     * the argument's accumulator - keeping it pinned the captured operand type, so the
     * substituted `+ 0.25` re-analyzed against LARGEINT, derived DECIMAL(38,0) and truncated
     * k = 1 to 1 instead of 1.25.
     */
    @Test
    public void testNestedWideningInsideAggregateArgumentIsStripped() {
        SlotReference k = new SlotReference("k", BigIntType.INSTANCE);
        Cast widening = new Cast(k, LargeIntType.INSTANCE);
        Add arithmetic = new Add(widening,
                new SpmConstVar(1L, new DecimalLiteral(new BigDecimal("0.25"))));
        Sum sum = new Sum(true, false, false, arithmetic);

        String printed = new SPMExprSqlBuilder().print(sum, new SQLRelation());
        Assertions.assertFalse(printed.contains("CAST"),
                "a cast nested inside the aggregate argument's arithmetic is an operand"
                        + " coercion and must not pin the captured type: " + printed);
        Assertions.assertTrue(printed.startsWith("sum("),
                "the aggregate itself is still rendered: " + printed);
        Assertions.assertTrue(printed.contains("_spm_const_var(1)"),
                "the placeholder still renders its id-only form: " + printed);
    }

    /**
     * The ROOT cast of the argument IS the AVG lowering's accumulator
     * (`sum(CAST((k + ph) AS LARGEINT))`) and must survive.
     */
    @Test
    public void testRootWideningOfAggregateArgumentIsPreserved() {
        SlotReference k = new SlotReference("k", BigIntType.INSTANCE);
        Add arithmetic = new Add(k, new SpmConstVar(1L, new DecimalLiteral(BigDecimal.ONE)));
        Cast widening = new Cast(arithmetic, LargeIntType.INSTANCE);
        Sum sum = new Sum(true, false, false, widening);

        String printed = new SPMExprSqlBuilder().print(sum, new SQLRelation());
        Assertions.assertTrue(printed.startsWith("sum(CAST(")
                        && printed.endsWith(" AS LARGEINT))"),
                "the LARGEINT accumulator of the AVG lowering must survive: " + printed);
    }

    // ==================== helpers ====================

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static boolean hasSetVarHint(LogicalPlan plan) {
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (node instanceof LogicalSelectHint) {
                for (SelectHint hint : ((LogicalSelectHint<?>) node).getHints()) {
                    if (hint instanceof SelectHintSetVar) {
                        found[0] = true;
                    }
                }
            }
        });
        return found[0];
    }

    private static String describeSetVars(LogicalPlan plan) {
        List<String> names = new java.util.ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (node instanceof LogicalSelectHint) {
                for (SelectHint hint : ((LogicalSelectHint<?>) node).getHints()) {
                    names.add((hint instanceof SelectHintSetVar ? "SET_VAR:" : "hint:")
                            + hint.getClass().getSimpleName());
                }
            }
        });
        return names.toString();
    }

    private static boolean hasHintWrapper(LogicalPlan plan) {
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan,
                node -> found[0] |= node instanceof LogicalSelectHint);
        return found[0];
    }
}
