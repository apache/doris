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
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BooleanType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-78 review fixes on the capture side.
 *
 * <ul>
 *   <li>#1 {@code GREATEST} / {@code LEAST} are custom-signature NullType producers: an all-NULL
 *       call is replaced by a Boolean consumer, so its markers must already be BOOLEAN;</li>
 *   <li>#2 a nested conditional in the RESULT of another conditional yields NullType although its
 *       result is not a NULL literal, so the producer test has to be recursive;</li>
 *   <li>#3 the qualifier discovery feeding the output-label alignment is block-scoped: an aliased
 *       relation of a NESTED block is not visible to the outer one, so an outer struct field path
 *       is no longer misclassified as relation-qualified.</li>
 * </ul>
 */
public class SPMRound78PlannerTest {

    // ==================== #1: GREATEST / LEAST producers =============================

    /**
     * `k > 0 OR greatest(NULL, NULL)`: GreatestLeast.customSignature returns the arguments' common
     * type, i.e. NullType, so the analyzer replaces the whole call with a typed constant NULL and
     * both markers vanish - a `greatest(1, 1)` variant then has nothing to substitute.
     */
    @Test
    public void testAllNullGreatestOperandParameterizesBooleanMarkers() {
        assertAllNullProducerMarkersAreBoolean("greatest");
    }

    /** LEAST shares the GreatestLeast path. */
    @Test
    public void testAllNullLeastOperandParameterizesBooleanMarkers() {
        assertAllNullProducerMarkersAreBoolean("least");
    }

    private static void assertAllNullProducerMarkersAreBoolean(String function) {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR " + function
                + "(NULL, NULL)");
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
                        "an argument of a Boolean " + function + " operand is parameterized"
                                + " BOOLEAN, got: "
                                + ((SpmConstVar) placeholder).getDataType());
            }
        }
        Assertions.assertEquals(2, nullMarkers,
                "the all-NULL " + function + " carries two argument markers: "
                        + builder.getPlaceholderExprs());
    }

    // ==================== #2: nested conditional producers ===========================

    /**
     * The outer CASE yields NullType although its THEN result is a CALL, not a NULL literal:
     * {@code ifnull(NULL, NULL)} is NullType, so the CASE is too - and the Boolean consumer replaces
     * the whole operand, deleting the inner markers. A literal-only producer test missed this.
     */
    @Test
    public void testConditionalNestedInAnotherConditionalKeepsBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR (CASE WHEN k = 0 THEN"
                + " ifnull(NULL, NULL) ELSE NULL END)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int booleanNullMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral
                    && ((SpmConstVar) placeholder).getDataType() instanceof BooleanType) {
                booleanNullMarkers++;
            }
        }
        Assertions.assertEquals(3, booleanNullMarkers,
                "the ELSE NULL and BOTH ifnull arguments must be BOOLEAN markers: "
                        + builder.getPlaceholderExprs());
    }

    /** The non-Boolean shape keeps its argument types (only the NullType producer is retyped). */
    @Test
    public void testNonNullGreatestKeepsItsMarkerTypes() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR greatest(k, NULL) = 1");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar
                    && ((SpmConstVar) marker.getPlaceholderExpr()).getValue()
                            instanceof NullLiteral) {
                Assertions.assertFalse(((SpmConstVar) marker.getPlaceholderExpr()).getDataType()
                                instanceof BooleanType,
                        "only an ALL-NULL GREATEST is a NullType producer");
            }
        }
    }

    // ==================== #3: block-scoped qualifier discovery =======================

    /**
     * `SELECT s.a, k + 1 AS z FROM t WHERE k IN (SELECT k FROM u s)`: the INNER alias `s` is not
     * visible to the outer block, so the outer `s.a` is a STRUCT field path - the correctly frozen
     * header must survive an identical replay.
     */
    @Test
    public void testInnerBlockAliasDoesNotQualifyAnOuterFieldPath() {
        String sql = "SELECT s.a, k + 1 AS z FROM t WHERE k IN (SELECT k FROM u s)";
        LogicalPlan user = parse(sql);
        LogicalPlan frozenReplay = parse(sql);

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, user);
        List<String> items = outputItemsOf(aligned);
        Assertions.assertTrue(items.get(0).startsWith("s.a"),
                "the outer s.a is a field path, not relation-qualified: " + items);
        Assertions.assertFalse(items.get(0).endsWith("AS a"),
                "the alignment must not rename s.a to a: " + items);
    }

    // ==================== helpers ====================

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static List<String> outputItemsOf(LogicalPlan plan) {
        final List<String> items = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (items.isEmpty() && node instanceof LogicalProject) {
                for (NamedExpression item : ((LogicalProject<?>) node).getProjects()) {
                    items.add(item.toSql());
                }
            }
        });
        return items;
    }
}
