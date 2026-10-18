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

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.NullType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Round-71 review fixes on the planner side:
 *
 * <ul>
 *   <li>#1 the AVG lowering ({@code AVG_DISTINCT_TO_SUM_DIV_COUNT}) pins a LARGEINT
 *       accumulator cast that the replay re-applies to a SUBSTITUTED literal of another
 *       type, so it must be excluded from the capture whitelist;</li>
 *   <li>#2 a USING key consumes the side label with that WHOLE name: matching the last
 *       component of a dotted alias ({@code x.k}) removed a column that is not the key;</li>
 *   <li>#5 a bare column header keeps its LAST name part only, so a qualified slot
 *       ({@code d.k}) is not frozen as {@code d.k};</li>
 *   <li>#7 the merged USING key label is the WRITTEN key spelling: the analyzer binds the
 *       key with {@code boundSlot.withName(writtenName)}, so deriving the label from the
 *       column's own alias renames the frozen header;</li>
 *   <li>#8 a CASE that IS a Boolean operand parameterizes its branch NULLs as BOOLEAN: an
 *       all-NULL CASE is NullType, and the analyzer replaces the whole operand with a typed
 *       constant, deleting every placeholder inside it.</li>
 * </ul>
 */
public class SPMRound71PlannerTest {

    // ==================== #2: USING consumes the WHOLE side label ====================

    /**
     * A side labelled {@code x.k} is a column named {@code x.k}, not the key {@code k}:
     * skipping it made the derived header list one column short, so every position behind
     * the key was renamed by one.
     */
    @Test
    public void testUsingConsumesTheWholeLabelOnly() {
        Set<String> consumed = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        consumed.add("k");

        Assertions.assertTrue(SpmPlanTreeSupportTestAccess.isConsumedUsingKey(consumed, "k"),
                "the key column itself is consumed by the merge");
        Assertions.assertFalse(SpmPlanTreeSupportTestAccess.isConsumedUsingKey(consumed, "x.k"),
                "a dotted alias x.k is NOT the key k - the analyzer binds whole names");
        Assertions.assertFalse(SpmPlanTreeSupportTestAccess.isConsumedUsingKey(consumed, "k2"),
                "only an exact label is the merged key");
    }

    // ==================== #5: a bare column header keeps its LAST name part ====================

    /**
     * A QUALIFIED slot ({@code d.k}) contributes the header {@code k}: the old rendering kept
     * the whole {@code d.k} name, so the frozen header never matched the caller's column.
     */
    @Test
    public void testQualifiedSlotHeaderIsTheColumnName() {
        Assertions.assertEquals("k",
                SpmPlanTreeSupportTestAccess.outputLabel(new UnboundSlot("d", "k")),
                "a qualified slot exposes its column name, not the qualified spelling");
    }

    /** A single QUOTED component is ONE name and must stay verbatim (see the EXCEPT fix). */
    @Test
    public void testQuotedSingleComponentHeaderIsKeptVerbatim() {
        Assertions.assertEquals("x.k",
                SpmPlanTreeSupportTestAccess.outputLabel(UnboundSlot.quoted("x.k")),
                "a quoted dotted alias is one name; splitting it would rename the column");
    }

    // ==================== #7: the merged key label is the WRITTEN key ====================

    /**
     * {@code USING(k)} over a side whose key column is aliased {@code K}: the merged output
     * slot is bound with the WRITTEN name, so the frozen header must be {@code k} - the
     * side-own alias renamed it to {@code K} and the replay's header no longer matched.
     */
    @Test
    public void testMergedUsingKeyKeepsTheWrittenSpelling() {
        Assertions.assertEquals("k",
                SpmPlanTreeSupportTestAccess.usingKeyLabel("k", List.of("K", "v"), false),
                "the analyzer binds the USING key with the name WRITTEN in the SQL");
    }

    /** An underivable side falls back to the raw spelling instead of guessing. */
    @Test
    public void testMergedUsingKeyFallsBackToTheWrittenSpelling() {
        Assertions.assertEquals("k",
                SpmPlanTreeSupportTestAccess.usingKeyLabel("k", List.of("v"), false),
                "no side label matches the key: keep the written spelling");
    }

    // ==================== #8: an all-NULL CASE operand keeps Boolean markers ====================

    /**
     * {@code k > 0 OR CASE WHEN k = 0 THEN NULL ELSE NULL END} used to expose a NullType
     * CASE operand: the analyzer replaced the WHOLE operand with a typed constant NULL, so
     * both branch markers vanished and a later TRUE variant had nothing to substitute
     * (the replayed query then returned NULL instead of TRUE at {@code k = -1}).
     */
    @Test
    public void testAllNullCaseOperandParameterizesBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR CASE WHEN k = 0"
                + " THEN NULL ELSE NULL END");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        List<PlaceholderExpr> markers = builder.getPlaceholderExprs();
        Assertions.assertEquals(4, markers.size(),
                "both comparison literals and BOTH branch NULLs must stay markers: "
                        + markers);
        int nullMarkers = 0;
        for (PlaceholderExpr marker : markers) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar constVar = (SpmConstVar) marker.getPlaceholderExpr();
            if (!(constVar.getValue() instanceof NullLiteral)) {
                continue;
            }
            nullMarkers++;
            Assertions.assertTrue(constVar.getDataType() instanceof BooleanType,
                    "a branch NULL of a Boolean CASE operand is parameterized BOOLEAN,"
                            + " got: " + constVar.getDataType());
        }
        Assertions.assertEquals(2, nullMarkers,
                "the all-NULL CASE carries two branch markers: " + markers);
    }

    /** The same CASE in a NON-Boolean position keeps its NullType marker (no widening). */
    @Test
    public void testCaseOutsideBooleanContextKeepsNullMarkers() {
        LogicalPlan plan = parse("SELECT CASE WHEN k = 0 THEN NULL ELSE NULL END AS c FROM t");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        boolean sawNullMarker = false;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                sawNullMarker = true;
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType() instanceof NullType,
                        "outside a Boolean operand the marker keeps the NULL type");
            }
        }
        Assertions.assertTrue(sawNullMarker, "the branch NULLs are still parameterized");
    }

    // ==================== #1: the AVG lowering is excluded from capture ====================

    /**
     * {@code avg(DISTINCT x)} lowers to {@code sum(DISTINCT CAST(x AS LARGEINT)) / count(x)}.
     * That cast is part of the lowered SEMANTICS and the replay re-applies it to a
     * SUBSTITUTED literal of another type ({@code avg(DISTINCT k + 0.25)} matching a captured
     * {@code avg(DISTINCT k + 1)}): the substituted DECIMAL is truncated to an integer first
     * and the replayed query returns a different value.
     */
    @Test
    public void testAvgDistinctLoweringIsExcludedFromCapture() {
        Assertions.assertTrue(SPMOptimizer.SPM_EXCLUDED_RULE_NAMES
                        .contains("AVG_DISTINCT_TO_SUM_DIV_COUNT"),
                "the LARGEINT accumulator cast pins a type the replay cannot honour");
    }

    // ==================== helpers ====================

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }
}
