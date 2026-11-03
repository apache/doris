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

import org.apache.doris.nereids.exceptions.AnalysisException;
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
 * Round-79 review fixes on the capture / replay side.
 *
 * <ul>
 *   <li>#2 an output LABEL consumed by HAVING is semantic, not cosmetic: the predicate binds the
 *       block's output name before the base columns, so renaming the item changes what the
 *       predicate reads;</li>
 *   <li>#3 {@code element_at(array(NULL), n)} is a NullType producer (the element type comes from
 *       the array), so its markers must be BOOLEAN;</li>
 *   <li>#5 the open-star alignment resolves a struct field path against the block's relation
 *       qualifiers instead of reading its last component;</li>
 *   <li>#6 a qualified {@code * EXCEPT} entry addressing a PARTLY open side's known prefix is
 *       removed from that prefix instead of being skipped as an unknown-tail column;</li>
 *   <li>#7 a capped CTE reference is identified by the binding VISIBLE at it, so a shadowed
 *       {@code WITH} name keeps a distinct definition index on both sides of a replay.</li>
 * </ul>
 */
public class SPMRound79PlannerTest {

    // ==================== #2: HAVING consumes output aliases ====================

    /**
     * Bind {@code SELECT k AS x FROM t HAVING x > 0} with plan {@code SELECT k AS y ...}: the
     * predicate text is identical, but HAVING resolves `x` against the block's OUTPUT names
     * first - the bind filters the alias (k) while the plan falls through to the base column t.x.
     * Restoring the header cannot repair the predicate, so the pair must be rejected.
     */
    @Test
    public void testHavingAliasRenameIsRejected() {
        String bind = "SELECT k AS x FROM t HAVING x > 0";
        String plan = "SELECT k AS y FROM t HAVING x > 0";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "a HAVING name that addresses the block's output alias cannot be renamed");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalHavingAliasIsAccepted() {
        String bind = "SELECT k AS x FROM t HAVING x > 0";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    /** A HAVING over a BASE column keeps its meaning under a label change. */
    @Test
    public void testHavingOnABaseColumnIsAccepted() {
        String bind = "SELECT k AS x FROM t HAVING k > 0";
        String plan = "SELECT k AS y FROM t HAVING k > 0";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "`k` addresses the base column on both sides");
    }

    /** Without HAVING the label really is only a label. */
    @Test
    public void testLabelRenameWithoutHavingIsAccepted() {
        String bind = "SELECT k AS x FROM t ORDER BY k";
        String plan = "SELECT k AS y FROM t ORDER BY k";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "a pure output label may differ between bind and plan");
    }

    // ==================== #3: array element producers ====================

    /**
     * `k > 0 OR element_at(array(NULL), 1)`: the array has element type NULL_TYPE, so
     * element_at returns NullType and the Boolean consumer replaces the WHOLE call with a typed
     * constant NULL - unless the array element's marker was already BOOLEAN, the
     * `element_at(array(TRUE), 1)` variant has nothing to substitute.
     */
    @Test
    public void testAllNullArrayElementKeepsBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at(array(NULL), 1)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int booleanNullMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (placeholder instanceof SpmConstVar
                    && ((SpmConstVar) placeholder).getValue() instanceof NullLiteral) {
                Assertions.assertTrue(((SpmConstVar) placeholder).getDataType()
                                instanceof BooleanType,
                        "the all-NULL array's element marker is consumed by a Boolean"
                                + " consumer, got: " + ((SpmConstVar) placeholder).getDataType());
                booleanNullMarkers++;
            }
        }
        Assertions.assertTrue(booleanNullMarkers >= 1,
                "the array element carries a NULL marker: " + builder.getPlaceholderExprs());
    }

    /** A non-NULL array element keeps its own type (only the NullType producer is retyped). */
    @Test
    public void testNonNullArrayElementKeepsItsMarkerType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at(array(1), 1)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar
                    && ((SpmConstVar) marker.getPlaceholderExpr()).getValue()
                            instanceof NullLiteral) {
                Assertions.assertFalse(((SpmConstVar) marker.getPlaceholderExpr()).getDataType()
                                instanceof BooleanType,
                        "only an all-NULL array makes element_at a NullType producer");
            }
        }
    }

    // ==================== #5: open-star struct field labels ====================

    /**
     * `SELECT *, s.a FROM t` over a base table whose width is unknown reaches the open-star
     * helper: `s` is no relation of the block, so `s.a` is a STRUCT FIELD PATH and its header is
     * `s.a` - reading only the last component renamed the correct header to `a` even for an
     * identical replay.
     */
    @Test
    public void testOpenStarTrailingStructFieldKeepsItsPath() {
        String sql = "SELECT *, s.a FROM t";
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(parse(sql), parse(sql));
        List<String> items = outputItemsOf(aligned);
        String last = items.get(items.size() - 1);
        Assertions.assertTrue(last.startsWith("s.a"),
                "the struct field path survives the alignment: " + items);
        Assertions.assertFalse(last.endsWith("AS a"),
                "the header must not be renamed to its last component: " + items);
    }

    /** The same for an item BEFORE the open-tail star. */
    @Test
    public void testOpenStarPrecedingStructFieldKeepsItsPath() {
        String sql = "SELECT s.a, * FROM t";
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(parse(sql), parse(sql));
        List<String> items = outputItemsOf(aligned);
        Assertions.assertTrue(items.get(0).startsWith("s.a"),
                "the preceding struct field path survives the alignment: " + items);
        Assertions.assertFalse(items.get(0).endsWith("AS a"),
                "the header must not be renamed to its last component: " + items);
    }

    // ==================== #6: qualified EXCEPT on a partly open side ====================

    /**
     * The addressed side `e` expands to `[x, OPEN]`, so `EXCEPT(e.x)` names a KNOWN leading
     * column: skipping the entry because the side is open left it in the derived prefix, turned
     * the prefix into `[z, x, OPEN]` and renamed the correctly frozen `k` header to `x`.
     */
    @Test
    public void testQualifiedExceptOnAPartlyOpenSideKeepsTheFrozenHeaders() {
        String user = "SELECT * EXCEPT(e.x) FROM (SELECT v + 1 AS z FROM t) d"
                + " CROSS JOIN (SELECT k + 1 AS x, u.* FROM u) e";
        String frozen = "SELECT d.z, e.k, e.q FROM (SELECT v + 1 AS z FROM t) d"
                + " CROSS JOIN (SELECT k + 1 AS x, u.* FROM u) e";
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(parse(frozen), parse(user));
        List<String> items = outputItemsOf(aligned);
        Assertions.assertEquals(3, items.size(), "arity: " + items);
        Assertions.assertTrue(items.get(1).startsWith("e.k"),
                "the correctly frozen k header must not be renamed to x: " + items);
    }

    // ==================== #7: scoped CTE reference identity ====================

    /**
     * A nested `WITH c` SHADOWS the outer one. The caller's inner reference must resolve to the
     * INNER definition's index, exactly like the replay's renamed `t_4` - a flat
     * first-definition-wins name map gave the caller's inner cap the OUTER index, so the cap key
     * of the two sides differed and an identical statement never hit its own baseline.
     */
    @Test
    public void testShadowedCteReferenceKeepsItsOwnCapIdentity() {
        String caller = "WITH c AS (SELECT k + 1 AS x FROM t)"
                + " SELECT c.x, s.y FROM c CROSS JOIN"
                + " (WITH c AS (SELECT k + 2 AS y FROM u) SELECT * FROM c LIMIT 1) s";
        String replay = "WITH t_3 AS (SELECT k + 1 AS x FROM t)"
                + " SELECT t_3.x, s.y FROM t_3 CROSS JOIN"
                + " (WITH t_4 AS (SELECT k + 2 AS y FROM u) SELECT * FROM t_4 LIMIT 1) s";
        Assertions.assertTrue(
                SPMPlanTreeSupport.rowLimitsWithin(parse(replay), parse(caller)),
                "the shadowed CTE reference keeps its own definition index on both sides");
    }

    /** The un-shadowed shape keeps working (regression guard for the scoped walk). */
    @Test
    public void testPlainCteReferenceKeepsItsCapIdentity() {
        String caller = "WITH c AS (SELECT k FROM t) SELECT * FROM c LIMIT 1";
        String replay = "WITH t_3 AS (SELECT k FROM t) SELECT * FROM t_3 LIMIT 1";
        Assertions.assertTrue(
                SPMPlanTreeSupport.rowLimitsWithin(parse(replay), parse(caller)),
                "a renamed CTE keeps its definition index");
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
