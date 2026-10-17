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
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-65 review fixes on the replay / alignment side:
 *
 * - #2 an open star followed by MORE stars: the trailing stars used to be counted as one
 *   output each, so the suffix loop read a null label and the captured headers of the
 *   substituted expressions survived the alignment;
 * - #7 SET_VAR hints are collected in ANALYZER order: the definitions of a WITH node are
 *   analyzed before the query consuming them, while the generic walk visited the main
 *   child first, so the frozen replay ended in the CTE definition's settings;
 * - #14 the JOIN branch of the star expansion keeps the last input's known label prefix
 *   (and its open tail) instead of dropping the whole child labels;
 * - #15 an INHERITED CTE is expanded in its DEFINING scope: the nested scope used to
 *   re-derive the reference through the shadowing definition;
 * - #16 LOGICAL REPEAT output lists participate in the alignment helpers like ordinary
 *   aggregates (ROLLUP / CUBE / GROUPING SETS headers match on replay);
 * - #17 a RIGHT USING join names its key column after the LEFT input (the binder's
 *   BindExpression#bindUsingJoin uses colName=leftSlot.getName()).
 */
public class SPMRound65PlannerTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** The output labels of the first Project / Repeat under the root. */
    private static List<String> rootOutputLabels(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject) && !(node instanceof LogicalRepeat)) {
            node = node.child(0);
        }
        List<String> labels = new ArrayList<>();
        if (node instanceof LogicalProject) {
            for (NamedExpression expr : ((LogicalProject<?>) node).getProjects()) {
                labels.add(labelOf(expr));
            }
        } else {
            for (NamedExpression expr : ((LogicalRepeat<?>) node).getOutputExpressions()) {
                labels.add(labelOf(expr));
            }
        }
        return labels;
    }

    /**
     * The item's label: an unbound expression (k + 1, sum(v)) has no name() yet, and the
     * alignment's RENAMED items render as "expr AS header" - the caller-visible header
     * is the part after the last " AS ".
     */
    private static String labelOf(NamedExpression expr) {
        String rendered;
        try {
            rendered = expr.getName();
        } catch (org.apache.doris.nereids.exceptions.UnboundException unbound) {
            rendered = expr.toSql();
        }
        int cut = rendered.lastIndexOf(" AS ");
        return cut < 0 ? rendered : rendered.substring(cut + 4);
    }

    // ==================== #2: stars following an open star ====================

    /**
     * The caller's `SELECT u.*, s.* FROM u CROSS JOIN (SELECT k+2, v+2 FROM t) s` has an
     * OPEN first star (u's width is unknown without the catalog) and a following star
     * that expands to TWO outputs. The fallback used to count `s.*` as one output, read
     * a null label for the second frozen position and left the captured headers on the
     * substituted values.
     */
    @Test
    public void testFollowingStarExpansionAfterAnOpenStar() {
        LogicalPlan userPlan = parse("SELECT u.*, s.* FROM u CROSS JOIN (SELECT k + 2, v + 2 FROM t) s");
        LogicalPlan frozenReplay = parse("SELECT u.k, k + 1, v + 1 FROM u CROSS JOIN (SELECT k + 1, v + 1 FROM t) s");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("u.k", "k + 2", "v + 2"), rootOutputLabels(aligned),
                "the following star must be expanded to its full width before the suffix is"
                        + " positioned: both substituted values must carry the caller's headers");
    }

    // ==================== #7: SET_VAR order across CTE blocks ====================

    /**
     * The definitions of a WITH node are analyzed BEFORE the query consuming them, so a
     * fresh parse of the caller ends in the CONSUMER's settings. The hint collector used
     * to visit the main child first and the CTE definitions last, so carrying the hints
     * onto the replay applied them in the inverted order (replay ended in +08:00).
     */
    @Test
    public void testSetVarHintsCarryInAnalysisOrder() {
        LogicalPlan caller = parse("WITH c AS (SELECT /*+ SET_VAR(time_zone='+08:00') */ k FROM t)\n"
                + "SELECT /*+ SET_VAR(time_zone='+00:00') */ from_unixtime(k) FROM c");
        LogicalPlan replay = parse("SELECT 1");

        LogicalPlan carried = SPMPlanTreeSupport.carrySetVarHints(replay, caller);
        Assertions.assertTrue(carried instanceof LogicalSelectHint,
                "the caller's SET_VAR payloads must be re-attached to the replay");
        List<String> zones = new ArrayList<>();
        for (SelectHint hint : ((LogicalSelectHint<?>) carried).getHints()) {
            if (hint instanceof SelectHintSetVar) {
                zones.add(((SelectHintSetVar) hint).getParameters().get("time_zone").orElse(null));
            }
        }
        Assertions.assertEquals(List.of("+08:00", "+00:00"), zones,
                "the CTE definition's settings are applied first, the consuming query's last"
                        + " (the replay must end in the caller's effective settings)");
    }

    // ==================== #14: the last join input's known label prefix ====================

    /**
     * The right subquery of the caller expands to StarLabels([v + 2], open): the JOIN
     * branch used to drop the whole child labels when the flatten ended in an open tail,
     * so the alignment treated the second output as unknown and retained the captured
     * v + 1 header.
     */
    @Test
    public void testLastJoinInputKeepsItsKnownLabelPrefix() {
        LogicalPlan userPlan = parse("SELECT * FROM (SELECT k AS x FROM a) l"
                + " CROSS JOIN (SELECT v + 2, u.* FROM t CROSS JOIN u) s");
        LogicalPlan frozenReplay = parse("SELECT x, v + 1, w FROM (SELECT k AS x FROM a) l"
                + " CROSS JOIN (SELECT v + 1, u.w FROM t CROSS JOIN u) s");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("x", "v + 2", "w"), rootOutputLabels(aligned),
                "the known [v + 2] prefix of the open right input must survive the flatten:"
                        + " the frozen v + 1 header must be renamed to the caller's label");
    }

    // ==================== #15: inherited CTEs expand in their defining scope ====================

    /**
     * The nested block re-declares `c` and references the OUTER `d`: AnalyzeCTE binds `d`
     * to the outer `c`, so the caller's header is `a`. The star expansion used to expand
     * `d` against the NESTED scope (where `c` is shadowed), derive `b` and rename the
     * correctly frozen `a` header.
     */
    @Test
    public void testInheritedCteExpandsInItsDefiningScope() {
        LogicalPlan userPlan = parse(
                "WITH c AS (SELECT k AS a FROM t), d AS (SELECT * FROM c)\n"
                        + "SELECT * FROM (WITH c AS (SELECT v AS b FROM u) SELECT * FROM d) s");
        LogicalPlan frozenReplay = parse(
                "WITH c AS (SELECT k AS a FROM t), d AS (SELECT * FROM c)\n"
                        + "SELECT a FROM (WITH c AS (SELECT v AS b FROM u) SELECT a FROM d) s");

        Assertions.assertEquals(List.of("a"), rootOutputLabels(frozenReplay),
                "precondition: the frozen tree exposes the outer binding's header");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("a"), rootOutputLabels(aligned),
                "the inherited d must expand in ITS defining scope: the frozen header must"
                        + " not be renamed to the nested shadowing definition's label");
    }

    // ==================== #16: GROUPING SETS output list alignment ====================

    /**
     * The caller and the replay are both LogicalRepeat (ROLLUP) aggregates with
     * PARAMETERIZED output expressions; the heterogeneous-output allowance used to cover
     * only LogicalAggregate, so the alignment returned the captured projection unchanged
     * and the replay computed sum(v + 2) under the sum(v + 1) header.
     */
    @Test
    public void testGroupingSetsOutputListAlignment() {
        LogicalPlan userPlan = parse("SELECT sum(v + 2) FROM t GROUP BY ROLLUP(k)");
        LogicalPlan frozenReplay = parse("SELECT sum(v + 1) FROM t GROUP BY ROLLUP(k)");

        List<String> userLabels = rootOutputLabels(userPlan);
        Assertions.assertNotEquals(userLabels, rootOutputLabels(frozenReplay),
                "precondition: the frozen header differs from the caller's label");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(userLabels, rootOutputLabels(aligned),
                "a LogicalRepeat output list must be aligned like an aggregate's: the replay"
                        + " must expose the caller's labels");
    }

    // ==================== #17: RIGHT USING key names come from the left input ====================

    /**
     * BindExpression#bindUsingJoin always names the merged column after the LEFT slot
     * (`colName = leftSlot.getName()`), including RIGHT joins that take the VALUE from
     * the right input. The star expansion used to derive `k` from the right source and
     * renamed the correctly frozen `K` header even for the identical query.
     */
    @Test
    public void testRightUsingJoinKeyNameComesFromTheLeftInput() {
        LogicalPlan userPlan = parse("SELECT * FROM (SELECT k AS K, x FROM t) l"
                + " RIGHT JOIN (SELECT k AS k, y FROM u) r USING(k)");
        LogicalPlan frozenReplay = parse("SELECT K, x, y FROM (SELECT k AS K, x FROM t) l"
                + " RIGHT JOIN (SELECT k AS k, y FROM u) r USING(k)");

        Assertions.assertEquals(List.of("K", "x", "y"), rootOutputLabels(frozenReplay),
                "precondition: the frozen tree exposes the binder's left-derived key name");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("K", "x", "y"), rootOutputLabels(aligned),
                "a RIGHT USING join's key column is named after the LEFT input (the binder's"
                        + " choice): the frozen K header must survive the identical replay");
    }
}
