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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-66 review fixes on the replay / alignment side:
 *
 * - #2 a GROUPING SETS caller is a LogicalRepeat (a SIBLING of LogicalAggregate), and the
 *   production frozen replay wraps it in the label-pinning Project-over-Subquery shape:
 *   the grouped-aggregate allowance accepted only LogicalAggregate, so the class mismatch
 *   returned the captured `sum(v + 1)` header on the replay that computed `sum(v + 2)`;
 * - #3 a WITH alias is compared with the ANALYZER's name policy: with
 *   lower_case_table_names=1 the declaration `C` and the reference `c` are the same CTE,
 *   but the scope registered the exact spelling and the lookup requested the exact
 *   spelling, so `SELECT * FROM c` missed its binding and kept the captured header;
 * - #4 the merged USING key's NAME always comes from the LEFT slot
 *   (BindExpression#bindUsingJoin), including RIGHT SEMI / RIGHT ANTI joins whose LEFT
 *   side contributes no VISIBLE non-key column: the unexpanded left side made the label
 *   fall back to the raw spelling and renamed the frozen `K` header to `k`.
 */
public class SPMRound66PlannerTest {

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

    /** The item's label (the caller-visible header is the part after the last " AS "). */
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

    // ==================== #2: GROUPING SETS output against the PRODUCTION shape ====================

    /**
     * The production frozen replay of a grouping-sets query is a label-pinning
     * `Project(c_N AS captured header)` over `Subquery(Repeat)`, while the caller's root
     * IS the LogicalRepeat. The grouped-aggregate allowance must cover LogicalRepeat or
     * the class mismatch returns the captured header before any alignment runs.
     */
    @Test
    public void testGroupingSetsReplayAgainstTheFrozenProjectShape() {
        assertGroupingSetsShape("ROLLUP(k)");
        assertGroupingSetsShape("CUBE(k)");
        assertGroupingSetsShape("GROUPING SETS ((k), ())");
    }

    private static void assertGroupingSetsShape(String grouping) {
        LogicalPlan userPlan = parse("SELECT sum(v + 2) FROM t GROUP BY " + grouping);
        // the decompiled frozen SQL pins the captured header with an OUTER projection
        LogicalPlan frozenReplay = parse("SELECT `sum(v + 1)` FROM (SELECT sum(v + 1) FROM t"
                + " GROUP BY " + grouping + ") s");

        Assertions.assertEquals(List.of("sum(v + 1)"), rootOutputLabels(frozenReplay),
                "precondition: the frozen projection carries the captured header");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("sum(v + 2)"), rootOutputLabels(aligned),
                "a GROUPING SETS replay (" + grouping + ") must expose the caller's label:"
                        + " the frozen Project-versus-Repeat shape takes the grouped-output"
                        + " alignment");
    }

    // ==================== #3: case-insensitive WITH references ====================

    /**
     * With lower_case_table_names=1 the analyzer resolves `SELECT * FROM c` against
     * `WITH C AS (...)`. The scope registered the exact declaration spelling and looked up
     * the exact reference spelling, so the binding was missed altogether: the star fell to
     * an empty open tail and the frozen captured `k + 1` header stayed on the replay that
     * computed `k + 2`.
     */
    @Test
    public void testCaseInsensitiveCteReferenceInStarExpansion() {
        int previous = GlobalVariable.lowerCaseTableNames;
        GlobalVariable.lowerCaseTableNames = 1;
        try {
            LogicalPlan userPlan = parse("WITH C AS (SELECT k + 2 FROM t) SELECT * FROM c");
            LogicalPlan frozenReplay = parse(
                    "WITH C AS (SELECT k + 1 FROM t) SELECT k + 1 FROM c");

            Assertions.assertEquals(List.of("k + 1"), rootOutputLabels(frozenReplay),
                    "precondition: the frozen tree exposes the captured header");
            LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
            Assertions.assertEquals(List.of("k + 2"), rootOutputLabels(aligned),
                    "the case-insensitive CTE reference must reach its binding: the frozen"
                            + " header must be realigned to the caller's label");
        } finally {
            GlobalVariable.lowerCaseTableNames = previous;
        }
    }

    // ==================== #4: RIGHT SEMI / ANTI USING key names ====================

    /**
     * A RIGHT SEMI / RIGHT ANTI join exposes ONLY the preserved right side, so the LEFT
     * side contributed no visible non-key column and was never expanded - the merged key's
     * name fell back to the raw `k`. The binder names that column from the LEFT slot
     * whatever side supplies the value (bound with the key as WRITTEN, see
     * ExpressionAnalyzer#bindSingleSlotByName), so the frozen `K` header of `USING(K)` must
     * survive.
     */
    @Test
    public void testRightSemiUsingKeyNameComesFromTheLeftInput() {
        assertRightUsingKey("RIGHT SEMI");
        assertRightUsingKey("RIGHT ANTI");
    }

    private static void assertRightUsingKey(String joinType) {
        LogicalPlan userPlan = parse("SELECT * FROM (SELECT k AS K, x FROM t) l "
                + joinType + " JOIN (SELECT k AS k, y FROM u) r USING(K)");
        LogicalPlan frozenReplay = parse("SELECT K, y FROM (SELECT k AS K, x FROM t) l "
                + joinType + " JOIN (SELECT k AS k, y FROM u) r USING(K)");

        Assertions.assertEquals(List.of("K", "y"), rootOutputLabels(frozenReplay),
                "precondition: the frozen tree exposes the binder's left-derived key name");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("K", "y"), rootOutputLabels(aligned),
                "a " + joinType + " USING join's key column keeps the LEFT input's name"
                        + " (the binder's choice): the frozen K header must survive");
    }
}
