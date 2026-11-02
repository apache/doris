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
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-68 review fixes with a planner-side surface:
 *
 * - #1 a captured NULL in a BOOLEAN context keeps its placeholder: the payload is typed
 *      BOOLEAN, so the analyzer's NullType coercion (ExpressionAnalyzer.visitOr / visitAnd /
 *      visitNot replaces a NullType operand with a typed null literal, i.e. DELETES the
 *      marker) no longer applies. The frozen SQL used to lose that id, so a matching
 *      variant supplying TRUE had no slot to substitute and the replay returned NULL;
 * - #3 a QUALIFIED star resolves by ALL the qualifier components the caller wrote: with
 *      `... FROM (SELECT k AS wrong FROM u) t CROSS JOIN db.t`, `db.t.*` must address the
 *      base table (labels [v]), not the derived alias that happens to be called `t`.
 */
public class SPMRound68PlannerTest {

    // ==================== #1: NULL placeholder in a boolean context ====================

    /**
     * `k > 0 OR NULL` parameterized to Or(k > ph1, ph2[NULL:NullType]); the analyzer coerced
     * the NullType operand and the marker disappeared from the frozen SQL. The placeholder
     * must now report BOOLEAN, which is what keeps the analyzer from rewriting it away.
     */
    @Test
    public void testNullPlaceholderInBooleanContextKeepsItsIdentity() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        Expression bind = new Or(new GreaterThan(k, new TinyIntLiteral((byte) 0)),
                NullLiteral.INSTANCE);

        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        Expression parameterized = builder.parameterizeExpressions(List.of(bind)).get(0);

        Assertions.assertTrue(parameterized instanceof Or,
                "the OR structure must be kept: " + parameterized);
        Expression right = ((Or) parameterized).child(1);
        Assertions.assertTrue(right instanceof SpmConstVar,
                "the captured NULL must stay a MARKER: " + right);
        SpmConstVar marker = (SpmConstVar) right;
        Assertions.assertFalse(marker.getDataType() instanceof NullType,
                "a NullType operand is exactly what the analyzer replaces (losing the"
                        + " marker); the payload must be BOOLEAN");
        Assertions.assertTrue(marker.getDataType() instanceof BooleanType,
                "the boolean payload keeps the marker where a boolean operand is expected: "
                        + marker.getDataType());
        Assertions.assertEquals("_spm_const_var(" + marker.getId() + ", NULL)", marker.toSql(),
                "the frozen text must keep the placeholder call (its id is what a matching"
                        + " TRUE / FALSE variant is substituted into)");
        Assertions.assertEquals(1, builder.getPlaceholderExprs().stream()
                .filter(record -> record.getPlaceholderExpr() == marker).count(),
                "exactly one placeholder record backs the marker");
    }

    // ==================== #3: the full qualifier selects the star input ====================

    /**
     * The parser keeps `db.t.*` as a two-part qualifier while the derived table is visible
     * under the BARE alias `t`. Matching by the last part alone accepted the derived alias
     * first (it is the LEFT join side) and derived its label `wrong`; the alignment then
     * renamed the correctly frozen `v` to `wrong`, even for identical SQL.
     */
    @Test
    public void testQualifiedStarSelectsTheQualifiedTable() {
        LogicalPlan userPlan = parse("SELECT db.t.* FROM (SELECT k AS wrong FROM u) t"
                + " CROSS JOIN db.t");
        LogicalPlan frozenReplay = parse("SELECT db.t.v AS v FROM (SELECT k AS wrong FROM u) t"
                + " CROSS JOIN db.t");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("v"), rootOutputLabels(aligned),
                "db.t.* must address db.t (label v), not the derived alias t (label wrong)");
    }

    /** The BARE alias still addresses the derived table (the suffix rule keeps it working). */
    @Test
    public void testBareAliasStillSelectsTheDerivedTable() {
        LogicalPlan userPlan = parse("SELECT t.* FROM (SELECT k AS wrong FROM u) t"
                + " CROSS JOIN db.t");
        LogicalPlan frozenReplay = parse("SELECT t.wrong AS wrong FROM (SELECT k AS wrong FROM u) t"
                + " CROSS JOIN db.t");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("wrong"), rootOutputLabels(aligned),
                "t.* addresses the derived alias t (label wrong)");
    }

    // ==================== helpers ====================

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    /** The output labels of the first Project / Repeat under the root (unbound trees). */
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
     * alignment's RENAMED items render as "expr AS header" - the caller-visible header is
     * the part after the last " AS ".
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
}
