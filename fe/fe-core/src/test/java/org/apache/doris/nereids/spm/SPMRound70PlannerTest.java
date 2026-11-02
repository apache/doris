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
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-70 review fixes on the planner side:
 *
 * - #3 a DOTTED output alias (`x.y`) is ONE column name: `* EXCEPT(y)` must not remove it
 *      because its last component is `y`, or the frozen header is renamed to another column;
 * - #5 a projected NULL (`SELECT NULL AS n`) is parameterized BOOLEAN, so an enclosing
 *      Boolean binding keeps the slot (and the marker) instead of replacing it with a
 *      constant NULL;
 * - #6 `AVG(DISTINCT k + 1)` keeps its LARGEINT accumulator cast even though a placeholder
 *      sits inside the arithmetic.
 */
public class SPMRound70PlannerTest {

    // ==================== #3: dotted output aliases ====================

    /**
     * `SELECT * EXCEPT(y)` over a relation exposing BOTH `x.y` (a quoted alias) and `y`:
     * matching by the label's last component removed `x.y` as well, and the alignment then
     * renamed the correctly frozen `x.y` header to `z`.
     */
    @Test
    public void testDottedAliasIsNotRemovedByItsLastComponent() {
        LogicalPlan userPlan = parse("SELECT * EXCEPT(y) FROM"
                + " (SELECT k AS `x.y`, v AS y, w AS z FROM t) d CROSS JOIN u");
        LogicalPlan frozenReplay = parse("SELECT d.`x.y`, w FROM"
                + " (SELECT k AS `x.y`, v AS y, w AS z FROM t) d CROSS JOIN u");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        List<String> labels = rootOutputLabels(aligned);
        Assertions.assertEquals("x.y", labels.get(0),
                "the dotted alias is one column name and EXCEPT(y) must not remove it: "
                        + labels);
        Assertions.assertFalse(labels.contains("z") && "z".equals(labels.get(0)),
                "the frozen header must not be renamed to the following column: " + labels);
    }

    /** The unqualified column IS still removed (the fix must not disable EXCEPT). */
    @Test
    public void testPlainExceptStillRemovesTheColumn() {
        LogicalPlan userPlan = parse("SELECT * EXCEPT(y) FROM"
                + " (SELECT k AS `x.y`, v AS y, w AS z FROM t) d");
        LogicalPlan frozenReplay = parse("SELECT d.`x.y`, w FROM"
                + " (SELECT k AS `x.y`, v AS y, w AS z FROM t) d");

        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("x.y", "z"), rootOutputLabels(aligned),
                "EXCEPT(y) removes exactly the column named y");
    }

    // ==================== #5: a projected NULL keeps its marker ====================

    /**
     * `SELECT NULL AS n` used to expose a NullType slot: an enclosing Boolean binding
     * replaced that operand with a typed constant NULL, dropping the slot AND the marker -
     * a later TRUE variant of the derived column then had nothing to substitute.
     */
    @Test
    public void testProjectedNullIsParameterizedBoolean() {
        LogicalPlan plan = parse("SELECT NULL AS n FROM t");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(plan, builder);

        Expression item = firstProjectItem(parameterized);
        Assertions.assertTrue(item instanceof Alias, "expected Alias(NULL), got: " + item);
        Expression value = ((Alias) item).child();
        Assertions.assertTrue(value instanceof SpmConstVar,
                "the projected NULL must stay a MARKER: " + value);
        Assertions.assertFalse(((SpmConstVar) value).getDataType() instanceof NullType,
                "a NullType slot is exactly what Boolean binding replaces");
        Assertions.assertTrue(((SpmConstVar) value).getDataType() instanceof BooleanType,
                "the projected NULL is parameterized BOOLEAN: "
                        + ((SpmConstVar) value).getDataType());
    }

    // ==================== #6: the accumulator widening under arithmetic ====================

    /**
     * `avg(DISTINCT k + 1)` lowers to `sum(DISTINCT CAST(k + ph AS LARGEINT)) / count(...)`.
     * The placeholder inside the arithmetic made the printer skip the widening, so the frozen
     * SQL summed in BIGINT and could overflow on 5e18-scale values. The widening lives INSIDE the
     * aggregate's argument list, which is exactly where the accumulator semantics apply
     * (round-73 #1 restricted the preservation to that frame).
     */
    @Test
    public void testAccumulatorWideningOverPlaceholderArithmeticIsPreserved() {
        SlotReference k = new SlotReference("k", BigIntType.INSTANCE);
        Add arithmetic = new Add(k, SpmConstVar.of(3L, new BigIntLiteral(1L)));
        Cast widening = new Cast(arithmetic, LargeIntType.INSTANCE);
        Sum sum = new Sum(true, false, false, widening);

        String printed = new SPMExprSqlBuilder().print(sum, new SQLRelation());
        Assertions.assertTrue(printed.startsWith("sum(CAST(")
                        && printed.endsWith(" AS LARGEINT))"),
                "the LARGEINT accumulator must survive the freeze: " + printed);
    }

    /** A widening over PLACEHOLDER-ONLY arithmetic still pins the captured literal types. */
    @Test
    public void testWideningOverPlaceholderOnlyArithmeticIsStillStripped() {
        Add arithmetic = new Add(SpmConstVar.of(4L, new BigIntLiteral(1L)),
                SpmConstVar.of(5L, new BigIntLiteral(2L)));
        Cast widening = new Cast(arithmetic, LargeIntType.INSTANCE);
        Sum sum = new Sum(true, false, false, widening);

        String printed = new SPMExprSqlBuilder().print(sum, new SQLRelation());
        Assertions.assertFalse(printed.contains("CAST"),
                "no captured-type pinning may leak into the frozen text: " + printed);
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

    /** The output labels of the first Project / Repeat under the root (unbound trees). */
    private static List<String> rootOutputLabels(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        List<String> labels = new ArrayList<>();
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
