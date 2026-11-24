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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.spm.placeholder.PlaceholderSnapshot;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.spm.placeholder.TestSPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayWithConstant;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-92 review fixes.
 *
 * <ul>
 *   <li>#1 {@code array_repeat(x, n)} declares {@code ARRAY<FollowToAnyDataType(0)>}, i.e. its
 *       ELEMENT type comes from its first argument, so {@code array_repeat(NULL, 1)} IS an
 *       all-NULL array producer. It was missing from {@code isAllNullArray}, the
 *       {@code element_at} operand therefore stayed a NullType producer, and the Boolean
 *       consumer replaced it with a typed NULL - erasing its markers. The value variant
 *       ({@code array_repeat(TRUE, 1)}) still matched the value-free digest, but the frozen
 *       replay had no marker left to substitute and returned NULL.</li>
 * </ul>
 */
public class SPMRound92GuardTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "element_at(array_with_constant(1, NULL), 1)",
            "element_at(array_flatten(array(array(NULL))), 1)",
            "element_at(array_flatten([[NULL]]), 1)",
            "element_at(deduplicate_map(map('x', NULL)), 'x')",
            "element_at(array_agg(NULL), 1)",
            "element_at(collect_list(NULL), 1)",
            "element_at(collect_set(NULL), 1)",
            "max_by(NULL, k)",
            "min_by(NULL, k)",
            "element_at(map_agg('x', NULL), 'x')",
            "element_at(map_agg_v2('x', NULL), 'x')"
    })
    public void testBooleanOperandKeepsTheMarkerType(String operand) {
        String query = "SELECT k, k > 0 OR %s AS c FROM t";
        List<SpmConstVar> nullMarkers = markers(String.format(query, operand));
        List<SpmConstVar> valueMarkers = markers(String.format(query, operand.replace("NULL", "TRUE")));
        Assertions.assertTrue(nullMarkers.size() > 1);
        Assertions.assertEquals(valueMarkers.stream().map(SpmConstVar::getDataType).toList(),
                nullMarkers.stream().map(SpmConstVar::getDataType).toList());

        Expression unbound = new NereidsParser().parseExpression("k > 0 OR " + operand);
        TestSPMPlaceholderBuilder builder = new TestSPMPlaceholderBuilder();
        Expression parameterized = builder.parameterizeExpressions(List.of(unbound)).get(0);
        Scope scope = new Scope(List.of(new SlotReference("k", IntegerType.INSTANCE)));
        Expression analyzed = new ExpressionAnalyzer(null, scope, null, true, true).analyze(parameterized);
        Assertions.assertEquals(nullMarkers.size(), analyzed.collect(expr -> expr instanceof SpmConstVar).size());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "ASSERT_ROWS (SELECT k FROM %s WHERE k > %d)",
            "(SELECT k FROM %s WHERE k > %d /*+PREAGGOPEN*/)"
    })
    public void testWrappedInputsContributeToSpmDigestInJoinOrder(String wrappedQuery) {
        String query = "SELECT a.k-b.k AS d FROM %s a CROSS JOIN %s b";
        String left = String.format(wrappedQuery, "t", 0);
        String right = String.format(wrappedQuery, "u", 0);
        String leftVariant = String.format(wrappedQuery, "t", 1);
        NereidsParser parser = new NereidsParser();
        Plan original = parser.parseSingle(String.format(query, left, right));
        Plan swapped = parser.parseSingle(String.format(query, right, left));
        Plan literalVariant = parser.parseSingle(String.format(query, leftVariant, right));

        Assertions.assertNotEquals(original.toSpmDigest(), swapped.toSpmDigest());
        Assertions.assertEquals(original.toSpmDigest(), literalVariant.toSpmDigest());
        Assertions.assertEquals(original.toDigest(), swapped.toDigest());
    }

    @ParameterizedTest
    @ValueSource(strings = {"struct(NULL, 7)", "named_struct('col1', NULL, 'col2', 7)"})
    public void testProjectedStructKeepsTheNullFieldMarker(String constructor) {
        String query = "SELECT k, k > 0 OR s.col1 AS c FROM (SELECT k, " + constructor + " AS s FROM t) x";
        List<SpmConstVar> nullMarkers = markers(query);
        List<SpmConstVar> valueMarkers = markers(query.replace("NULL", "TRUE"));
        Assertions.assertEquals(3, nullMarkers.size(), "threshold and two struct fields");
        Assertions.assertEquals(valueMarkers.stream().map(SpmConstVar::getDataType).toList(),
                nullMarkers.stream().map(SpmConstVar::getDataType).toList());
        Assertions.assertTrue(nullMarkers.stream().anyMatch(marker ->
                marker.getValue().isNullLiteral() && marker.getDataType().isBooleanType()));
    }

    @Test
    public void testBoundArrayWithConstantUsesTheElementArgument() {
        IntegerLiteral one = new IntegerLiteral(1);
        Expression operand = new ElementAt(new ArrayWithConstant(one, new NullLiteral()), one);
        Expression predicate = new Or(new EqualTo(one, new IntegerLiteral(2)), operand);
        TestSPMPlaceholderBuilder builder = new TestSPMPlaceholderBuilder();
        builder.parameterizeExpressions(List.of(predicate));
        List<SpmConstVar> nullMarkers = builder.getPlaceholderExprs().stream()
                .map(entry -> (SpmConstVar) entry.placeholderExpr())
                .filter(marker -> marker.getValue().isNullLiteral())
                .toList();
        Assertions.assertEquals(1, nullMarkers.size());
        Assertions.assertTrue(nullMarkers.get(0).getDataType().isBooleanType());
    }

    /**
     * The NULL element variant keeps a marker for the element, and the marker is RETYPED: an
     * all-NULL element argument makes the whole call {@code ARRAY<NULL_TYPE>}, so a NullType
     * payload would be erased together with the operand.
     */
    @Test
    public void testArrayRepeatNullElementKeepsItsMarker() {
        String sql = booleanContext("array_repeat(NULL, 1)");
        List<SpmConstVar> markers = markers(sql);
        Assertions.assertFalse(markers.isEmpty(), "the capture must keep markers: " + sql);
        for (SpmConstVar marker : markers) {
            Assertions.assertFalse(marker.getDataType().isNullType(),
                    "no marker may keep a NullType payload (the operand would be erased): "
                            + marker);
        }
    }

    /**
     * The NULL variant and the VALUE variant must produce the SAME marker skeleton: their frozen
     * parameterized SQL is what makes a value-free digest match, so a marker missing on one side
     * is exactly the replay that returns NULL.
     */
    @Test
    public void testArrayRepeatNullAndValueVariantsShareTheMarkerSkeleton() {
        List<SpmConstVar> nullMarkers = markers(booleanContext("array_repeat(NULL, 1)"));
        List<SpmConstVar> valueMarkers = markers(booleanContext("array_repeat(TRUE, 1)"));
        Assertions.assertFalse(nullMarkers.isEmpty(), "the NULL variant must keep markers");
        Assertions.assertEquals(valueMarkers.size(), nullMarkers.size(),
                "array_repeat(NULL, 1) and array_repeat(TRUE, 1) must freeze the same skeleton: "
                        + nullMarkers + " vs " + valueMarkers);
        for (int i = 0; i < nullMarkers.size(); i++) {
            Assertions.assertEquals(valueMarkers.get(i).getDataType(), nullMarkers.get(i).getDataType(),
                    "marker " + i + " must keep the type the value variant gives it");
        }
    }

    /** The Boolean-OR carrier of the review's reproduction. */
    private static String booleanContext(String arrayRepeatCall) {
        return "SELECT k, k > 0 OR element_at(" + arrayRepeatCall + ", 1) AS c FROM t";
    }

    /** Every {@code SpmConstVar} marker the parameterization produced for one query. */
    private static List<SpmConstVar> markers(String sql) {
        TestSPMPlaceholderBuilder builder = new TestSPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(parse(sql), builder);
        List<SpmConstVar> markers = new ArrayList<>();
        for (PlaceholderSnapshot marker : builder.getPlaceholderExprs()) {
            if (marker.placeholderExpr() instanceof SpmConstVar) {
                markers.add((SpmConstVar) marker.placeholderExpr());
            }
        }
        return markers;
    }

    private static LogicalPlan parse(String sql) {
        context();
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static ConnectContext context() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("db1");
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
