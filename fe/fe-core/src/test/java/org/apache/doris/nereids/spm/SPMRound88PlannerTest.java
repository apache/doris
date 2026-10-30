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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-88 review fixes.
 *
 * <p>#1: {@code isTypePreservingArrayCall} only recognized {@code array_sort} /
 * {@code array_distinct}. Every member of the {@code retArgType(0)} ARRAY family forwards its
 * input's element type to the enclosing call, so an all-NULL container under any of them keeps a
 * NullType operand: {@code k > 0 OR element_at(array_reverse_sort(array(NULL)), 1)} lost its
 * placeholder, the matching {@code array(TRUE)} query extracted TRUE and the replay still
 * returned NULL for {@code k <= 0}. The classifier now covers the whole family (the resolved
 * signature first, the name list for the unbound parse tree) plus the {@code map_filter} MAP
 * counterpart.
 */
public class SPMRound88PlannerTest {

    /**
     * Every {@code retArgType(0)} wrapper leaves NO NullType marker in the operand, so the Boolean
     * consumer cannot replace the call together with its ids and the {@code array(TRUE)} variant
     * keeps something to substitute.
     */
    @Test
    public void testEveryTypePreservingArrayWrapperKeepsTheMarker() {
        // {function name, its array argument list}
        String[][] cases = {
                {"array_sort", "array(NULL)"},
                {"array_distinct", "array(NULL)"},
                {"array_reverse_sort", "array(NULL)"},
                {"reverse", "array(NULL)"},
                {"array_slice", "array(NULL), 1, 2"},
                {"array_union", "array(NULL), array(NULL)"},
                {"array_intersect", "array(NULL), array(NULL)"},
                {"array_except", "array(NULL), array(NULL)"},
                {"array_compact", "array(NULL)"},
                {"array_shuffle", "array(NULL)"},
                {"array_popback", "array(NULL)"},
                {"array_pushback", "array(NULL), NULL"},
                {"trim_array", "array(NULL), 1"},
        };
        for (String[] testCase : cases) {
            String sql = "SELECT k, k > 0 OR element_at(" + testCase[0] + "(" + testCase[1]
                    + "), 1) AS c FROM t";
            // the container's marker AND the element_at index keep their ids
            Assertions.assertTrue(markers(sql).size() >= 2,
                    testCase[0] + " must keep its markers: " + describe(sql));
            Assertions.assertEquals(0, nullTypedMarkers(sql).size(),
                    testCase[0] + " must leave no NullType marker in a Boolean operand of a"
                            + " retArgType(0) chain: " + describe(sql));
        }
    }

    /**
     * The BRACKET spelling of the same chain keeps the payload typed ARRAY&lt;BOOLEAN&gt;: the
     * literal IS the container, so its one marker is retyped as a whole (the repair the
     * {@code array(...)} CONSTRUCTOR spelling needed at its ELEMENTS).
     */
    @Test
    public void testBracketContainerUnderAWrapperKeepsTheArrayPayload() {
        String sql = "SELECT k, k > 0 OR element_at(array_reverse_sort([NULL]), 1) AS c FROM t";
        int arrayMarkers = 0;
        for (SpmConstVar marker : markers(sql)) {
            if (marker.getDataType() instanceof ArrayType
                    && ((ArrayType) marker.getDataType()).getItemType() instanceof BooleanType) {
                arrayMarkers++;
            }
        }
        Assertions.assertEquals(1, arrayMarkers,
                "the bracket container marker must be ARRAY<BOOLEAN>: " + describe(sql));
    }

    /** The map counterpart of the family: {@code map_filter} forwards its MAP's type. */
    @Test
    public void testTypePreservingMapWrapperKeepsTheMarker() {
        String sql = "SELECT k, k > 0 OR element_at(map_filter({'a': NULL},"
                + " (m_k, m_v) -> TRUE), 'a') AS c FROM t";
        int mapMarkers = 0;
        for (SpmConstVar marker : markers(sql)) {
            if (marker.getDataType() instanceof MapType) {
                mapMarkers++;
            }
        }
        Assertions.assertEquals(1, mapMarkers,
                "map_filter's container keeps a MAP marker: " + describe(sql));
        Assertions.assertEquals(0, nullTypedMarkers(sql).size(),
                "map_filter's container must leave no NullType marker: " + describe(sql));
    }

    /**
     * The generalization must not swallow unrelated calls: {@code size(...)} declares no
     * {@code retArgType(0)} and takes no container position, so the all-NULL constructor keeps its
     * own typing - the family is recognized by the PROPERTY, not by "some array function".
     */
    @Test
    public void testUnrelatedArrayCallsAreNotRetyped() {
        String sql = "SELECT k, k > 0 OR size(array(NULL)) > 1 AS c FROM t";
        for (SpmConstVar marker : markers(sql)) {
            Assertions.assertFalse(marker.getDataType() instanceof BooleanType
                            || marker.getDataType() instanceof ArrayType,
                    "size() is not type-preserving, so `array(NULL)` keeps its own typing: "
                            + describe(sql));
        }
    }

    /** Every {@code SpmConstVar} marker the parameterization produced for one query. */
    private static List<SpmConstVar> markers(String sql) {
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(parse(sql), builder);
        List<SpmConstVar> markers = new ArrayList<>();
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar) {
                markers.add((SpmConstVar) marker.getPlaceholderExpr());
            }
        }
        return markers;
    }

    /** The markers whose payload still produces NullType (the defect this round removes). */
    private static List<SpmConstVar> nullTypedMarkers(String sql) {
        List<SpmConstVar> nullTyped = new ArrayList<>();
        for (SpmConstVar marker : markers(sql)) {
            if (marker.getDataType().isNullType()) {
                nullTyped.add(marker);
                continue;
            }
            if (marker.getDataType() instanceof ArrayType
                    && ((ArrayType) marker.getDataType()).getItemType().isNullType()) {
                nullTyped.add(marker);
                continue;
            }
            if (marker.getDataType() instanceof MapType
                    && ((MapType) marker.getDataType()).getValueType().isNullType()) {
                nullTyped.add(marker);
            }
        }
        return nullTyped;
    }

    private static String describe(String sql) {
        return "markers=" + markers(sql);
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
