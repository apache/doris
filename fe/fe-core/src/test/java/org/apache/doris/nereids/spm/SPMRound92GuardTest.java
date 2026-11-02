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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
