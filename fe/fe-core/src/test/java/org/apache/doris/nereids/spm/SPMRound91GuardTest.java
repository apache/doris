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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-91 review fixes.
 *
 * <ul>
 *   <li>#1 {@code isAllNullMap} did not follow {@code map_filter} - a type-preserving MAP wrapper -
 *       so an all-NULL-valued MAP under it stayed a NullType producer and the Boolean consumer
 *       erased the whole {@code element_at} operand together with its markers.</li>
 *   <li>#4 {@code utc_time()} / {@code utc_date()} were missing from the replay-time context
 *       guard, so the optimizer folded the CREATE clock into the frozen planSql.</li>
 *   <li>#5 a session-time-zone dependent CAST CHAIN over literals slipped past the name-based
 *       guard, so an inline VALUES cell / generator argument / star REPLACE payload froze the
 *       creator's zone value.</li>
 * </ul>
 */
public class SPMRound91GuardTest {

    /**
     * The MAP wrapper path: every NULL in the {@code map(...)} VALUE positions keeps a marker, and
     * no marker keeps a NullType payload, so the Boolean consumer cannot drop the operand.
     */
    @Test
    public void testMapFilterWrapperKeepsItsValueMarkers() {
        String sql = "SELECT k, k > 0 OR element_at(map_filter(map('x', NULL),"
                + " (m_k, m_v) -> TRUE), 'x') AS c FROM t";
        List<SpmConstVar> markers = markers(sql);
        Assertions.assertFalse(markers.isEmpty(), "the capture must keep markers: " + sql);
        for (SpmConstVar marker : markers) {
            Assertions.assertFalse(marker.getDataType().isNullType(),
                    "no marker may keep a NullType payload (the operand would be erased): "
                            + marker);
        }
    }

    /** {@code utc_time()} and {@code utc_date()} read the statement clock like {@code now()}. */
    @Test
    public void testUtcClockFunctionsAreReplayContextExpressions() {
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                firstProjection("SELECT utc_time() AS t")),
                "utc_time() is the statement clock evaluated in the UTC zone");
        Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                firstProjection("SELECT utc_date() AS d")),
                "utc_date() is the statement clock evaluated in the UTC zone");
        // control: a pure function of its argument stays freezable
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextExpression(
                firstProjection("SELECT unix_timestamp('2020-01-01 00:00:00') AS u")),
                "unix_timestamp(arg) is a pure function of the argument");
    }

    /**
     * The CAST-only chain: a TIMESTAMPTZ / zone-less transition over literals is folded in the
     * CREATOR's session zone, so an inline VALUES cell carrying it must not be frozen.
     */
    @Test
    public void testZoneSensitiveCastChainInValuesCellIsDetected() {
        String sql = "SELECT * REPLACE (CAST(CAST(CAST('2020-01-01 00:00:00+00:00'"
                + " AS TIMESTAMPTZ) AS DATETIME(6)) AS STRING) AS a) FROM t";
        Assertions.assertTrue(SPMPlanTreeSupport.containsFoldedSessionSensitivePayload(parse(sql)),
                "a TIMESTAMPTZ cast chain over literals freezes the creator's session zone");
    }

    /** Control: a CAST chain that stays inside zone-less types remains freezable. */
    @Test
    public void testZoneLessCastChainStaysFreezable() {
        String sql = "SELECT * REPLACE (CAST(CAST(1 AS BIGINT) AS STRING) AS a) FROM t";
        Assertions.assertFalse(
                SPMPlanTreeSupport.containsFoldedSessionSensitivePayload(parse(sql)),
                "a zone-less cast chain is session independent");
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

    private static Expression firstProjection(String select) {
        Expression found = firstNodeExpression(parse(select));
        if (found == null) {
            throw new AssertionError("no expression in " + select);
        }
        return found;
    }

    private static Expression firstNodeExpression(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            return expression;
        }
        for (Plan child : plan.children()) {
            Expression found = firstNodeExpression(child);
            if (found != null) {
                return found;
            }
        }
        for (Plan extra : plan.extraPlans()) {
            Expression found = firstNodeExpression(extra);
            if (found != null) {
                return found;
            }
        }
        return null;
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
