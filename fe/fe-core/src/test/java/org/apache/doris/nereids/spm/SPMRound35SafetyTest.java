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
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-35 review fixes:
 *
 * - #3 (SPMPlanTreeSupport#rowLimitsWithin): the retained-LIMIT check compared the
 *   multiset of (limit, offset) VALUES only, so a manual plan capping a different input
 *   than the caller's own cap of the same value was accepted - and the positional merge
 *   left the cap where it was, returning fewer rows than the caller's own plan.
 * - #4 (SPMPlanTreeSupport#collectRowLimits): the walker visited children() only, so a
 *   cap inside a CTE body (LogicalCTE.extraPlans()) or an expression-owned subquery plan
 *   was invisible - the caller raising only the OUTER limit kept the frozen body cap.
 * - #5 (SPMPlanTreeSupport#containsReplayContextExpression): the BARE clock keywords
 *   (CURRENT_DATE / CURRENT_TIME / CURRENT_TIMESTAMP / LOCALTIME / LOCALTIMESTAMP) are
 *   parsed into the BOUND CurrentDate / CurrentTime / Now leaves, not UnboundFunction
 *   calls, so the name-based guard missed them and a global baseline could freeze the
 *   CREATE date for every later match.
 */
public class SPMRound35SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    // ==================== #3: the input a retained cap truncates ====================

    /**
     * The same (limit, offset) pair on a DIFFERENT input does not justify the replayed
     * cap: the caller's own plan caps t1, the manual plan caps t2, and the positional
     * merge leaves the cap on t2 - the variant would return one row where the caller's
     * own plan returns two (one matching t1 row x two matching t2 rows).
     */
    @Test
    public void testInnerCapOnAnotherInputIsNotJustifiedByTheCaller() {
        LogicalPlan replayed = parse("SELECT t1.k FROM t1 JOIN"
                + " (SELECT g FROM t2 ORDER BY g LIMIT 1) b ON t1.k = b.g"
                + " ORDER BY t1.k LIMIT 2");
        LogicalPlan user = parse("SELECT t2.g FROM"
                + " (SELECT k FROM t1 ORDER BY k LIMIT 1) a JOIN t2 ON a.k = t2.g"
                + " ORDER BY t2.g LIMIT 2");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(replayed, user),
                "the replayed cap truncates t2 while the caller's own cap sits on t1");
        // the same cap on the SAME input is the caller's own contract
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(user, user),
                "identical trees justify every cap");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(parse(
                "SELECT a.k FROM (SELECT k FROM t1 ORDER BY k LIMIT 1) a JOIN t2"
                        + " ON a.k = t2.g ORDER BY a.k LIMIT 2"), user),
                "a replayed cap on the SAME input the caller caps is justified");
    }

    // ==================== #4: caps outside children() ====================

    /**
     * A cap inside a CTE body lives in {@code LogicalCTE.extraPlans()}: the caller
     * raising only the outer limit must not keep the frozen body cap, which would return
     * one row instead of two.
     */
    @Test
    public void testCteBodyCapIsSeenByTheRetainedLimitCheck() {
        LogicalPlan replayed = parse("WITH c AS (SELECT g FROM t2 ORDER BY g LIMIT 1)"
                + " SELECT g FROM c ORDER BY g LIMIT 2");
        LogicalPlan user = parse("WITH c AS (SELECT g FROM t2)"
                + " SELECT g FROM c ORDER BY g LIMIT 2");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(replayed, user),
                "the CTE-body cap must be visible to the retained-limit check");
        LogicalPlan userWithOwnCap = parse("WITH c AS (SELECT g FROM t2 ORDER BY g LIMIT 1)"
                + " SELECT g FROM c ORDER BY g LIMIT 2");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(replayed, userWithOwnCap),
                "a CTE-body cap the caller itself wrote is justified");
    }

    /** An expression-owned subquery plan (IN / EXISTS / scalar) is walked as well. */
    @Test
    public void testExpressionSubqueryCapIsSeenByTheRetainedLimitCheck() {
        LogicalPlan replayed = parse("SELECT k FROM t1 WHERE k IN"
                + " (SELECT g FROM t2 ORDER BY g LIMIT 1) ORDER BY k LIMIT 2");
        LogicalPlan user = parse("SELECT k FROM t1 WHERE k IN (SELECT g FROM t2)"
                + " ORDER BY k LIMIT 2");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(replayed, user),
                "a cap inside an IN subquery must be visible to the retained-limit check");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(replayed, parse(
                "SELECT k AS k FROM t1 WHERE k IN (SELECT g FROM t2 ORDER BY g LIMIT 1)"
                        + " ORDER BY k LIMIT 2")),
                "the caller's own subquery cap justifies the replayed one");
    }

    // ==================== #5: bare clock keywords ====================

    /**
     * The bare forms must be rejected at freeze time exactly like their parenthesized
     * counterparts: the parser builds bound leaves ({@code CurrentDate} /
     * {@code CurrentTime} / {@code Now}), so a baseline for
     * {@code SELECT CURRENT_DATE AS d FROM t} would otherwise serve the CREATE date to
     * every later matching query.
     */
    @Test
    public void testBareClockKeywordsAreReplayTimeContext() {
        String[] bareForms = {"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
                "LOCALTIME", "LOCALTIMESTAMP"};
        for (String form : bareForms) {
            Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                    parse("SELECT " + form + " AS d FROM t")),
                    "the bare " + form + " freezes the statement clock and must be rejected");
        }
        // the parenthesized forms stay covered by the name-based check
        for (String form : new String[] {"now()", "current_timestamp()", "localtime()"}) {
            Assertions.assertTrue(SPMPlanTreeSupport.containsReplayContextExpression(
                    parse("SELECT " + form + " AS d FROM t")),
                    form + " must stay rejected");
        }
        Assertions.assertFalse(SPMPlanTreeSupport.containsReplayContextExpression(
                parse("SELECT d AS d FROM t")),
                "a plain column is freezable");
    }
}
