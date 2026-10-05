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
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Round-40 review fixes without their own regression suite:
 *
 * - #1 (SPMPlanTreeSupport#rowLimitsWithin): the retained-LIMIT guard keyed each cap by
 *   (limit, offset, input relations) only. Two occurrences of the SAME table were
 *   therefore interchangeable: a manual plan that moved {@code ORDER BY k LIMIT 1} from
 *   derived alias a to alias b passed the multiset check against a caller whose own cap
 *   sat under a, and the raised-limit variant truncated the WRONG side (t={1,2} yields
 *   (1,1),(2,1) instead of (1,1),(1,2)). Every input now carries its per-name
 *   OCCURRENCE ORDINAL in walk order - the alias NAME cannot be used because the frozen
 *   text of a baseline is the decompiled plan with regenerated aliases (an
 *   identical-text baseline must keep matching) - so a moved cap is skipped.
 */
public class SPMRound40SafetyTest {

    @BeforeEach
    public void setUp() {
        BaselineManager.getInstance().clearForTest();
    }

    @AfterEach
    public void tearDown() {
        BaselineManager.getInstance().clearForTest();
        ConnectContext.remove();
    }

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static void installConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext(ctx, new OriginStatement("SELECT 1", 0)));
    }

    /**
     * The reviewer's case: bind caps the derived table aliased a; the manual plan moved
     * the same {@code ORDER BY k LIMIT 1} to the derived table aliased b, and both read
     * the same table t - both inner caps used to key as {@code 1:0:t}, so a caller
     * raising only the outer LIMIT was accepted and the replay truncated b instead of a.
     * The occurrence-ordinal keys must reject the pairing (the caller's own query then
     * plans normally, which is always safe).
     */
    @Test
    public void testCapMovedBetweenAliasesOfTheSameTableIsRejected() {
        installConnectContext();
        LogicalPlan caller = parse("SELECT a.k AS ak, b.k AS bk"
                + " FROM (SELECT k FROM t ORDER BY k LIMIT 1) a"
                + " CROSS JOIN (SELECT k FROM t) b ORDER BY ak, bk LIMIT 2");
        LogicalPlan manual = parse("SELECT a.k AS ak, b.k AS bk"
                + " FROM (SELECT k FROM t) a"
                + " CROSS JOIN (SELECT k FROM t ORDER BY k LIMIT 1) b ORDER BY ak, bk LIMIT 2");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(manual, caller),
                "a cap on ANOTHER occurrence of the same table is not the caller's cap");
    }

    /** Control: the identical placement (same occurrence) stays accepted. */
    @Test
    public void testTheSameAliasPlacementStaysAccepted() {
        installConnectContext();
        LogicalPlan caller = parse("SELECT a.k AS ak, b.k AS bk"
                + " FROM (SELECT k FROM t ORDER BY k LIMIT 1) a"
                + " CROSS JOIN (SELECT k FROM t) b ORDER BY ak, bk LIMIT 2");
        LogicalPlan same = parse("SELECT a.k AS ak, b.k AS bk"
                + " FROM (SELECT k FROM t ORDER BY k LIMIT 1) a"
                + " CROSS JOIN (SELECT k FROM t) b ORDER BY ak, bk LIMIT 2");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(same, caller),
                "a cap truncating the same occurrence is justified");
    }

    /**
     * SET OPERANDS are occurrences of their own: moving a cap from operand 0 to operand 1
     * (both reading t, both under the same derived alias x) must not be accepted - the
     * relation ordinal of the truncated scan differs (t#1 vs t#2).
     */
    @Test
    public void testCapMovedBetweenSetOperandsIsRejected() {
        installConnectContext();
        LogicalPlan caller = parse("SELECT x.k FROM ((SELECT k FROM t ORDER BY k LIMIT 1)"
                + " UNION ALL SELECT k FROM t) x");
        LogicalPlan manual = parse("SELECT x.k FROM (SELECT k FROM t UNION ALL"
                + " (SELECT k FROM t ORDER BY k LIMIT 1)) x");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(manual, caller),
                "the cap of the OTHER set operand is a different occurrence");
    }

    /**
     * The per-occurrence multiset semantics: when the caller caps BOTH occurrences of a
     * self-joined derived table, a replay that caps both stays justified even when its
     * caps sit on the "other" occurrence - each (value, occurrence) key still has a
     * matching caller entry. A replay whose cap VALUES differ per occurrence is rejected.
     */
    @Test
    public void testCapsOnEveryOccurrenceStayAcceptedRegardlessOfOrder() {
        installConnectContext();
        LogicalPlan caller = parse("SELECT a.k AS ak, b.k AS bk"
                + " FROM (SELECT k FROM t ORDER BY k LIMIT 1) a"
                + " CROSS JOIN (SELECT k FROM t ORDER BY k LIMIT 1) b ORDER BY ak, bk LIMIT 2");
        LogicalPlan wrongValues = parse("SELECT a.k AS ak, b.k AS bk"
                + " FROM (SELECT k FROM t ORDER BY k LIMIT 2) a"
                + " CROSS JOIN (SELECT k FROM t ORDER BY k LIMIT 1) b ORDER BY ak, bk LIMIT 1");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(wrongValues, caller),
                "the cap VALUES still have to match per occurrence");
        LogicalPlan swapped = parse("SELECT b.k AS bk, a.k AS ak"
                + " FROM (SELECT k FROM t ORDER BY k LIMIT 1) b"
                + " CROSS JOIN (SELECT k FROM t ORDER BY k LIMIT 1) a ORDER BY ak, bk LIMIT 2");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(swapped, caller),
                "both occurrences are capped by the caller as well");
    }

    /** Control: from-less caps (no relation beneath) keep keying by their values alone. */
    @Test
    public void testCapsWithoutRelationsStillCompareByValues() {
        installConnectContext();
        LogicalPlan caller = parse("SELECT 1 AS v LIMIT 2");
        LogicalPlan same = parse("SELECT 1 AS v LIMIT 2");
        LogicalPlan extraCap = parse("SELECT 1 AS v LIMIT 1");
        LogicalPlan unboundedCaller = parse("SELECT 1 AS v");
        Assertions.assertTrue(SPMPlanTreeSupport.rowLimitsWithin(same, caller),
                "the identical from-less cap stays equivalent");
        Assertions.assertFalse(SPMPlanTreeSupport.rowLimitsWithin(extraCap, unboundedCaller),
                "a cap the caller does not have must be rejected, relations or not");
    }
}
