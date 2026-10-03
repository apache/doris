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
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-29 review fixes without their own regression suite:
 *
 * - #1 (SPMPlanTreeSupport, sameScanIdentity / describeScanSelector): TableSample
 *   overrides equals / hashCode by value but NOT toString, so its default text is the
 *   IDENTITY hash. The textual fallback and the selector rendering therefore compared
 *   two SEPARATE PARSES of one and the same TABLESAMPLE clause as different - a legitimate
 *   bind / plan pair was rejected by the mismatch guard and one statement's audit dedup
 *   identity changed between two parses - while two DIFFERENT samples compared equal
 *   whenever the two instances' identity hashes collided.
 * - #6 (StatementContext#resetPlannerStateForReplan): the hints the ABANDONED pass
 *   registered (e.g. a plan-side NO_USE_MV of a frozen baseline) survived into the
 *   fallback, which plans the ORIGINAL statement and must not be filtered by them.
 */
public class SPMRound29SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** The FIRST base-table relation of a plan tree (children walk). */
    private static UnboundRelation firstRelation(Plan plan) {
        final UnboundRelation[] found = new UnboundRelation[1];
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (found[0] == null && node instanceof UnboundRelation) {
                found[0] = (UnboundRelation) node;
            }
        });
        Assertions.assertNotNull(found[0], "the parsed plan must contain a relation");
        return found[0];
    }

    // ==================== #1: TABLESAMPLE identity / rendering ====================

    /**
     * The two clauses are IDENTICAL, but they come from two separate parses - which is
     * exactly what a baseline with a bind text and a plan text does. Comparing the
     * default {@code Object#toString} (the identity hash) made them "different", so
     * CREATE's scan-selector mismatch guard rejected a legitimate baseline and the audit
     * fingerprint of one statement depended on the parse instance.
     */
    @Test
    public void testIdenticalSamplesFromSeparateParsesAreTheSameScanIdentity() {
        UnboundRelation bind = firstRelation(
                parse("SELECT * FROM db.t TABLESAMPLE(10 ROWS) REPEATABLE 1000"));
        UnboundRelation plan = firstRelation(
                parse("select * from db.t TABLESAMPLE(10 ROWS) REPEATABLE 1000"));
        Assertions.assertNotSame(bind.getTableSample().get(), plan.getTableSample().get(),
                "precondition: two parses create two TableSample instances");

        Assertions.assertTrue(SPMPlanTreeSupport.sameScanIdentityForTest(bind, plan),
                "two parses of the SAME sample are the same scan identity");
        Assertions.assertEquals(SPMPlanTreeSupport.describeScanSelector(bind),
                SPMPlanTreeSupport.describeScanSelector(plan),
                "the audit fingerprint must not depend on the parse instance");
        Assertions.assertEquals("rows:10,seek:1000",
                SPMPlanTreeSupport.describeTableSample(bind.getTableSample().get()),
                "the rendering carries the sample's FIELDS");
    }

    /**
     * The values the reviewer used to show that the sample fields' {@code Objects.hash}
     * is not an identity: different samples must never compare or render equal.
     */
    @Test
    public void testDifferentSamplesAreNeverTheSameScanIdentity() {
        UnboundRelation rows = firstRelation(
                parse("SELECT * FROM db.t TABLESAMPLE(10 ROWS) REPEATABLE 1000"));
        UnboundRelation otherRows = firstRelation(
                parse("SELECT * FROM db.t TABLESAMPLE(11 ROWS) REPEATABLE 39"));
        UnboundRelation percent = firstRelation(
                parse("SELECT * FROM db.t TABLESAMPLE(10 PERCENT) REPEATABLE 1000"));
        UnboundRelation noRepeat = firstRelation(
                parse("SELECT * FROM db.t TABLESAMPLE(10 ROWS)"));

        Assertions.assertFalse(SPMPlanTreeSupport.sameScanIdentityForTest(rows, otherRows),
                "a different sample size / seed is a different scan identity");
        Assertions.assertNotEquals(SPMPlanTreeSupport.describeScanSelector(rows),
                SPMPlanTreeSupport.describeScanSelector(otherRows),
                "different samples must render differently");
        Assertions.assertFalse(SPMPlanTreeSupport.sameScanIdentityForTest(rows, percent),
                "ROWS and PERCENT with the same value are different samples");
        Assertions.assertFalse(SPMPlanTreeSupport.sameScanIdentityForTest(rows, noRepeat),
                "REPEATABLE changes the sample");
        Assertions.assertNotEquals("rows:10,seek:1000",
                SPMPlanTreeSupport.describeTableSample(percent.getTableSample().get()));
        Assertions.assertEquals("percent:10,seek:1000",
                SPMPlanTreeSupport.describeTableSample(percent.getTableSample().get()));
        Assertions.assertEquals("rows:10,seek:-1",
                SPMPlanTreeSupport.describeTableSample(noRepeat.getTableSample().get()),
                "an absent REPEATABLE is a value (-1), not a missing rendering");
    }

    /** A relation without a sample keeps rendering / comparing as before. */
    @Test
    public void testAbsentSampleKeepsItsRendering() {
        UnboundRelation plain = firstRelation(parse("SELECT * FROM db.t"));
        Assertions.assertEquals("", SPMPlanTreeSupport.describeTableSample(null));
        Assertions.assertFalse(plain.getTableSample().isPresent());
        Assertions.assertTrue(SPMPlanTreeSupport.sameScanIdentityForTest(plain,
                firstRelation(parse("select * from db.t"))),
                "no sample on both sides still matches");
    }

    // ==================== #6: the abandoned pass's hints ====================

    /**
     * The fallback re-plans the ORIGINAL statement on the same StatementContext. A
     * plan-side hint of the abandoned pass (the frozen baseline's plan text carrying
     * NO_USE_MV(mv1)) used to stay registered, so MV planning of the fallback was
     * filtered by a hint the ORIGINAL statement never had.
     */
    @Test
    public void testAbandonedPassHintsDoNotReachTheFallback() {
        StatementContext statementContext = new StatementContext();
        statementContext.addHint(new Hint("NO_USE_MV"));
        Assertions.assertEquals(1, statementContext.getHints().size(),
                "precondition: the abandoned pass registered one hint");

        statementContext.resetPlannerStateForReplan();
        Assertions.assertTrue(statementContext.getHints().isEmpty(),
                "the fallback must plan the ORIGINAL statement's hints only");

        // the fallback registers the ORIGINAL statement's own hints again
        statementContext.addHint(new Hint("LEADING"));
        Assertions.assertEquals(1, statementContext.getHints().size(),
                "a hint registered after the reset is kept");
    }
}
