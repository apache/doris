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

package org.apache.doris.nereids.hint;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class LeadingHintTest {

    @Test
    public void testLeftSemiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint semiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.LEFT_SEMI_JOIN);

        Pair<JoinConstraint, Boolean> exactReversed = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), rightHand, leftHand);
        assertMatchedJoinConstraint(semiJoinConstraint, exactReversed, true);

        Pair<JoinConstraint, Boolean> withoutRetainedSide = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(rightHand, extraTable), rightHand, extraTable);
        assertNoMatchedJoinConstraint(withoutRetainedSide);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(rightHand, extraTable),
                leftHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testLeftAntiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint antiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.LEFT_ANTI_JOIN);

        Pair<JoinConstraint, Boolean> exactReversed = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), rightHand, leftHand);
        assertMatchedJoinConstraint(antiJoinConstraint, exactReversed, true);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(rightHand, extraTable),
                leftHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testRightSemiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint semiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.RIGHT_SEMI_JOIN);

        Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), leftHand, rightHand);
        assertMatchedJoinConstraint(semiJoinConstraint, exact, false);

        Pair<JoinConstraint, Boolean> withoutConstrainedSide = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(rightHand, extraTable), rightHand, extraTable);
        assertNoMatchedJoinConstraint(withoutConstrainedSide);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(leftHand, extraTable),
                rightHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testRightAntiJoinConstrainedSideMatchesExactly() {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint antiJoinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                JoinType.RIGHT_ANTI_JOIN);

        Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), leftHand, rightHand);
        assertMatchedJoinConstraint(antiJoinConstraint, exact, false);

        Pair<JoinConstraint, Boolean> withExtraTable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable),
                LongBitmap.newBitmapUnion(leftHand, extraTable),
                rightHand);
        assertConstraintViolated(withExtraTable);
    }

    @Test
    public void testCompositeLeftSemiAndAntiJoinConstrainedSideCanNotBeSplit() {
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.LEFT_SEMI_JOIN);
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.LEFT_ANTI_JOIN);
    }

    @Test
    public void testCompositeRightSemiAndAntiJoinConstrainedSideCanNotBeSplit() {
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.RIGHT_SEMI_JOIN);
        assertCompositeConstrainedSideCanNotBeSplit(JoinType.RIGHT_ANTI_JOIN);
    }

    @Test
    public void testCompositeLeftSemiAndAntiJoinRetainedSideCanUseMinHand() {
        assertCompositeRetainedSideCanUseMinHand(JoinType.LEFT_SEMI_JOIN);
        assertCompositeRetainedSideCanUseMinHand(JoinType.LEFT_ANTI_JOIN);
    }

    @Test
    public void testCompositeRightSemiAndAntiJoinRetainedSideCanUseMinHand() {
        assertCompositeRetainedSideCanUseMinHand(JoinType.RIGHT_SEMI_JOIN);
        assertCompositeRetainedSideCanUseMinHand(JoinType.RIGHT_ANTI_JOIN);
    }

    @Test
    public void testConditionJoinTypeMultipleTypes() {
        // Issue 2: same Expression used in both LEFT_SEMI_JOIN and INNER_JOIN
        // The conditionJoinType map should preserve both types, not overwrite
        LeadingHint leading = new LeadingHint("Leading");
        Expression expr = new IntegerLiteral(1);

        // Simulate CollectJoinConstraint processing: semi join first, then inner join
        leading.putConditionJoinType(expr, JoinType.LEFT_SEMI_JOIN);
        leading.putConditionJoinType(expr, JoinType.INNER_JOIN);

        // LEFT_SEMI_JOIN should match (was added first, should not be overwritten)
        Assertions.assertTrue(
                leading.isConditionJoinTypeMatched(Arrays.asList(expr), JoinType.LEFT_SEMI_JOIN),
                "LEFT_SEMI_JOIN should match the expression's condition type set");

        // INNER_JOIN should also match (was also added)
        Assertions.assertTrue(
                leading.isConditionJoinTypeMatched(Arrays.asList(expr), JoinType.INNER_JOIN),
                "INNER_JOIN should also match the expression's condition type set");

        // RIGHT_ANTI_JOIN should NOT match (was never added)
        Assertions.assertFalse(
                leading.isConditionJoinTypeMatched(Arrays.asList(expr), JoinType.RIGHT_ANTI_JOIN),
                "RIGHT_ANTI_JOIN should NOT match (not in the set)");

        // Different expression should be independent
        Expression expr2 = new IntegerLiteral(2);
        leading.putConditionJoinType(expr2, JoinType.LEFT_ANTI_JOIN);

        // expr should still match LEFT_SEMI_JOIN (independent of expr2)
        Assertions.assertTrue(
                leading.isConditionJoinTypeMatched(Arrays.asList(expr), JoinType.LEFT_SEMI_JOIN),
                "expr should match LEFT_SEMI_JOIN independently");

        // expr2 should match LEFT_ANTI_JOIN (its own type)
        Assertions.assertTrue(
                leading.isConditionJoinTypeMatched(Arrays.asList(expr2), JoinType.LEFT_ANTI_JOIN),
                "expr2 should match LEFT_ANTI_JOIN (its own type)");

        // Mixing incompatible types should fail: expr2 is ANTI, not SEMI
        Assertions.assertFalse(
                leading.isConditionJoinTypeMatched(Arrays.asList(expr, expr2), JoinType.LEFT_SEMI_JOIN),
                "mixing SEMI (expr) and ANTI (expr2) should fail for SEMI join type");

        // Empty conditions list always passes
        Assertions.assertTrue(
                leading.isConditionJoinTypeMatched(Collections.emptyList(), JoinType.INNER_JOIN),
                "Empty conditions should always pass");
    }

    @Test
    public void testSemiJoinConstrainedSideViolatedWithMinLeftHand() {
        // Reproduces SQL 1 bug scenario:
        // LEFT SEMI JOIN: retained side leftHand={0,1}, rightHand={2}
        // minLeftHand={1} (condition only references table 1, not table 0)
        // So minLeftHand is a proper subset of leftHand (table 0 is an inner-join
        // table on the retained side that was propagated)
        // When constrained side {2} is mixed with {0} on a child → should fail
        LeadingHint leading = new LeadingHint("Leading");
        long retainedTable = LongBitmap.newBitmap(0);
        long minLeftTable = LongBitmap.newBitmap(1);
        long probeTable = LongBitmap.newBitmap(2);
        long leftHand = LongBitmap.newBitmapUnion(retainedTable, minLeftTable);
        long rightHand = probeTable;

        // minLeftHand={1}, minRightHand={2}, leftHand={0,1}, rightHand={2}
        addJoinConstraint(leading, minLeftTable, probeTable, leftHand, rightHand, JoinType.LEFT_SEMI_JOIN);

        // All tables present: joinTableBitmap={0,1,2}
        long joinTable = LongBitmap.newBitmapUnion(leftHand, rightHand);

        // Case 1: constrained side (rightHand={2}) intact on left, retained side intact on right → OK (reversed)
        Pair<JoinConstraint, Boolean> ok = leading.getJoinConstraint(
                joinTable, rightHand, leftHand);
        Assertions.assertTrue(ok.second, "constrained side intact on left should match (reversed)");

        // Case 2: constrained side {2} mixed with extra table {0} on left,
        // retained side {1} on right → VIOLATED
        Pair<JoinConstraint, Boolean> violated = leading.getJoinConstraint(
                joinTable,
                LongBitmap.newBitmapUnion(probeTable, retainedTable), // left={0,2} (constrained mixed with extra)
                minLeftTable); // right={1} (min retained)
        assertConstraintViolated(violated);
    }

    @Test
    public void testRightAntiJoinConstrainedSideViolatedWithMinLeftHand() {
        // Reproduces SQL 2 bug scenario:
        // RIGHT ANTI JOIN: leftHand={0}, rightHand={1}
        // minLeftHand={0}, minRightHand={1} (both are minimal)
        // Extra inner join table {2} is mixed with constrained side {0}
        // When constrained side {0} is mixed with {2} on a child → should fail
        LeadingHint leading = new LeadingHint("Leading");
        long constrainedTable = LongBitmap.newBitmap(0);
        long rightTable = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);

        // minLeftHand={0}, minRightHand={1}, leftHand={0}, rightHand={1}
        addJoinConstraint(leading, constrainedTable, rightTable,
                constrainedTable, rightTable, JoinType.RIGHT_ANTI_JOIN);

        // All tables present (including extra inner join table): {0,1,2}
        long joinTable = LongBitmap.newBitmapUnion(constrainedTable, rightTable, extraTable);

        // Case 1: constrained side {0} intact, right side {1} intact → OK
        Pair<JoinConstraint, Boolean> ok = leading.getJoinConstraint(
                joinTable, constrainedTable, rightTable);
        Assertions.assertTrue(ok.second, "constrained side intact should match");

        // Case 2: constrained side {0} mixed with extra {2} on left,
        // right side {1} on right → VIOLATED
        Pair<JoinConstraint, Boolean> violated = leading.getJoinConstraint(
                joinTable,
                LongBitmap.newBitmapUnion(constrainedTable, extraTable), // left={0,2}
                rightTable); // right={1}
        assertConstraintViolated(violated);
    }

    @Test
    public void testRightAntiJoinCompositeConstrainedSideMixedWithExtra() {
        // RIGHT ANTI JOIN with composite constrained side:
        // leftHand={0,2} (composite: constrainedTable + propagated inner join table)
        // rightHand={1}
        // minLeftHand={0,2}, minRightHand={1}
        // When constrained side {0,2} is split across both children → VIOLATED
        LeadingHint leading = new LeadingHint("Leading");
        long constrainedTable = LongBitmap.newBitmap(0);
        long innerJoinOnLeft = LongBitmap.newBitmap(2);
        long rightTable = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(3);

        long leftHand = LongBitmap.newBitmapUnion(constrainedTable, innerJoinOnLeft);
        long rightHand = rightTable;

        // composite constrained side: leftHand={0,2}, minLeftHand={0,2}
        addJoinConstraint(leading, leftHand, rightHand, leftHand, rightHand, JoinType.RIGHT_ANTI_JOIN);

        long joinTable = LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable);

        // Constrained side {0,2} intact on left → OK
        Pair<JoinConstraint, Boolean> ok = leading.getJoinConstraint(
                joinTable, leftHand, rightHand);
        Assertions.assertTrue(ok.second, "composite constrained side intact should match");

        // Constrained side {0,2} split: {0} on left, {2} on right (mixed with right table) → VIOLATED
        Pair<JoinConstraint, Boolean> splitViolated = leading.getJoinConstraint(
                joinTable,
                constrainedTable, // left={0} (part of constrained)
                LongBitmap.newBitmapUnion(innerJoinOnLeft, rightTable)); // right={1,2}
        assertConstraintViolated(splitViolated);
    }

    @Test
    public void testLeftSemiJoinConstraintNotApplicableWithoutMinLeftHand() {
        // When minLeftHand is NOT yet present in joinTableBitmap,
        // the constraint should NOT be applied (not applicable yet),
        // returning (null, true) — not (null, false)!
        // Regression test: ensure the first guard (isSubset minLeftHand/minRightHand)
        // correctly does "continue", not "return failure".
        LeadingHint leading = new LeadingHint("Leading");
        long retainedTable = LongBitmap.newBitmap(0);
        long minLeftTable = LongBitmap.newBitmap(1);
        long probeTable = LongBitmap.newBitmap(2);
        long leftHand = LongBitmap.newBitmapUnion(retainedTable, minLeftTable);
        long rightHand = probeTable;

        addJoinConstraint(leading, minLeftTable, probeTable, leftHand, rightHand, JoinType.LEFT_SEMI_JOIN);

        // Only probe table and extra table present — minLeftHand={1} NOT present
        long extraTable = LongBitmap.newBitmap(3);
        Pair<JoinConstraint, Boolean> notApplicable = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(probeTable, extraTable),
                probeTable,
                extraTable);
        // Should be "no constraint matched" (null, true), NOT "violated" (null, false)
        assertNoMatchedJoinConstraint(notApplicable);
    }

    private JoinConstraint addJoinConstraint(LeadingHint leading, long leftHand, long rightHand,
            JoinType joinType) {
        JoinConstraint joinConstraint = new JoinConstraint(leftHand, rightHand, leftHand, rightHand, joinType, true);
        leading.getJoinConstraintList().add(joinConstraint);
        return joinConstraint;
    }

    private JoinConstraint addJoinConstraint(LeadingHint leading, long minLeftHand, long minRightHand,
            long leftHand, long rightHand, JoinType joinType) {
        JoinConstraint joinConstraint = new JoinConstraint(minLeftHand, minRightHand, leftHand, rightHand,
                joinType, true);
        leading.getJoinConstraintList().add(joinConstraint);
        return joinConstraint;
    }

    private void assertCompositeConstrainedSideCanNotBeSplit(JoinType joinType) {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        long joinTable = LongBitmap.newBitmapUnion(leftHand, rightHand, extraTable);

        if (joinType.isRightSemiOrAntiJoin()) {
            JoinConstraint joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand, joinType);

            Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                    joinTable, LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand);
            assertMatchedJoinConstraint(joinConstraint, exact, false);

            Pair<JoinConstraint, Boolean> splitConstrainedSide = leading.getJoinConstraint(
                    joinTable, leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable));
            assertConstraintViolated(splitConstrainedSide);
        } else {
            JoinConstraint joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable), joinType);

            Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                    joinTable, leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable));
            assertMatchedJoinConstraint(joinConstraint, exact, false);

            Pair<JoinConstraint, Boolean> splitConstrainedSide = leading.getJoinConstraint(
                    joinTable, LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand);
            assertConstraintViolated(splitConstrainedSide);
        }
    }

    private void assertCompositeRetainedSideCanUseMinHand(JoinType joinType) {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);

        JoinConstraint joinConstraint;
        if (joinType.isRightSemiOrAntiJoin()) {
            joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable), joinType);
        } else {
            joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand, joinType);
        }

        Pair<JoinConstraint, Boolean> minRetainedSide = leading.getJoinConstraint(
                LongBitmap.newBitmapUnion(leftHand, rightHand), leftHand, rightHand);
        assertMatchedJoinConstraint(joinConstraint, minRetainedSide, false);
    }

    private void assertMatchedJoinConstraint(JoinConstraint expected, Pair<JoinConstraint, Boolean> actual,
            boolean reversed) {
        Assertions.assertSame(expected, actual.first);
        Assertions.assertTrue(actual.second);
        Assertions.assertEquals(reversed, actual.first.isReversed());
    }

    private void assertNoMatchedJoinConstraint(Pair<JoinConstraint, Boolean> actual) {
        Assertions.assertNull(actual.first);
        Assertions.assertTrue(actual.second);
    }

    private void assertConstraintViolated(Pair<JoinConstraint, Boolean> actual) {
        Assertions.assertNull(actual.first);
        Assertions.assertFalse(actual.second);
    }
}
