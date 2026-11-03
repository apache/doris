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
import org.apache.doris.nereids.trees.plans.JoinType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        assertNoMatchedJoinConstraint(withExtraTable);
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
        assertNoMatchedJoinConstraint(withExtraTable);
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
        assertNoMatchedJoinConstraint(withExtraTable);
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
        assertNoMatchedJoinConstraint(withExtraTable);
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
            assertNoMatchedJoinConstraint(splitConstrainedSide);
        } else {
            JoinConstraint joinConstraint = addJoinConstraint(leading, leftHand, rightHand,
                    leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable), joinType);

            Pair<JoinConstraint, Boolean> exact = leading.getJoinConstraint(
                    joinTable, leftHand, LongBitmap.newBitmapUnion(rightHand, extraTable));
            assertMatchedJoinConstraint(joinConstraint, exact, false);

            Pair<JoinConstraint, Boolean> splitConstrainedSide = leading.getJoinConstraint(
                    joinTable, LongBitmap.newBitmapUnion(leftHand, extraTable), rightHand);
            assertNoMatchedJoinConstraint(splitConstrainedSide);
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
}
