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
    public void testRejectExtraTableOnSemiOrAntiJoinConstrainedSide() {
        assertRejectedWithExtraTable(JoinType.LEFT_SEMI_JOIN);
        assertRejectedWithExtraTable(JoinType.LEFT_ANTI_JOIN);
        assertRejectedWithExtraTable(JoinType.RIGHT_SEMI_JOIN);
        assertRejectedWithExtraTable(JoinType.RIGHT_ANTI_JOIN);
    }

    private void assertRejectedWithExtraTable(JoinType joinType) {
        LeadingHint leading = new LeadingHint("Leading");
        long leftHand = LongBitmap.newBitmap(0);
        long rightHand = LongBitmap.newBitmap(1);
        long extraTable = LongBitmap.newBitmap(2);
        JoinConstraint joinConstraint = new JoinConstraint(
                leftHand, rightHand, leftHand, rightHand, joinType, true);
        leading.getJoinConstraintList().add(joinConstraint);

        Pair<JoinConstraint, Boolean> exactMatch = leading.getJoinConstraint(
                LongBitmap.or(leftHand, rightHand), leftHand, rightHand);
        Assertions.assertSame(joinConstraint, exactMatch.first);
        Assertions.assertTrue(exactMatch.second);

        long leftTableBitmap;
        long rightTableBitmap;
        if (joinType.isRightSemiOrAntiJoin()) {
            leftTableBitmap = LongBitmap.newBitmapUnion(leftHand, extraTable);
            rightTableBitmap = rightHand;
        } else {
            leftTableBitmap = LongBitmap.newBitmapUnion(rightHand, extraTable);
            rightTableBitmap = leftHand;
        }

        Pair<JoinConstraint, Boolean> result = leading.getJoinConstraint(
                LongBitmap.or(leftTableBitmap, rightTableBitmap), leftTableBitmap, rightTableBitmap);
        Assertions.assertNull(result.first);
        Assertions.assertFalse(result.second);
    }
}
