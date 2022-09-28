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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SemiJoinLogicalJoinTransposeTest {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private static final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testSemiJoinLogicalTransposeLAsscom() {
        /*
         *    topSemiJoin                newTopJoin
         *      /     \                 /         \
         * bottomJoin  C   -->  newBottomSemiJoin  B
         *  /    \                  /    \
         * A      B                A      C
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .hashJoinUsing(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0)) // t1.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinLogicalJoinTranspose.LEFT_DEEP.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan plan = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    LogicalJoin<?, ?> newTopJoin = (LogicalJoin<?, ?>) plan;
                    LogicalJoin<?, ?> newBottomJoin = (LogicalJoin<?, ?>) newTopJoin.left();
                    Assertions.assertEquals(JoinType.INNER_JOIN, newTopJoin.getJoinType());
                    Assertions.assertEquals(JoinType.LEFT_SEMI_JOIN, newBottomJoin.getJoinType());

                    LogicalOlapScan newBottomJoinLeft = (LogicalOlapScan) newBottomJoin.left();
                    LogicalOlapScan newBottomJoinRight = (LogicalOlapScan) newBottomJoin.right();
                    LogicalOlapScan newTopJoinRight = (LogicalOlapScan) newTopJoin.right();

                    Assertions.assertEquals("t1", newBottomJoinLeft.getTable().getName());
                    Assertions.assertEquals("t3", newBottomJoinRight.getTable().getName());
                    Assertions.assertEquals("t2", newTopJoinRight.getTable().getName());
                });
    }

    @Test
    public void testSemiJoinLogicalTransposeLAsscomFail() {
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .hashJoinUsing(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(2, 0)) // t2.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinLogicalJoinTranspose.LEFT_DEEP.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                });
    }

    @Test
    public void testSemiJoinLogicalTransposeAll() {
        /*
         *    topSemiJoin            newTopJoin
         *      /     \             /         \
         * bottomJoin  C   -->     A   newBottomSemiJoin
         *  /    \                         /      \
         * A      B                       B        C
         */
        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .hashJoinUsing(scan3, JoinType.LEFT_SEMI_JOIN, Pair.of(2, 0)) // t2.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(SemiJoinLogicalJoinTranspose.ALL.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan plan = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    LogicalJoin<?, ?> newTopJoin = (LogicalJoin<?, ?>) plan;
                    LogicalJoin<?, ?> newBottomJoin = (LogicalJoin<?, ?>) newTopJoin.right();
                    Assertions.assertEquals(JoinType.INNER_JOIN, newTopJoin.getJoinType());
                    Assertions.assertEquals(JoinType.LEFT_SEMI_JOIN, newBottomJoin.getJoinType());

                    LogicalOlapScan newTopJoinLeft = (LogicalOlapScan) newTopJoin.left();
                    LogicalOlapScan newBottomJoinLeft = (LogicalOlapScan) newBottomJoin.left();
                    LogicalOlapScan newBottomJoinRight = (LogicalOlapScan) newBottomJoin.right();

                    Assertions.assertEquals("t1", newTopJoinLeft.getTable().getName());
                    Assertions.assertEquals("t2", newBottomJoinLeft.getTable().getName());
                    Assertions.assertEquals("t3", newBottomJoinRight.getTable().getName());
                });
    }
}
