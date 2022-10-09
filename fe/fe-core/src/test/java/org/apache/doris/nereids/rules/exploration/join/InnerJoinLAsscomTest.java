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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InnerJoinLAsscomTest implements PatternMatchSupported {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testStarJoinLAsscom() {
        /*
         * Star-Join
         * t1 -- t2
         * |
         * t3
         * <p>
         *     t1.id=t3.id               t1.id=t2.id
         *       topJoin                  newTopJoin
         *       /     \                   /     \
         * t1.id=t2.id  t3          t1.id=t3.id   t2
         * bottomJoin       -->    newBottomJoin
         *   /    \                   /    \
         * t1      t2               t1      t3
         */

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .hashJoinUsing(scan3, JoinType.INNER_JOIN, Pair.of(1, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .matchesExploration(
                        logicalJoin(
                                logicalJoin(
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t1")),
                                        logicalOlapScan().when(scan -> scan.getTable().getName().equals("t3"))
                                ),
                                logicalOlapScan().when(scan -> scan.getTable().getName().equals("t2"))
                        )
                );
    }

    @Test
    public void testChainJoinLAsscom() {
        /*
         * Chain-Join
         * t1 -- t2 -- t3
         * <p>
         *     t2.id=t3.id               t2.id=t3.id
         *       topJoin                  newTopJoin
         *       /     \                   /     \
         * t1.id=t2.id  t3          t1.id=t3.id   t2
         * bottomJoin       -->    newBottomJoin
         *   /    \                   /    \
         * t1      t2               t1      t3
         */

        Expression bottomJoinOnCondition = new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0));
        Expression topJoinOnCondition = new EqualTo(scan2.getOutput().get(0), scan3.getOutput().get(0));
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> bottomJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(bottomJoinOnCondition), scan1, scan2);
        LogicalJoin<LogicalJoin<LogicalOlapScan, LogicalOlapScan>, LogicalOlapScan> topJoin = new LogicalJoin<>(
                JoinType.INNER_JOIN, Lists.newArrayList(topJoinOnCondition), bottomJoin, scan3);

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();

                    // TODO: need infer onCondition.
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                });
    }
}
