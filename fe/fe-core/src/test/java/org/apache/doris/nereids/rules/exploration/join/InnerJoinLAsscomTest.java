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
import org.apache.doris.nereids.rules.rewrite.logical.PushProjectInsideJoin;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class InnerJoinLAsscomTest {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testInnerJoinLAsscom() {
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

        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoin(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .hashJoin(scan3, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyTopDown(PushProjectInsideJoin.INSTANCE)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan plan = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    Assertions.assertTrue(plan instanceof LogicalJoin);
                    LogicalJoin<?, ?> newTopJoin = (LogicalJoin<?, ?>) plan;
                    LogicalJoin<?, ?> newBottomJoin = (LogicalJoin<?, ?>) newTopJoin.left();

                    Assertions.assertTrue(newTopJoin.right() instanceof LogicalOlapScan);
                    LogicalOlapScan right = (LogicalOlapScan) newTopJoin.right();

                    Assertions.assertEquals(6, newTopJoin.getProjects().size());
                    Assertions.assertEquals(0, newBottomJoin.getProjects().size());
                    Assertions.assertEquals("t1", ((LogicalOlapScan) newBottomJoin.left()).getTable().getName());
                    Assertions.assertEquals("t3", ((LogicalOlapScan) newBottomJoin.right()).getTable().getName());
                    Assertions.assertEquals("t2", right.getTable().getName());
                });
    }

    @Test
    public void testInnerJoinLAsscomInsideProject() {
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

        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoin(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .project(ImmutableList.of(0, 1, 2)) // t1.id, t1.name, t2.id
                .hashJoin(scan3, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t3.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyTopDown(PushProjectInsideJoin.INSTANCE)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan plan = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    Assertions.assertTrue(plan instanceof LogicalJoin);
                    LogicalJoin<?, ?> newTopJoin = (LogicalJoin<?, ?>) plan;
                    LogicalJoin<?, ?> newBottomJoin = (LogicalJoin<?, ?>) newTopJoin.left();

                    Assertions.assertTrue(newTopJoin.right() instanceof LogicalProject);
                    LogicalProject<?> rightProject = (LogicalProject<?>) newTopJoin.right();
                    Assertions.assertEquals(1, rightProject.getProjects().size());

                    Assertions.assertEquals(5, newTopJoin.getProjects().size());
                    Assertions.assertEquals(4, newBottomJoin.getProjects().size());
                    Assertions.assertEquals("t1", ((LogicalOlapScan) newBottomJoin.left()).getTable().getName());
                    Assertions.assertEquals("t3", ((LogicalOlapScan) newBottomJoin.right()).getTable().getName());
                    Assertions.assertEquals("t2", ((LogicalOlapScan) rightProject.child()).getTable().getName());
                });
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
                Lists.newArrayList(bottomJoinOnCondition),
                Optional.empty(), scan1, scan2);
        LogicalJoin<LogicalJoin<LogicalOlapScan, LogicalOlapScan>, LogicalOlapScan> topJoin = new LogicalJoin<>(
                JoinType.INNER_JOIN, Lists.newArrayList(topJoinOnCondition),
                Optional.empty(), bottomJoin, scan3);

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();

                    // TODO: need infer onCondition.
                    Assertions.assertEquals(1, root.getLogicalExpressions().size());
                });
    }

    @Test
    public void testInnerJoinLAsscomProject() {
        /*
         * Star-Join
         * t1 -- t2
         * |
         * t3
         * <p>
         *     t1.id=t3.id               t1.id=t2.id
         *       topJoin                  newTopJoin
         *       /     \                   /     \
         *    project   t3           project    project
         * t1.id=t2.id             t1.id=t3.id    t2
         * bottomJoin       -->   newBottomJoin
         *   /    \                   /    \
         * t1      t2               t1      t3
         */

        LogicalPlan topJoin = new LogicalPlanBuilder(scan1)
                .hashJoin(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id=t2.id
                .project(ImmutableList.of(0, 1, 2)) // t1.id, t1.name, t2.id
                .hashJoin(scan3, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id=t3.id
                .project(ImmutableList.of(0, 2)) // t1.id, t1.name, t2.id
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .applyTopDown(PushProjectInsideJoin.INSTANCE)
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    Plan plan = memo.copyOut(root.getLogicalExpressions().get(1), false);

                    Assertions.assertTrue(plan instanceof LogicalJoin);
                    LogicalJoin<?, ?> newTopJoin = (LogicalJoin<?, ?>) plan;
                    LogicalJoin<?, ?> newBottomJoin = (LogicalJoin<?, ?>) newTopJoin.left();

                    Assertions.assertTrue(newTopJoin.right() instanceof LogicalProject);
                    LogicalProject<?> rightProject = (LogicalProject<?>) newTopJoin.right();
                    Assertions.assertEquals(1, rightProject.getProjects().size());

                    Assertions.assertEquals(2, newTopJoin.getProjects().size());
                    Assertions.assertEquals(4, newBottomJoin.getProjects().size());
                    Assertions.assertEquals("t1", ((LogicalOlapScan) newBottomJoin.left()).getTable().getName());
                    Assertions.assertEquals("t3", ((LogicalOlapScan) newBottomJoin.right()).getTable().getName());
                    Assertions.assertEquals("t2", ((LogicalOlapScan) rightProject.child()).getTable().getName());
                });
    }
}
