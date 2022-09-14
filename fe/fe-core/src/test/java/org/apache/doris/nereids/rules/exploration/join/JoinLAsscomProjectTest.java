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

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class JoinLAsscomProjectTest {

    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

    @Test
    public void testStarJoinLAsscomProject() {
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

        Expression bottomJoinOnCondition = new EqualTo(scan1.getOutput().get(0), scan2.getOutput().get(0));
        Expression topJoinOnCondition = new EqualTo(scan1.getOutput().get(1), scan3.getOutput().get(1));

        LogicalJoin<LogicalOlapScan, LogicalOlapScan> bottomJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(bottomJoinOnCondition),
                Optional.empty(), scan1, scan2);

        List<Slot> output = bottomJoin.getOutput();
        List<NamedExpression> projectExprs = output.subList(0, output.size() - 1).stream()
                .map(NamedExpression.class::cast).collect(Collectors.toList());
        LogicalProject<LogicalJoin<LogicalOlapScan, LogicalOlapScan>> project = new LogicalProject<>(
                projectExprs, bottomJoin);
        LogicalJoin<LogicalProject<LogicalJoin<LogicalOlapScan, LogicalOlapScan>>, LogicalOlapScan>
                topJoin = new LogicalJoin<>(JoinType.INNER_JOIN, Lists.newArrayList(topJoinOnCondition),
                Optional.empty(), project, scan3);

        PlanChecker.from(MemoTestUtils.createConnectContext(), topJoin)
                .transform(JoinLAsscomProject.INNER.build())
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());

                    Assertions.assertTrue(root.logicalExpressionsAt(0).getPlan() instanceof LogicalJoin);
                    Assertions.assertTrue(root.logicalExpressionsAt(1).getPlan() instanceof LogicalProject);

                    GroupExpression newTopJoinGroupExpr = root.logicalExpressionsAt(1).child(0).getLogicalExpression();
                    GroupExpression leftProjectGroupExpr = newTopJoinGroupExpr.child(0).getLogicalExpression();
                    GroupExpression rightProjectGroupExpr = newTopJoinGroupExpr.child(1).getLogicalExpression();
                    Plan leftProject = newTopJoinGroupExpr.child(0).getLogicalExpression().getPlan();
                    Plan rightProject = newTopJoinGroupExpr.child(1).getLogicalExpression().getPlan();
                    Assertions.assertEquals(4, ((LogicalProject) leftProject).getProjects().size());
                    Assertions.assertEquals(1, ((LogicalProject) rightProject).getProjects().size());

                    Plan t2 = rightProjectGroupExpr.child(0).getLogicalExpression().getPlan();
                    Plan t1 = leftProjectGroupExpr.child(0).getLogicalExpression().child(0).getLogicalExpression()
                            .getPlan();
                    Plan t3 = leftProjectGroupExpr.child(0).getLogicalExpression().child(1).getLogicalExpression()
                            .getPlan();
                    Assertions.assertEquals("t2", ((LogicalOlapScan) t2).getTable().getName());
                    Assertions.assertEquals("t1", ((LogicalOlapScan) t1).getTable().getName());
                    Assertions.assertEquals("t3", ((LogicalOlapScan) t3).getTable().getName());
                });
    }
}
