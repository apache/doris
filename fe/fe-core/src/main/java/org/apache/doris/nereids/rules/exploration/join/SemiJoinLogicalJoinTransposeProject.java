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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Set;

/**
 * Planner rule that pushes a SemoJoin down in a tree past a LogicalJoin
 * in order to trigger other rules that will convert {@code SemiJoin}s.
 *
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(SemiJoin(X, Z), Y)
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
 * <p>
 * Whether this first or second conversion is applied depends on
 * which operands actually participate in the semi-join.
 */
public class SemiJoinLogicalJoinTransposeProject extends OneExplorationRuleFactory {
    @Override
    public Rule build() {
        return leftSemiLogicalJoin(logicalProject(logicalJoin()), group())
                .when(this::conditionChecker)
                .then(topSemiJoin -> {
                    LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topSemiJoin.left();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = project.child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    boolean lasscom = a.getOutputSet().containsAll(project.getOutput());

                    if (lasscom) {
                        /*-
                         *     topSemiJoin                   newTopProject
                         *      /     \                          |
                         *   project   C                    newTopJoin
                         *      |            ->            /         \
                         *  bottomJoin             newBottomSemiJoin  B
                         *   /    \                    /      \
                         *  A      B             aNewProject   C
                         *                          |
                         *                          A
                         */
                        List<NamedExpression> projects = project.getProjects();
                        LogicalProject<GroupPlan> aNewProject = new LogicalProject<>(projects, a);
                        LogicalJoin<LogicalProject<GroupPlan>, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(), topSemiJoin.getHashJoinConjuncts(),
                                topSemiJoin.getOtherJoinCondition(), aNewProject, c);
                        LogicalJoin<LogicalJoin<LogicalProject<GroupPlan>, GroupPlan>, GroupPlan> newTopJoin
                                = new LogicalJoin<>(bottomJoin.getJoinType(), bottomJoin.getHashJoinConjuncts(),
                                bottomJoin.getOtherJoinCondition(), newBottomSemiJoin, b);
                        return new LogicalProject<>(projects, newTopJoin);
                    } else {
                        /*-
                         *     topSemiJoin              newTopProject
                         *       /     \                     |
                         *    project   C                newTopJoin
                         *       |                      /          \
                         *  bottomJoin  C     -->      A   newBottomSemiJoin
                         *   /    \                            /      \
                         *  A      B                     bNewProject   C
                         *                                   |
                         *                                   B
                         */
                        List<NamedExpression> projects = project.getProjects();
                        LogicalProject<GroupPlan> bNewProject = new LogicalProject<>(projects, b);
                        LogicalJoin<LogicalProject<GroupPlan>, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(), topSemiJoin.getHashJoinConjuncts(),
                                topSemiJoin.getOtherJoinCondition(), bNewProject, c);

                        LogicalJoin<GroupPlan, LogicalJoin<LogicalProject<GroupPlan>, GroupPlan>> newTopJoin
                                = new LogicalJoin<>(bottomJoin.getJoinType(), bottomJoin.getHashJoinConjuncts(),
                                bottomJoin.getOtherJoinCondition(), a, newBottomSemiJoin);
                        return new LogicalProject<>(projects, newTopJoin);
                    }
                }).toRule(RuleType.LOGICAL_JOIN_L_ASSCOM);
    }

    // bottomJoin just return A OR B, else return false.
    private boolean conditionChecker(
            LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>, GroupPlan> topJoin) {
        Set<Slot> projectOutputSet = topJoin.left().getOutputSet();

        Set<Slot> aOutputSet = topJoin.left().child().left().getOutputSet();
        Set<Slot> bOutputSet = topJoin.left().child().right().getOutputSet();

        boolean isProjectA = !ExpressionUtils.isIntersecting(projectOutputSet, aOutputSet);
        boolean isProjectB = !ExpressionUtils.isIntersecting(projectOutputSet, bOutputSet);

        Preconditions.checkState(isProjectA || isProjectB, "project must contain child");
        return !(isProjectA && isProjectB);
    }
}
