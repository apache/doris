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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(SemiJoin(X, Z), Y)
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
 */
public class SemiJoinLogicalJoinTransposeProject extends OneExplorationRuleFactory {
    public static final SemiJoinLogicalJoinTransposeProject LEFT_DEEP = new SemiJoinLogicalJoinTransposeProject(true);

    public static final SemiJoinLogicalJoinTransposeProject ALL = new SemiJoinLogicalJoinTransposeProject(false);

    private final boolean leftDeep;

    public SemiJoinLogicalJoinTransposeProject(boolean leftDeep) {
        this.leftDeep = leftDeep;
    }

    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), group())
                .when(topJoin -> (topJoin.getJoinType().isLeftSemiOrAntiJoin()
                        && (topJoin.left().child().getJoinType().isInnerJoin()
                        || topJoin.left().child().getJoinType().isLeftOuterJoin()
                        || topJoin.left().child().getJoinType().isRightOuterJoin())))
                .whenNot(join -> join.hasJoinHint() || join.left().child().hasJoinHint())
                .whenNot(join -> join.isMarkJoin() || join.left().child().isMarkJoin())
                .when(join -> JoinReorderUtils.isAllSlotProject(join.left()))
                .then(topSemiJoin -> {
                    LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topSemiJoin.left();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = project.child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    Set<ExprId> conjunctsIds = Stream.concat(topSemiJoin.getHashJoinConjuncts().stream(),
                                    topSemiJoin.getOtherJoinConjuncts().stream())
                            .flatMap(expr -> expr.getInputSlotExprIds().stream()).collect(Collectors.toSet());
                    ContainsType containsType = containsChildren(conjunctsIds, a.getOutputExprIdSet(),
                            b.getOutputExprIdSet());
                    if (containsType == ContainsType.ALL) {
                        return null;
                    }

                    if (containsType == ContainsType.LEFT) {
                        /*-
                         *     topSemiJoin                    project
                         *      /     \                         |
                         *   project   C                    newTopJoin
                         *      |            ->            /         \
                         *  bottomJoin            newBottomSemiJoin   B
                         *   /    \                    /      \
                         *  A      B                  A        C
                         */
                        // RIGHT_OUTER_JOIN should be eliminated in rewrite phase
                        Preconditions.checkState(bottomJoin.getJoinType() != JoinType.RIGHT_OUTER_JOIN);

                        Plan newBottomSemiJoin = topSemiJoin.withChildren(a, c);
                        Plan newTopJoin = bottomJoin.withChildren(newBottomSemiJoin, b);
                        return project.withChildren(newTopJoin);
                    } else {
                        if (leftDeep) {
                            return null;
                        }
                        /*-
                         *     topSemiJoin                  project
                         *       /     \                       |
                         *    project   C                  newTopJoin
                         *       |                        /         \
                         *  bottomJoin  C     -->        A    newBottomSemiJoin
                         *    /    \                              /      \
                         *   A      B                             B       C
                         */
                        // LEFT_OUTER_JOIN should be eliminated in rewrite phase
                        Preconditions.checkState(bottomJoin.getJoinType() != JoinType.LEFT_OUTER_JOIN);

                        Plan newBottomSemiJoin = topSemiJoin.withChildren(b, c);
                        Plan newTopJoin = bottomJoin.withChildren(a, newBottomSemiJoin);
                        return project.withChildren(newTopJoin);
                    }
                }).toRule(RuleType.LOGICAL_SEMI_JOIN_LOGICAL_JOIN_TRANSPOSE_PROJECT);
    }

    enum ContainsType {
        LEFT, RIGHT, ALL
    }

    private ContainsType containsChildren(Set<ExprId> conjunctsExprIdSet, Set<ExprId> left, Set<ExprId> right) {
        boolean containsLeft = Utils.isIntersecting(conjunctsExprIdSet, left);
        boolean containsRight = Utils.isIntersecting(conjunctsExprIdSet, right);
        Preconditions.checkState(containsLeft || containsRight, "join output must contain child");
        if (containsLeft && containsRight) {
            return ContainsType.ALL;
        } else if (containsLeft) {
            return ContainsType.LEFT;
        } else {
            return ContainsType.RIGHT;
        }
    }
}
