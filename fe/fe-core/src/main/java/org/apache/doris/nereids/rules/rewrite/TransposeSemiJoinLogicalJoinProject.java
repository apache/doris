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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.Set;

/**
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(SemiJoin(X, Z), Y)
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
 */
public class TransposeSemiJoinLogicalJoinProject extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), any())
                .whenNot(join -> ConnectContext.get().getSessionVariable().isDisableJoinReorder())
                .when(topJoin -> (topJoin.getJoinType().isLeftSemiOrAntiJoin()
                        && (topJoin.left().child().getJoinType().isInnerJoin()
                        || topJoin.left().child().getJoinType().isLeftOuterJoin()
                        || topJoin.left().child().getJoinType().isRightOuterJoin())))
                .when(join -> join.left().isAllSlots())
                .whenNot(join -> join.hasDistributeHint() || join.left().child().hasDistributeHint())
                .whenNot(topJoin -> topJoin.isLeadingJoin() || topJoin.left().child().isLeadingJoin())
                .when(join -> join.left().getProjects().stream().allMatch(expr -> expr instanceof Slot))
                .then(topSemiJoin -> {
                    LogicalProject<LogicalJoin<Plan, Plan>> project = topSemiJoin.left();
                    LogicalJoin<Plan, Plan> bottomJoin = project.child();
                    Plan a = bottomJoin.left();
                    Plan b = bottomJoin.right();
                    Plan c = topSemiJoin.right();

                    Set<ExprId> conjunctsIds = topSemiJoin.getConditionExprId();
                    ContainsType containsType = containsChildren(conjunctsIds, a.getOutputExprIdSet(),
                            b.getOutputExprIdSet());
                    if (containsType == ContainsType.ALL || containsType == ContainsType.NONE) {
                        return null;
                    }
                    ImmutableList<NamedExpression> topProjects = topSemiJoin.getOutput().stream()
                            .map(slot -> (NamedExpression) slot).collect(ImmutableList.toImmutableList());
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
                        // TODO: when top join is ANTI JOIN,  bottomJoin may be RIGHT_OUTER_JOIN
                        // Can we also do the transformation?
                        if (bottomJoin.getJoinType() == JoinType.RIGHT_OUTER_JOIN) {
                            return null;
                        }

                        Plan newBottomSemiJoin = topSemiJoin.withChildren(a, c);
                        Plan newTopJoin = bottomJoin.withChildren(newBottomSemiJoin, b);
                        return project.withProjectsAndChild(topProjects, newTopJoin);
                    } else {
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
                        // TODO: when top join is ANTI JOIN,  bottomJoin may be RIGHT_OUTER_JOIN
                        // Can we also do the transformation?
                        if (bottomJoin.getJoinType() == JoinType.LEFT_OUTER_JOIN) {
                            return null;
                        }

                        Plan newBottomSemiJoin = topSemiJoin.withChildren(b, c);
                        Plan newTopJoin = bottomJoin.withChildren(a, newBottomSemiJoin);
                        return project.withProjectsAndChild(topProjects, newTopJoin);
                    }
                }).toRule(RuleType.TRANSPOSE_LOGICAL_SEMI_JOIN_LOGICAL_JOIN_PROJECT);
    }

    enum ContainsType {
        LEFT, RIGHT, ALL, NONE
    }

    /**
     * Check conjuncts contain children.
     */
    public static ContainsType containsChildren(Set<ExprId> conjunctsExprIdSet, Set<ExprId> left, Set<ExprId> right) {
        boolean containsLeft = Utils.isIntersecting(conjunctsExprIdSet, left);
        boolean containsRight = Utils.isIntersecting(conjunctsExprIdSet, right);
        if (containsLeft && containsRight) {
            return ContainsType.ALL;
        } else if (containsLeft) {
            return ContainsType.LEFT;
        } else if (containsRight) {
            return ContainsType.RIGHT;
        } else {
            return ContainsType.NONE;
        }
    }
}
