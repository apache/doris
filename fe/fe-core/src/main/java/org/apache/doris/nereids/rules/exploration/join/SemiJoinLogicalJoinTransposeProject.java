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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
                .whenNot(topJoin -> topJoin.left().child().getJoinType().isSemiOrAntiJoin())
                .whenNot(join -> join.hasJoinHint() || join.left().child().hasJoinHint())
                .when(join -> JoinReorderUtils.isAllSlotProject(join.left()))
                .then(topSemiJoin -> {
                    LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topSemiJoin.left();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = project.child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    // push topSemiJoin down project, so we need replace conjuncts by project.
                    Pair<List<Expression>, List<Expression>> conjuncts = replaceConjuncts(topSemiJoin, project);
                    Set<ExprId> conjunctsIds = Stream.concat(conjuncts.first.stream(), conjuncts.second.stream())
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

                        Plan newBottomSemiJoin = topSemiJoin.withConjunctsChildren(conjuncts.first, conjuncts.second,
                                a, c);
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
                         *  bottomJoin  C     -->       A     newBottomSemiJoin
                         *   /    \                               /      \
                         *  A      B                             B       C
                         */
                        // LEFT_OUTER_JOIN should be eliminated in rewrite phase
                        Preconditions.checkState(bottomJoin.getJoinType() != JoinType.LEFT_OUTER_JOIN);

                        Plan newBottomSemiJoin = topSemiJoin.withConjunctsChildren(conjuncts.first, conjuncts.second,
                                b, c);
                        Plan newTopJoin = bottomJoin.withChildren(a, newBottomSemiJoin);
                        return project.withChildren(newTopJoin);
                    }
                }).toRule(RuleType.LOGICAL_SEMI_JOIN_LOGICAL_JOIN_TRANSPOSE_PROJECT);
    }

    private Pair<List<Expression>, List<Expression>> replaceConjuncts(LogicalJoin<? extends Plan, ? extends Plan> join,
            LogicalProject<? extends Plan> project) {
        Map<ExprId, Slot> outputToInput = new HashMap<>();
        for (NamedExpression outputExpr : project.getProjects()) {
            Set<Slot> usedSlots = outputExpr.getInputSlots();
            Preconditions.checkState(usedSlots.size() == 1);
            Slot inputSlot = usedSlots.iterator().next();
            outputToInput.put(outputExpr.getExprId(), inputSlot);
        }
        List<Expression> topHashConjuncts =
                JoinReorderUtils.replaceJoinConjuncts(join.getHashJoinConjuncts(), outputToInput);
        List<Expression> topOtherConjuncts =
                JoinReorderUtils.replaceJoinConjuncts(join.getOtherJoinConjuncts(), outputToInput);
        return Pair.of(topHashConjuncts, topOtherConjuncts);
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
