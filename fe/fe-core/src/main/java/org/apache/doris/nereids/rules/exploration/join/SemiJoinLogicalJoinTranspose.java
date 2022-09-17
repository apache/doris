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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
public class SemiJoinLogicalJoinTranspose extends OneExplorationRuleFactory {

    public static final SemiJoinLogicalJoinTranspose LEFT_DEEP = new SemiJoinLogicalJoinTranspose(true);

    public static final SemiJoinLogicalJoinTranspose ALL = new SemiJoinLogicalJoinTranspose(false);

    private final boolean leftDeep;

    public SemiJoinLogicalJoinTranspose(boolean leftDeep) {
        this.leftDeep = leftDeep;
    }

    @Override
    public Rule build() {
        return leftSemiLogicalJoin(logicalJoin(), group())
                .whenNot(topJoin -> topJoin.left().getJoinType().isSemiOrAntiJoin())
                .when(this::conditionChecker)
                .then(topSemiJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topSemiJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    List<Expression> hashJoinConjuncts = topSemiJoin.getHashJoinConjuncts();
                    Set<Slot> aOutputSet = a.getOutputSet();

                    boolean lasscom = false;
                    for (Expression hashJoinConjunct : hashJoinConjuncts) {
                        Set<Slot> usedSlot = hashJoinConjunct.collect(Slot.class::isInstance);
                        lasscom = ExpressionUtils.isIntersecting(usedSlot, aOutputSet) || lasscom;
                    }

                    // Merge topProjects-bottomProjects -> newTopProjects
                    List<NamedExpression> newTopProjects = JoinReorderUtil.mergeProjects(topSemiJoin.getProjects(),
                            bottomJoin.getProjects());
                    List<NamedExpression> newBottomProjects = ImmutableList.of();
                    if (lasscom) {
                        /* if exists inside-project, we will merge topProjects and bottomProjects.
                         *       topProjects                     topProjects
                         *       topSemiJoin                    bottomProjects
                         *       /         \                      newTopJoin
                         * bottomProjects  C                    /         \
                         *   bottomJoin            -->  newBottomSemiJoin   B
                         *    /    \                        /      \
                         *   A      B                      A       C
                         */
                        LogicalJoin<GroupPlan, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(), topSemiJoin.getHashJoinConjuncts(),
                                topSemiJoin.getOtherJoinCondition(), newBottomProjects, a, c,
                                topSemiJoin.getJoinReorderContext());

                        return new LogicalJoin<>(bottomJoin.getJoinType(), bottomJoin.getHashJoinConjuncts(),
                                bottomJoin.getOtherJoinCondition(), newTopProjects, newBottomSemiJoin, b,
                                bottomJoin.getJoinReorderContext());
                    } else {
                        /* if exists inside-project, we will merge topProject and inside-project.
                         *       topProjects                  topProjects
                         *       topSemiJoin                 newTopProjects
                         *       /         \                   newTopJoin
                         *  bottomProjects  C                 /         \
                         *   bottomJoin            -->       A    newBottomSemiJoin
                         *    /    \                                  /      \
                         *   A      B                                B       C
                         */
                        LogicalJoin<GroupPlan, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(), topSemiJoin.getHashJoinConjuncts(),
                                topSemiJoin.getOtherJoinCondition(), newBottomProjects, b, c,
                                topSemiJoin.getJoinReorderContext());

                        return new LogicalJoin<>(bottomJoin.getJoinType(), bottomJoin.getHashJoinConjuncts(),
                                bottomJoin.getOtherJoinCondition(), newTopProjects, a, newBottomSemiJoin,
                                bottomJoin.getJoinReorderContext());
                    }
                }).toRule(RuleType.LOGICAL_SEMI_JOIN_LOGICAL_JOIN_TRANSPOSE);
    }

    // bottomJoin just return A OR B, else return false.
    private boolean conditionChecker(LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan> topSemiJoin) {
        List<Expression> hashJoinConjuncts = topSemiJoin.getHashJoinConjuncts();

        List<Slot> aOutput = topSemiJoin.left().left().getOutput();
        List<Slot> bOutput = topSemiJoin.left().right().getOutput();

        boolean hashContainsA = false;
        boolean hashContainsB = false;
        for (Expression hashJoinConjunct : hashJoinConjuncts) {
            Set<Slot> usedSlot = hashJoinConjunct.collect(Slot.class::isInstance);
            hashContainsA = ExpressionUtils.isIntersecting(usedSlot, aOutput) || hashContainsA;
            hashContainsB = ExpressionUtils.isIntersecting(usedSlot, bOutput) || hashContainsB;
        }
        if (leftDeep && hashContainsB) {
            return false;
        }
        Preconditions.checkState(hashContainsA || hashContainsB, "join output must contain child");
        return !(hashContainsA && hashContainsB);
    }
}
