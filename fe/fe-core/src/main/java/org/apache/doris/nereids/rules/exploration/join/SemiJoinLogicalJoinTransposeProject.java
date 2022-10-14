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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
                .when(topJoin -> topJoin.getJoinType() == JoinType.LEFT_SEMI_JOIN
                        || topJoin.getJoinType() == JoinType.LEFT_ANTI_JOIN)
                .whenNot(topJoin -> topJoin.left().child().getJoinType().isSemiOrAntiJoin())
                .when(this::conditionChecker)
                .then(topSemiJoin -> {
                    LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> project = topSemiJoin.left();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = project.child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    Set<Slot> aOutputSet = a.getOutputSet();

                    List<Expression> hashJoinConjuncts = topSemiJoin.getHashJoinConjuncts();

                    boolean lasscom = false;
                    for (Expression hashJoinConjunct : hashJoinConjuncts) {
                        Set<Slot> usedSlot = hashJoinConjunct.collect(Slot.class::isInstance);
                        lasscom = ExpressionUtils.isIntersecting(usedSlot, aOutputSet) || lasscom;
                    }

                    if (lasscom) {
                        /*-
                         *     topSemiJoin                    project
                         *      /     \                         |
                         *   project   C                    newTopJoin
                         *      |            ->            /         \
                         *  bottomJoin            newBottomSemiJoin   B
                         *   /    \                    /      \
                         *  A      B                  A        C
                         */
                        LogicalJoin<GroupPlan, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(), topSemiJoin.getHashJoinConjuncts(),
                                topSemiJoin.getOtherJoinConjuncts(), a, c);

                        LogicalJoin<Plan, Plan> newTopJoin = new LogicalJoin<>(bottomJoin.getJoinType(),
                                bottomJoin.getHashJoinConjuncts(), bottomJoin.getOtherJoinConjuncts(),
                                newBottomSemiJoin, b);

                        return new LogicalProject<>(new ArrayList<>(topSemiJoin.getOutput()), newTopJoin);
                    } else {
                        /*-
                         *     topSemiJoin                  project
                         *       /     \                       |
                         *    project   C                  newTopJoin
                         *       |                        /         \
                         *  bottomJoin  C     -->       A     newBottomSemiJoin
                         *   /    \                               /      \
                         *  A      B                             B       C
                         */
                        LogicalJoin<GroupPlan, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(), topSemiJoin.getHashJoinConjuncts(),
                                topSemiJoin.getOtherJoinConjuncts(), b, c);

                        LogicalJoin<Plan, Plan> newTopJoin = new LogicalJoin<>(bottomJoin.getJoinType(),
                                bottomJoin.getHashJoinConjuncts(), bottomJoin.getOtherJoinConjuncts(),
                                a, newBottomSemiJoin);

                        return new LogicalProject<>(new ArrayList<>(topSemiJoin.getOutput()), newTopJoin);
                    }
                }).toRule(RuleType.LOGICAL_SEMI_JOIN_LOGICAL_JOIN_TRANSPOSE_PROJECT);
    }

    // project of bottomJoin just return A OR B, else return false.
    private boolean conditionChecker(
            LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>, GroupPlan> topSemiJoin) {
        List<Expression> hashJoinConjuncts = topSemiJoin.getHashJoinConjuncts();

        List<Slot> aOutput = topSemiJoin.left().child().left().getOutput();
        List<Slot> bOutput = topSemiJoin.left().child().right().getOutput();

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
