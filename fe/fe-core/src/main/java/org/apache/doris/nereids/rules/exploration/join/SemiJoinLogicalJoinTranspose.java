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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Set;

/**
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(SemiJoin(X, Z), Y)
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
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
        return logicalJoin(logicalJoin(), group())
                .when(topJoin -> (topJoin.getJoinType().isLeftSemiOrAntiJoin()
                        && (topJoin.left().getJoinType().isInnerJoin()
                                || topJoin.left().getJoinType().isLeftOuterJoin()
                                || topJoin.left().getJoinType().isRightOuterJoin())))
                .whenNot(topJoin -> topJoin.left().getJoinType().isSemiOrAntiJoin())
                .when(this::conditionChecker)
                .whenNot(topJoin -> topJoin.hasJoinHint() || topJoin.left().hasJoinHint())
                .whenNot(LogicalJoin::isMarkJoin)
                .then(topSemiJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topSemiJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    List<Expression> hashJoinConjuncts = topSemiJoin.getHashJoinConjuncts();
                    Set<ExprId> aOutputExprIdSet = a.getOutputExprIdSet();

                    boolean lasscom = false;
                    for (Expression hashJoinConjunct : hashJoinConjuncts) {
                        Set<ExprId> usedSlotExprIds = hashJoinConjunct.getInputSlotExprIds();
                        lasscom = Utils.isIntersecting(usedSlotExprIds, aOutputExprIdSet) || lasscom;
                    }

                    if (lasscom) {
                        /*
                         *    topSemiJoin                newTopJoin
                         *      /     \                 /         \
                         * bottomJoin  C   -->  newBottomSemiJoin  B
                         *  /    \                  /    \
                         * A      B                A      C
                         */
                        // RIGHT_OUTER_JOIN should be eliminated in rewrite phase
                        Preconditions.checkState(bottomJoin.getJoinType() != JoinType.RIGHT_OUTER_JOIN);

                        Plan newBottomSemiJoin = topSemiJoin.withChildren(a, c);
                        return bottomJoin.withChildren(newBottomSemiJoin, b);
                    } else {
                        /*
                         *    topSemiJoin            newTopJoin
                         *      /     \             /         \
                         * bottomJoin  C   -->     A   newBottomSemiJoin
                         *  /    \                         /      \
                         * A      B                       B        C
                         */
                        // LEFT_OUTER_JOIN should be eliminated in rewrite phase
                        Preconditions.checkState(bottomJoin.getJoinType() != JoinType.LEFT_OUTER_JOIN);

                        Plan newBottomSemiJoin = topSemiJoin.withChildren(b, c);
                        return bottomJoin.withChildren(a, newBottomSemiJoin);
                    }
                }).toRule(RuleType.LOGICAL_SEMI_JOIN_LOGICAL_JOIN_TRANSPOSE);
    }

    // bottomJoin just return A OR B, else return false.
    private boolean conditionChecker(LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan> topSemiJoin) {
        List<Expression> hashJoinConjuncts = topSemiJoin.getHashJoinConjuncts();

        Set<ExprId> aOutputExprIdSet = topSemiJoin.left().left().getOutputExprIdSet();
        Set<ExprId> bOutputExprIdSet = topSemiJoin.left().right().getOutputExprIdSet();

        boolean hashContainsA = false;
        boolean hashContainsB = false;
        for (Expression hashJoinConjunct : hashJoinConjuncts) {
            Set<ExprId> usedSlotExprIds = hashJoinConjunct.getInputSlotExprIds();
            hashContainsA = Utils.isIntersecting(usedSlotExprIds, aOutputExprIdSet) || hashContainsA;
            hashContainsB = Utils.isIntersecting(usedSlotExprIds, bOutputExprIdSet) || hashContainsB;
        }
        if (leftDeep && hashContainsB) {
            return false;
        }
        Preconditions.checkState(hashContainsA || hashContainsB, "join output must contain child");
        return !(hashContainsA && hashContainsB);
    }
}
