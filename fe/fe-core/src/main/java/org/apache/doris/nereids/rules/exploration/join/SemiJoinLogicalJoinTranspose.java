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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

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
    @Override
    public Rule build() {
        return leftSemiLogicalJoin(logicalJoin(), group())
                .when(this::conditionChecker)
                .then(topSemiJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topSemiJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    boolean lasscom = bottomJoin.getOutputSet().containsAll(a.getOutput());

                    if (lasscom) {
                        /*
                         *    topSemiJoin                newTopJoin
                         *      /     \                 /         \
                         * bottomJoin  C   -->  newBottomSemiJoin  B
                         *  /    \                  /    \
                         * A      B                A      C
                         */
                        LogicalJoin<GroupPlan, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(),
                                topSemiJoin.getHashJoinConjuncts(), topSemiJoin.getOtherJoinCondition(), a, c);
                        return new LogicalJoin<>(bottomJoin.getJoinType(), bottomJoin.getHashJoinConjuncts(),
                                bottomJoin.getOtherJoinCondition(), newBottomSemiJoin, b);
                    } else {
                        /*
                         *    topSemiJoin            newTopJoin
                         *      /     \             /         \
                         * bottomJoin  C   -->     A   newBottomSemiJoin
                         *  /    \                         /      \
                         * A      B                       B        C
                         */
                        LogicalJoin<GroupPlan, GroupPlan> newBottomSemiJoin = new LogicalJoin<>(
                                topSemiJoin.getJoinType(),
                                topSemiJoin.getHashJoinConjuncts(), topSemiJoin.getOtherJoinCondition(), b, c);
                        return new LogicalJoin<>(bottomJoin.getJoinType(), bottomJoin.getHashJoinConjuncts(),
                                bottomJoin.getOtherJoinCondition(), a, newBottomSemiJoin);
                    }
                }).toRule(RuleType.LOGICAL_JOIN_L_ASSCOM);
    }

    // bottomJoin just return A OR B, else return false.
    private boolean conditionChecker(LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan> topJoin) {
        Set<Slot> bottomOutputSet = topJoin.left().getOutputSet();

        Set<Slot> aOutputSet = topJoin.left().left().getOutputSet();
        Set<Slot> bOutputSet = topJoin.left().right().getOutputSet();

        return !ExpressionUtils.isIntersecting(bottomOutputSet, aOutputSet)
                && !ExpressionUtils.isIntersecting(bottomOutputSet, bOutputSet);
    }
}
