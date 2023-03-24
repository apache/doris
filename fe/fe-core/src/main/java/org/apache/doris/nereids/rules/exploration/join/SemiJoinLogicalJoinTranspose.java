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
import org.apache.doris.nereids.rules.exploration.join.SemiJoinLogicalJoinTransposeProject.ContainsType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;

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
                .whenNot(topJoin -> topJoin.hasJoinHint() || topJoin.left().hasJoinHint())
                .whenNot(LogicalJoin::isMarkJoin)
                .then(topSemiJoin -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topSemiJoin.left();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topSemiJoin.right();

                    Set<ExprId> conjunctsIds = topSemiJoin.getConditionExprId();
                    ContainsType containsType = SemiJoinLogicalJoinTransposeProject.containsChildren(conjunctsIds,
                            a.getOutputExprIdSet(), b.getOutputExprIdSet());
                    if (containsType == ContainsType.ALL) {
                        return null;
                    }
                    if (containsType == ContainsType.LEFT) {
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
                        if (leftDeep) {
                            return null;
                        }
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
}
