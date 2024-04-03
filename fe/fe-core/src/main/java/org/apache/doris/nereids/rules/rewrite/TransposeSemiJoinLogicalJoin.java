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
import org.apache.doris.nereids.rules.rewrite.TransposeSemiJoinLogicalJoinProject.ContainsType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.qe.ConnectContext;

import java.util.Set;

/**
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(SemiJoin(X, Z), Y)
 * <li>SemiJoin(LogicalJoin(X, Y), Z) -> LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
 */
public class TransposeSemiJoinLogicalJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin(logicalJoin(), any())
                .whenNot(join -> ConnectContext.get().getSessionVariable().isDisableJoinReorder())
                .when(topJoin -> (topJoin.getJoinType().isLeftSemiOrAntiJoin()
                        && (topJoin.left().getJoinType().isInnerJoin()
                        || topJoin.left().getJoinType().isLeftOuterJoin()
                        || topJoin.left().getJoinType().isRightOuterJoin())))
                .whenNot(topJoin -> topJoin.hasDistributeHint() || topJoin.left().hasDistributeHint())
                .whenNot(topJoin -> topJoin.isLeadingJoin() || topJoin.left().isLeadingJoin())
                .then(topSemiJoin -> {
                    LogicalJoin<Plan, Plan> bottomJoin = topSemiJoin.left();
                    Plan a = bottomJoin.left();
                    Plan b = bottomJoin.right();
                    Plan c = topSemiJoin.right();

                    Set<ExprId> conjunctsIds = topSemiJoin.getConditionExprId();
                    ContainsType containsType = TransposeSemiJoinLogicalJoinProject.containsChildren(conjunctsIds,
                            a.getOutputExprIdSet(), b.getOutputExprIdSet());
                    if (containsType == ContainsType.ALL || containsType == ContainsType.NONE) {
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
                        if (bottomJoin.getJoinType() == JoinType.RIGHT_OUTER_JOIN) {
                            return null;
                        }
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
                        if (bottomJoin.getJoinType() == JoinType.LEFT_OUTER_JOIN) {
                            return null;
                        }
                        Plan newBottomSemiJoin = topSemiJoin.withChildren(b, c);
                        return bottomJoin.withChildren(a, newBottomSemiJoin);
                    }
                }).toRule(RuleType.TRANSPOSE_LOGICAL_SEMI_JOIN_LOGICAL_JOIN);
    }
}
