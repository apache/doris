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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;

/**
 * Common function for LeftAssociate
 */
class JoinLeftAssociateHelper extends ThreeJoinHelper {
    /*
     *    topJoin                  newTopJoin
     *    /     \                  /        \
     *   A    bottomJoin  ->  newBottomJoin  C
     *           /    \        /    \
     *          B      C      A      B
     */
    public JoinLeftAssociateHelper(LogicalJoin<GroupPlan, ? extends Plan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        super(topJoin, bottomJoin, topJoin.left(), bottomJoin.left(), bottomJoin.right());
    }

    public static JoinLeftAssociateHelper of(LogicalJoin<GroupPlan, ? extends Plan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        return new JoinLeftAssociateHelper(topJoin, bottomJoin);
    }

    /**
     * Create newTopJoin.
     */
    public LogicalJoin<? extends Plan, ? extends Plan> newTopJoin() {
        Pair<List<NamedExpression>, List<NamedExpression>> projectPair = splitProjectExprs(cOutput);
        List<NamedExpression> newLeftProjectExpr = projectPair.first;
        List<NamedExpression> newRightProjectExprs = projectPair.second;

        LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                newBottomHashJoinConjuncts, ExpressionUtils.andByOptional(newBottomNonHashJoinConjuncts), a, b);
        Plan left = JoinReorderCommon.project(newLeftProjectExpr, newBottomJoin).orElse(newBottomJoin);
        Plan right = JoinReorderCommon.project(newRightProjectExprs, c).orElse(c);

        return new LogicalJoin<>(bottomJoin.getJoinType(), newTopHashJoinConjuncts,
                ExpressionUtils.andByOptional(newTopNonHashJoinConjuncts), left, right);
    }

    /**
     * Check JoinReorderContext.
     */
    public static boolean check(LogicalJoin<GroupPlan, ? extends Plan> topJoin) {
        return !topJoin.getJoinReorderContext().isHasCommute()
                && !topJoin.getJoinReorderContext().isHasLeftAssociate()
                && !topJoin.getJoinReorderContext().isHasRightAssociate()
                && !topJoin.getJoinReorderContext().isHasExchange();
    }
}
