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
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Common function for RightAssociate
 */
class JoinRightAssociateHelper extends ThreeJoinHelper {
    //       topJoin        newTopJoin
    //       /     \         /     \
    //  bottomJoin  C  ->   A   newBottomJoin
    //   /    \                     /    \
    //  A      B                   B      C
    public static Set<Pair<JoinType, JoinType>> outerSet = ImmutableSet.of(
            Pair.of(JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN),
            Pair.of(JoinType.LEFT_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN));

    public JoinRightAssociateHelper(LogicalJoin<? extends Plan, GroupPlan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        super(topJoin, bottomJoin, bottomJoin.left(), bottomJoin.right(), topJoin.right());
    }

    /**
     * Create newTopJoin.
     */
    public LogicalJoin<? extends Plan, ? extends Plan> newTopJoin() {
        Pair<List<NamedExpression>, List<NamedExpression>> projectPair = splitProjectExprs(aOutput);
        List<NamedExpression> newLeftProjectExpr = projectPair.first;
        List<NamedExpression> newRightProjectExprs = projectPair.second;

        LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                newBottomHashJoinConjuncts, ExpressionUtils.andByOptional(newBottomNonHashJoinConjuncts), b, c);
        Plan left = JoinReorderCommon.project(newRightProjectExprs, a).orElse(a);
        Plan right = JoinReorderCommon.project(newLeftProjectExpr, newBottomJoin).orElse(newBottomJoin);

        return new LogicalJoin<>(bottomJoin.getJoinType(), newTopHashJoinConjuncts,
                ExpressionUtils.andByOptional(newTopNonHashJoinConjuncts), left, right);
    }


    /**
     * Check JoinReorderContext.
     */
    public static boolean check(LogicalJoin<? extends Plan, GroupPlan> topJoin) {
        return !topJoin.getJoinReorderContext().hasCommute() && !topJoin.getJoinReorderContext().hasRightAssociate()
                && !topJoin.getJoinReorderContext().hasRightAssociate() && !topJoin.getJoinReorderContext()
                .hasExchange();
    }
}
