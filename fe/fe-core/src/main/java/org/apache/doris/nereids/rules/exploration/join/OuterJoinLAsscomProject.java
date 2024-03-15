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
import org.apache.doris.nereids.rules.exploration.CBOUtils;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.Set;

/**
 * Rule for change inner join LAsscom (associative and commutive).
 */
public class OuterJoinLAsscomProject extends OneExplorationRuleFactory {
    public static final OuterJoinLAsscomProject INSTANCE = new OuterJoinLAsscomProject();

    /*
     *        topJoin                   newTopJoin
     *        /     \                   /        \
     *    project    C          newLeftProject newRightProject
     *      /            ──►          /            \
     * bottomJoin                newBottomJoin      B
     *    /   \                     /   \
     *   A     B                   A     C
     */
    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), group())
                .when(join -> OuterJoinLAsscom.VALID_TYPE_PAIR_SET.contains(
                        Pair.of(join.left().child().getJoinType(), join.getJoinType())))
                .when(topJoin -> OuterJoinLAsscom.checkReorder(topJoin, topJoin.left().child()))
                .whenNot(join -> join.hasDistributeHint() || join.left().child().hasDistributeHint())
                .when(topJoin -> OuterJoinLAsscom.checkCondition(topJoin,
                        topJoin.left().child().right().getOutputExprIdSet()))
                .when(join -> join.left().isAllSlots())
                .then(topJoin -> {
                    /* ********** init ********** */
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    /* ********** new Plan ********** */
                    LogicalJoin newBottomJoin = topJoin.withChildrenNoContext(a, c, null);
                    newBottomJoin.getJoinReorderContext().copyFrom(bottomJoin.getJoinReorderContext());
                    newBottomJoin.getJoinReorderContext().setHasLAsscom(false);
                    newBottomJoin.getJoinReorderContext().setHasCommute(false);

                    Set<ExprId> topUsedExprIds = new HashSet<>(topJoin.getOutputExprIdSet());
                    bottomJoin.getHashJoinConjuncts().forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                    bottomJoin.getOtherJoinConjuncts().forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, newBottomJoin);
                    Plan right = CBOUtils.newProject(topUsedExprIds, b);

                    LogicalJoin newTopJoin = bottomJoin.withChildrenNoContext(left, right, null);
                    newTopJoin.getJoinReorderContext().copyFrom(topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasLAsscom(true);

                    return CBOUtils.projectOrSelf(ImmutableList.copyOf(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_LASSCOM_PROJECT);
    }
}
