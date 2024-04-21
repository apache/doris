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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

/**
 * OuterJoinAssocProject.
 */
public class OuterJoinAssocProject extends OneExplorationRuleFactory {
    /*
     *        topJoin        newTopJoin
     *        /     \         /     \
     *   bottomJoin  C  ->   A   newBottomJoin
     *    /    \                     /    \
     *   A      B                   B      C
     */
    public static final OuterJoinAssocProject INSTANCE = new OuterJoinAssocProject();

    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), group())
                .when(join -> OuterJoinAssoc.VALID_TYPE_PAIR_SET.contains(
                        Pair.of(join.left().child().getJoinType(), join.getJoinType())))
                .when(topJoin -> OuterJoinLAsscom.checkReorder(topJoin, topJoin.left().child()))
                .whenNot(join -> join.hasDistributeHint() || join.left().child().hasDistributeHint())
                .when(join -> OuterJoinAssoc.checkCondition(join, join.left().child().left().getOutputSet()))
                .when(join -> join.left().isAllSlots())
                .thenApply(ctx -> {
                    LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>, GroupPlan> topJoin = ctx.root;
                    /* ********** init ********** */
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();

                    /*
                     * Paper `On the Correct and Complete Enumeration of the Core Search Space`.
                     * p23 need to reject nulls on A(e2) (Eqv. 1).
                     * It means that when slot is null, condition must return false or unknown.
                     */
                    if (bottomJoin.getJoinType().isLeftOuterJoin() && topJoin.getJoinType().isLeftOuterJoin()) {
                        Set<Slot> conditionSlot = topJoin.getConditionSlot();
                        Set<Expression> on = ImmutableSet.<Expression>builder()
                                .addAll(topJoin.getHashJoinConjuncts())
                                .addAll(topJoin.getOtherJoinConjuncts()).build();
                        Set<Slot> notNullSlots = ExpressionUtils.inferNotNullSlots(on,
                                ctx.cascadesContext);
                        if (conditionSlot.isEmpty() || !conditionSlot.equals(notNullSlots)) {
                            return null;
                        }
                    }

                    /* ********** new Plan ********** */
                    LogicalJoin newBottomJoin = topJoin.withChildrenNoContext(b, c, null);
                    newBottomJoin.getJoinReorderContext().copyFrom(bottomJoin.getJoinReorderContext());

                    Set<ExprId> topUsedExprIds = new HashSet<>(topJoin.getOutputExprIdSet());
                    bottomJoin.getHashJoinConjuncts().forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                    bottomJoin.getOtherJoinConjuncts().forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                    Plan left = CBOUtils.newProject(topUsedExprIds, a);
                    Plan right = CBOUtils.newProject(topUsedExprIds, newBottomJoin);

                    LogicalJoin newTopJoin = bottomJoin.withChildrenNoContext(left, right, null);
                    newTopJoin.getJoinReorderContext().copyFrom(topJoin.getJoinReorderContext());
                    OuterJoinAssoc.setReorderContext(newTopJoin, newBottomJoin);

                    return CBOUtils.projectOrSelf(ImmutableList.copyOf(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_ASSOC_PROJECT);
    }
}
