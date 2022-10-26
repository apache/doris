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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Common function for JoinLAsscom
 */
class JoinLAsscomHelper extends ThreeJoinHelper {
    /*
     *      topJoin                newTopJoin
     *      /     \                 /     \
     * bottomJoin  C   -->  newBottomJoin  B
     *  /    \                  /    \
     * A      B                A      C
     */

    /**
     * Init plan and output.
     */
    public JoinLAsscomHelper(LogicalJoin<? extends Plan, GroupPlan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        super(topJoin, bottomJoin, bottomJoin.left(), bottomJoin.right(), topJoin.right());
        if (topJoin.left() instanceof LogicalProject) {
            insideProjects.addAll(((LogicalProject<? extends Plan>) topJoin.left()).getProjects());
        }
    }

    /**
     * Create newTopJoin.
     */
    public Plan newTopJoin() {
        // Split bottomJoinProject into two part.
        Map<Boolean, List<NamedExpression>> projectExprsMap = insideProjects.stream()
                .collect(Collectors.partitioningBy(projectExpr -> {
                    Set<Slot> usedSlots = projectExpr.collect(Slot.class::isInstance);
                    return bOutputSet.containsAll(usedSlots);
                }));
        List<NamedExpression> newLeftProjects = projectExprsMap.get(Boolean.FALSE);
        List<NamedExpression> newRightProjects = projectExprsMap.get(Boolean.TRUE);

        // Add all slots used by hashOnCondition when projects not empty.
        // TODO: Does nonHashOnCondition also need to be considered.
        Map<Boolean, Set<Slot>> onUsedSlots = bottomJoin.getHashJoinConjuncts().stream()
                .flatMap(onExpr -> {
                    Set<Slot> usedSlotRefs = onExpr.collect(Slot.class::isInstance);
                    return usedSlotRefs.stream();
                }).collect(Collectors.partitioningBy(bOutputSet::contains, Collectors.toSet()));
        Set<Slot> leftUsedSlots = onUsedSlots.get(Boolean.FALSE);
        Set<Slot> rightUsedSlots = onUsedSlots.get(Boolean.TRUE);

        JoinUtils.addSlotsUsedByOn(rightUsedSlots, newRightProjects);
        JoinUtils.addSlotsUsedByOn(leftUsedSlots, newLeftProjects);

        if (!newLeftProjects.isEmpty()) {
            newLeftProjects.addAll(cOutputSet);
        }
        LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                newBottomHashJoinConjuncts, newBottomNonHashJoinConjuncts, a, c,
                bottomJoin.getJoinReorderContext());
        newBottomJoin.getJoinReorderContext().setHasLAsscom(false);
        newBottomJoin.getJoinReorderContext().setHasCommute(false);

        Plan left = newBottomJoin;
        if (!newLeftProjects.stream().map(NamedExpression::toSlot).collect(Collectors.toSet())
                .equals(newBottomJoin.getOutputSet())) {
            left = PlanUtils.projectOrSelf(newLeftProjects, newBottomJoin);
        }
        Plan right = b;
        if (!newRightProjects.stream().map(NamedExpression::toSlot).collect(Collectors.toSet())
                .equals(b.getOutputSet())) {
            right = PlanUtils.projectOrSelf(newRightProjects, b);
        }

        LogicalJoin<Plan, Plan> newTopJoin = new LogicalJoin<>(bottomJoin.getJoinType(),
                newTopHashJoinConjuncts, newTopNonHashJoinConjuncts, left, right,
                topJoin.getJoinReorderContext());
        newTopJoin.getJoinReorderContext().setHasLAsscom(true);

        if (topJoin.getLogicalProperties().equals(newTopJoin.getLogicalProperties())) {
            return newTopJoin;
        }

        return PlanUtils.projectOrSelf(new ArrayList<>(topJoin.getOutput()), newTopJoin);
    }

    // TODO: consider nonHashJoinCondition.
    public boolean initJoinOnCondition() {
        // top: (A B)(forbidden) (A C) (B C) (A B C)
        // Split topJoin hashCondition to two part according to include B.
        Map<Boolean, List<Expression>> on = topJoin.getHashJoinConjuncts().stream()
                .collect(Collectors.partitioningBy(topHashOn -> {
                    Set<Slot> usedSlot = topHashOn.collect(Slot.class::isInstance);
                    Preconditions.checkArgument(
                            !(ExpressionUtils.isIntersecting(aOutputSet, usedSlot) && ExpressionUtils.isIntersecting(
                                    bOutputSet, usedSlot)));
                    return ExpressionUtils.isIntersecting(bOutputSet, usedSlot);
                }));

        if (!insideProjects.isEmpty()) {
            Set<Slot> inputSlots = insideProjects.get(0).getInputSlots();
            Preconditions.checkState(!inputSlots.isEmpty());
        }

        // don't include B, it means that just include (A C)
        // we add it into newBottomJoin HashJoinConjuncts.
        newBottomHashJoinConjuncts = on.get(false);
        if (newBottomHashJoinConjuncts.size() == 0) {
            return false;
        }
        newTopHashJoinConjuncts = on.get(true);
        newTopHashJoinConjuncts.addAll(bottomJoin.getHashJoinConjuncts());
        Preconditions.checkState(!newTopHashJoinConjuncts.isEmpty(),
                "LAsscom newTopHashJoinConjuncts join can't empty");

        return true;
    }
}
