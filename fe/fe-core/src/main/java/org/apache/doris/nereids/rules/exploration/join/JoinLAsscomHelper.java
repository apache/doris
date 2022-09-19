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

import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import java.util.ArrayList;
import java.util.HashSet;
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
    }

    /**
     * Create newTopJoin.
     */
    public Plan newTopJoin() {
        // Split bottomJoinProject into two part.
        Map<Boolean, List<NamedExpression>> projectExprsMap = bottomProjects.stream()
                .collect(Collectors.partitioningBy(projectExpr -> {
                    Set<Slot> usedSlots = projectExpr.collect(Slot.class::isInstance);
                    return bOutputSet.containsAll(usedSlots);
                }));
        List<NamedExpression> newLeftProjects = projectExprsMap.get(Boolean.FALSE);
        List<NamedExpression> newRightProjects = projectExprsMap.get(Boolean.TRUE);

        // Add all slots used by hashOnCondition when projects not empty.
        // TODO: Does nonHashOnCondition also need to be considered.
        Map<Boolean, List<Slot>> onUsedSlots = bottomJoin.getHashJoinConjuncts().stream()
                .flatMap(onExpr -> {
                    Set<Slot> usedSlotRefs = onExpr.collect(Slot.class::isInstance);
                    return usedSlotRefs.stream();
                }).collect(Collectors.partitioningBy(bOutputSet::contains));
        List<Slot> leftUsedSlots = onUsedSlots.get(Boolean.FALSE);
        List<Slot> rightUsedSlots = onUsedSlots.get(Boolean.TRUE);

        addSlotsUsedByOn(rightUsedSlots, newRightProjects);
        addSlotsUsedByOn(leftUsedSlots, newLeftProjects);

        if (!newLeftProjects.isEmpty()) {
            newLeftProjects.addAll(cOutputSet);
        }
        LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                newBottomHashJoinConjuncts, ExpressionUtils.optionalAnd(newBottomNonHashJoinConjuncts), a, c,
                bottomJoin.getJoinReorderContext());
        newBottomJoin.getJoinReorderContext().setHasLAsscom(false);
        newBottomJoin.getJoinReorderContext().setHasCommute(false);

        Plan left = PlanUtils.projectOrSelf(newLeftProjects, newBottomJoin);
        Plan right = PlanUtils.projectOrSelf(newRightProjects, b);

        LogicalJoin<Plan, Plan> newTopJoin = new LogicalJoin<>(bottomJoin.getJoinType(),
                newTopHashJoinConjuncts,
                ExpressionUtils.optionalAnd(newTopNonHashJoinConjuncts), left, right,
                topJoin.getJoinReorderContext());
        newTopJoin.getJoinReorderContext().setHasLAsscom(true);

        return PlanUtils.projectOrSelf(new ArrayList<>(topJoin.getOutput()), newTopJoin);
    }

    // When project not empty, we add all slots used by hashOnCondition into projects.
    private void addSlotsUsedByOn(List<Slot> usedSlots, List<NamedExpression> projects) {
        if (projects.isEmpty()) {
            return;
        }
        Set<NamedExpression> projectsSet = new HashSet<>(projects);
        usedSlots.forEach(slot -> {
            if (!projectsSet.contains(slot)) {
                projects.add(slot);
            }
        });
    }
}
