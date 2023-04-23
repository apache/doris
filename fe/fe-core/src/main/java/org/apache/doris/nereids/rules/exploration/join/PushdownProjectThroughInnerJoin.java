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
import org.apache.doris.nereids.rules.exploration.CBOUtils;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * rule for pushdown project through inner/outer join
 */
public class PushdownProjectThroughInnerJoin extends OneExplorationRuleFactory {
    public static final PushdownProjectThroughInnerJoin INSTANCE = new PushdownProjectThroughInnerJoin();

    /*
     *    Project                   Join
     *      |            ──►       /    \
     *     Join               Project  Project
     *    /   \                  |       |
     *   A     B                 A       B
     */
    @Override
    public Rule build() {
        return logicalProject(logicalJoin())
            .whenNot(LogicalProject::isAllSlots)
            .when(project -> project.child().getJoinType().isInnerJoin())
            .whenNot(project -> project.child().hasJoinHint())
            .then(project -> {
                LogicalJoin<GroupPlan, GroupPlan> join = project.child();
                Set<ExprId> aOutputExprIdSet = join.left().getOutputExprIdSet();
                Set<ExprId> bOutputExprIdSet = join.right().getOutputExprIdSet();

                // reject hyper edge in Project.
                if (!project.getProjects().stream().allMatch(expr -> {
                    Set<ExprId> inputSlotExprIds = expr.getInputSlotExprIds();
                    return aOutputExprIdSet.containsAll(inputSlotExprIds)
                            || bOutputExprIdSet.containsAll(inputSlotExprIds);
                })) {
                    return null;
                }

                List<NamedExpression> aProjects = new ArrayList<>();
                List<NamedExpression> bProjects = new ArrayList<>();
                for (NamedExpression namedExpression : project.getProjects()) {
                    Set<ExprId> usedExprIds = namedExpression.getInputSlotExprIds();
                    if (aOutputExprIdSet.containsAll(usedExprIds)) {
                        aProjects.add(namedExpression);
                    } else {
                        bProjects.add(namedExpression);
                    }
                }

                boolean leftContains = aProjects.stream().anyMatch(e -> !(e instanceof Slot));
                boolean rightContains = bProjects.stream().anyMatch(e -> !(e instanceof Slot));
                // due to JoinCommute, we don't need to consider just right contains.
                if (!leftContains) {
                    return null;
                }

                Builder<NamedExpression> newAProject = ImmutableList.<NamedExpression>builder().addAll(aProjects);
                Set<Slot> aConditionSlots = CBOUtils.joinChildConditionSlots(join, true);
                Set<Slot> aProjectSlots = aProjects.stream().map(NamedExpression::toSlot).collect(Collectors.toSet());
                aConditionSlots.stream().filter(slot -> !aProjectSlots.contains(slot)).forEach(newAProject::add);
                Plan newLeft = CBOUtils.projectOrSelf(newAProject.build(), join.left());

                if (!rightContains) {
                    Plan newJoin = join.withChildrenNoContext(newLeft, join.right());
                    return CBOUtils.projectOrSelf(new ArrayList<>(project.getOutput()), newJoin);
                }

                Builder<NamedExpression> newBProject = ImmutableList.<NamedExpression>builder().addAll(bProjects);
                Set<Slot> bConditionSlots = CBOUtils.joinChildConditionSlots(join, false);
                Set<Slot> bProjectSlots = bProjects.stream().map(NamedExpression::toSlot).collect(Collectors.toSet());
                bConditionSlots.stream().filter(slot -> !bProjectSlots.contains(slot)).forEach(newBProject::add);
                Plan newRight = CBOUtils.projectOrSelf(newBProject.build(), join.right());

                Plan newJoin = join.withChildrenNoContext(newLeft, newRight);
                return CBOUtils.projectOrSelf(new ArrayList<>(project.getOutput()), newJoin);
            }).toRule(RuleType.PUSH_DOWN_PROJECT_THROUGH_INNER_JOIN);
    }
}
