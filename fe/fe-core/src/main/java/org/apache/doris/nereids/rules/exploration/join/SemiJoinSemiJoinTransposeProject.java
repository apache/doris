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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * rule for semi-semi transpose
 */
public class SemiJoinSemiJoinTransposeProject extends OneExplorationRuleFactory {
    public static final SemiJoinSemiJoinTransposeProject INSTANCE = new SemiJoinSemiJoinTransposeProject();

    /*
     *        topSemi                   newTopSemi
     *        /     \                   /       \
     *    abProject   C              acProject     B
     *      |            ──►          |
     * bottomSemi                newBottomSemi
     *    /   \                     /   \
     *   A     B                   A     C
     */
    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), group())
                .when(this::typeChecker)
                .when(topSemi -> InnerJoinLAsscom.checkReorder(topSemi, topSemi.left().child(), false))
                .whenNot(join -> join.hasDistributeHint() || join.left().child().hasDistributeHint())
                .when(join -> join.left().isAllSlots())
                .then(topSemi -> {
                    LogicalJoin<GroupPlan, GroupPlan> bottomSemi = topSemi.left().child();
                    LogicalProject<LogicalJoin<GroupPlan, GroupPlan>> abProject = topSemi.left();
                    GroupPlan a = bottomSemi.left();
                    GroupPlan b = bottomSemi.right();
                    GroupPlan c = topSemi.right();
                    Set<ExprId> aOutputExprIdSet = a.getOutputExprIdSet();
                    // if bottom semi join is mark join, we need remove the mark join slot creating by bottom semi join
                    // from the project list before swapping the bottom semi to top semi
                    Set<NamedExpression> acProjects = abProject.getProjects().stream()
                            .filter(slot -> !(abProject.child().isMarkJoin()
                                    && abProject.child().getMarkJoinSlotReference().get()
                                            .getExprId() == slot.getExprId()))
                            .collect(Collectors.toSet());

                    bottomSemi.getConditionSlot()
                            .forEach(slot -> {
                                if (aOutputExprIdSet.contains(slot.getExprId())) {
                                    acProjects.add(slot);
                                }
                            });
                    LogicalJoin newBottomSemi = topSemi.withChildrenNoContext(a, c, null);
                    if (topSemi.isMarkJoin()) {
                        acProjects.add(topSemi.getMarkJoinSlotReference().get());
                    }
                    newBottomSemi.getJoinReorderContext().copyFrom(bottomSemi.getJoinReorderContext());
                    newBottomSemi.getJoinReorderContext().setHasCommute(false);
                    newBottomSemi.getJoinReorderContext().setHasLAsscom(false);

                    LogicalProject acProject = new LogicalProject<>(Lists.newArrayList(acProjects), newBottomSemi);
                    LogicalJoin newTopSemi = bottomSemi.withChildrenNoContext(acProject, b, null);
                    newTopSemi.getJoinReorderContext().copyFrom(topSemi.getJoinReorderContext());
                    newTopSemi.getJoinReorderContext().setHasLAsscom(true);
                    return CBOUtils.projectOrSelf(ImmutableList.copyOf(topSemi.getOutput()), newTopSemi);
                }).toRule(RuleType.LOGICAL_SEMI_JOIN_SEMI_JOIN_TRANSPOSE_PROJECT);
    }

    public boolean typeChecker(LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>, GroupPlan> topJoin) {
        return SemiJoinSemiJoinTranspose.VALID_TYPE_PAIR_SET
                .contains(Pair.of(topJoin.getJoinType(), topJoin.left().child().getJoinType()));
    }
}
