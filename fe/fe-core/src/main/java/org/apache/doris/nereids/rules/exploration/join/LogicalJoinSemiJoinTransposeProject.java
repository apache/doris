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
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * LogicalJoin(SemiJoin(A, B), C) -> SemiJoin(LogicalJoin(A, C), B)
 */
public class LogicalJoinSemiJoinTransposeProject implements ExplorationRuleFactory {

    public static final LogicalJoinSemiJoinTransposeProject INSTANCE = new LogicalJoinSemiJoinTransposeProject();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalProject(logicalJoin(logicalProject(logicalJoin()), group())
                        .when(topJoin -> (topJoin.left().child().getJoinType().isLeftSemiOrAntiJoin()
                                && (topJoin.getJoinType().isInnerJoin()
                                || topJoin.getJoinType().isLeftOuterJoin())))
                        .whenNot(topJoin -> topJoin.hasDistributeHint()
                                || topJoin.left().child().hasDistributeHint())
                        .when(join -> join.left().isAllSlots())
                        // the transpose moves the bottom semi join's conjuncts and its RIGHT
                        // subtree above the new bottom join (built from the top join, which
                        // prunes rows when it is inner): a NoneMovableFunction (assert_true)
                        // or volatile expression there would be evaluated on fewer rows and
                        // its required error could be suppressed, so the transpose must be
                        // rejected in that case
                        .whenNot(topJoin -> isBottomSemiSensitive(topJoin.left().child()))
                        // the transpose installs the old top join as the new BOTTOM join (over
                        // A x C), evaluated BEFORE the new top semi join prunes with B: a
                        // NoneMovableFunction (assert_true) or volatile expression owned by the
                        // old top join would then run on rows the original semi join removed,
                        // turning a successful query into an error, so the transpose must also
                        // be rejected then
                        .whenNot(topJoin -> isTopJoinSensitive(topJoin)))
                        .then(topProject -> {
                            LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>, GroupPlan> topJoin
                                    = topProject.child();
                            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                            if (!JoinUtils.checkReorderPrecondition(topJoin, bottomJoin)) {
                                return null;
                            }
                            GroupPlan a = bottomJoin.left();
                            GroupPlan b = bottomJoin.right();
                            GroupPlan c = topJoin.right();

                            Set<ExprId> topUsedExprIds = new HashSet<>();
                            topProject.getProjects().forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                            bottomJoin.getHashJoinConjuncts()
                                    .forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                            bottomJoin.getOtherJoinConjuncts()
                                    .forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                            bottomJoin.getMarkJoinConjuncts()
                                    .forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                            Plan newBottomJoin = topJoin.withChildrenNoContext(a, c, null);
                            Plan left = CBOUtils.newProject(topUsedExprIds, newBottomJoin);
                            Plan right = CBOUtils.newProjectIfNeeded(topUsedExprIds, b);

                            Plan newTopJoin = bottomJoin.withChildrenNoContext(left, right, null);
                            return topProject.withChildren(newTopJoin);
                        }).toRule(RuleType.LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_LEFT_PROJECT),

                logicalProject(logicalJoin(group(), logicalProject(logicalJoin()))
                        .when(topJoin -> (topJoin.right().child().getJoinType().isLeftSemiOrAntiJoin()
                                && (topJoin.getJoinType().isInnerJoin()
                                || topJoin.getJoinType().isRightOuterJoin())))
                        .whenNot(topJoin -> topJoin.hasDistributeHint()
                                || topJoin.right().child().hasDistributeHint())
                        .when(join -> join.right().isAllSlots())
                        // same as the left variant: reject the transpose when the bottom semi
                        // join's conjuncts or its RIGHT subtree contain a NoneMovableFunction
                        // (assert_true) or a volatile expression, which the transpose would
                        // move above the row-pruning new bottom join
                        .whenNot(topJoin -> isBottomSemiSensitive(topJoin.right().child()))
                        // same as the left variant: reject the transpose when the old top join
                        // (which becomes the new BOTTOM join over A x B) owns a
                        // NoneMovableFunction (assert_true) or volatile expression
                        .whenNot(topJoin -> isTopJoinSensitive(topJoin)))
                        .then(topProject -> {
                            LogicalJoin<GroupPlan, LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>> topJoin
                                    = topProject.child();
                            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.right().child();
                            if (!JoinUtils.checkReorderPrecondition(topJoin, bottomJoin)) {
                                return null;
                            }
                            GroupPlan a = topJoin.left();
                            GroupPlan b = bottomJoin.left();
                            GroupPlan c = bottomJoin.right();

                            Set<ExprId> topUsedExprIds = new HashSet<>();
                            topProject.getProjects().forEach(expr -> topUsedExprIds.addAll(expr.getInputSlotExprIds()));
                            bottomJoin.getHashJoinConjuncts()
                                    .forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                            bottomJoin.getOtherJoinConjuncts()
                                    .forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                            bottomJoin.getMarkJoinConjuncts()
                                    .forEach(e -> topUsedExprIds.addAll(e.getInputSlotExprIds()));
                            Plan newBottomJoin = topJoin.withChildrenNoContext(a, b, null);
                            Plan left = CBOUtils.newProject(topUsedExprIds, newBottomJoin);
                            Plan right = CBOUtils.newProjectIfNeeded(topUsedExprIds, c);

                            Plan newTopJoin = bottomJoin.withChildrenNoContext(left, right, null);
                            return topProject.withChildren(newTopJoin);
                        }).toRule(RuleType.LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_RIGHT_PROJECT)
        );
    }

    /**
     * whether the bottom semi join's own conjuncts or its RIGHT subtree contain a
     * NoneMovableFunction (assert_true) or a volatile expression. the transpose moves those up
     * above the new bottom join (built from the top join, which prunes rows when it is inner),
     * so they would be evaluated on fewer rows and their error could be suppressed; the
     * transpose must be rejected then.
     */
    private static boolean isBottomSemiSensitive(LogicalJoin<GroupPlan, GroupPlan> bottomSemi) {
        return bottomSemi.getExpressions().stream()
                .anyMatch(expr -> expr.containsNoneMovableOrVolatile())
                || JoinUtils.groupContainsSensitiveExpression(bottomSemi.right());
    }

    /**
     * whether the old top join's own conjuncts contain a NoneMovableFunction (assert_true) or a
     * volatile expression. the transpose installs the old top join as the new BOTTOM join (over
     * A x C), evaluated BEFORE the new top semi join prunes with B: a sensitive expression there
     * would then run on rows the original semi join removed, turning a successful query into an
     * error. the transpose must be rejected.
     */
    private static boolean isTopJoinSensitive(LogicalJoin<?, ?> topJoin) {
        return topJoin.getExpressions().stream()
                .anyMatch(expr -> expr.containsNoneMovableOrVolatile());
    }
}
