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
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                .whenNot(join -> join.hasJoinHint() || join.left().child().hasJoinHint())
                .when(join -> JoinReorderCommon.checkProject(join.left()))
                .then(topJoin -> {

                    /* ********** init ********** */
                    List<NamedExpression> projects = topJoin.left().getProjects();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();
                    Set<Slot> aOutputSet = a.getOutputSet();
                    Set<Slot> cOutputSet = c.getOutputSet();

                    /* ********** Split projects ********** */
                    Map<Boolean, List<NamedExpression>> projectExprsMap = projects.stream()
                            .collect(Collectors.partitioningBy(projectExpr -> {
                                Set<Slot> usedSlots = projectExpr.collect(SlotReference.class::isInstance);
                                return aOutputSet.containsAll(usedSlots);
                            }));
                    List<NamedExpression> newLeftProjects = projectExprsMap.get(Boolean.TRUE);
                    List<NamedExpression> newRightProjects = projectExprsMap.get(Boolean.FALSE);
                    Set<ExprId> aExprIdSet = getExprIdSetForA(bottomJoin.left(),
                            newLeftProjects);

                    /* ********** split Conjuncts ********** */
                    Map<Boolean, List<Expression>> newHashJoinConjuncts
                            = createNewConjunctsWithAlias(
                            topJoin.getHashJoinConjuncts(), bottomJoin.getHashJoinConjuncts(), aExprIdSet);
                    List<Expression> newTopHashJoinConjuncts = newHashJoinConjuncts.get(true);
                    Preconditions.checkState(!newTopHashJoinConjuncts.isEmpty(),
                            "LAsscom newTopHashJoinConjuncts join can't empty");
                    // When newTopHashJoinConjuncts.size() != bottomJoin.getHashJoinConjuncts().size()
                    // It means that topHashJoinConjuncts contain A, B, C, we shouldn't do LAsscom.
                    if (topJoin.getJoinType() != bottomJoin.getJoinType()
                            && newTopHashJoinConjuncts.size() != bottomJoin.getHashJoinConjuncts().size()) {
                        return null;
                    }
                    List<Expression> newBottomHashJoinConjuncts = newHashJoinConjuncts.get(false);
                    if (newBottomHashJoinConjuncts.size() == 0) {
                        return null;
                    }

                    Map<Boolean, List<Expression>> newOtherJoinConjuncts
                            = createNewConjunctsWithAlias(
                            topJoin.getOtherJoinConjuncts(), bottomJoin.getOtherJoinConjuncts(),
                            aExprIdSet);
                    List<Expression> newTopOtherJoinConjuncts = newOtherJoinConjuncts.get(true);
                    List<Expression> newBottomOtherJoinConjuncts = newOtherJoinConjuncts.get(false);
                    if (newBottomOtherJoinConjuncts.size() != topJoin.getOtherJoinConjuncts().size()
                            || newTopOtherJoinConjuncts.size() != bottomJoin.getOtherJoinConjuncts().size()) {
                        return null;
                    }

                    /* ********** replace Conjuncts by projects ********** */
                    Map<Slot, Slot> inputToOutput = new HashMap<>();
                    Map<Slot, Slot> outputToInput = new HashMap<>();
                    for (NamedExpression expr : projects) {
                        if (expr instanceof Alias) {
                            Alias alias = (Alias) expr;
                            Slot outputSlot = alias.toSlot();
                            Expression child = alias.child();
                            // checkProject already confirmed.
                            Preconditions.checkState(child instanceof Slot);
                            Slot inputSlot = (Slot) child;
                            inputToOutput.put(inputSlot, outputSlot);
                            outputToInput.put(outputSlot, inputSlot);
                        }
                    }
                    // replace hashJoinConjuncts
                    newBottomHashJoinConjuncts = JoinUtils.replaceJoinConjuncts(
                            newBottomHashJoinConjuncts, outputToInput);
                    newTopHashJoinConjuncts = JoinUtils.replaceJoinConjuncts(
                            newTopHashJoinConjuncts, inputToOutput);
                    // replace otherJoinConjuncts
                    newBottomOtherJoinConjuncts = JoinUtils.replaceJoinConjuncts(
                            newBottomOtherJoinConjuncts, outputToInput);
                    newTopOtherJoinConjuncts = JoinUtils.replaceJoinConjuncts(
                            newTopOtherJoinConjuncts, inputToOutput);

                    // Add all slots used by OnCondition when projects not empty.
                    Map<Boolean, Set<Slot>> abOnUsedSlots = Stream.concat(
                                    newTopHashJoinConjuncts.stream(),
                                    newTopOtherJoinConjuncts.stream())
                            .flatMap(onExpr -> {
                                Set<Slot> usedSlotRefs = onExpr.collect(SlotReference.class::isInstance);
                                return usedSlotRefs.stream();
                            })
                            .filter(slot -> !cOutputSet.contains(slot))
                            .collect(Collectors.partitioningBy(slot -> aExprIdSet.contains(slot.getExprId()),
                                    Collectors.toSet()));
                    Set<Slot> aUsedSlots = abOnUsedSlots.get(Boolean.TRUE);
                    Set<Slot> bUsedSlots = abOnUsedSlots.get(Boolean.FALSE);

                    JoinUtils.addSlotsUsedByOn(bUsedSlots, newRightProjects);
                    JoinUtils.addSlotsUsedByOn(aUsedSlots, newLeftProjects);

                    if (!newLeftProjects.isEmpty()) {
                        newLeftProjects.addAll(cOutputSet);
                    }

                    /* ********** new Plan ********** */
                    LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                            newBottomHashJoinConjuncts, newBottomOtherJoinConjuncts, JoinHint.NONE,
                            a, c, bottomJoin.getJoinReorderContext());
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
                            newTopHashJoinConjuncts, newTopOtherJoinConjuncts, JoinHint.NONE,
                            left, right, topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasLAsscom(true);

                    if (topJoin.getLogicalProperties().equals(newTopJoin.getLogicalProperties())) {
                        return newTopJoin;
                    }

                    return PlanUtils.project(new ArrayList<>(topJoin.getOutput()), newTopJoin).get();
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_LASSCOM_PROJECT);
    }

    private Map<Boolean, List<Expression>> createNewConjunctsWithAlias(List<Expression> topConjuncts,
            List<Expression> bottomConjuncts, Set<ExprId> bExprIdSet) {
        // if top join's conjuncts are all related to A, we can do reorder
        Map<Boolean, List<Expression>> splitOn = new HashMap<>();
        splitOn.put(true, new ArrayList<>());
        if (topConjuncts.stream().allMatch(topHashOn -> {
            Set<Slot> usedSlots = topHashOn.getInputSlots();
            Set<ExprId> usedSlotsId = usedSlots.stream().map(NamedExpression::getExprId)
                    .collect(Collectors.toSet());

            return ExpressionUtils.isIntersecting(bExprIdSet, usedSlotsId);
        })) {
            // do reorder, create new bottom join conjuncts
            splitOn.put(false, new ArrayList<>(topConjuncts));
        } else {
            // can't reorder, return empty list
            splitOn.put(false, new ArrayList<>());
        }

        List<Expression> newTopHashJoinConjuncts = splitOn.get(true);
        newTopHashJoinConjuncts.addAll(bottomConjuncts);

        return splitOn;
    }

    private Set<ExprId> getExprIdSetForA(GroupPlan a, List<NamedExpression> aProject) {
        return Stream.concat(
                a.getOutput().stream().map(NamedExpression::getExprId),
                aProject.stream().map(NamedExpression::getExprId)).collect(Collectors.toSet());
    }
}
