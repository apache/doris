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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

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
                .whenNot(join -> join.hasJoinHint() || join.left().child().hasJoinHint())
                .when(join -> OuterJoinLAsscom.VALID_TYPE_PAIR_SET.contains(
                        Pair.of(join.left().child().getJoinType(), join.getJoinType())))
                .when(topJoin -> OuterJoinLAsscom.checkReorder(topJoin, topJoin.left().child()))
                .when(join -> JoinReorderUtils.checkProjectForJoin(join.left()))
                .when(topJoin -> checkCondition(topJoin,
                        topJoin.left().child().right().getOutputExprIdSet()))
                .then(topJoin -> {
                    /* ********** init ********** */
                    List<NamedExpression> projects = topJoin.left().getProjects();
                    LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                    GroupPlan a = bottomJoin.left();
                    GroupPlan b = bottomJoin.right();
                    GroupPlan c = topJoin.right();
                    Set<Slot> cOutputSet = c.getOutputSet();
                    Set<ExprId> aOutputExprIdSet = a.getOutputExprIdSet();

                    /* ********** Split projects ********** */
                    Map<Boolean, List<NamedExpression>> projectExprsMap = JoinReorderUtils.splitProjection(projects, a);
                    List<NamedExpression> newLeftProjects = projectExprsMap.get(Boolean.TRUE);
                    List<NamedExpression> newRightProjects = projectExprsMap.get(Boolean.FALSE);
                    Set<ExprId> aExprIdSet = JoinReorderUtils.combineProjectAndChildExprId(a, newLeftProjects);

                    /* ********** swap Conjuncts ********** */
                    List<Expression> newTopHashConjuncts = bottomJoin.getHashJoinConjuncts();
                    List<Expression> newTopOtherConjuncts = bottomJoin.getOtherJoinConjuncts();
                    List<Expression> newBottomHashConjuncts = topJoin.getHashJoinConjuncts();
                    List<Expression> newBottomOtherConjuncts = topJoin.getOtherJoinConjuncts();

                    /* ********** replace Conjuncts by projects ********** */
                    Map<ExprId, Expression> replaceMapForNewTopJoin = new HashMap<>();
                    Map<ExprId, Expression> replaceMapForNewBottomJoin = new HashMap<>();
                    boolean needNewProjectChildForA = JoinReorderUtils.processProjects(projects, aOutputExprIdSet,
                            replaceMapForNewTopJoin, replaceMapForNewBottomJoin);

                    // replace top join conjuncts
                    newTopHashConjuncts = JoinUtils.replaceJoinConjuncts(
                            newTopHashConjuncts, replaceMapForNewTopJoin);
                    newTopOtherConjuncts = JoinUtils.replaceJoinConjuncts(
                            newTopOtherConjuncts, replaceMapForNewTopJoin);

                    // Add all slots used by OnCondition when projects not empty.
                    Map<Boolean, Set<Slot>> abOnUsedSlots = Stream.concat(
                                    newTopHashConjuncts.stream(),
                                    newTopOtherConjuncts.stream())
                            .flatMap(onExpr -> {
                                Set<Slot> usedSlotRefs = onExpr.getInputSlots();
                                return usedSlotRefs.stream();
                            })
                            .collect(Collectors.partitioningBy(
                                    slot -> aExprIdSet.contains(slot.getExprId()), Collectors.toSet()));
                    Set<Slot> aUsedSlots = abOnUsedSlots.get(true);
                    Set<Slot> bUsedSlots = abOnUsedSlots.get(false);

                    JoinUtils.addSlotsUsedByOn(bUsedSlots, newRightProjects);
                    JoinUtils.addSlotsUsedByOn(aUsedSlots, newLeftProjects);

                    /* ********** new Plan ********** */
                    LogicalJoin newBottomJoin;
                    if (needNewProjectChildForA) {
                        /*
                        *        topJoin                   newTopJoin
                        *        /     \                   /        \
                        *    project    C          newLeftProject newRightProject
                        *      /            ──►          /            \
                        * bottomJoin                newBottomJoin      B
                        *    /   \                     /   \
                        *   A     B                   A     C
                        *                            /
                        *                  needNewProjectChildForA
                        */
                        // create a new project node as A's child
                        newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                                newBottomHashConjuncts, newBottomOtherConjuncts,
                                JoinHint.NONE, JoinReorderUtils.projectOrSelf(newLeftProjects, a), c,
                                bottomJoin.getJoinReorderContext());
                    } else {
                        // replace the join conjuncts
                        newBottomHashConjuncts = JoinUtils.replaceJoinConjuncts(
                                newBottomHashConjuncts, replaceMapForNewBottomJoin);
                        newBottomOtherConjuncts = JoinUtils.replaceJoinConjuncts(
                                newBottomOtherConjuncts, replaceMapForNewBottomJoin);
                        newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                                newBottomHashConjuncts, newBottomOtherConjuncts,
                                JoinHint.NONE, a, c, bottomJoin.getJoinReorderContext());
                    }
                    newBottomJoin.getJoinReorderContext().setHasLAsscom(false);
                    newBottomJoin.getJoinReorderContext().setHasCommute(false);

                    // new left project should contain all output slots from C
                    if (!newLeftProjects.isEmpty()) {
                        if (topJoin.getJoinType().isLeftJoin()) {
                            Set<Slot> nullableCOutputSet = forceToNullable(cOutputSet);
                            newLeftProjects.addAll(nullableCOutputSet);
                        } else {
                            newLeftProjects.addAll(cOutputSet);
                        }
                    }

                    Plan left = JoinReorderUtils.projectOrSelf(newLeftProjects, newBottomJoin);
                    Plan right = JoinReorderUtils.projectOrSelf(newRightProjects, b);

                    LogicalJoin<Plan, Plan> newTopJoin = new LogicalJoin<>(bottomJoin.getJoinType(),
                            newTopHashConjuncts, newTopOtherConjuncts, JoinHint.NONE,
                            left, right, topJoin.getJoinReorderContext());
                    newTopJoin.getJoinReorderContext().setHasLAsscom(true);
                    return JoinReorderUtils.projectOrSelf(new ArrayList<>(topJoin.getOutput()), newTopJoin);
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_LASSCOM_PROJECT);
    }

    private Set<Slot> forceToNullable(Set<Slot> slotSet) {
        return slotSet.stream().map(s -> (Slot) s.rewriteUp(e -> {
            if (e instanceof SlotReference) {
                return ((SlotReference) e).withNullable(true);
            } else {
                return e;
            }
        })).collect(Collectors.toSet());
    }

    /**
     * topHashConjunct possibility: (A B) (A C) (B C) (A B C).
     * (A B) is forbidden, because it should be in bottom join.
     * (B C) (A B C) check failed, because it contains B.
     * So, just allow: top (A C), bottom (A B), we can exchange HashConjunct directly.
     * <p>
     * Same with OtherJoinConjunct.
     */
    private boolean checkCondition(LogicalJoin<? extends Plan, GroupPlan> topJoin, Set<ExprId> bOutputExprIdSet) {
        List<NamedExpression> projects = ((LogicalProject) topJoin.left()).getProjects();
        // find expression using slots from B in left project
        Set<ExprId> exprIdSet = projects.stream().filter(expr ->
                expr.getInputSlotExprIds().stream().anyMatch(exprId -> bOutputExprIdSet.contains(exprId))
        ).map(NamedExpression::getExprId).collect(Collectors.toSet());
        // exprIdSet contains all slots from B or using B's slot in some way.
        exprIdSet.addAll(bOutputExprIdSet);

        // return true if top join only contains (A C)
        return Stream.concat(
                        topJoin.getHashJoinConjuncts().stream(),
                        topJoin.getOtherJoinConjuncts().stream())
                .allMatch(expr -> {
                    Set<ExprId> usedExprIdSet = expr.getInputSlotExprIds();
                    return !ExpressionUtils.isIntersecting(usedExprIdSet, exprIdSet);
                });
    }
}
