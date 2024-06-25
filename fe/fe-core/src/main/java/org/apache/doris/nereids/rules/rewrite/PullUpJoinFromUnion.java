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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Pull up join from union all rules.
 *       Union
 *       /    \
 *     Join   Join
 *     / \    / \
 *    t1 t2   t1 t3
 *  =====>
 *            Join
 *          /    \
 *       Union   t1
 *       /    \
 *       t2   t3
 */
public class PullUpJoinFromUnion extends OneRewriteRuleFactory {

    HashMap<Slot, Slot> originChildSlotToUnion = new HashMap<>();
    HashMap<Slot, NamedExpression> joinSlotToProject = new HashMap<>();

    @Override
    public Rule build() {
        return logicalUnion()
                .when(union -> union.getQualifier() != Qualifier.DISTINCT)
                .then(union -> {
                    HashMap<Plan, Set<Pair<LogicalJoin<?, ?>, Plan>>> commonChildCount =
                            tryToExtractCommonChild(union);
                    if (commonChildCount == null) {
                        return null;
                    }

                    Set<Pair<LogicalJoin<?, ?>, Plan>> commonChild = null;
                    for (Set<Pair<LogicalJoin<?, ?>, Plan>> childSet : commonChildCount.values()) {
                        if (childSet.size() == union.children().size()) {
                            commonChild = childSet;
                            break;
                        }
                    }
                    if (commonChild == null) {
                        return null;
                    }

                    boolean success = tryMapJoinSlotToUnion(union);
                    if (!success) {
                        return null;
                    }
                    if (!checkJoinCondition(commonChild)) {
                        return null;
                    }

                    return constructNewJoinUnion(union, commonChild);
                }).toRule(RuleType.PULL_UP_JOIN_FROM_UNION);
    }

    // For union children, we must keep the join condition satisfied the following conditions:
    // 1. The one sides are the same with others
    // 2. The other side has the same Sndex/Slot in the union
    // this function is checking the second condition
    private boolean checkJoinCondition(
            Set<Pair<LogicalJoin<?, ?>, Plan>> commonChild) {
        HashMap<Slot, Slot> joinCondIndices = new HashMap<>();
        for (Pair<LogicalJoin<?, ?>, Plan> joinChildPair : commonChild) {
            for (Expression expression : joinChildPair.first.getHashJoinConjuncts()) {
                if (!(expression instanceof EqualTo)
                        && expression.child(0).isSlot()
                        && expression.child(1).isSlot()) {
                    return false;
                }
                Plan commonPlan = joinChildPair.second;

                Slot otherChildSlot;
                Slot commonChildSlot;
                if (commonPlan.getOutputSet().contains((Slot) expression.child(0))) {
                    otherChildSlot = (Slot) expression.child(1);
                    commonChildSlot = (Slot) expression.child(0);
                } else if (commonPlan.getOutputSet().contains((Slot) expression.child(1))) {
                    otherChildSlot = (Slot) expression.child(0);
                    commonChildSlot = (Slot) expression.child(1);
                } else {
                    return false;
                }

                if (!joinSlotToProject.containsKey(otherChildSlot)
                        || !originChildSlotToUnion.containsKey(joinSlotToProject.get(otherChildSlot).toSlot())) {
                    return false;
                }
                if (!joinSlotToProject.containsKey(commonChildSlot)
                        || !originChildSlotToUnion.containsKey(joinSlotToProject.get(commonChildSlot).toSlot())) {
                    return false;
                }
                Slot commonUnionSlot = originChildSlotToUnion.get(
                        joinSlotToProject.get(commonChildSlot).toSlot());
                Slot otherUnionSlot = originChildSlotToUnion.get(
                        joinSlotToProject.get(otherChildSlot).toSlot());
                joinCondIndices.putIfAbsent(commonUnionSlot, otherUnionSlot);
                if (joinCondIndices.get(commonUnionSlot) != otherUnionSlot) {
                    return false;
                }
            }
        }
        return true;
    }

    private Plan constructNewJoinUnion(
            LogicalUnion union, Set<Pair<LogicalJoin<?, ?>, Plan>> commonChild) {

        // 1. construct new children of union
        LogicalUnion newUnion = constructNewUnion(commonChild);
        HashMap<Slot, Slot> newChildToUnionSlot = new HashMap<>();
        for (Plan child : newUnion.children()) {
            for (int i = 0; i < child.getOutput().size(); i++) {
                newChildToUnionSlot.put(child.getOutput().get(i), newUnion.getOutput().get(i));
            }
        }

        // 3. construct join union
        LogicalJoin<?, ?> originalJoin = commonChild.iterator().next().first;
        Plan originalChild = constructNewChild(commonChild.iterator().next().second);
        Plan newJoin = constructNewJoin(originalJoin, newUnion, originalChild, newChildToUnionSlot);

        // 4. map the output to origin output
        List<NamedExpression> newOutput = constructFinalOutput(newJoin, union, newChildToUnionSlot);
        return new LogicalProject<>(newOutput, newJoin);
    }

    private List<NamedExpression> constructFinalOutput(Plan plan,
            LogicalUnion originalUnion,
            Map<Slot, Slot> newChildToUnion) {
        HashMap<Slot, Slot> originalUnionToNewSlot = new HashMap<>();
        for (Entry<Slot, Slot> orginalEntry : originChildSlotToUnion.entrySet()) {
            if (newChildToUnion.containsKey(orginalEntry.getKey())) {
                // below new union
                originalUnionToNewSlot.put(orginalEntry.getValue(), newChildToUnion.get(orginalEntry.getKey()));
            } else if (plan.getOutputSet().contains(orginalEntry.getKey())) {
                // below common child
                originalUnionToNewSlot.put(orginalEntry.getValue(), orginalEntry.getKey());
            }
        }
        List<NamedExpression> newOutput = new ArrayList<>();
        for (int i = 0; i < originalUnion.getOutput().size(); i++) {
            Slot originalSlot = originalUnion.getOutput().get(i);
            Slot newSlot = originalUnionToNewSlot.get(originalSlot);
            newOutput.add(new Alias(originalSlot.getExprId(), newSlot, originalSlot.getName()));
        }
        return newOutput;
    }

    private LogicalUnion constructNewUnion(
            Set<Pair<LogicalJoin<?, ?>, Plan>> commonChild) {
        List<Plan> newChildren = new ArrayList<>();
        for (Pair<LogicalJoin<?, ?>, Plan> child : commonChild) {
            // find the child that is not the common side
            Plan newChild;
            if (child.first.child(0).equals(child.second)) {
                newChild = constructNewChild(child.first.child(1));
            } else {
                newChild = constructNewChild(child.first.child(0));
            }
            newChildren.add(newChild);
        }

        //2. construct new union
        LogicalUnion newUnion = new LogicalUnion(Qualifier.ALL, newChildren);
        List<List<SlotReference>> childrenOutputs = newChildren.stream()
                .map(p -> p.getOutput().stream()
                        .map(SlotReference.class::cast)
                        .collect(ImmutableList.toImmutableList()))
                .collect(ImmutableList.toImmutableList());
        newUnion = (LogicalUnion) newUnion.withChildrenAndTheirOutputs(newChildren, childrenOutputs);
        newUnion = newUnion.withNewOutputs(newUnion.buildNewOutputs());
        return newUnion;
    }

    private Plan constructNewChild(Plan plan) {
        Set<Slot> output = plan.getOutputSet();
        List<NamedExpression> projects = originChildSlotToUnion.entrySet()
                .stream()
                .sorted(Comparator.comparing(e -> e.getValue().getExprId().hashCode()))
                .filter(e -> output.containsAll(e.getKey().getInputSlots()))
                .map(Entry::getKey)
                .collect(Collectors.toList());
        if (plan.getOutput().equals(projects)) {
            return plan;
        }
        return new LogicalProject<>(projects, plan);
    }

    private Plan constructNewJoin(LogicalJoin<?, ?> originalJoin,
            LogicalUnion unionChild, Plan commonChild, Map<Slot, Slot> newChildToUnionSlot) {
        HashMap<Expression, Expression> replacedSlotMap = new HashMap<>();
        for (Slot originSlot : originChildSlotToUnion.keySet()) {
            replacedSlotMap.put(originSlot, newChildToUnionSlot.get(originSlot));
        }
        List<Expression> newHashExpressions = new ArrayList<>();
        for (Expression expression : originalJoin.getHashJoinConjuncts()) {
            Expression newExpr = expression.rewriteUp(e -> replacedSlotMap.getOrDefault(e, e));
            newHashExpressions.add(newExpr);
        }
        return originalJoin
                .withJoinConjuncts(newHashExpressions, ImmutableList.of(), originalJoin.getJoinReorderContext())
                .withChildren(unionChild, commonChild);
    }

    private @Nullable HashMap<Plan, Set<Pair<LogicalJoin<?, ?>, Plan>>> tryToExtractCommonChild(LogicalUnion union) {
        HashMap<Plan, Set<Pair<LogicalJoin<?, ?>, Plan>>> planCount = new HashMap<>();
        for (Plan child : union.children()) {
            LogicalJoin<? extends Plan, ? extends Plan> join = tryToGetJoin(child);
            if (join == null) {
                return null;
            }
            boolean added = false;
            for (Plan plan : planCount.keySet()) {
                LogicalPlanComparator comparator = new LogicalPlanComparator();
                if (comparator.isLogicalEqual(join.left(), plan)) {
                    planCount.get(plan).add(Pair.of(join, join.left()));
                    added = true;
                    break;
                }
                if (comparator.isLogicalEqual(join.right(), plan)) {
                    planCount.get(plan).add(Pair.of(join, join.right()));
                    added = true;
                    break;
                }
            }

            if (!added) {
                planCount.put(join.left(), Sets.newHashSet(Pair.of(join, join.left())));
                planCount.put(join.right(), Sets.newHashSet(Pair.of(join, join.right())));
            }
        }
        return planCount;
    }

    private boolean tryMapJoinSlotToUnion(LogicalUnion union) {
        for (int i = 0; i < union.children().size(); i++) {
            Plan child = union.child(i);
            if (union.getRegularChildOutput(i).isEmpty()) {
                for (int slotIdx = 0; slotIdx < child.getOutput().size(); slotIdx++) {
                    originChildSlotToUnion.put(child.getOutput().get(slotIdx), union.getOutput().get(slotIdx));
                }
            } else {
                for (int slotIdx = 0; slotIdx < union.getRegularChildOutput(i).size(); slotIdx++) {
                    originChildSlotToUnion.put(child.getOutput().get(slotIdx), union.getOutput().get(slotIdx));
                }
            }

            if (child instanceof LogicalProject) {
                for (NamedExpression expr : ((LogicalProject<?>) child).getProjects()) {
                    Set<Slot> inputs = expr.getInputSlots();
                    if (inputs.size() != 1) {
                        return false;
                    }
                    joinSlotToProject.put(inputs.iterator().next(), expr);
                }
            } else {
                for (Slot slot : child.getOutput()) {
                    joinSlotToProject.put(slot, slot);
                }
            }
        }
        return true;
    }

    // we only allow project(join) or join()
    private @Nullable LogicalJoin<?, ?> tryToGetJoin(Plan child) {
        if (child instanceof LogicalProject) {
            child = child.child(0);
        }
        if (child instanceof LogicalJoin
                && ((LogicalJoin<?, ?>) child).getJoinType().isInnerJoin()
                && ((LogicalJoin<?, ?>) child).getOtherJoinConjuncts().isEmpty()
                && !((LogicalJoin<?, ?>) child).isMarkJoin()) {
            return (LogicalJoin<?, ?>) child;
        }
        return null;
    }

    class LogicalPlanComparator {
        private HashMap<Expression, Expression> plan1ToPlan2 = new HashMap<>();

        public boolean isLogicalEqual(Plan plan1, Plan plan2) {
            if (plan1.children().size() != plan2.children().size()) {
                return false;
            }
            for (int i = 0; i < plan1.children().size(); i++) {
                if (!isLogicalEqual(plan1.child(i), plan2.child(i))) {
                    return false;
                }
            }
            if (isNotSupported(plan1) || isNotSupported(plan2)) {
                return false;
            }
            return comparePlan(plan1, plan2);
        }

        boolean isNotSupported(Plan plan) {
            return !(plan instanceof LogicalFilter)
                    && !(plan instanceof LogicalCatalogRelation)
                    && !(plan instanceof LogicalProject);
        }

        boolean comparePlan(Plan plan1, Plan plan2) {
            boolean isEqual = true;
            if (plan1 instanceof LogicalCatalogRelation && plan2 instanceof LogicalCatalogRelation) {
                isEqual = new TableIdentifier(((LogicalCatalogRelation) plan1).getTable())
                        .equals(new TableIdentifier(((LogicalCatalogRelation) plan2).getTable()));
            } else if (plan1 instanceof LogicalProject && plan2 instanceof LogicalProject) {
                for (int i = 0; i < plan2.getOutput().size(); i++) {
                    NamedExpression expr = ((LogicalProject<?>) plan1).getProjects().get(i);
                    NamedExpression replacedExpr = (NamedExpression)
                            expr.rewriteUp(e -> plan1ToPlan2.getOrDefault(e, e));
                    if (!replacedExpr.equals(((LogicalProject<?>) plan2).getProjects().get(i))) {
                        isEqual = false;
                        break;
                    }
                }

            } else if (plan1 instanceof LogicalFilter && plan2 instanceof LogicalFilter) {
                Set<Expression> replacedConjuncts = new HashSet<>();
                for (Expression expr : ((LogicalFilter<?>) plan1).getConjuncts()) {
                    replacedConjuncts.add(expr.rewriteUp(e -> plan1ToPlan2.getOrDefault(e, e)));
                }
                isEqual = replacedConjuncts.equals(((LogicalFilter<?>) plan2).getConjuncts());
            }
            if (!isEqual) {
                return false;
            }
            for (int i = 0; i < plan1.getOutput().size(); i++) {
                plan1ToPlan2.put(plan1.getOutput().get(i), plan2.getOutput().get(i));
            }
            return true;
        }
    }
}
