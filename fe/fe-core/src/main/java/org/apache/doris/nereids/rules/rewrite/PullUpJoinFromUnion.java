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
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
    @Override
    public Rule build() {
        return logicalUnion()
                .when(union -> union.getQualifier() != Qualifier.DISTINCT
                        && union.getConstantExprsList().isEmpty())
                .then(union -> {
                    // map union child slot to union slot
                    Map<Slot, Slot> originChildSlotToUnion = new HashMap<>();
                    // if union child is project, map the join slot to the project
                    Map<Slot, NamedExpression> joinSlotToProject = new HashMap<>();
                    // save the constant alias in project above join
                    Map<Plan, Set<NamedExpression>> constantAlias = new HashMap<>();
                    // save the miss slot of other child that not in the union, we need pull up them in new union
                    Map<Plan, List<Slot>> missSlotMap = new HashMap<>();
                    HashMap<Plan, List<Pair<LogicalJoin<?, ?>, Plan>>> commonChildrenMap =
                            tryToExtractCommonChild(union);
                    if (commonChildrenMap == null) {
                        return null;
                    }

                    List<Pair<LogicalJoin<?, ?>, Plan>> commonChild = null;
                    for (List<Pair<LogicalJoin<?, ?>, Plan>> childSet : commonChildrenMap.values()) {
                        if (childSet.size() == union.children().size()) {
                            commonChild = childSet;
                            break;
                        }
                    }
                    if (commonChild == null) {
                        return null;
                    }

                    boolean success = tryMapJoinSlotToUnion(
                            union, commonChild, joinSlotToProject, constantAlias, originChildSlotToUnion);
                    if (!success) {
                        return null;
                    }
                    if (!checkJoinCondition(commonChild, joinSlotToProject, missSlotMap, originChildSlotToUnion)) {
                        return null;
                    }

                    return constructNewJoinUnion(
                            union, commonChild, constantAlias, missSlotMap, originChildSlotToUnion, joinSlotToProject);
                }).toRule(RuleType.PULL_UP_JOIN_FROM_UNION);
    }

    // For union children, we must keep the join condition satisfied the following conditions:
    // 1. The one sides are the same with others
    // 2. The other side has the same Index/Slot in the union
    // this function is checking the second condition
    private boolean checkJoinCondition(
            List<Pair<LogicalJoin<?, ?>, Plan>> commonChild,
            Map<Slot, NamedExpression> joinSlotToProject,
            Map<Plan, List<Slot>> missSlotMap,
            Map<Slot, Slot> originChildSlotToUnion) {
        HashMap<Integer, Integer> joinCondIndices = new HashMap<>();
        HashMap<Integer, HashMap<Plan, Slot>> missSlotOfSameSlot = new HashMap<>();
        for (Pair<LogicalJoin<?, ?>, Plan> joinChildPair : commonChild) {
            Plan commonPlan = joinChildPair.second;
            Plan otherPlan;
            if (joinChildPair.first.child(0).equals(commonPlan)) {
                otherPlan = joinChildPair.first.child(1);
            } else {
                otherPlan = joinChildPair.first.child(0);
            }
            for (Expression expression : joinChildPair.first.getHashJoinConjuncts()) {
                if (!(expression instanceof EqualTo)
                        && expression.child(0).isSlot()
                        && expression.child(1).isSlot()) {
                    return false;
                }

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
                int otherUnionId = -1;
                int commonIndex = commonPlan.getOutput().indexOf(commonChildSlot);
                if (joinSlotToProject.containsKey(otherChildSlot)
                        && originChildSlotToUnion.containsKey(joinSlotToProject.get(otherChildSlot).toSlot())) {
                    otherUnionId = originChildSlotToUnion.get(
                            joinSlotToProject.get(otherChildSlot).toSlot()).hashCode();
                } else {
                    missSlotOfSameSlot.computeIfAbsent(commonIndex,
                            k -> new HashMap<>()).put(otherPlan, otherChildSlot);
                }
                joinCondIndices.putIfAbsent(commonIndex, otherUnionId);
                if (joinCondIndices.get(commonIndex) != otherUnionId) {
                    return false;
                }
            }
        }
        for (Map<Plan, Slot> miss : missSlotOfSameSlot.values()) {
            for (Entry<Plan, Slot> e : miss.entrySet()) {
                missSlotMap.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(e.getValue());
            }
        }
        return true;
    }

    private Plan constructNewJoinUnion(
            LogicalUnion union, List<Pair<LogicalJoin<?, ?>, Plan>> commonChild,
            Map<Plan, Set<NamedExpression>> constantAlias,
            Map<Plan, List<Slot>> missSlotMap,
            Map<Slot, Slot> originChildSlotToUnion,
            Map<Slot, NamedExpression> joinSlotToProject) {

        // 1. construct new children of union
        LogicalUnion newUnion = constructNewUnion(
                commonChild, joinSlotToProject, constantAlias, missSlotMap, originChildSlotToUnion);

        // 2. construct join union
        LogicalJoin<?, ?> originalJoin = commonChild.iterator().next().first;
        Plan newChild = commonChild.iterator().next().second;
        Plan newJoin = constructNewJoin(originalJoin, newUnion, newChild);

        // 3. map the output to origin output
        List<NamedExpression> newOutput = constructFinalOutput(
                newUnion, newChild, union, originChildSlotToUnion, joinSlotToProject);
        return new LogicalProject<>(newOutput, newJoin);
    }

    private List<NamedExpression> constructFinalOutput(LogicalUnion newUnion,
            Plan commonChild,
            LogicalUnion originalUnion,
            Map<Slot, Slot> originChildSlotToUnion,
            Map<Slot, NamedExpression> joinSlotToProject) {
        HashMap<Slot, Slot> originalToNewSlot = new HashMap<>();
        for (int i = 0; i < newUnion.getOutput().size(); i++) {
            Slot newSlot = newUnion.getOutput().get(i);
            Slot unionChildSlot = newUnion.child(0).getOutput().get(i);
            if (joinSlotToProject.containsKey(unionChildSlot)) {
                Slot originUnionSlot = originChildSlotToUnion.get(joinSlotToProject.get(unionChildSlot).toSlot());
                originalToNewSlot.put(originUnionSlot, newSlot);
            } else if (originChildSlotToUnion.containsKey(unionChildSlot)) {
                originalToNewSlot.put(originChildSlotToUnion.get(unionChildSlot), newSlot);
            }
        }

        for (Slot joinChildSlot : commonChild.getOutput()) {
            if (joinSlotToProject.containsKey(joinChildSlot)) {
                Slot originUnionSlot = originChildSlotToUnion.get(joinSlotToProject.get(joinChildSlot).toSlot());
                originalToNewSlot.put(originUnionSlot, joinChildSlot);
            }
        }
        List<NamedExpression> newOutput = new ArrayList<>();
        for (int i = 0; i < originalUnion.getOutput().size(); i++) {
            Slot originalSlot = originalUnion.getOutput().get(i);
            NamedExpression newSlot = originalToNewSlot.get(originalSlot);
            if (joinSlotToProject.containsKey(newSlot) && !joinSlotToProject.get(newSlot).isSlot()) {
                newSlot = joinSlotToProject.get(newSlot.toSlot());
            }
            newOutput.add(new Alias(originalSlot.getExprId(), newSlot, originalSlot.getName()));
        }
        return newOutput;
    }

    private LogicalUnion constructNewUnion(
            List<Pair<LogicalJoin<?, ?>, Plan>> commonChild,
            Map<Slot, NamedExpression> joinSlotToProject,
            Map<Plan, Set<NamedExpression>> constantAlias,
            Map<Plan, List<Slot>> missSlotMap,
            Map<Slot, Slot> originChildSlotToUnion) {
        List<Plan> newChildren = new ArrayList<>();
        for (Pair<LogicalJoin<?, ?>, Plan> child : commonChild) {
            // find the child that is not the common side
            Plan newChild;
            if (child.first.child(0).equals(child.second)) {
                newChild = constructNewChild(
                        child.first.child(1), joinSlotToProject, constantAlias, missSlotMap, originChildSlotToUnion);
            } else {
                newChild = constructNewChild(
                        child.first.child(0), joinSlotToProject, constantAlias, missSlotMap, originChildSlotToUnion);
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

    private Plan constructNewChild(Plan plan, Map<Slot, NamedExpression> joinSlotToProject,
            Map<Plan, Set<NamedExpression>> constantAlias,
            Map<Plan, List<Slot>> missSlotMap,
            Map<Slot, Slot> originChildSlotToUnion) {
        List<Pair<Slot, NamedExpression>> projectEntry = new ArrayList<>();
        for (Entry<Slot, NamedExpression> entry : joinSlotToProject.entrySet()) {
            if (!plan.getOutput().contains(entry.getKey())
                    || !plan.getOutputSet().containsAll(entry.getValue().getInputSlots())) {
                continue;
            }
            projectEntry.add(Pair.of(entry.getKey(), entry.getValue()));
        }
        for (NamedExpression namedExpression : constantAlias.getOrDefault(plan, new HashSet<>())) {
            projectEntry.add(Pair.of(namedExpression.toSlot(), namedExpression));
        }
        projectEntry.sort(Comparator.comparing(e -> originChildSlotToUnion.get(e.second.toSlot()).hashCode()));
        List<NamedExpression> projects = new ArrayList<>();
        for (Pair<Slot, NamedExpression> entry : projectEntry) {
            if (entry.second.getInputSlots().isEmpty()) {
                projects.add(entry.second);
            } else {
                projects.add(entry.first);
            }
        }
        if (missSlotMap.containsKey(plan) && !missSlotMap.get(plan).isEmpty()) {
            projects.addAll(missSlotMap.get(plan));
        }
        if (plan.getOutput().equals(projects)) {
            return plan;
        }
        return new LogicalProject<>(projects, plan);
    }

    private Plan constructNewJoin(LogicalJoin<?, ?> originalJoin,
            LogicalUnion newUnion, Plan commonChild) {
        HashMap<Slot, Slot> newChildToUnionSlot = new HashMap<>();
        for (Plan child : newUnion.children()) {
            for (int i = 0; i < child.getOutput().size(); i++) {
                newChildToUnionSlot.put(child.getOutput().get(i), newUnion.getOutput().get(i));
            }
        }
        HashMap<Expression, Expression> replacedSlotMap = new HashMap<>();
        for (Slot originSlot : originalJoin.getInputSlots()) {
            if (newChildToUnionSlot.containsKey(originSlot)) {
                replacedSlotMap.put(originSlot, newChildToUnionSlot.get(originSlot));
            }
        }
        List<Expression> newHashExpressions = new ArrayList<>();
        for (Expression expression : originalJoin.getHashJoinConjuncts()) {
            Expression newExpr = expression.rewriteUp(e -> replacedSlotMap.getOrDefault(e, e));
            newHashExpressions.add(newExpr);
        }
        return originalJoin
                .withJoinConjuncts(newHashExpressions, ImmutableList.of(), originalJoin.getJoinReorderContext())
                .withChildren(newUnion, commonChild);
    }

    /**
     * Attempts to extract common children from a LogicalUnion.
     *
     * This method iterates through all children of the union, looking for LogicalJoin operations,
     * and tries to identify common left or right subtrees. The results are stored in a Map where
     * keys are potential common subtrees and values are lists of pairs containing the original
     * join and the corresponding subtree.
     *
     * For example, given the following union:
     *   Union
     *    ├─ Join(A, B)
     *    ├─ Join(A, C)
     *    └─ Join(D, B)
     *
     * The returned Map would contain:
     *   A -> [(Join(A,B), A), (Join(A,C), A)]
     *   B -> [(Join(A,B), B), (Join(D,B), B)]
     *
     * This indicates that both A and B are potential common subtrees that could be extracted.
     *
     * @param union The LogicalUnion to analyze
     * @return A Map containing potential common subtrees, or null if extraction is not possible
     */
    private @Nullable HashMap<Plan, List<Pair<LogicalJoin<?, ?>, Plan>>> tryToExtractCommonChild(LogicalUnion union) {
        HashMap<Plan, List<Pair<LogicalJoin<?, ?>, Plan>>> planCount = new HashMap<>();
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
                } else if (comparator.isLogicalEqual(join.right(), plan)) {
                    planCount.get(plan).add(Pair.of(join, join.right()));
                    added = true;
                    break;
                }
            }

            if (!added) {
                planCount.put(join.left(), Lists.newArrayList(Pair.of(join, join.left())));
                planCount.put(join.right(), Lists.newArrayList(Pair.of(join, join.right())));
            }
        }
        return planCount;
    }

    private boolean tryMapJoinSlotToUnion(LogicalUnion union, List<Pair<LogicalJoin<?, ?>, Plan>> commonChild,
            Map<Slot, NamedExpression> joinSlotToProject,
            Map<Plan, Set<NamedExpression>> constantAlias,
            Map<Slot, Slot> originChildSlotToUnion) {
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
                    if (inputs.size() > 1) {
                        return false;
                    } else if (inputs.isEmpty()) {
                        // for constant literal, we must put it in the other child
                        Pair<LogicalJoin<?, ?>, Plan> joinWithChild = commonChild.get(i);
                        if (joinWithChild.first.child(0).equals(joinWithChild.second)) {
                            constantAlias.computeIfAbsent(joinWithChild.first.child(1), k -> new HashSet<>())
                                    .add(expr);
                        } else {
                            constantAlias.computeIfAbsent(joinWithChild.first.child(0), k -> new HashSet<>())
                                    .add(expr);
                        }
                    } else {
                        joinSlotToProject.put(inputs.iterator().next(), expr);
                    }
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
