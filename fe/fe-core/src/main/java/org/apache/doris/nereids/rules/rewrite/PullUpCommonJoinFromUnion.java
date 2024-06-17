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
 */
public class PullUpCommonJoinFromUnion extends OneRewriteRuleFactory {

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

                    HashMap<Expression, Integer> slotToIndex = mapSlotToIndex(union);
                    if (!checkJoinCondition(commonChild, slotToIndex)) {
                        return null;
                    }

                    return constructNewJoinUnion(union, commonChild, slotToIndex);
                }).toRule(RuleType.PULL_UP_COMMON_JOIN_FROM_UNION);
    }

    // For union children, we must keep the join condition satisfied the following conditions:
    // 1. The one sides are the same with others
    // 2. The other side has the same index in the union
    // this function is checking the second condition
    private boolean checkJoinCondition(
            Set<Pair<LogicalJoin<?, ?>, Plan>> commonChild, HashMap<Expression, Integer> slotToIndex) {
        HashMap<Integer, Integer> joinCondIndices = new HashMap<>();
        for (Pair<LogicalJoin<?, ?>, Plan> joinChildPair : commonChild) {
            for (Expression expression : joinChildPair.first.getHashJoinConjuncts()) {
                if (!(expression instanceof EqualTo)
                        && expression.child(0).isSlot()
                        && expression.child(1).isSlot()) {
                    return false;
                }
                int left = slotToIndex.get(expression.child(0));
                int right = slotToIndex.get(expression.child(1));
                if (joinCondIndices.containsKey(left) && joinCondIndices.get(left) != right) {
                    return false;
                }
                if (joinCondIndices.containsKey(right) && joinCondIndices.get(right) != left) {
                    return false;
                }
                joinCondIndices.put(left, right);
                joinCondIndices.put(right, left);
            }
        }
        return true;
    }

    private Plan constructNewJoinUnion(
            LogicalUnion union, Set<Pair<LogicalJoin<?, ?>, Plan>> commonChild,
            HashMap<Expression, Integer> slotToIndex) {

        // 1. construct new children of union
        List<Plan> newChildren = new ArrayList<>();
        for (Pair<LogicalJoin<?, ?>, Plan> child : commonChild) {
            // find the child that is not the common side
            Plan newChild;
            if (child.first.child(0).equals(child.second)) {
                newChild = constructNewChild(child.first.child(1), slotToIndex);
            } else {
                newChild = constructNewChild(child.first.child(0), slotToIndex);
            }
            newChildren.add(newChild);
        }

        //2. construct new union
        LogicalUnion newUnion = union.withChildren(newChildren);
        HashMap<Expression, Integer> newSlotToIndex = new HashMap<>();
        for (Plan child : newUnion.children()) {
            for (int i = 0; i < child.getOutput().size(); i++) {
                newSlotToIndex.put(child.getOutput().get(i), i);
            }
        }

        // 3. construct join union
        LogicalJoin<?, ?> originalJoin = commonChild.iterator().next().first;
        Plan originalChild = commonChild.iterator().next().second;
        Plan newJoin = constructNewJoin(originalJoin, newUnion, originalChild, newSlotToIndex);

        // 4. map the output to origin output
        List<NamedExpression> newOutput = constructFinalOutput(newJoin, union, slotToIndex, newUnion, newSlotToIndex);
        return new LogicalProject<>(newOutput, newJoin);
    }

    private List<NamedExpression> constructFinalOutput(Plan plan,
            LogicalUnion originalUnion,
            Map<Expression, Integer> originalSlotToIndex,
            LogicalUnion newUnion,
            Map<Expression, Integer> newSlotToIndex) {
        HashMap<Integer, Integer> originalUnionToNewUnion = new HashMap<>();
        for (Expression expr : originalSlotToIndex.keySet()) {
            if (!newSlotToIndex.containsKey(expr)) {
                continue;
            }
            originalUnionToNewUnion.put(originalSlotToIndex.get(expr), newSlotToIndex.get(expr));
        }

        List<NamedExpression> newOutput = new ArrayList<>();
        Set<Slot> outputs = plan.getOutputSet();
        for (int i = 0; i < originalUnion.getOutput().size(); i++) {
            Slot outputSlot = originalUnion.getOutput().get(i);
            if (outputs.contains(outputSlot)) {
                continue;
            }
            int newUnionIndex = originalUnionToNewUnion.get(i);
            Slot newUnionSlot = newUnion.getOutput().get(newUnionIndex);
            newOutput.add(new Alias(outputSlot.getExprId(), newUnionSlot, outputSlot.getName()));
        }
        return newOutput;
    }

    private Plan constructNewChild(Plan child, HashMap<Expression, Integer> slotToIndex) {
        List<NamedExpression> output = slotToIndex.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(e -> (Slot) (e.getKey()))
                .filter(child.getOutputSet()::contains)
                .collect(Collectors.toList());
        if (child.getOutput().equals(output)) {
            return child;
        }
        return new LogicalProject<>(output, child);
    }

    private Plan constructNewJoin(LogicalJoin<?, ?> originalJoin,
            LogicalUnion unionChild, Plan commonChild, Map<Expression, Integer> slotToIndex) {
        HashMap<Expression, Expression> replacedSlotMap = new HashMap<>();
        for (Entry<Expression, Integer> entry : slotToIndex.entrySet()) {
            replacedSlotMap.put(entry.getKey(), unionChild.getOutput().get(entry.getValue()));
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

    private HashMap<Expression, Integer> mapSlotToIndex(LogicalUnion union) {
        HashMap<Expression, Integer> slotToIndex = new HashMap<>();
        for (int i = 0; i < union.children().size(); i++) {
            Plan child = union.child(i);
            if (union.getRegularChildOutput(i).isEmpty()) {
                for (int slotIdx = 0; slotIdx < child.getOutput().size(); slotIdx++) {
                    slotToIndex.put(child.getOutput().get(slotIdx), slotIdx);
                }
            } else {
                for (int slotIdx = 0; slotIdx < union.getRegularChildOutput(i).size(); slotIdx++) {
                    slotToIndex.put(union.getRegularChildOutput(i).get(slotIdx), slotIdx);
                }
            }
        }
        return slotToIndex;
    }

    // we only allow project(join) or join()
    private @Nullable LogicalJoin<?, ?> tryToGetJoin(Plan child) {
        if (child instanceof LogicalProject) {
            if (!((LogicalProject<?>) child).isAllSlots()) {
                return null;
            }
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
        HashMap<Expression, Expression> plan1ToPlan2 = new HashMap<>();

        public boolean isLogicalEqual(Plan plan1, Plan plan2) {
            if (plan1.children().size() != plan2.children().size()) {
                return false;
            }
            for (int i = 0; i < plan1.children().size(); i++) {
                if (!isLogicalEqual(plan1.child(i), plan2.child(i))) {
                    return false;
                }
            }
            return comparePlan(plan1, plan2);
        }

        public boolean comparePlan(Plan plan1, Plan plan2) {
            boolean isEqual = true;
            if (plan1 instanceof LogicalCatalogRelation && plan2 instanceof LogicalCatalogRelation) {
                isEqual = new TableIdentifier(((LogicalCatalogRelation) plan1).getTable())
                        .equals(new TableIdentifier(((LogicalCatalogRelation) plan2).getTable()));
            } else if (plan1 instanceof LogicalProject && plan2 instanceof LogicalProject) {
                for (int i = 0; i < plan2.getOutput().size(); i++) {
                    NamedExpression expr = ((LogicalProject<?>) plan2).getProjects().get(i);
                    NamedExpression replacedExpr = (NamedExpression)
                            expr.rewriteUp(e -> plan1ToPlan2.getOrDefault(e, e));
                    if (!replacedExpr.equals(((LogicalProject<?>) plan1).getProjects().get(i))) {
                        isEqual = false;
                        break;
                    }
                }

            } else if (plan1 instanceof LogicalFilter && plan2 instanceof LogicalFilter) {
                Set<Expression> replacedConjuncts = new HashSet<>();
                for (Expression expr : ((LogicalFilter<?>) plan2).getConjuncts()) {
                    replacedConjuncts.add(expr.rewriteUp(e -> plan1ToPlan2.getOrDefault(e, e)));
                }
                isEqual = replacedConjuncts.equals(((LogicalFilter<?>) plan1).getConjuncts());
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
