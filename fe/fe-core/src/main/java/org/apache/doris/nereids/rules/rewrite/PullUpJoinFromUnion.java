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
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Pull up join from union all rules:
 *       Union
 *       /    \
 *     Join   Join
 *     / \    / \
 *    t1 t2   t1 t3   (t1 is common side; t2,t3 is other side)
 *  =====>
 *           project
 *              |
 *            Join
 *          /    \
 *       Union   t1
 *       /    \
 *       t2   t3
 *
 * Pull up join from union all rules with project:
 *       Union
 *       /    \
 *    project project
 *      |      |
 *     Join   Join
 *     / \    / \
 *    t1 t2   t1 t3   (t1 is common side; t2,t3 is other side)
 *  =====>
 *          project
 *            |
 *           Join
 *          /    \
 *       Union   t1
 *       /    \
 *   project project
 *      |      |
 *      t2    t3
 */
public class PullUpJoinFromUnion extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalUnion()
                .when(union -> union.getQualifier() != Qualifier.DISTINCT
                        && union.getConstantExprsList().isEmpty())
                .then(union -> {
                    HashMap<Plan, List<Pair<LogicalJoin<?, ?>, Plan>>> commonChildrenMap =
                            tryToExtractCommonChild(union);
                    if (commonChildrenMap == null) {
                        return null;
                    }

                    // The joinsAndCommonSides size is the same as the number of union children.
                    List<Pair<LogicalJoin<?, ?>, Plan>> joinsAndCommonSides = null;
                    for (List<Pair<LogicalJoin<?, ?>, Plan>> childSet : commonChildrenMap.values()) {
                        if (childSet.size() == union.children().size()) {
                            joinsAndCommonSides = childSet;
                            break;
                        }
                    }
                    if (joinsAndCommonSides == null) {
                        return null;
                    }

                    List<List<NamedExpression>> otherOutputsList = new ArrayList<>();
                    List<Pair<Boolean, ExpressionOrIndex>> upperProjectExpressionOrIndex = new ArrayList<>();
                    // First, check whether the output of the union child meets the requirements.
                    if (!checkUnionChildrenOutput(union, joinsAndCommonSides, otherOutputsList,
                            upperProjectExpressionOrIndex)) {
                        return null;
                    }

                    List<Map<SlotReference, List<SlotReference>>> commonSlotToOtherSlotMaps = new ArrayList<>();
                    Set<SlotReference> joinCommonSlots = new LinkedHashSet<>();
                    if (!checkJoinCondition(joinsAndCommonSides, commonSlotToOtherSlotMaps, joinCommonSlots)) {
                        return null;
                    }

                    Map<SlotReference, List<Integer>> commonSlotToProjectsIndex = new HashMap<>();
                    LogicalUnion newUnion = constructNewUnion(joinsAndCommonSides, otherOutputsList,
                            commonSlotToOtherSlotMaps, joinCommonSlots, commonSlotToProjectsIndex);
                    LogicalJoin<LogicalUnion, Plan> newJoin = constructNewJoin(newUnion,
                            commonSlotToProjectsIndex, joinsAndCommonSides);
                    LogicalProject newProject = constructNewProject(union, newJoin, upperProjectExpressionOrIndex);
                    return newProject;
                }).toRule(RuleType.PULL_UP_JOIN_FROM_UNION);
    }

    private LogicalProject<Plan> constructNewProject(LogicalUnion originUnion, LogicalJoin<LogicalUnion, Plan> newJoin,
            List<Pair<Boolean, ExpressionOrIndex>> upperProjectExpressionOrIndex) {
        List<Slot> originOutput = originUnion.getOutput();
        List<NamedExpression> upperProjects = new ArrayList<>();
        List<Slot> newUnionOutput = newJoin.left().getOutput();
        if (originOutput.size() != upperProjectExpressionOrIndex.size()) {
            return null;
        }
        for (int i = 0; i < upperProjectExpressionOrIndex.size(); ++i) {
            Pair<Boolean, ExpressionOrIndex> pair = upperProjectExpressionOrIndex.get(i);
            boolean fromCommon = pair.first;
            if (fromCommon) {
                upperProjects.add(new Alias(originOutput.get(i).getExprId(), pair.second.exprFromCommonSide,
                        originOutput.get(i).getName()));
            } else {
                upperProjects.add(new Alias(originOutput.get(i).getExprId(),
                        newUnionOutput.get(pair.second.indexOfNewUnionOutput), originOutput.get(i).getName()));
            }
        }
        return new LogicalProject<>(upperProjects, newJoin);
    }

    private LogicalJoin<LogicalUnion, Plan> constructNewJoin(LogicalUnion union,
            Map<SlotReference, List<Integer>> commonSlotToProjectsIndex,
            List<Pair<LogicalJoin<?, ?>, Plan>> commonChild) {
        LogicalJoin<?, ?> originalJoin = commonChild.iterator().next().first;
        Plan newCommon = commonChild.iterator().next().second;
        List<Expression> newHashExpressions = new ArrayList<>();
        List<Slot> unionOutputs = union.getOutput();
        for (Map.Entry<SlotReference, List<Integer>> entry : commonSlotToProjectsIndex.entrySet()) {
            SlotReference commonSlot = entry.getKey();
            for (Integer index : entry.getValue()) {
                newHashExpressions.add(new EqualTo(unionOutputs.get(index), commonSlot));
            }
        }
        return (LogicalJoin<LogicalUnion, Plan>) originalJoin
                .withJoinConjuncts(newHashExpressions, ImmutableList.of(), originalJoin.getJoinReorderContext())
                .withChildren(union, newCommon);
    }

    // Output parameter: commonSlotToProjectsIndex, key is the common slot of join condition,
    // value is the index of the other slot corresponding to this common slot in the union output,
    // which is used to construct the join condition of the new join.
    private LogicalUnion constructNewUnion(List<Pair<LogicalJoin<?, ?>, Plan>> joinsAndCommonSides,
            List<List<NamedExpression>> otherOutputsList, List<Map<SlotReference,
            List<SlotReference>>> commonSlotToOtherSlotMaps,
            Set<SlotReference> joinCommonSlots, Map<SlotReference, List<Integer>> commonSlotToProjectsIndex) {
        List<Plan> newChildren = new ArrayList<>();
        for (int i = 0; i < joinsAndCommonSides.size(); ++i) {
            Pair<LogicalJoin<?, ?>, Plan> pair = joinsAndCommonSides.get(i);
            // find the child that is not the common side
            Plan otherSide;
            if (pair.second == pair.first.left()) {
                otherSide = pair.first.right();
            } else {
                otherSide = pair.first.left();
            }
            List<NamedExpression> projects = otherOutputsList.get(i);
            // In projects, we also need to add the other slot in join condition
            Map<SlotReference, List<SlotReference>> commonSlotToOtherSlotMap = commonSlotToOtherSlotMaps.get(i);
            for (SlotReference commonSlot : joinCommonSlots) {
                List<SlotReference> otherSlots = commonSlotToOtherSlotMap.get(commonSlot);
                for (SlotReference otherSlot : otherSlots) {
                    if (i == 0) {
                        int index = projects.size();
                        commonSlotToProjectsIndex.computeIfAbsent(commonSlot, k -> new ArrayList<>()).add(index);
                    }
                    projects.add(otherSlot);
                }
            }
            LogicalProject<Plan> logicalProject = new LogicalProject<>(projects, otherSide);
            newChildren.add(logicalProject);
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

    // This function is used to check whether the join condition meets the optimization condition
    // Check the join condition, requiring that the join condition of each join is equal and the number is the same.
    // Generate commonSlotToOtherSlotMaps. In each map of the list, the keySet must be the same,
    // and the length of the value list of the same key must be the same.
    // Output parameter: commonSlotToOtherSlotMaps, which records the join condition of each join.
    // The key is the slot on the common side of the join, and the value is the slot on the other side of the join.
    // e.g. select t2.a+1,2 from test_like1 t1 join test_like2 t2 on t1.a=t2.a and t1.a=t2.c and t1.b=t2.b union ALL
    // select t3.a+1,3 from test_like1 t1 join test_like3 t3 on t1.a=t3.a and t1.a=t3.d and t1.b=t3.b
    // commonSlotToOtherSlotMaps： {{t1.a:t2.a,t2.c; t1.b:t2.b},{t1.a:t3.a,t3.d; t1.b:t3.b}}
    // commonSlotToOtherSlotMaps is used to check whether the join condition meets the optimization conditions
    // and to generate the join condition for the new join
    private boolean checkJoinCondition(List<Pair<LogicalJoin<?, ?>, Plan>> joinsAndCommonSides,
            List<Map<SlotReference, List<SlotReference>>> commonSlotToOtherSlotMaps,
            Set<SlotReference> joinCommonSlots) {
        Map<SlotReference, List<SlotReference>> conditionMapFirst = new HashMap<>();
        Map<Slot, Slot> commonJoinSlotMap = buildCommonJoinMap(joinsAndCommonSides);
        for (int i = 0; i < joinsAndCommonSides.size(); ++i) {
            Pair<LogicalJoin<?, ?>, Plan> pair = joinsAndCommonSides.get(i);
            LogicalJoin<?, ?> join = pair.first;
            Plan commonSide = pair.second;
            Map<SlotReference, List<SlotReference>> conditionMapSubsequent = new HashMap<>();
            for (Expression condition : join.getHashJoinConjuncts()) {
                if (!(condition instanceof EqualTo)) {
                    return false;
                }
                EqualTo equalTo = (EqualTo) condition;
                if (!(equalTo.left() instanceof SlotReference) || !(equalTo.right() instanceof SlotReference)) {
                    return false;
                }
                SlotReference commonSideSlot;
                SlotReference otherSideSlot;
                if (commonSide.getOutputSet().contains(equalTo.left())) {
                    commonSideSlot = (SlotReference) equalTo.left();
                    otherSideSlot = (SlotReference) equalTo.right();
                } else {
                    commonSideSlot = (SlotReference) equalTo.right();
                    otherSideSlot = (SlotReference) equalTo.left();
                }
                if (i == 0) {
                    conditionMapFirst.computeIfAbsent(commonSideSlot, k -> new ArrayList<>()).add(otherSideSlot);
                    joinCommonSlots.add(commonSideSlot);
                } else {
                    conditionMapSubsequent.computeIfAbsent(
                            (SlotReference) ExpressionUtils.replace(commonSideSlot, commonJoinSlotMap),
                            k -> new ArrayList<>()).add(otherSideSlot);
                }
            }
            if (i == 0) {
                commonSlotToOtherSlotMaps.add(conditionMapFirst);
            } else {
                if (conditionMapSubsequent.size() != conditionMapFirst.size()) {
                    return false;
                }
                if (!conditionMapSubsequent.keySet().equals(conditionMapFirst.keySet())) {
                    return false;
                }
                for (Map.Entry<SlotReference, List<SlotReference>> entry : conditionMapFirst.entrySet()) {
                    SlotReference commonSlot = entry.getKey();
                    if (conditionMapSubsequent.get(commonSlot).size() != entry.getValue().size()) {
                        return false;
                    }
                }
                commonSlotToOtherSlotMaps.add(conditionMapSubsequent);
            }
        }
        return true;
    }

    // Make a map to map the output of all other joins to the output of the first join
    private Map<Slot, Slot> buildCommonJoinMap(List<Pair<LogicalJoin<?, ?>, Plan>> commonChild) {
        Map<Slot, Slot> commonJoinSlotMap = new HashMap<>();
        List<Slot> firstJoinOutput = new ArrayList<>();
        for (int i = 0; i < commonChild.size(); ++i) {
            Pair<LogicalJoin<?, ?>, Plan> pair = commonChild.get(i);
            Plan commonSide = pair.second;
            if (i == 0) {
                firstJoinOutput.addAll(commonSide.getOutput());
                for (Slot slot : commonSide.getOutput()) {
                    commonJoinSlotMap.put(slot, slot);
                }
            } else {
                for (int j = 0; j < commonSide.getOutput().size(); ++j) {
                    commonJoinSlotMap.put(commonSide.getOutput().get(j), firstJoinOutput.get(j));
                }
            }
        }
        return commonJoinSlotMap;
    }

    private class ExpressionOrIndex {
        Expression exprFromCommonSide = null;
        int indexOfNewUnionOutput = -1;

        private ExpressionOrIndex(Expression expr) {
            exprFromCommonSide = expr;
        }

        private ExpressionOrIndex(int index) {
            indexOfNewUnionOutput = index;
        }
    }

    // In the union child output, the number of outputs from the common side must be the same in each child output,
    // and the outputs from the common side must be isomorphic (both a+1) and have the same index in the union output.
    // In the union child output, the number of outputs from the non-common side must also be the same,
    // but they do not need to be isomorphic.
    // Output parameters1: otherOutputsList stores the outputs of the other side. The length of each element
    // in otherOutputsList must be the same.
    // The i-th element represents the output of the other side in the i-th child of the union.
    // otherOutputsList is Used to create child nodes of a new Union in the constructNewUnion function.
    // Output parameter2: upperProjectExpressionOrIndex, used in the constructNewProject function
    // of creating the top-level project,
    // records the output column order of the original union, and is used on the basis of the new join output,
    // setting the column or expression that should be output in the upper-level project operator.
    // The size of upperProjectExpressionOrIndex must be the same as the output size of the original union.
    // Pair.first in List represents whether the output comes from the common side or the other side.
    // True represents the output from the common side, and false represents the output from the other side.
    // Pair.second in List is a structure. When Pair.first is true,
    // Pair.second saves the output expression of the common side.
    // When Pair.first is false, Pair.second saves the output subscript of the other side.
    // Because the output of the new union has not been constructed at this time, the index is saved.
    // Since the check part of this function ensures that the outputs at the same position in the union children
    // must all come from the common side or from the other side,
    // and when the join is constructed at the end, the common side will use the common side of the first join,
    // so we only need to fill in upperProjectExpressionOrIndex when processing the output of the first child.
    private boolean checkUnionChildrenOutput(LogicalUnion union,
            List<Pair<LogicalJoin<?, ?>, Plan>> joinsAndCommonSides,
            List<List<NamedExpression>> otherOutputsList,
            List<Pair<Boolean, ExpressionOrIndex>> upperProjectExpressionOrIndex) {
        List<List<SlotReference>> regularChildrenOutputs = union.getRegularChildrenOutputs();
        int arity = union.arity();
        if (arity == 0) {
            return false;
        }
        // fromCommonSide is used to ensure that the outputs at the same position in the union children
        // must all come from the common side or from the other side
        boolean[] fromCommonSide = new boolean[regularChildrenOutputs.get(0).size()];
        // checkSameExpr and commonJoinSlotMap are used to ensure that Expr from the common side have the same structure
        Expression[] checkSameExpr = new Expression[regularChildrenOutputs.get(0).size()];
        Map<Slot, Slot> commonJoinSlotMap = buildCommonJoinMap(joinsAndCommonSides);
        for (int i = 0; i < arity; ++i) {
            List<SlotReference> regularChildrenOutput = regularChildrenOutputs.get(i);
            Plan child = union.child(i);
            List<NamedExpression> otherOutputs = new ArrayList<>();
            for (int j = 0; j < regularChildrenOutput.size(); ++j) {
                SlotReference slot = regularChildrenOutput.get(j);
                // 判断这个slot是来自于join的common side还是other side
                if (child instanceof LogicalProject) {
                    LogicalProject<Plan> project = (LogicalProject<Plan>) child;
                    int index = project.getOutput().indexOf(slot);
                    NamedExpression expr = project.getOutputs().get(index);
                    Slot insideSlot;
                    Expression insideExpr;
                    Set<Slot> inputSlots = expr.getInputSlots();
                    if (inputSlots.size() > 1) {
                        return false;
                    } else if (inputSlots.size() == 1) {
                        if (expr instanceof Alias) {
                            insideSlot = inputSlots.iterator().next();
                            insideExpr = expr.child(0);
                        } else if (expr instanceof SlotReference) {
                            insideSlot = (Slot) expr;
                            insideExpr = expr;
                        } else {
                            return false;
                        }

                        Plan commonSide = joinsAndCommonSides.get(i).second;
                        if (i == 0) {
                            if (commonSide.getOutputSet().contains(insideSlot)) {
                                fromCommonSide[j] = true;
                                checkSameExpr[j] = insideExpr;
                                upperProjectExpressionOrIndex.add(Pair.of(true, new ExpressionOrIndex(insideExpr)));
                            } else {
                                fromCommonSide[j] = false;
                                upperProjectExpressionOrIndex.add(Pair.of(false, new ExpressionOrIndex(
                                        otherOutputs.size())));
                                otherOutputs.add(expr);
                            }
                        } else {
                            if (commonSide.getOutputSet().contains(insideSlot) != fromCommonSide[j]) {
                                return false;
                            }
                            if (commonSide.getOutputSet().contains(insideSlot)) {
                                Expression sameExpr = ExpressionUtils.replace(insideExpr, commonJoinSlotMap);
                                if (!sameExpr.equals(checkSameExpr[j])) {
                                    return false;
                                }
                            } else {
                                otherOutputs.add(expr);
                            }
                        }
                    } else if (expr.getInputSlots().isEmpty()) {
                        // Constants must come from other side
                        if (i == 0) {
                            fromCommonSide[j] = false;
                            upperProjectExpressionOrIndex.add(Pair.of(false, new ExpressionOrIndex(
                                    otherOutputs.size())));
                        } else {
                            if (fromCommonSide[j]) {
                                return false;
                            }
                        }
                        otherOutputs.add(expr);
                    }
                } else if (child instanceof LogicalJoin) {
                    Plan commonSide = joinsAndCommonSides.get(i).second;
                    if (i == 0) {
                        if (commonSide.getOutputSet().contains(slot)) {
                            fromCommonSide[j] = true;
                            checkSameExpr[j] = slot;
                            upperProjectExpressionOrIndex.add(Pair.of(true, new ExpressionOrIndex(slot)));
                        } else {
                            fromCommonSide[j] = false;
                            upperProjectExpressionOrIndex.add(Pair.of(false,
                                    new ExpressionOrIndex(otherOutputs.size())));
                            otherOutputs.add(slot);
                        }
                    } else {
                        if (commonSide.getOutputSet().contains(slot) != fromCommonSide[j]) {
                            return false;
                        }
                        if (commonSide.getOutputSet().contains(slot)) {
                            Expression sameExpr = ExpressionUtils.replace(slot, commonJoinSlotMap);
                            if (!sameExpr.equals(checkSameExpr[j])) {
                                return false;
                            }
                        } else {
                            otherOutputs.add(slot);
                        }
                    }
                }
            }
            otherOutputsList.add(otherOutputs);
        }
        return true;
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
                if (plan1.getOutput().size() != plan2.getOutput().size()) {
                    isEqual = false;
                }
                for (int i = 0; isEqual && i < plan2.getOutput().size(); i++) {
                    NamedExpression expr = ((LogicalProject<?>) plan1).getProjects().get(i);
                    NamedExpression replacedExpr = (NamedExpression)
                            expr.rewriteUp(e -> plan1ToPlan2.getOrDefault(e, e));
                    if (!replacedExpr.equals(((LogicalProject<?>) plan2).getProjects().get(i))) {
                        isEqual = false;
                    }
                }

            } else if (plan1 instanceof LogicalFilter && plan2 instanceof LogicalFilter) {
                Set<Expression> replacedConjuncts = new HashSet<>();
                for (Expression expr : ((LogicalFilter<?>) plan1).getConjuncts()) {
                    replacedConjuncts.add(expr.rewriteUp(e -> plan1ToPlan2.getOrDefault(e, e)));
                }
                isEqual = replacedConjuncts.equals(((LogicalFilter<?>) plan2).getConjuncts());
            } else {
                isEqual = false;
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
