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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.StatsDerive;
import org.apache.doris.nereids.rules.rewrite.eageraggregation.EagerAggHints.Action;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * eager aggregation
 * agg[sum(t1.A) group by t1.B]
 *    ->join(t1.C=t2.D)
 *        ->T1(A, B, C)
 *        ->T2(D)
 *
 * =>
 * agg[sum(x) group by t1.B]
 *     ->join(t1.C=t2.D)
 *         ->agg[sum(A) as x, group by B]
 *             ->T1(A, B, C)
 *         ->T2(D)
 */
public class EagerAggRewriter extends DefaultPlanRewriter<PushDownAggContext> {
    public static final int BIG_JOIN_BUILD_SIZE = 400_000;
    private static final double LOWER_AGGREGATE_EFFECT_COEFFICIENT = 10000;
    private static final double LOW_AGGREGATE_EFFECT_COEFFICIENT = 1000;
    private static final double MEDIUM_AGGREGATE_EFFECT_COEFFICIENT = 100;
    private static final String JOIN_CNT = "joinCnt";
    private final StatsDerive derive = new StatsDerive(false);

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, PushDownAggContext context) {
        Pair<Boolean, Boolean> pushSide = decideJoinPushSide(join, context);
        boolean toLeft = pushSide.first;
        boolean toRight = pushSide.second;
        if (!toLeft && !toRight) {
            if (SessionVariable.isEagerAggregationOnJoin()) {
                return genAggregate(join, context);
            } else {
                return join;
            }
        }

        // construct left and right group by keys
        List<SlotReference> leftChildGroupByKeys = new ArrayList<>();
        List<SlotReference> rightChildGroupByKeys = new ArrayList<>();
        if (toLeft) {
            fillGroupByKeys(join, join.left(), context, leftChildGroupByKeys);
        }
        if (toRight) {
            fillGroupByKeys(join, join.right(), context, rightChildGroupByKeys);
        }
        // construct left and right aggFuncs and aliasMap
        List<AggregateFunction> leftFuncs = new ArrayList<>();
        List<AggregateFunction> rightFuncs = new ArrayList<>();
        Map<AggregateFunction, Alias> leftAliasMap = new IdentityHashMap<>();
        Map<AggregateFunction, Alias> rightAliasMap = new IdentityHashMap<>();
        for (AggregateFunction f : context.getAggFunctions()) {
            Set<Slot> inputs = f.getInputSlots();
            Alias a = context.getAliasMap().get(f);
            if (join.left().getOutputSet().containsAll(inputs)) {
                leftFuncs.add(f);
                leftAliasMap.put(f, a);
            } else if (join.right().getOutputSet().containsAll(inputs)) {
                rightFuncs.add(f);
                rightAliasMap.put(f, a);
            } else {
                return join;
            }
        }

        boolean passThroughBigJoin = isPassThroughBigJoin(join, context);
        boolean leftNeedOutputCount = needOutputCountForJoinChild(join, toLeft, toRight,
                context.needOutputCount(), rightFuncs);
        boolean rightNeedOutputCount = needOutputCountForJoinChild(join, toRight, toLeft,
                context.needOutputCount(), leftFuncs);
        Optional<PushDownAggContext> leftChildContext = toLeft ? Optional.of(context.forOneBranch(leftFuncs,
                leftAliasMap, leftChildGroupByKeys, passThroughBigJoin, leftNeedOutputCount)) : Optional.empty();
        Optional<PushDownAggContext> rightChildContext = toRight ? Optional.of(context.forOneBranch(rightFuncs,
                rightAliasMap, rightChildGroupByKeys, passThroughBigJoin, rightNeedOutputCount)) : Optional.empty();

        Plan newLeft = join.left();
        Plan newRight = join.right();
        if (leftChildContext.isPresent()) {
            newLeft = join.left().accept(this, leftChildContext.get());
        }
        if (rightChildContext.isPresent()) {
            newRight = join.right().accept(this, rightChildContext.get());
        }

        if (newLeft == join.left() && newRight == join.right()) {
            context.getBilateralState().registerNoCountSlot(join);
            return join;
        }
        Optional<Slot> leftChildCountSlot = context.getBilateralState().getCountSlot(newLeft);
        Optional<Slot> rightChildCountSlot = context.getBilateralState().getCountSlot(newRight);
        LogicalJoin<? extends Plan, ? extends Plan> newJoin = (LogicalJoin<? extends Plan, ? extends Plan>)
                join.withChildren(newLeft, newRight);

        if (leftChildCountSlot.isPresent() || rightChildCountSlot.isPresent()) {
            return buildCanonicalJoinProject(newJoin, context, leftChildContext, rightChildContext,
                    leftChildCountSlot, rightChildCountSlot);
        }
        context.getBilateralState().registerNoCountSlot(newJoin);
        return newJoin;
    }

    private Pair<Boolean, Boolean> decideJoinPushSide(
            LogicalJoin<? extends Plan, ? extends Plan> join, PushDownAggContext context) {
        if (join.getJoinType().isAsofJoin()) {
            // do nothing for asof join
            return Pair.of(false, false);
        }

        boolean deduplicateOnly = context.getAggFunctions().isEmpty();
        boolean toLeft = false;
        boolean toRight = false;
        if (deduplicateOnly) {
            toLeft = true;
            toRight = true;
        } else {
            for (AggregateFunction aggFunc : context.getAggFunctions()) {
                if (join.left().getOutputSet().containsAll(aggFunc.getInputSlots())) {
                    toLeft = true;
                } else if (join.right().getOutputSet().containsAll(aggFunc.getInputSlots())) {
                    toRight = true;
                } else {
                    toLeft = false;
                    toRight = false;
                    break;
                }
            }
        }
        if (!toLeft && !toRight) {
            return Pair.of(false, false);
        }
        if (deduplicateOnly) {
            return adjustPushSideForCaseWhen(join, context, toLeft, toRight);
        }
        if (toLeft && toRight) {
            return join.getJoinType().isInnerOrCrossJoin()
                    ? Pair.of(true, true)
                    : Pair.of(false, false);
        }
        // one-side push down
        Pair<Boolean, Boolean> pushSide = adjustPushSideForCaseWhen(join, context, toLeft, toRight);
        if (!pushSide.first && !pushSide.second) {
            return pushSide;
        }
        return adjustPushSideForNullable(join, context, pushSide.first, pushSide.second);
    }

    private boolean needOutputCountForJoinChild(LogicalJoin<? extends Plan, ? extends Plan> join,
            boolean toChild, boolean toOpposite, boolean parentNeedsOutputCount,
            List<AggregateFunction> oppositeAggFunctions) {
        if (!toChild) {
            return false;
        }
        if (parentNeedsOutputCount) {
            return true;
        }
        return toOpposite && join.getJoinType().isInnerOrCrossJoin()
                && hasAggNeedJoinMultiplicityRecovery(oppositeAggFunctions);
    }

    private Pair<Boolean, Boolean> adjustPushSideForCaseWhen(
            LogicalJoin<? extends Plan, ? extends Plan> join, PushDownAggContext context,
            boolean toLeft, boolean toRight) {
        // Do not push aggregation to the nullable side of outer joins when agg function contains case-when.
        // CaseWhen expressions may produce non-null values from null-padded rows (e.g., WHEN col IS NULL THEN -54),
        // so pre-aggregation before the join loses those contributions.
        if (!(context.hasDecomposedAggIf || context.hasCaseWhen)) {
            return Pair.of(toLeft, toRight);
        }
        JoinType joinType = join.getJoinType();
        if (joinType.isFullOuterJoin()) {
            toLeft = false;
            toRight = false;
        }
        if (joinType.isRightOuterJoin()) {
            toLeft = false;
        }
        if (joinType.isLeftOuterJoin()) {
            toRight = false;
        }
        return Pair.of(toLeft, toRight);
    }

    // Do not push agg(literal) or agg(preserved_side_col) to the nullable side of outer joins.
    // Aggregates like count(*), sum(2), min(1) etc. aggregate over all physical rows,
    // including null-extended rows from the outer join.
    // After pushdown to the nullable side, unmatched rows produce NULL for the pre-aggregated value,
    // losing the contribution of those rows (e.g. sum(2) should add 2 per unmatched row,
    // but sum(NULL) skips them).
    // However, agg(nullable_side_col) is safe to push down because for unmatched rows,
    // nullable_side_col IS NULL, and the aggregate naturally handles NULL values correctly.
    private Pair<Boolean, Boolean> adjustPushSideForNullable(LogicalJoin<? extends Plan, ? extends Plan> join,
            PushDownAggContext context, boolean toLeft, boolean toRight) {
        if (!join.getJoinType().isInnerJoin() && !join.getJoinType().isCrossJoin()) {
            JoinType joinType = join.getJoinType();
            boolean leftIsNullable = joinType.isRightOuterJoin() || joinType.isFullOuterJoin();
            boolean rightIsNullable = joinType.isLeftOuterJoin() || joinType.isFullOuterJoin();
            for (AggregateFunction aggFunc : context.getAggFunctions()) {
                Set<Slot> inputSlots = aggFunc.getInputSlots();
                if (toLeft && leftIsNullable) {
                    boolean hasLeftInput = inputSlots.stream()
                            .anyMatch(slot -> join.left().getOutputSet().contains(slot));
                    if (!hasLeftInput) {
                        toLeft = false;
                    }
                }
                if (toRight && rightIsNullable) {
                    boolean hasRightInput = inputSlots.stream()
                            .anyMatch(slot -> join.right().getOutputSet().contains(slot));
                    if (!hasRightInput) {
                        toRight = false;
                    }
                }
            }
        }
        return Pair.of(toLeft, toRight);
    }

    private boolean isPassThroughBigJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
            PushDownAggContext context) {
        if (context.isPassThroughBigJoin()) {
            return true;
        } else {
            Statistics stats = join.right().getStats();
            if (stats == null) {
                stats = join.right().accept(derive, new StatsDerive.DeriveContext());
            }
            return stats.getRowCount() > BIG_JOIN_BUILD_SIZE || SessionVariable.getEagerAggregationMode() > 0;
        }
    }

    private void fillGroupByKeys(LogicalJoin<? extends Plan, ? extends Plan> join, Plan child,
            PushDownAggContext context, List<SlotReference> leftChildGroupByKeys) {
        for (SlotReference key : context.getGroupKeys()) {
            if (child.getOutputSet().containsAll(key.getInputSlots())) {
                leftChildGroupByKeys.add(key);
            }
        }
        List<SlotReference> joinConditionSlots = getJoinConditionsInputSlotsFromOneSide(join, child);
        for (SlotReference slot : joinConditionSlots) {
            if (!leftChildGroupByKeys.contains(slot)) {
                leftChildGroupByKeys.add(slot);
            }
        }
    }

    private List<SlotReference> getJoinConditionsInputSlotsFromOneSide(
            LogicalJoin<? extends Plan, ? extends Plan> join,
            Plan side) {
        List<SlotReference> oneSideSlots = new ArrayList<>();
        for (Expression condition : join.getHashJoinConjuncts()) {
            for (Slot slot : condition.getInputSlots()) {
                if (side.getOutputSet().contains(slot)) {
                    oneSideSlots.add((SlotReference) slot);
                }
            }
        }
        for (Expression condition : join.getOtherJoinConjuncts()) {
            for (Slot slot : condition.getInputSlots()) {
                if (side.getOutputSet().contains(slot)) {
                    oneSideSlots.add((SlotReference) slot);
                }
            }
        }
        return oneSideSlots;
    }

    private PushDownAggContext createContextFromProject(
            LogicalProject<? extends Plan> project,
            PushDownAggContext context) {
        /*
         * context: sum(a) groupBy(y+z as x, l)
         * proj: b+c as a, u+v as y, m+n as l
         * newContext: sum(b+c), groupBy((u+v)+z as x, m+n as l)
         */

        List<SlotReference> groupKeys = new ArrayList<>();
        for (SlotReference key : context.getGroupKeys()) {
            groupKeys.addAll(
                    project.pushDownExpressionPastProject(key).getInputSlots()
                            .stream().map(slot -> (SlotReference) slot).collect(Collectors.toList()));
        }

        List<AggregateFunction> aggFunctions = new ArrayList<>();
        Map<AggregateFunction, Alias> aliasMap = new IdentityHashMap<>();
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            AggregateFunction newAggFunc = (AggregateFunction) project.pushDownExpressionPastProject(aggFunc);
            Alias alias = context.getAliasMap().get(aggFunc);
            aliasMap.put(newAggFunc, (Alias) alias.withChildren(newAggFunc));
            aggFunctions.add(newAggFunc);
        }
        // After pushing expressions past the project, the agg functions may now
        // contain If/CaseWhen that were hidden behind slot references before.
        // e.g. count(#slot) where #slot = if(cond, a, b) in the project.
        // We must re-check and update hasCaseWhen accordingly.
        boolean newHasCaseWhen = context.hasCaseWhen;
        if (!newHasCaseWhen) {
            for (AggregateFunction aggFunc : aggFunctions) {
                if (aggFunc.anyMatch(e -> e instanceof CaseWhen || e instanceof If)) {
                    newHasCaseWhen = true;
                    break;
                }
            }
        }
        PushDownAggContext newContext = new PushDownAggContext(aggFunctions, groupKeys, aliasMap,
                context.getCascadesContext(), context.isPassThroughBigJoin(),
                context.hasDecomposedAggIf, newHasCaseWhen,
                context.getBilateralState(), context.needOutputCount());
        return newContext;
    }

    private boolean canPushThroughProject(LogicalProject<? extends Plan> project, PushDownAggContext context) {
        for (SlotReference slot : context.getGroupKeys()) {
            if (!project.getOutputSet().contains(slot)) {
                SessionVariable.throwAnalysisExceptionWhenFeDebug("eager agg failed: can not find group key("
                        + slot + ") in " + project);
                return false;
            }
        }
        for (Slot slot : context.getAggFunctionsInputSlots()) {
            if (!project.getOutputSet().contains(slot)) {
                SessionVariable.throwAnalysisExceptionWhenFeDebug("eager agg failed: can not find aggFunc slot("
                        + slot + ") in " + project);
                return false;
            }
        }

        // push sum(A) through project(x, x+y as A)
        // if x is not used as group key, do not push through
        for (Slot slot : context.getAggFunctionsInputSlots()) {
            for (NamedExpression prj : project.getProjects()) {
                if (prj instanceof Alias && prj.getExprId().equals(slot.getExprId())) {
                    if (prj.getInputSlots().stream()
                            .anyMatch(
                                    s -> project.getOutputSet().contains(s)
                                            && !context.getGroupKeys().contains(s))) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private List<DataType> getCanonicalProjectDataTypes(PushDownAggContext context, Optional<Slot> countSlot) {
        List<DataType> outputDataType = Lists.newArrayList();
        Set<ExprId> outputIds = new HashSet<>();
        for (AggregateFunction func : context.getAggFunctions()) {
            Alias alias = context.getAliasMap().get(func);
            outputDataType.add(alias.getDataType());
            outputIds.add(alias.getExprId());
        }
        for (SlotReference groupKey : context.getGroupKeys()) {
            if (outputIds.add(groupKey.getExprId())) {
                outputDataType.add(groupKey.getDataType());
            }
        }
        if (countSlot.isPresent() && outputIds.add(countSlot.get().getExprId())) {
            outputDataType.add(countSlot.get().getDataType());
        }
        return outputDataType;
    }

    private Plan alignUnionChildrenDataType(Plan child, PushDownAggContext context, Optional<Slot> countSlot) {
        int outputSize = child.getOutput().size();
        List<DataType> outputDataType = getCanonicalProjectDataTypes(context, countSlot);
        List<NamedExpression> projection = Lists.newArrayListWithExpectedSize(outputSize);
        boolean needProject = false;
        for (int colIdx = 0; colIdx < outputSize; colIdx++) {
            SlotReference slot = (SlotReference) child.getOutput().get(colIdx);
            if (!slot.getDataType().equals(outputDataType.get(colIdx))) {
                projection.add(new Alias(new Cast(slot, outputDataType.get(colIdx))));
                needProject = true;
            } else {
                projection.add(slot);
            }
        }
        if (needProject) {
            LogicalProject<Plan> project = new LogicalProject<>(projection, child);
            if (countSlot.isPresent()) {
                int countSlotIndex = findOutputIndex(child, countSlot.get());
                if (countSlotIndex >= 0) {
                    context.getBilateralState().registerCountSlot(project, projection.get(countSlotIndex).toSlot());
                } else {
                    context.getBilateralState().registerNoCountSlot(project);
                }
            } else {
                context.getBilateralState().registerNoCountSlot(project);
            }
            return project;
        } else {
            return child;
        }
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, PushDownAggContext context) {
        if (union.getQualifier() != Qualifier.ALL || !union.getConstantExprsList().isEmpty()
                || !union.getOutputs().stream().allMatch(e -> e instanceof SlotReference)) {
            context.getBilateralState().registerNoCountSlot(union);
            return union;
        }
        List<Plan> newChildren = Lists.newArrayList();
        List<PushDownAggContext> childContexts = new ArrayList<>();
        List<Optional<Slot>> childCountSlots = new ArrayList<>();
        /*
            if any child can not push, do not push
            example
            agg(output=[sum(a),min(a)], groupkey=[b])
              ->union(a, b)
                 ->child1(a1, b1)
                 ->child2(a2, b2)
             if agg pushdown through child1, newChild1 output is (sum(a1), min(a1) b1)
             but if agg can not pushdown through child2, the output of child2 is (a2, b2).
             Output size of newChild1 and child2 are different
         */
        boolean allChildrenChanged = false;
        for (int idx = 0; idx < union.children().size(); idx++) {
            Plan child = union.children().get(idx);
            final int childIdx = idx;
            List<AggregateFunction> aggFunctionsForChild = new ArrayList<>();
            IdentityHashMap<AggregateFunction, Alias> aliasMapForChild = new IdentityHashMap<>();
            for (AggregateFunction func : context.getAggFunctions()) {
                AggregateFunction newFunc = (AggregateFunction) union.pushDownExpressionPastSetOperator(func, childIdx);
                aggFunctionsForChild.add(newFunc);
                Alias alias = context.getAliasMap().get(func);
                // aliasForChild should have its own ExprId
                Alias aliasForChild = new Alias(newFunc, alias.getName(), alias.getQualifier());
                aliasMapForChild.put(newFunc, aliasForChild);
            }

            List<SlotReference> groupKeysForChild = context.getGroupKeys().stream()
                    .map(slot -> (SlotReference) union.pushDownExpressionPastSetOperator(slot, childIdx))
                    .collect(Collectors.toList());
            PushDownAggContext contextForChild = new PushDownAggContext(aggFunctionsForChild, groupKeysForChild,
                    aliasMapForChild, context.getCascadesContext(),
                    context.isPassThroughBigJoin(), context.hasDecomposedAggIf, context.hasCaseWhen,
                    context.getBilateralState(), context.needOutputCount());
            inheritHintActionsToUnionChild(context, contextForChild, aggFunctionsForChild);
            Plan newChild = child.accept(this, contextForChild);
            if (newChild != child) {
                Optional<Slot> childCountSlot = context.getBilateralState().getCountSlot(newChild);
                if (context.needOutputCount() && !childCountSlot.isPresent()) {
                    allChildrenChanged = false;
                    break;
                }
                if (!allAggFunctionsPushed(contextForChild)) {
                    allChildrenChanged = false;
                    break;
                }
                newChild = buildCanonicalProject(newChild, contextForChild, childCountSlot);
                newChildren.add(newChild);
                childContexts.add(contextForChild);
                childCountSlots.add(childCountSlot);
                allChildrenChanged = true;
            } else {
                allChildrenChanged = false;
                break;
            }
        }
        if (allChildrenChanged) {
            for (int idx = 0; idx < union.children().size(); idx++) {
                // all children need align data type
                Plan newChild = alignUnionChildrenDataType(newChildren.get(idx), childContexts.get(idx),
                        childCountSlots.get(idx));
                newChildren.set(idx, newChild);
            }
            List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayListWithExpectedSize(union.arity());
            int regularOutputSize = newChildren.get(0).getOutput().size();
            for (int childIdx = 0; childIdx < union.arity(); childIdx++) {
                int childOutputSize = newChildren.get(childIdx).getOutput().size();
                if (childOutputSize != regularOutputSize) {
                    SessionVariable.throwAnalysisExceptionWhenFeDebug(
                            "push down agg failed: union child output size mismatch. "
                            + "expected " + regularOutputSize + " but child " + childIdx + " has "
                            + childOutputSize + ". union: " + union + " context: " + context);
                    return union;
                }
                newRegularChildrenOutputs.add(
                        newChildren.get(childIdx).getOutput().stream()
                                .map(s -> (SlotReference) s).collect(Collectors.toList()));
            }

            List<NamedExpression> newOutput = Lists.newArrayList();
            for (AggregateFunction func : context.getAggFunctions()) {
                Alias alias = context.getAliasMap().get(func);
                if (alias == null) {
                    SessionVariable.throwAnalysisExceptionWhenFeDebug("push down agg failed. union: " + union
                            + " context: " + context);
                    return union;
                }
                newOutput.add(alias.toSlot());
            }
            newOutput.addAll(context.getGroupKeys());
            Optional<Alias> countStarAlias = findCountStarAlias(context);
            Optional<SlotReference> unionCnt = Optional.empty();
            if (context.needOutputCount() && !countStarAlias.isPresent()) {
                unionCnt = Optional.of(new SlotReference(
                        "unionCnt" + context.getCascadesContext().getStatementContext().generateColumnName(),
                        BigIntType.INSTANCE));
                newOutput.add(unionCnt.get());
            }
            LogicalUnion newUnion = (LogicalUnion) union
                    .withChildrenAndOutputs(newChildren, newOutput, newRegularChildrenOutputs);
            for (int idx = 0; idx < context.getAggFunctions().size(); idx++) {
                AggregateFunction func = context.getAggFunctions().get(idx);
                ExprId exprId = context.getAliasMap().get(func).getExprId();
                context.getBilateralState().registerPushedAggFuncSlot(exprId, newUnion.getOutput().get(idx));
            }
            if (context.needOutputCount()) {
                if (countStarAlias.isPresent()) {
                    context.getBilateralState().registerCountSlot(newUnion,
                            newUnion.getOutput().get(findCountStarAggFunctionIndex(context).get()));
                } else {
                    context.getBilateralState().registerCountSlot(newUnion, unionCnt.get());
                }
            } else {
                context.getBilateralState().registerNoCountSlot(newUnion);
            }
            return newUnion;
        } else {
            context.getBilateralState().registerNoCountSlot(union);
            return union;
        }
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, PushDownAggContext context) {
        if (project.child() instanceof LogicalCatalogRelation
                || (project.child() instanceof LogicalFilter
                        && project.child().child(0) instanceof LogicalCatalogRelation)) {
            // project
            //   --> scan
            // =>
            // aggregate
            //   --> project
            //     --> scan
            return genAggregate(project, context);
        }

        if (!canPushThroughProject(project, context)) {
            return genAggregate(project, context);
        }
        PushDownAggContext newContext = createContextFromProject(project, context);
        Plan newChild = project.child().accept(this, newContext);
        if (newChild != project.child()) {
            /*
             * agg[sum(a), groupBy(b)]
             *    -> proj(a, b1+b2 as b)
             *       -> join(c = d)
             *          -> any(a, b1, b2, c,...)
             *          -> any(d, ...)
             *  =>
             *  agg[sum(x), groupBy(b)]
             *    -> proj(x, b1+b2 as b)
             *      -> join(c=d)
             *          ->agg[sum(a) as x, groupBy(b1, b2, c)]
             *              ->proj(a, b1, b2, c, ...)
             *                  -> any(a, b1, b2, c)
             *          -> any(d, ...)
            */
            List<NamedExpression> newProjections = new ArrayList<>();
            BilateralState state = context.getBilateralState();
            for (AggregateFunction aggFunc : context.getAggFunctions()) {
                Alias alias = context.getAliasMap().get(aggFunc);
                NamedExpression namedExpression = state.getPushedAggFuncSlot(alias.getExprId());
                newProjections.add(namedExpression.toSlot());
            }
            for (SlotReference slot : context.getGroupKeys()) {
                boolean valid = false;
                for (NamedExpression ne : project.getProjects()) {
                    if (ne.toSlot().getExprId().equals(slot.getExprId())) {
                        valid = true;
                        newProjections.add(ne);
                        break;
                    }
                }
                if (!valid) {
                    SessionVariable.throwAnalysisExceptionWhenFeDebug(
                            "push agg failed. slot: " + "not found in " + project);
                    return project;
                }
            }
            Optional<Slot> childCountSlot = context.getBilateralState().getCountSlot(newChild);
            if (childCountSlot.isPresent() && newProjections.stream()
                    .noneMatch(ne -> ne.getExprId().equals(childCountSlot.get().getExprId()))) {
                newProjections.add(childCountSlot.get());
            }
            LogicalProject<Plan> result = new LogicalProject<>(newProjections, newChild);
            if (childCountSlot.isPresent()) {
                context.getBilateralState().registerCountSlot(result,
                        (Slot) result.getOutput().get(findOutputIndex(result, childCountSlot.get())));
            } else {
                context.getBilateralState().registerNoCountSlot(result);
            }
            return result;
        }

        context.getBilateralState().registerNoCountSlot(project);
        return project;
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, PushDownAggContext context) {
        return agg;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, PushDownAggContext context) {
        if (filter.child() instanceof LogicalRelation) {
            return genAggregate(filter, context);
        }
        if (filter.getConjuncts().stream().anyMatch(Expression::containsVolatileExpression)) {
            return genAggregate(filter, context);
        }
        List<SlotReference> filterInputSlots = filter.getInputSlots().stream()
                .map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<SlotReference> childGroupKeys = Stream.concat(
                        context.getGroupKeys().stream(),
                        filterInputSlots.stream())
                .distinct()
                .collect(Collectors.toList());
        PushDownAggContext childContext = context.withGroupKeys(childGroupKeys);
        if (!childContext.isValid()) {
            return genAggregate(filter, context);
        }
        Plan newChild = filter.child().accept(this, childContext);
        if (newChild != filter.child()) {
            Plan newFilter = filter.withChildren(newChild);
            Optional<Slot> countSlot = context.getBilateralState().getCountSlot(newChild);
            if (countSlot.isPresent()) {
                context.getBilateralState().registerCountSlot(newFilter, countSlot.get());
            } else {
                context.getBilateralState().registerNoCountSlot(newFilter);
            }
            return newFilter;
        }
        return genAggregate(filter, context);
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, PushDownAggContext context) {
        return genAggregate(relation, context);
    }

    private Optional<Alias> findCountStarAlias(PushDownAggContext context) {
        for (AggregateFunction func : context.getAggFunctions()) {
            if (func instanceof Count && ((Count) func).isCountStar()) {
                return Optional.of(context.getAliasMap().get(func));
            }
        }
        return Optional.empty();
    }

    private Optional<Integer> findCountStarAggFunctionIndex(PushDownAggContext context) {
        for (int i = 0; i < context.getAggFunctions().size(); i++) {
            AggregateFunction func = context.getAggFunctions().get(i);
            if (func instanceof Count && ((Count) func).isCountStar()) {
                return Optional.of(i);
            }
        }
        return Optional.empty();
    }

    private Plan genAggregate(Plan child, PushDownAggContext context) {
        if (isPushDisabledByVariable(context)) {
            context.getBilateralState().registerNoCountSlot(child);
            return child;
        }
        if (checkStats(child, context) || isPushEnabledByVariable(context)) {
            List<NamedExpression> aggOutputExpressions = new ArrayList<>();
            for (AggregateFunction func : context.getAggFunctions()) {
                aggOutputExpressions.add(context.getAliasMap().get(func));
            }
            Optional<Alias> countStarAlias = findCountStarAlias(context);
            Optional<Alias> outputCountAlias = Optional.empty();
            if (context.needOutputCount()) {
                if (countStarAlias.isPresent()) {
                    outputCountAlias = countStarAlias;
                } else {
                    outputCountAlias = Optional.of(new Alias(new Count(),
                            "cnt" + context.getCascadesContext().getStatementContext().generateColumnName()));
                }
            }
            aggOutputExpressions.addAll(context.getGroupKeys());
            if (outputCountAlias.isPresent() && !countStarAlias.isPresent()) {
                aggOutputExpressions.add(outputCountAlias.get());
            }
            LogicalAggregate genAgg = new LogicalAggregate(context.getGroupKeys(), aggOutputExpressions, child);
            NormalizeAggregate normalizeAggregate = new NormalizeAggregate();
            Plan normalized = normalizeAggregate.normalizeAgg(genAgg, Optional.empty(),
                    context.getCascadesContext());

            for (AggregateFunction func : context.getAggFunctions()) {
                Alias a = context.getAliasMap().get(func);
                Slot pushedSlot = normalized.getOutput().stream()
                        .filter(slot -> slot.getExprId().equals(a.getExprId()))
                        .findFirst()
                        .orElse(a.toSlot());
                context.getBilateralState().registerPushedAggFuncSlot(a.getExprId(), pushedSlot);
            }

            if (outputCountAlias.isPresent()) {
                context.getBilateralState().registerCountSlot(normalized, outputCountAlias.get().toSlot());
            } else {
                context.getBilateralState().registerNoCountSlot(normalized);
            }
            return normalized;
        } else {
            context.getBilateralState().registerNoCountSlot(child);
            return child;
        }
    }

    // Build the canonical project above a rewritten join after eager-aggregation pushdown.
    // Responsibilities:
    // 1. Restore the outputs expected by the parent rollup. If a join side has a childContext, materialize
    //    that side's aggregate current values and group keys; otherwise forward the original join outputs.
    // 2. For inner joins, recover join multiplicity by multiplying non-MIN/MAX aggregate current values by
    //    the opposite side's count slot when that side contributes rows to the parent aggregate.
    // 3. Append and register a synthetic join-count slot `cnt` (logical jcnt) for upper-level rollup.
    //
    // The examples below are schematic. The real project may keep extra forwarded slots such as join keys.
    //
    // Inner join + sum, single-side rewrite:
    //   Before:
    //     agg(sum(t1.a), sum(t2.a), gby t2.k)
    //       -> inner join(k = k)
    //            -> scan(t1)
    //            -> scan(t2)
    //   After:
    //     agg(sum(s1), sum(s2), gby t2.k)
    //       -> project(s1, t2.a * cnt1 as s2, t2.k, cnt1)
    //            -> inner join(k = k)
    //                 -> agg(sum(t1.a) as s1, count(*) as cnt1, gby k)
    //                      -> scan(t1)
    //                 -> scan(t2)
    //
    // Inner join + sum, bilateral rewrite:
    //   Before:
    //     agg(sum(t1.a), sum(t2.a), gby t2.k)
    //       -> inner join(k = k)
    //            -> scan(t1)
    //            -> scan(t2)
    //   After:
    //     agg(sum(s1'), sum(s2'), gby t2.k)
    //       -> project(s1 * cnt2 as s1', s2 * cnt1 as s2', t2.k, cnt1 * cnt2 as cnt)
    //            -> inner join(k = k)
    //                 -> agg(sum(t1.a) as s1, count(*) as cnt1, gby k)
    //                      -> scan(t1)
    //                 -> agg(sum(t2.a) as s2, count(*) as cnt2, gby k)
    //                      -> scan(t2)
    //
    // Inner join + count(col), single-side rewrite:
    //   Before:
    //     agg(count(t1.a), count(t2.a), gby t2.k)
    //       -> inner join(k = k)
    //            -> scan(t1)
    //            -> scan(t2)
    //   After:
    //     agg(sum0(c1), sum0(c2), gby t2.k)
    //       -> project(c1, if(t2.a is null, 0, 1) * cnt1 as c2, t2.k, cnt1 as cnt)
    //            -> inner join(k = k)
    //                 -> agg(count(t1.a) as c1, count(*) as cnt1, gby k)
    //                      -> scan(t1)
    //                 -> scan(t2)
    //
    // Inner join + count(col), bilateral rewrite:
    //   Before:
    //     agg(count(t1.a), count(t2.a), gby t2.k)
    //       -> inner join(k = k)
    //            -> scan(t1)
    //            -> scan(t2)
    //   After:
    //     agg(sum0(c1'), sum0(c2'), gby t2.k)
    //       -> project(c1 * cnt2 as c1', c2 * cnt1 as c2', t2.k, cnt1 * cnt2 as cnt)
    //            -> inner join(k = k)
    //                 -> agg(count(t1.a) as c1, count(*) as cnt1, gby k)
    //                      -> scan(t1)
    //                 -> agg(count(t2.a) as c2, count(*) as cnt2, gby k)
    //                      -> scan(t2)
    //   For count(*), the current row value is 1 instead of if(col is null, 0, 1).
    //
    // Semi/anti join:
    //   The project does not multiply by the opposite-side count
    //
    // Outer join:
    //   Aggregate outputs are not multiplied by the opposite-side count either; only `cnt` changes:
    //     left outer join with left push  -> project(s1, t2.k, cnt1 as cnt)
    //     right outer join with left push -> project(s1, t2.k, nvl(cnt1, 1) as cnt)
    private Plan buildCanonicalJoinProject(LogicalJoin<? extends Plan, ? extends Plan> join, PushDownAggContext context,
            Optional<PushDownAggContext> leftChildContext, Optional<PushDownAggContext> rightChildContext,
            Optional<Slot> leftCountSlot, Optional<Slot> rightCountSlot) {
        List<NamedExpression> projections = new ArrayList<>();
        Set<ExprId> outputIds = new HashSet<>();
        boolean remainLeft = join.getJoinType().isRemainLeftJoin();
        boolean remainRight = join.getJoinType().isRemainRightJoin();
        boolean shouldAdjustLeft = shouldUseJoinOppositeCntAdjustAggOutput(join, leftChildContext, rightCountSlot);
        boolean shouldAdjustRight = shouldUseJoinOppositeCntAdjustAggOutput(join, rightChildContext, leftCountSlot);

        if (remainLeft) {
            appendJoinSideOutputs(projections, outputIds, join.left(), leftChildContext, context,
                    rightCountSlot, shouldAdjustLeft);
        }
        if (remainRight) {
            appendJoinSideOutputs(projections, outputIds, join.right(), rightChildContext, context,
                    leftCountSlot, shouldAdjustRight);
        }

        Optional<? extends NamedExpression> joinCount = Optional.empty();
        if (context.needOutputCount()) {
            joinCount = findProjectedCountStarOutput(context, outputIds);
            if (!joinCount.isPresent()) {
                joinCount = computeJoinCount(join, leftChildContext, rightChildContext,
                        leftCountSlot, rightCountSlot, context);
            }
        }
        Optional<Slot> projectedCountSlot = Optional.empty();
        if (joinCount.isPresent()) {
            appendProjectionIfAbsent(projections, outputIds, joinCount.get());
            projectedCountSlot = Optional.of(joinCount.get().toSlot());
        }
        LogicalProject<Plan> project = new LogicalProject<>(projections, join);
        if (projectedCountSlot.isPresent()) {
            context.getBilateralState().registerCountSlot(project, projectedCountSlot.get());
        } else {
            context.getBilateralState().registerNoCountSlot(project);
        }
        return project;
    }

    private void appendJoinSideOutputs(List<NamedExpression> projections, Set<ExprId> outputIds, Plan originalSide,
            Optional<PushDownAggContext> childContext, PushDownAggContext parentContext,
            Optional<Slot> oppositeCountSlot, boolean shouldAdjustOutput) {
        if (childContext.isPresent()) {
            for (AggregateFunction aggFunc : childContext.get().getAggFunctions()) {
                NamedExpression aggOutput = shouldAdjustOutput
                        ? adjustAggOutputUseOppositeCountOnJoin(aggFunc, parentContext, oppositeCountSlot)
                        : buildAggOutputWithoutJoinAdjustment(aggFunc, parentContext);
                appendProjectionIfAbsent(projections, outputIds, aggOutput);
            }
            for (SlotReference groupKey : childContext.get().getGroupKeys()) {
                appendProjectionIfAbsent(projections, outputIds, groupKey);
            }
        } else {
            for (Slot slot : originalSide.getOutput()) {
                appendProjectionIfAbsent(projections, outputIds, slot);
            }
        }
    }

    private void appendProjectionIfAbsent(List<NamedExpression> projections, Set<ExprId> outputIds,
            NamedExpression expression) {
        if (outputIds.add(expression.getExprId())) {
            projections.add(expression);
        }
    }

    private boolean shouldUseJoinOppositeCntAdjustAggOutput(LogicalJoin<? extends Plan, ? extends Plan> join,
            Optional<PushDownAggContext> childContext, Optional<Slot> oppositeCountSlot) {
        return join.getJoinType().isInnerOrCrossJoin() && childContext.isPresent() && oppositeCountSlot.isPresent()
                && hasAggNeedJoinMultiplicityRecovery(childContext.get().getAggFunctions());
    }

    private Optional<NamedExpression> findProjectedCountStarOutput(PushDownAggContext context, Set<ExprId> outputIds) {
        BilateralState state = context.getBilateralState();
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            if (aggFunc instanceof Count && ((Count) aggFunc).isCountStar()) {
                ExprId exprId = context.getAliasMap().get(aggFunc).getExprId();
                if (state.hasAggFuncOutput(exprId)) {
                    NamedExpression countStarOutput = state.getPushedAggFuncSlot(exprId);
                    if (outputIds.contains(countStarOutput.getExprId())) {
                        return Optional.of(countStarOutput);
                    }
                }
            }
        }
        return Optional.empty();
    }

    private boolean hasAggNeedJoinMultiplicityRecovery(List<AggregateFunction> aggFunctions) {
        return aggFunctions.stream().anyMatch(this::needJoinMultiplicityRecovery);
    }

    private boolean needJoinMultiplicityRecovery(AggregateFunction aggFunc) {
        return !(aggFunc instanceof Max) && !(aggFunc instanceof Min);
    }

    private Optional<? extends NamedExpression> computeJoinCount(LogicalJoin<? extends Plan, ? extends Plan> join,
            Optional<PushDownAggContext> leftChildContext, Optional<PushDownAggContext> rightChildContext,
            Optional<Slot> leftCountSlot, Optional<Slot> rightCountSlot, PushDownAggContext context) {
        JoinType joinType = join.getJoinType();
        if (joinType.isInnerJoin()) {
            if (leftCountSlot.isPresent() && rightCountSlot.isPresent()) {
                Expression joinCnt = TypeCoercionUtils.processBinaryArithmetic(
                        new Multiply(leftCountSlot.get(), rightCountSlot.get()));
                return Optional.of(new Alias(joinCnt,
                        JOIN_CNT + context.getCascadesContext().getStatementContext().generateColumnName()));
            } else if (leftCountSlot.isPresent()) {
                return leftCountSlot;
            } else if (rightCountSlot.isPresent()) {
                return rightCountSlot;
            }
            return Optional.empty();
        }
        if (joinType.isLeftOuterJoin()) {
            if (leftChildContext.isPresent()) {
                return leftCountSlot;
            }
            if (rightChildContext.isPresent() && rightCountSlot.isPresent()) {
                Expression joinCnt = TypeCoercionUtils.processBoundFunction(
                        new Nvl(rightCountSlot.get(), BigIntLiteral.of(1)));
                return Optional.of(new Alias(joinCnt,
                        JOIN_CNT + context.getCascadesContext().getStatementContext().generateColumnName()));
            }
            return Optional.empty();
        }
        if (joinType.isRightOuterJoin()) {
            if (leftChildContext.isPresent() && leftCountSlot.isPresent()) {
                Expression joinCnt = TypeCoercionUtils.processBoundFunction(
                        new Nvl(leftCountSlot.get(), BigIntLiteral.of(1)));
                return Optional.of(new Alias(joinCnt,
                        JOIN_CNT + context.getCascadesContext().getStatementContext().generateColumnName()));
            }
            if (rightChildContext.isPresent()) {
                return rightCountSlot;
            }
            return Optional.empty();
        }
        if (joinType.isLeftSemiOrAntiJoin()) {
            return leftCountSlot;
        }
        if (joinType.isRightSemiOrAntiJoin()) {
            return rightCountSlot;
        }
        return Optional.empty();
    }

    private Plan buildCanonicalProject(Plan child, PushDownAggContext context, Optional<Slot> countSlot) {
        List<NamedExpression> projections = new ArrayList<>();
        Set<ExprId> outputIds = new HashSet<>();
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            ExprId exprId = context.getAliasMap().get(aggFunc).getExprId();
            NamedExpression aggOutput = context.getBilateralState().getPushedAggFuncSlot(exprId);
            projections.add(aggOutput);
            outputIds.add(aggOutput.getExprId());
        }
        for (SlotReference groupKey : context.getGroupKeys()) {
            if (outputIds.add(groupKey.getExprId())) {
                projections.add(groupKey);
            }
        }
        countSlot.ifPresent(slot -> appendProjectionIfAbsent(projections, outputIds, slot));
        if (projections.equals(child.getOutput())) {
            return child;
        } else {
            LogicalProject<Plan> project = new LogicalProject<>(projections, child);
            if (countSlot.isPresent()) {
                context.getBilateralState().registerCountSlot(project, countSlot.get());
            } else {
                context.getBilateralState().registerNoCountSlot(project);
            }
            return project;
        }
    }

    private NamedExpression buildAggOutputWithoutJoinAdjustment(AggregateFunction aggFunc, PushDownAggContext context) {
        Alias alias = context.getAliasMap().get(aggFunc);
        ExprId exprId = alias.getExprId();
        BilateralState state = context.getBilateralState();
        NamedExpression output;
        if (state.hasAggFuncOutput(exprId)) {
            output = state.getPushedAggFuncSlot(exprId);
        } else {
            Expression currentValue;
            if (aggFunc instanceof Count) {
                if (aggFunc.arity() == 0) {
                    currentValue = BigIntLiteral.of(1);
                } else {
                    currentValue = new If(new IsNull(aggFunc.child(0)), BigIntLiteral.of(0), BigIntLiteral.of(1));
                }
            } else {
                currentValue = aggFunc.child(0);
            }
            output = (Alias) alias.withChildren(currentValue);
            state.registerAggFuncOutput(exprId, output.toSlot(), state.isAggFuncActuallyPushed(exprId));
        }
        return output;
    }

    private NamedExpression adjustAggOutputUseOppositeCountOnJoin(AggregateFunction aggFunc, PushDownAggContext context,
            Optional<Slot> countSlot) {
        Alias alias = context.getAliasMap().get(aggFunc);
        ExprId exprId = alias.getExprId();
        BilateralState state = context.getBilateralState();
        Expression currentValue = getCurrentAggValue(aggFunc, exprId, state);
        Optional<Expression> multiplier = Optional.empty();
        if (!(aggFunc instanceof Max) && !(aggFunc instanceof Min)) {
            multiplier = countSlot.map(cnt -> (Expression) cnt);
        }
        NamedExpression output;
        Expression outputExpr;
        if (multiplier.isPresent()) {
            outputExpr = TypeCoercionUtils.processBinaryArithmetic(new Multiply(currentValue, multiplier.get()));
        } else {
            outputExpr = currentValue;
        }
        if (outputExpr instanceof NamedExpression) {
            output = (NamedExpression) outputExpr;
        } else {
            output = new Alias(outputExpr, alias.getName());
        }
        state.registerAggFuncOutput(exprId, output.toSlot(), state.isAggFuncActuallyPushed(exprId));
        return output;
    }

    private Expression getCurrentAggValue(AggregateFunction aggFunc, ExprId exprId, BilateralState state) {
        if (state.hasAggFuncOutput(exprId)) {
            return state.getPushedAggFuncSlot(exprId);
        }
        if (aggFunc instanceof Count) {
            if (aggFunc.arity() == 0) {
                return BigIntLiteral.of(1);
            }
            return new If(new IsNull(aggFunc.child(0)), BigIntLiteral.of(0), BigIntLiteral.of(1));
        }
        return aggFunc.child(0);
    }

    private void inheritHintActionsToUnionChild(PushDownAggContext parentContext,
            PushDownAggContext childContext, List<AggregateFunction> childAggFunctions) {
        BilateralState state = parentContext.getBilateralState();
        for (int i = 0; i < parentContext.getAggFunctions().size(); i++) {
            AggregateFunction parentAggFunction = parentContext.getAggFunctions().get(i);
            AggregateFunction childAggFunction = childAggFunctions.get(i);
            ExprId parentExprId = parentContext.getAliasMap().get(parentAggFunction).getExprId();
            ExprId childExprId = childContext.getAliasMap().get(childAggFunction).getExprId();
            state.inheritActionIfAbsent(parentExprId, childExprId);
        }
    }

    private boolean allAggFunctionsPushed(PushDownAggContext context) {
        BilateralState state = context.getBilateralState();
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            ExprId exprId = context.getAliasMap().get(aggFunc).getExprId();
            if (!state.isAggFuncActuallyPushed(exprId)) {
                return false;
            }
        }
        return true;
    }

    private int findOutputIndex(Plan plan, Slot target) {
        for (int i = 0; i < plan.getOutput().size(); i++) {
            if (plan.getOutput().get(i).getExprId().equals(target.getExprId())) {
                return i;
            }
        }
        return -1;
    }

    private boolean isPushEnabledByVariable(PushDownAggContext context) {
        if (context.getBilateralState().noAction()) {
            return false;
        }
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            Alias alias = context.getAliasMap().get(aggFunc);
            ExprId id = alias.getExprId();
            Action action = context.getBilateralState().getAction(id);
            if (action != null && action.equals(Action.PUSH)) {
                return true;
            }
        }
        return false;
    }

    private boolean isPushDisabledByVariable(PushDownAggContext context) {
        if (context.getBilateralState().noAction()) {
            return false;
        }
        for (AggregateFunction aggFunc : context.getAggFunctions()) {
            Alias alias = context.getAliasMap().get(aggFunc);
            ExprId id = alias.getExprId();
            Action action = context.getBilateralState().getAction(id);
            if (action != null && action.equals(Action.NOPUSH)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkStats(Plan plan, PushDownAggContext context) {
        int mode = SessionVariable.getEagerAggregationMode();
        if (mode < 0) {
            return false;
        }

        if (mode > 0) {
            // when mode=1, any join is regarded as big join in order to
            // push down aggregation through at least one join
            return context.isPassThroughBigJoin();
        }

        if (!context.isPassThroughBigJoin() && !context.hasDecomposedAggIf) {
            return false;
        }

        Statistics stats = plan.getStats();
        if (stats == null) {
            stats = plan.accept(derive, new StatsDerive.DeriveContext());
        }
        if (stats.getRowCount() <= 0) {
            return false;
        }

        List<ColumnStatistic> groupKeysStats = new ArrayList<>();

        List<ColumnStatistic> lower = Lists.newArrayList();
        List<ColumnStatistic> medium = Lists.newArrayList();
        List<ColumnStatistic> high = Lists.newArrayList();

        List<ColumnStatistic>[] cards = new List[] { lower, medium, high };

        for (NamedExpression key : context.getGroupKeys()) {
            ColumnStatistic colStats = ExpressionEstimation.INSTANCE.estimate(key, stats);
            if (colStats.isUnKnown) {
                return false;
            }
            if (stats.getRowCount() * 0.9 <= colStats.ndv) {
                return false;
            }
            groupKeysStats.add(colStats);
            cards[groupByCardinality(colStats, stats.getRowCount())].add(colStats);
        }

        double lowerCartesian = 1.0;
        for (ColumnStatistic colStats : lower) {
            lowerCartesian = lowerCartesian * colStats.ndv;
        }

        // pow(row_count/20, a half of lower column size)
        double lowerUpper = Math.max(stats.getRowCount() / 20, 1);
        lowerUpper = Math.pow(lowerUpper, Math.max(lower.size() / 2, 1));

        if (high.isEmpty() && (lower.size() + medium.size()) <= 2) {
            return true;
        }

        if (high.isEmpty() && medium.isEmpty()) {
            if (lower.size() == 1 && lowerCartesian * 20 <= stats.getRowCount()) {
                return true;
            } else if (lower.size() == 2 && lowerCartesian * 7 <= stats.getRowCount()) {
                return true;
            } else if (lower.size() <= 3 && lowerCartesian * 20 <= stats.getRowCount() && lowerCartesian < lowerUpper) {
                return true;
            } else {
                return false;
            }
        }

        if (high.size() >= 2 || medium.size() > 2 || (high.size() == 1 && !medium.isEmpty())) {
            return false;
        }

        // 3. Extremely low cardinality for lower with at most one medium or high.
        double lowerCartesianLowerBound = stats.getRowCount() / LOWER_AGGREGATE_EFFECT_COEFFICIENT;
        if (high.size() + medium.size() == 1 && lower.size() <= 2 && lowerCartesian <= lowerCartesianLowerBound) {
            return true;
        }

        return false;
    }

    // high(2): row_count / cardinality < MEDIUM_AGGREGATE_EFFECT_COEFFICIENT
    // medium(1): row_count / cardinality >= MEDIUM_AGGREGATE_EFFECT_COEFFICIENT and
    // < LOW_AGGREGATE_EFFECT_COEFFICIENT
    // lower(0): row_count / cardinality >= LOW_AGGREGATE_EFFECT_COEFFICIENT
    private int groupByCardinality(ColumnStatistic colStats, double rowCount) {
        if (rowCount == 0 || colStats.ndv * MEDIUM_AGGREGATE_EFFECT_COEFFICIENT > rowCount) {
            return 2;
        } else if (colStats.ndv * MEDIUM_AGGREGATE_EFFECT_COEFFICIENT <= rowCount
                && colStats.ndv * LOW_AGGREGATE_EFFECT_COEFFICIENT > rowCount) {
            return 1;
        } else if (colStats.ndv * LOW_AGGREGATE_EFFECT_COEFFICIENT <= rowCount) {
            return 0;
        }
        return 2;
    }
}
