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

import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.rewrite.StatsDerive;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
    private static final double LOWER_AGGREGATE_EFFECT_COEFFICIENT = 10000;
    private static final double LOW_AGGREGATE_EFFECT_COEFFICIENT = 1000;
    private static final double MEDIUM_AGGREGATE_EFFECT_COEFFICIENT = 100;
    private final StatsDerive derive = new StatsDerive(false);

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, PushDownAggContext context) {
        boolean toLeft = false;
        boolean toRight = false;
        boolean pushHere = false;
        if (context.getAggFunctions().isEmpty()) {
            // select t1.v from t1 join t2 on t1.id = t2.id group by t1.v, t2.v
            // if no agg function, try to push agg to the child which contains all group keys
            // TODO: consider t1.rows/(t1.id, t1.v).ndv and t2.rows/(t2.id, t2.v).ndv to determine push target
            if (join.left().getOutputSet().containsAll(context.getGroupKeys())) {
                toLeft = true;
            } else if (join.right().getOutputSet().containsAll(context.getGroupKeys())) {
                toRight = true;
            } else {
                pushHere = true;
            }
        } else {
            for (AggregateFunction aggFunc : context.getAggFunctions()) {
                if (join.left().getOutputSet().containsAll(aggFunc.getInputSlots())) {
                    toLeft = true;
                } else if (join.right().getOutputSet().containsAll(aggFunc.getInputSlots())) {
                    toRight = true;
                } else {
                    pushHere = true;
                }
            }
        }

        if (pushHere || (toLeft && toRight)) {
            if (SessionVariable.isEagerAggregationOnJoin()) {
                return genAggregate(join, context);
            } else {
                return join;
            }
        }

        List<SlotReference> joinConditionSlots;
        List<SlotReference> childGroupByKeys = new ArrayList<>();
        if (toLeft) {
            joinConditionSlots = getJoinConditionsInputSlotsFromOneSide(join, join.left());
            for (SlotReference key : context.getGroupKeys()) {
                if (join.left().getOutputSet().containsAll(key.getInputSlots())) {
                    childGroupByKeys.add(key);
                }
            }
        } else {
            joinConditionSlots = getJoinConditionsInputSlotsFromOneSide(join, join.right());
            for (SlotReference key : context.getGroupKeys()) {
                if (join.right().getOutputSet().containsAll(key.getInputSlots())) {
                    childGroupByKeys.add(key);
                }
            }
        }

        for (SlotReference slot : joinConditionSlots) {
            if (!childGroupByKeys.contains(slot)) {
                childGroupByKeys.add(slot);
            }
        }

        PushDownAggContext childContext = context.withGroupKeys(childGroupByKeys);
        Statistics stats = join.right().getStats();
        if (stats == null) {
            stats = join.right().accept(derive, new StatsDerive.DeriveContext());
        }
        if (stats.getRowCount() > PushDownAggContext.BIG_JOIN_BUILD_SIZE
                || SessionVariable.getEagerAggregationMode() > 0) {
            childContext = childContext.passThroughBigJoin();
        }
        if (toLeft) {
            Plan newLeft = join.left().accept(this, childContext);
            if (newLeft != join.left()) {
                return join.withChildren(newLeft, join.right());
            }
        } else {
            Plan newRight = join.right().accept(this, childContext);
            if (newRight != join.right()) {
                return join.withChildren(join.left(), newRight);
            }
        }
        return join;
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
        return new PushDownAggContext(aggFunctions, groupKeys, aliasMap,
                context.getCascadesContext(), context.isPassThroughBigJoin());
    }

    private boolean canPushThroughProject(LogicalProject<? extends Plan> project, PushDownAggContext context) {
        for (SlotReference slot : context.getGroupKeys()) {
            if (!project.getOutputSet().contains(slot)) {
                SessionVariable.throwRuntimeExceptionWhenFeDebug("eager agg failed: can not find group key("
                        + slot + ") in " + project);
                return false;
            }
        }
        for (Slot slot : context.getAggFunctionsInputSlots()) {
            if (!project.getOutputSet().contains(slot)) {
                SessionVariable.throwRuntimeExceptionWhenFeDebug("eager agg failed: can not find aggFunc slot("
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

    private Plan alignUnionChildrenDataType(Plan child, PushDownAggContext context) {
        int outputSize = child.getOutput().size();
        List<DataType> outputDataType = Lists.newArrayListWithExpectedSize(outputSize);
        outputDataType.addAll(context.getAggFunctions().stream()
                .map(func -> context.getAliasMap().get(func).getDataType()).collect(Collectors.toList()));
        outputDataType.addAll(context.getGroupKeys().stream().map(s -> s.getDataType()).collect(Collectors.toList()));
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
            return new LogicalProject<Plan>(projection, child);
        } else {
            return child;
        }
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, PushDownAggContext context) {
        if (!union.getConstantExprsList().isEmpty()) {
            return union;
        }

        if (!union.getOutputs().stream().allMatch(e -> e instanceof SlotReference)) {
            return union;
        }
        List<Plan> newChildren = Lists.newArrayList();
        List<PushDownAggContext> childrenContext = new ArrayList<>();
        boolean changed = false;
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
                    aliasMapForChild, context.getCascadesContext(), context.isPassThroughBigJoin());
            childrenContext.add(contextForChild);
            Plan newChild = child.accept(this, contextForChild);
            if (newChild != child) {
                changed = true;
            }
            // all children need align data type, even if it is not rewritten
            newChild = alignUnionChildrenDataType(newChild, context);
            newChildren.add(newChild);
        }
        if (changed) {
            List<List<SlotReference>> newRegularChildrenOutputs = Lists.newArrayListWithExpectedSize(union.arity());
            for (int childIdx = 0; childIdx < union.arity(); childIdx++) {
                newRegularChildrenOutputs.add(
                        newChildren.get(childIdx).getOutput().stream()
                                .map(s -> (SlotReference) s).collect(Collectors.toList()));
            }

            List<NamedExpression> newOutput = Lists.newArrayList();
            for (AggregateFunction func : context.getAggFunctions()) {
                Alias alias = context.getAliasMap().get(func);
                if (alias == null) {
                    SessionVariable.throwRuntimeExceptionWhenFeDebug("push down agg failed. union: " + union
                            + " context: " + context);
                    return union;
                }
                newOutput.add(alias.toSlot());
            }
            newOutput.addAll(context.getGroupKeys());

            LogicalUnion newUnion = (LogicalUnion) union
                    .withChildrenAndOutputs(newChildren, newOutput, newRegularChildrenOutputs);
            return newUnion;
        } else {
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
            //for (Alias alias : context.getAliasMap().values()) {
            //    newProjections.add(alias.toSlot());
            //}
            for (AggregateFunction aggFunc : context.getAggFunctions()) {
                newProjections.add(context.getAliasMap().get(aggFunc).toSlot());
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
                    SessionVariable.throwRuntimeExceptionWhenFeDebug(
                            "push agg failed. slot: " + "not found in " + project);
                    return project;
                }
            }
            LogicalProject result = new LogicalProject(newProjections, newChild);
            return result;
        }

        return project;
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, PushDownAggContext context) {
        return agg;
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, PushDownAggContext context) {
        return genAggregate(filter, context);
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, PushDownAggContext context) {
        return genAggregate(relation, context);
    }

    private Plan genAggregate(Plan child, PushDownAggContext context) {
        if (checkStats(child, context)) {
            List<NamedExpression> aggOutputExpressions = new ArrayList<>();
            for (AggregateFunction func : context.getAggFunctions()) {
                aggOutputExpressions.add(context.getAliasMap().get(func));
            }
            aggOutputExpressions.addAll(context.getGroupKeys());
            LogicalAggregate genAgg = new LogicalAggregate(context.getGroupKeys(), aggOutputExpressions, child);
            NormalizeAggregate normalizeAggregate = new NormalizeAggregate();
            return normalizeAggregate.normalizeAgg(genAgg, Optional.empty(),
                    context.getCascadesContext());
        } else {
            return child;
        }
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

        if (!context.isPassThroughBigJoin()) {
            return false;
        }

        Statistics stats = plan.getStats();
        if (stats == null) {
            stats = plan.accept(derive, new StatsDerive.DeriveContext());
        }
        if (stats.getRowCount() == 0) {
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
