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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.NormalizeToSlot.NormalizeToSlotContext;
import org.apache.doris.nereids.rules.rewrite.NormalizeToSlot.NormalizeToSlotTriplet;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils.CollectNonWindowedAggFuncs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/** NormalizeRepeat
 * eg: select sum(b + 1), grouping(a+1) from t1 group by grouping sets ((b+1));
 * Original Plan:
 * LogicalRepeat ( groupingSets=[[(a#0 + 1)]],
 *                 outputExpressions=[sum((b#1 + 1)) AS `sum((b + 1))`#2,
 *                 Grouping((a#0 + 1)) AS `Grouping((a + 1))`#3] )
 *      +--LogicalOlapScan (t1)
 *
 * After:
 * LogicalAggregate[62] ( groupByExpr=[(a + 1)#4, GROUPING_ID#7, GROUPING_PREFIX_(a + 1)#6],
 *                        outputExpr=[sum((b + 1)#5) AS `sum((b + 1))`#2,
 *                                    GROUPING_PREFIX_(a + 1)#6 AS `GROUPING_PREFIX_(a + 1)`#3] )
 *    +--LogicalRepeat ( groupingSets=[[(a + 1)#4]],
 *                       outputExpressions=[(a + 1)#4,
 *                                          (b + 1)#5,
 *                                          GROUPING_ID#7,
 *                                          GROUPING_PREFIX_(a + 1)#6] )
 *      +--LogicalProject[60] ( projects=[(a#0 + 1) AS `(a + 1)`#4, (b#1 + 1) AS `(b + 1)`#5], excepts=[]
 *          +--LogicalOlapScan ( t1 )
 */
public class NormalizeRepeat extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.NORMALIZE_REPEAT.build(
            logicalRepeat(any()).when(LogicalRepeat::canBindVirtualSlot).then(repeat -> {
                if (repeat.getGroupingSets().size() == 1
                        && ExpressionUtils.collect(repeat.getOutputExpressions(),
                        GroupingScalarFunction.class::isInstance).isEmpty()) {
                    return new LogicalAggregate<>(repeat.getGroupByExpressions(),
                            repeat.getOutputExpressions(), repeat.child());
                }
                return doNormalize(repeat);
            })
        );
    }

    /**
     * Normalize repeat, this can be used directly, if optimize the repeat
     */
    public static LogicalAggregate<Plan> doNormalize(LogicalRepeat<Plan> repeat) {
        checkRepeatLegality(repeat);
        repeat = removeDuplicateColumns(repeat);
        // add virtual slot, LogicalAggregate and LogicalProject for normalize
        LogicalAggregate<Plan> agg = normalizeRepeat(repeat);
        return dealSlotAppearBothInAggFuncAndGroupingSets(agg);
    }

    private static LogicalRepeat<Plan> removeDuplicateColumns(LogicalRepeat<Plan> repeat) {
        List<List<Expression>> groupingSets = repeat.getGroupingSets();
        ImmutableList.Builder<List<Expression>> builder = ImmutableList.builder();
        for (List<Expression> sets : groupingSets) {
            List<Expression> newList = ImmutableList.copyOf(ImmutableSet.copyOf(sets));
            builder.add(newList);
        }
        return repeat.withGroupSets(builder.build());
    }

    private static void checkRepeatLegality(LogicalRepeat<Plan> repeat) {
        checkGroupingSetsSize(repeat);
    }

    private static void checkGroupingSetsSize(LogicalRepeat<Plan> repeat) {
        Set<Expression> flattenGroupingSetExpr = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));
        if (flattenGroupingSetExpr.size() > LogicalRepeat.MAX_GROUPING_SETS_NUM) {
            throw new AnalysisException(
                    "Too many sets in GROUP BY clause, the max grouping sets item is "
                            + LogicalRepeat.MAX_GROUPING_SETS_NUM);
        }
    }

    private static LogicalAggregate<Plan> normalizeRepeat(LogicalRepeat<Plan> repeat) {
        Set<Expression> needToSlotsGroupingExpr = collectNeedToSlotGroupingExpr(repeat);
        NormalizeToSlotContext groupingExprContext = buildContext(repeat, needToSlotsGroupingExpr);
        Map<Expression, NormalizeToSlotTriplet> groupingExprMap = groupingExprContext.getNormalizeToSlotMap();
        Set<Alias> existsAlias = getExistsAlias(repeat, groupingExprMap);
        Set<Expression> needToSlotsArgs = collectNeedToSlotArgsOfGroupingScalarFuncAndAggFunc(repeat);
        NormalizeToSlotContext argsContext = NormalizeToSlotContext.buildContext(existsAlias, needToSlotsArgs);

        // normalize grouping sets to List<List<Slot>>
        ImmutableList.Builder<List<Slot>> normalizedGroupingSetBuilder = ImmutableList.builder();
        for (List<Expression> groupingSet : repeat.getGroupingSets()) {
            List<Slot> normalizedSet = (List<Slot>) (List) groupingExprContext.normalizeToUseSlotRef(groupingSet);
            normalizedGroupingSetBuilder.add(normalizedSet);
        }
        List<List<Slot>> normalizedGroupingSets = normalizedGroupingSetBuilder.build();

        // use argsContext
        // rewrite the arguments of grouping scalar function to slots
        // rewrite grouping scalar function to virtual slots
        // rewrite the arguments of agg function to slots
        List<NamedExpression> normalizedAggOutput = Lists.newArrayList();
        for (Expression expr : repeat.getOutputExpressions()) {
            Expression rewrittenExpr = expr.rewriteDownShortCircuit(
                    e -> normalizeAggFuncChildrenAndGroupingScalarFunc(argsContext, e));
            normalizedAggOutput.add((NamedExpression) rewrittenExpr);
        }

        // use groupingExprContext rewrite the normalizedAggOutput
        normalizedAggOutput = groupingExprContext.normalizeToUseSlotRef(normalizedAggOutput);

        Set<VirtualSlotReference> virtualSlotsInFunction =
                ExpressionUtils.collect(normalizedAggOutput, VirtualSlotReference.class::isInstance);

        List<VirtualSlotReference> allVirtualSlots = ImmutableList.<VirtualSlotReference>builder()
                // add the virtual grouping id slot
                .add(Repeat.generateVirtualGroupingIdSlot())
                // add other virtual slots in the grouping scalar functions
                .addAll(virtualSlotsInFunction)
                .build();

        Set<SlotReference> aggUsedNonVirtualSlots = ExpressionUtils.collect(
                normalizedAggOutput, expr -> expr.getClass().equals(SlotReference.class));

        Set<Slot> groupingSetsUsedSlot = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(normalizedGroupingSets));

        SetView<SlotReference> aggUsedSlotInAggFunction
                = Sets.difference(aggUsedNonVirtualSlots, groupingSetsUsedSlot);

        List<Slot> normalizedRepeatOutput = ImmutableList.<Slot>builder()
                .addAll(groupingSetsUsedSlot)
                .addAll(aggUsedSlotInAggFunction)
                .addAll(allVirtualSlots)
                .build();

        // 3 parts need push down:
        // flattenGroupingSetExpr, argumentsOfGroupingScalarFunction, argumentsOfAggregateFunction
        Set<Expression> needToSlots = Sets.union(needToSlotsArgs, needToSlotsGroupingExpr);
        NormalizeToSlotContext fullContext = argsContext.mergeContext(groupingExprContext);
        Set<NamedExpression> pushedProject = fullContext.pushDownToNamedExpression(needToSlots);

        Plan normalizedChild = pushDownProject(pushedProject, repeat.child());

        LogicalRepeat<Plan> normalizedRepeat = repeat.withNormalizedExpr(
                (List) normalizedGroupingSets, (List) normalizedRepeatOutput, normalizedChild);

        List<Expression> normalizedAggGroupBy = ImmutableList.<Expression>builder()
                .addAll(groupingSetsUsedSlot)
                .addAll(allVirtualSlots)
                .build();

        normalizedAggOutput = getExprIdUnchangedNormalizedAggOutput(normalizedAggOutput, repeat.getOutputExpressions());
        return new LogicalAggregate<>(normalizedAggGroupBy, (List) normalizedAggOutput,
                Optional.of(normalizedRepeat), normalizedRepeat);
    }

    private static Set<Expression> collectNeedToSlotGroupingExpr(LogicalRepeat<Plan> repeat) {
        // grouping sets should be pushed down, e.g. grouping sets((k + 1)),
        // we should push down the `k + 1` to the bottom plan
        return ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));
    }

    private static Set<Expression> collectNeedToSlotArgsOfGroupingScalarFuncAndAggFunc(LogicalRepeat<Plan> repeat) {
        Set<GroupingScalarFunction> groupingScalarFunctions = ExpressionUtils.collect(
                repeat.getOutputExpressions(), GroupingScalarFunction.class::isInstance);
        ImmutableSet.Builder<Expression> argumentsSetBuilder = ImmutableSet.builder();
        for (GroupingScalarFunction function : groupingScalarFunctions) {
            argumentsSetBuilder.addAll(function.getArguments());
        }
        ImmutableSet<Expression> argumentsOfGroupingScalarFunction = argumentsSetBuilder.build();

        List<AggregateFunction> aggregateFunctions = CollectNonWindowedAggFuncs.collect(repeat.getOutputExpressions());
        ImmutableSet.Builder<Expression> argumentsOfAggregateFunctionBuilder = ImmutableSet.builder();
        for (AggregateFunction function : aggregateFunctions) {
            for (Expression arg : function.getArguments()) {
                if (arg instanceof OrderExpression) {
                    argumentsOfAggregateFunctionBuilder.add(arg.child(0));
                } else {
                    argumentsOfAggregateFunctionBuilder.add(arg);
                }
            }
        }
        ImmutableSet<Expression> argumentsOfAggregateFunction = argumentsOfAggregateFunctionBuilder.build();

        return ImmutableSet.<Expression>builder()
                // grouping sets should be pushed down, e.g. grouping sets((k + 1)),
                // we should push down the `k + 1` to the bottom plan
                // e.g. grouping_id(k + 1), we should push down the `k + 1` to the bottom plan
                .addAll(argumentsOfGroupingScalarFunction)
                // e.g. sum(k + 1), we should push down the `k + 1` to the bottom plan
                .addAll(argumentsOfAggregateFunction)
                .build();
    }

    private static Plan pushDownProject(Set<NamedExpression> pushedExprs, Plan originBottomPlan) {
        if (!pushedExprs.equals(originBottomPlan.getOutputSet()) && !pushedExprs.isEmpty()) {
            return new LogicalProject<>(ImmutableList.copyOf(pushedExprs), originBottomPlan);
        }
        return originBottomPlan;
    }

    /** buildContext */
    public static NormalizeToSlotContext buildContext(Repeat<? extends Plan> repeat,
            Set<? extends Expression> sourceExpressions) {
        Set<Alias> aliases = ExpressionUtils.collect(repeat.getOutputExpressions(), Alias.class::isInstance);
        Map<Expression, Alias> existsAliasMap = Maps.newLinkedHashMap();
        for (Alias existsAlias : aliases) {
            existsAliasMap.put(existsAlias.child(), existsAlias);
        }

        List<Expression> groupingSetExpressions = ExpressionUtils.flatExpressions(repeat.getGroupingSets());
        Map<Expression, NormalizeToSlotTriplet> normalizeToSlotMap = Maps.newLinkedHashMap();
        for (Expression expression : sourceExpressions) {
            Optional<NormalizeToSlotTriplet> pushDownTriplet;
            if (groupingSetExpressions.contains(expression)) {
                pushDownTriplet = toGroupingSetExpressionPushDownTriplet(expression, existsAliasMap.get(expression));
            } else {
                pushDownTriplet = Optional.of(
                        NormalizeToSlotTriplet.toTriplet(expression, existsAliasMap.get(expression)));
            }

            if (pushDownTriplet.isPresent()) {
                normalizeToSlotMap.put(expression, pushDownTriplet.get());
            }
        }
        return new NormalizeToSlotContext(normalizeToSlotMap);
    }

    private static Optional<NormalizeToSlotTriplet> toGroupingSetExpressionPushDownTriplet(
            Expression expression, @Nullable Alias existsAlias) {
        NormalizeToSlotTriplet originTriplet = NormalizeToSlotTriplet.toTriplet(expression, existsAlias);
        SlotReference remainSlot = (SlotReference) originTriplet.remainExpr;
        Slot newSlot = remainSlot.withNullable(true);
        return Optional.of(new NormalizeToSlotTriplet(expression, newSlot, originTriplet.pushedExpr));
    }

    private static Expression normalizeAggFuncChildrenAndGroupingScalarFunc(NormalizeToSlotContext context,
            Expression expr) {
        if (expr instanceof AggregateFunction) {
            AggregateFunction function = (AggregateFunction) expr;
            List<Expression> normalizedRealExpressions = context.normalizeToUseSlotRef(function.getArguments());
            function = function.withChildren(normalizedRealExpressions);
            return function;
        } else if (expr instanceof GroupingScalarFunction) {
            GroupingScalarFunction function = (GroupingScalarFunction) expr;
            List<Expression> normalizedRealExpressions = context.normalizeToUseSlotRef(function.getArguments());
            function = function.withChildren(normalizedRealExpressions);
            // eliminate GroupingScalarFunction and replace to VirtualSlotReference
            return Repeat.generateVirtualSlotByFunction(function);
        } else {
            return expr;
        }
    }

    private static Set<Alias> getExistsAlias(LogicalRepeat<Plan> repeat,
            Map<Expression, NormalizeToSlotTriplet> groupingExprMap) {
        Set<Alias> existsAlias = Sets.newHashSet();
        Set<Alias> aliases = ExpressionUtils.collect(repeat.getOutputExpressions(), Alias.class::isInstance);
        existsAlias.addAll(aliases);
        for (NormalizeToSlotTriplet triplet : groupingExprMap.values()) {
            if (triplet.pushedExpr instanceof Alias) {
                Alias alias = (Alias) triplet.pushedExpr;
                existsAlias.add(alias);
            }
        }
        return existsAlias;
    }

    /*
     * compute slots that appear both in agg func and grouping sets,
     * copy the slots and output in the project below the repeat as new copied slots,
     * and refer the new copied slots in aggregate parameters.
     * eg: original plan after normalizedRepeat
     * LogicalAggregate (groupByExpr=[a#0, GROUPING_ID#1], outputExpr=[a#0, GROUPING_ID#1, sum(a#0) as `sum(a)`#2])
     *   +--LogicalRepeat (groupingSets=[[a#0]], outputExpr=[a#0, GROUPING_ID#1]
     *      +--LogicalProject (projects =[a#0])
     * After:
     * LogicalAggregate (groupByExpr=[a#0, GROUPING_ID#1], outputExpr=[a#0, GROUPING_ID#1, sum(a#3) as `sum(a)`#2])
     *   +--LogicalRepeat (groupingSets=[[a#0]], outputExpr=[a#0, a#3, GROUPING_ID#1]
     *      +--LogicalProject (projects =[a#0, a#0 as `a`#3])
     */
    private static LogicalAggregate<Plan> dealSlotAppearBothInAggFuncAndGroupingSets(
            @NotNull LogicalAggregate<Plan> aggregate) {
        LogicalRepeat<Plan> repeat = (LogicalRepeat<Plan>) aggregate.child();
        Map<Slot, Alias> commonSlotToAliasMap = getCommonSlotToAliasMap(repeat, aggregate);
        if (commonSlotToAliasMap.isEmpty()) {
            return aggregate;
        }
        // modify repeat child to a new project with more projections
        Set<Alias> newAliases = new HashSet<>(commonSlotToAliasMap.values());
        List<Slot> originSlots = repeat.child().getOutput();
        ImmutableList<NamedExpression> newProjects =
                ImmutableList.<NamedExpression>builder().addAll(originSlots).addAll(newAliases).build();
        LogicalProject<Plan> newLogicalProject = new LogicalProject<>(newProjects, repeat.child());
        repeat = repeat.withChildren(ImmutableList.of(newLogicalProject));

        // modify repeat outputs
        List<Slot> originRepeatSlots = repeat.getOutput();
        List<Slot> virtualSlots = Lists.newArrayList();
        List<Slot> nonVirtualSlots = Lists.newArrayList();
        for (Slot slot : originRepeatSlots) {
            if (slot instanceof VirtualSlotReference) {
                virtualSlots.add(slot);
            } else {
                nonVirtualSlots.add(slot);
            }
        }
        List<Slot> newSlots = Lists.newArrayList();
        for (Alias alias : newAliases) {
            newSlots.add(alias.toSlot());
        }
        repeat = repeat.withAggOutput(ImmutableList.<NamedExpression>builder()
                .addAll(nonVirtualSlots)
                .addAll(newSlots)
                .addAll(virtualSlots)
                .build());
        aggregate = aggregate.withChildren(ImmutableList.of(repeat));

        ImmutableList.Builder<NamedExpression> newOutputExpressionBuilder = ImmutableList.builder();
        for (NamedExpression expression : aggregate.getOutputExpressions()) {
            NamedExpression newExpression = (NamedExpression) expression
                    .accept(RewriteAggFuncWithoutWindowAggFunc.INSTANCE, commonSlotToAliasMap);
            newOutputExpressionBuilder.add(newExpression);
        }
        List<NamedExpression> newOutputExpressions = newOutputExpressionBuilder.build();
        return aggregate.withAggOutput(newOutputExpressions);
    }

    private static Map<Slot, Alias> getCommonSlotToAliasMap(LogicalRepeat<Plan> repeat,
            LogicalAggregate<Plan> aggregate) {
        List<AggregateFunction> aggregateFunctions =
                CollectNonWindowedAggFuncs.collect(aggregate.getOutputExpressions());
        ImmutableSet.Builder<Slot> aggUsedSlotBuilder = ImmutableSet.builder();
        for (AggregateFunction function : aggregateFunctions) {
            aggUsedSlotBuilder.addAll(function.<SlotReference>collect(SlotReference.class::isInstance));
        }
        ImmutableSet<Slot> aggUsedSlots = aggUsedSlotBuilder.build();

        ImmutableSet.Builder<Slot> groupingSetsUsedSlotBuilder = ImmutableSet.builder();
        for (List<Expression> groupingSet : repeat.getGroupingSets()) {
            for (Expression expr : groupingSet) {
                groupingSetsUsedSlotBuilder.addAll(expr.<SlotReference>collect(SlotReference.class::isInstance));
            }
        }
        ImmutableSet<Slot> groupingSetsUsedSlot = groupingSetsUsedSlotBuilder.build();

        Set<Slot> resSet = new HashSet<>(aggUsedSlots);
        resSet.retainAll(groupingSetsUsedSlot);
        Map<Slot, Alias> commonSlotToAliasMap = Maps.newHashMap();
        for (Slot key : resSet) {
            Alias alias = new Alias(key);
            commonSlotToAliasMap.put(key, alias);
        }
        return commonSlotToAliasMap;
    }

    /**
     * This class use the map(slotMapping) to rewrite all slots in trival-agg.
     * The purpose of this class is to only rewrite the slots in trival-agg and not to rewrite the slots in window-agg.
     */
    private static class RewriteAggFuncWithoutWindowAggFunc
            extends DefaultExpressionRewriter<Map<Slot, Alias>> {

        private static final RewriteAggFuncWithoutWindowAggFunc
                INSTANCE = new RewriteAggFuncWithoutWindowAggFunc();

        private RewriteAggFuncWithoutWindowAggFunc() {}

        @Override
        public Expression visitAggregateFunction(AggregateFunction aggregateFunction, Map<Slot, Alias> slotMapping) {
            return aggregateFunction.rewriteDownShortCircuit(e -> {
                if (e instanceof Slot && slotMapping.containsKey(e)) {
                    return slotMapping.get(e).toSlot();
                }
                return e;
            });
        }

        @Override
        public Expression visitWindow(WindowExpression windowExpression, Map<Slot, Alias> slotMapping) {
            List<Expression> newChildren = new ArrayList<>();
            Expression function = windowExpression.getFunction();
            Expression oldFuncChild = function.child(0);
            boolean hasNewChildren = false;
            if (oldFuncChild != null) {
                Expression newFuncChild;
                newFuncChild = function.child(0).accept(this, slotMapping);
                hasNewChildren = (newFuncChild != oldFuncChild);
                newChildren.add(hasNewChildren
                        ? function.withChildren(ImmutableList.of(newFuncChild)) : function);
            } else {
                newChildren.add(function);
            }
            for (Expression partitionKey : windowExpression.getPartitionKeys()) {
                Expression newChild = partitionKey.accept(this, slotMapping);
                if (newChild != partitionKey) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            for (Expression orderKey : windowExpression.getOrderKeys()) {
                Expression newChild = orderKey.accept(this, slotMapping);
                if (newChild != orderKey) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            if (windowExpression.getWindowFrame().isPresent()) {
                newChildren.add(windowExpression.getWindowFrame().get());
            }
            return hasNewChildren ? windowExpression.withChildren(newChildren) : windowExpression;
        }
    }

    private static List<NamedExpression> getExprIdUnchangedNormalizedAggOutput(
            List<NamedExpression> normalizedAggOutput, List<NamedExpression> originalAggOutput) {
        Builder<NamedExpression> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < originalAggOutput.size(); i++) {
            NamedExpression e = normalizedAggOutput.get(i);
            // process Expression like Alias(SlotReference#0)#0
            if (e instanceof Alias && e.child(0) instanceof SlotReference) {
                SlotReference slotReference = (SlotReference) e.child(0);
                if (slotReference.getExprId().equals(e.getExprId())) {
                    e = slotReference;
                }
            }
            // Make the output ExprId unchanged
            if (!e.getExprId().equals(originalAggOutput.get(i).getExprId())) {
                e = new Alias(originalAggOutput.get(i).getExprId(), e, originalAggOutput.get(i).getName());
            }
            builder.add(e);
        }
        return builder.build();
    }
}
