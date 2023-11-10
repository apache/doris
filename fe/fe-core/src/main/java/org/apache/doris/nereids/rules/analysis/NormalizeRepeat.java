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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
                checkRepeatLegality(repeat);
                // add virtual slot, LogicalAggregate and LogicalProject for normalize
                return normalizeRepeat(repeat);
            })
        );
    }

    private void checkRepeatLegality(LogicalRepeat<Plan> repeat) {
        checkIfAggFuncSlotInGroupingSets(repeat);
        checkGroupingSetsSize(repeat);
    }

    private void checkIfAggFuncSlotInGroupingSets(LogicalRepeat<Plan> repeat) {
        Set<Slot> aggUsedSlots = repeat.getOutputExpressions().stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                .flatMap(e -> e.<Set<SlotReference>>collect(SlotReference.class::isInstance).stream())
                .collect(ImmutableSet.toImmutableSet());
        Set<ExprId> groupingSetsUsedSlotExprIds = repeat.getGroupingSets().stream()
                .flatMap(Collection::stream)
                .flatMap(e -> e.<Set<SlotReference>>collect(SlotReference.class::isInstance).stream())
                .map(SlotReference::getExprId)
                .collect(Collectors.toSet());
        for (Slot slot : aggUsedSlots) {
            if (groupingSetsUsedSlotExprIds.contains(slot.getExprId())) {
                throw new AnalysisException("column: " + slot.toSql() + " cannot both in select "
                        + "list and aggregate functions when using GROUPING SETS/CUBE/ROLLUP, "
                        + "please use union instead.");
            }
        }
    }

    private void checkGroupingSetsSize(LogicalRepeat<Plan> repeat) {
        Set<Expression> flattenGroupingSetExpr = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));
        if (flattenGroupingSetExpr.size() > LogicalRepeat.MAX_GROUPING_SETS_NUM) {
            throw new AnalysisException(
                    "Too many sets in GROUP BY clause, the max grouping sets item is "
                            + LogicalRepeat.MAX_GROUPING_SETS_NUM);
        }
    }

    private LogicalAggregate<Plan> normalizeRepeat(LogicalRepeat<Plan> repeat) {
        Set<Expression> needToSlots = collectNeedToSlotExpressions(repeat);
        NormalizeToSlotContext context = buildContext(repeat, needToSlots);

        // normalize grouping sets to List<List<Slot>>
        List<List<Slot>> normalizedGroupingSets = repeat.getGroupingSets()
                .stream()
                .map(groupingSet -> (List<Slot>) (List) context.normalizeToUseSlotRef(groupingSet))
                .collect(ImmutableList.toImmutableList());

        // replace the arguments of grouping scalar function to virtual slots
        // replace some complex expression to slot, e.g. `a + 1`
        List<NamedExpression> normalizedAggOutput = context.normalizeToUseSlotRef(
                        repeat.getOutputExpressions(), this::normalizeGroupingScalarFunction);

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

        Set<NamedExpression> pushedProject = context.pushDownToNamedExpression(needToSlots);
        Plan normalizedChild = pushDownProject(pushedProject, repeat.child());

        LogicalRepeat<Plan> normalizedRepeat = repeat.withNormalizedExpr(
                (List) normalizedGroupingSets, (List) normalizedRepeatOutput, normalizedChild);

        List<Expression> normalizedAggGroupBy = ImmutableList.<Expression>builder()
                .addAll(groupingSetsUsedSlot)
                .addAll(allVirtualSlots)
                .build();
        return new LogicalAggregate<>(normalizedAggGroupBy, (List) normalizedAggOutput,
                Optional.of(normalizedRepeat), normalizedRepeat);
    }

    private Set<Expression> collectNeedToSlotExpressions(LogicalRepeat<Plan> repeat) {
        // 3 parts need push down:
        // flattenGroupingSetExpr, argumentsOfGroupingScalarFunction, argumentsOfAggregateFunction

        Set<Expression> flattenGroupingSetExpr = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));

        Set<GroupingScalarFunction> groupingScalarFunctions = ExpressionUtils.collect(
                repeat.getOutputExpressions(), GroupingScalarFunction.class::isInstance);

        ImmutableSet<Expression> argumentsOfGroupingScalarFunction = groupingScalarFunctions.stream()
                .flatMap(function -> function.getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());

        Set<AggregateFunction> aggregateFunctions = ExpressionUtils.collect(
                repeat.getOutputExpressions(), AggregateFunction.class::isInstance);

        ImmutableSet<Expression> argumentsOfAggregateFunction = aggregateFunctions.stream()
                .flatMap(function -> function.getArguments().stream().map(arg -> {
                    if (arg instanceof OrderExpression) {
                        return arg.child(0);
                    } else {
                        return arg;
                    }
                }))
                .collect(ImmutableSet.toImmutableSet());

        ImmutableSet<Expression> needPushDown = ImmutableSet.<Expression>builder()
                // grouping sets should be pushed down, e.g. grouping sets((k + 1)),
                // we should push down the `k + 1` to the bottom plan
                .addAll(flattenGroupingSetExpr)
                // e.g. grouping_id(k + 1), we should push down the `k + 1` to the bottom plan
                .addAll(argumentsOfGroupingScalarFunction)
                // e.g. sum(k + 1), we should push down the `k + 1` to the bottom plan
                .addAll(argumentsOfAggregateFunction)
                .build();
        return needPushDown;
    }

    private Plan pushDownProject(Set<NamedExpression> pushedExprs, Plan originBottomPlan) {
        if (!pushedExprs.equals(originBottomPlan.getOutputSet()) && !pushedExprs.isEmpty()) {
            return new LogicalProject<>(ImmutableList.copyOf(pushedExprs), originBottomPlan);
        }
        return originBottomPlan;
    }

    /** buildContext */
    public NormalizeToSlotContext buildContext(Repeat<? extends Plan> repeat,
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

    private Optional<NormalizeToSlotTriplet> toGroupingSetExpressionPushDownTriplet(
            Expression expression, @Nullable Alias existsAlias) {
        NormalizeToSlotTriplet originTriplet = NormalizeToSlotTriplet.toTriplet(expression, existsAlias);
        SlotReference remainSlot = (SlotReference) originTriplet.remainExpr;
        Slot newSlot = remainSlot.withNullable(true);
        return Optional.of(new NormalizeToSlotTriplet(expression, newSlot, originTriplet.pushedExpr));
    }

    private Expression normalizeGroupingScalarFunction(NormalizeToSlotContext context, Expression expr) {
        if (expr instanceof GroupingScalarFunction) {
            GroupingScalarFunction function = (GroupingScalarFunction) expr;
            List<Expression> normalizedRealExpressions = context.normalizeToUseSlotRef(function.getArguments());
            function = function.withChildren(normalizedRealExpressions);
            // eliminate GroupingScalarFunction and replace to VirtualSlotReference
            return Repeat.generateVirtualSlotByFunction(function);
        } else {
            return expr;
        }
    }
}
