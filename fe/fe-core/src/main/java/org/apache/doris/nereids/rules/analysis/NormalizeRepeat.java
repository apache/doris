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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
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

/** NormalizeRepeat
 * eg: select sum(k2 + 1), grouping(k1) from t1 group by grouping sets ((k1));
 * Original Plan:
 *     +-- GroupingSets(
 *         keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *         outputs:sum(k2#2 + 1) as `sum(k2 + 1)`#3, group(grouping_prefix(k1#1)#7) as `grouping(k1 + 1)`#4
 *
 * After:
 * Project(sum((k2 + 1)#8) AS `sum((k2 + 1))`#9, grouping(GROUPING_PREFIX_(k1#1)#7)) as `grouping(k1)`#10)
 *   +-- Aggregate(
 *          keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *          outputs:[(K2 + 1)#8), grouping_prefix(k1#1)#7]
 *         +-- GropingSets(
 *             keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
 *             outputs:k1#1, (k2 + 1)#8, grouping_id()#0, grouping_prefix(k1#1)#7
 *             +-- Project(k1#1, (K2#2 + 1) as `(k2 + 1)`#8)
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
        Set<Slot> aggUsedSlot = repeat.getOutputExpressions().stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance).stream())
                .flatMap(e -> e.<Set<SlotReference>>collect(SlotReference.class::isInstance).stream())
                .collect(ImmutableSet.toImmutableSet());
        Set<Slot> groupingSetsUsedSlot = repeat.getGroupingSets().stream()
                .flatMap(e -> e.stream())
                .flatMap(e -> e.<Set<SlotReference>>collect(SlotReference.class::isInstance).stream())
                .collect(Collectors.toSet());
        for (Slot slot : aggUsedSlot) {
            if (groupingSetsUsedSlot.contains(slot)) {
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
        Set<Expression> needPushDownExpr = collectPushDownExpressions(repeat);
        PushDownContext pushDownContext = PushDownContext.toPushDownContext(repeat, needPushDownExpr);

        // normalize grouping sets to List<List<Slot>>
        List<List<Slot>> normalizedGroupingSets = repeat.getGroupingSets()
                .stream()
                .map(groupingSet -> (List<Slot>) (List) pushDownContext.normalizeToUseSlotRef(groupingSet))
                .collect(ImmutableList.toImmutableList());

        // replace the arguments of grouping scalar function to virtual slots
        // replace some complex expression to slot, e.g. `a + 1`
        List<NamedExpression> normalizedAggOutput =
                pushDownContext.normalizeToUseSlotRef(repeat.getOutputExpressions());

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

        Set<NamedExpression> pushedProject = pushDownContext.pushDownToNamedExpression(needPushDownExpr);
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

    private Set<Expression> collectPushDownExpressions(LogicalRepeat<Plan> repeat) {
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
                .flatMap(function -> function.getArguments().stream())
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
        if (!pushedExprs.equals(originBottomPlan.getOutputSet())) {
            return new LogicalProject<>(ImmutableList.copyOf(pushedExprs), originBottomPlan);
        }
        return originBottomPlan;
    }

    private static class PushDownContext {
        private final Map<Expression, PushDownTriplet> pushDownMap;

        public PushDownContext(Map<Expression, PushDownTriplet> pushDownMap) {
            this.pushDownMap = pushDownMap;
        }

        public static PushDownContext toPushDownContext(Repeat<? extends Plan> repeat,
                Set<? extends Expression> sourceExpressions) {
            List<Expression> groupingSetExpressions = ExpressionUtils.flatExpressions(repeat.getGroupingSets());
            Set<Expression> commonGroupingSetExpressions = repeat.getCommonGroupingSetExpressions();

            Map<Expression, PushDownTriplet> pushDownMap = Maps.newLinkedHashMap();
            for (Expression expression : sourceExpressions) {
                Optional<PushDownTriplet> pushDownTriplet;
                if (groupingSetExpressions.contains(expression)) {
                    boolean isCommonGroupingSetExpression = commonGroupingSetExpressions.contains(expression);
                    pushDownTriplet = PushDownTriplet.toGroupingSetExpressionPushDownTriplet(
                            isCommonGroupingSetExpression, expression);
                } else {
                    pushDownTriplet = PushDownTriplet.toPushDownTriplet(expression);
                }

                if (pushDownTriplet.isPresent()) {
                    pushDownMap.put(expression, pushDownTriplet.get());
                }
            }
            return new PushDownContext(pushDownMap);
        }

        public <E extends Expression> List<E> normalizeToUseSlotRef(List<E> expressions) {
            return expressions.stream()
                    .map(expr -> (E) expr.rewriteDownShortCircuit(child -> {
                        if (child instanceof GroupingScalarFunction) {
                            GroupingScalarFunction function = (GroupingScalarFunction) child;
                            List<Expression> normalizedRealExpressions = normalizeToUseSlotRef(function.getArguments());
                            function = function.withChildren(normalizedRealExpressions);
                            // eliminate GroupingScalarFunction and replace to VirtualSlotReference
                            return Repeat.generateVirtualSlotByFunction(function);
                        }

                        PushDownTriplet pushDownTriplet = pushDownMap.get(child);
                        return pushDownTriplet == null ? child : pushDownTriplet.remainExpr;
                    })).collect(ImmutableList.toImmutableList());
        }

        /**
         * generate bottom projections with groupByExpressions.
         * eg:
         * groupByExpressions: k1#0, k2#1 + 1;
         * bottom: k1#0, (k2#1 + 1) AS (k2 + 1)#2;
         */
        public Set<NamedExpression> pushDownToNamedExpression(Collection<? extends Expression> needToPushExpressions) {
            return needToPushExpressions.stream()
                    .map(expr -> {
                        PushDownTriplet pushDownTriplet = pushDownMap.get(expr);
                        return pushDownTriplet == null ? (NamedExpression) expr : pushDownTriplet.pushedExpr;
                    }).collect(ImmutableSet.toImmutableSet());
        }
    }

    private static class PushDownTriplet {
        public final Expression originExpr;
        public final Slot remainExpr;
        public final NamedExpression pushedExpr;

        public PushDownTriplet(Expression originExpr, Slot remainExpr, NamedExpression pushedExpr) {
            this.originExpr = originExpr;
            this.remainExpr = remainExpr;
            this.pushedExpr = pushedExpr;
        }

        private static Optional<PushDownTriplet> toGroupingSetExpressionPushDownTriplet(
                boolean isCommonGroupingSetExpression, Expression expression) {
            Optional<PushDownTriplet> pushDownTriplet = toPushDownTriplet(expression);
            if (!pushDownTriplet.isPresent()) {
                return pushDownTriplet;
            }

            PushDownTriplet originTriplet = pushDownTriplet.get();
            SlotReference remainSlot = (SlotReference) originTriplet.remainExpr;
            Slot newSlot = remainSlot.withCommonGroupingSetExpression(isCommonGroupingSetExpression);
            return Optional.of(new PushDownTriplet(expression, newSlot, originTriplet.pushedExpr));
        }

        private static Optional<PushDownTriplet> toPushDownTriplet(Expression expression) {

            if (expression instanceof SlotReference) {
                PushDownTriplet pushDownTriplet =
                        new PushDownTriplet(expression, (SlotReference) expression, (SlotReference) expression);
                return Optional.of(pushDownTriplet);
            }

            if (expression instanceof NamedExpression) {
                NamedExpression namedExpression = (NamedExpression) expression;
                PushDownTriplet pushDownTriplet =
                        new PushDownTriplet(expression, namedExpression.toSlot(), namedExpression);
                return Optional.of(pushDownTriplet);
            }

            Alias alias = new Alias(expression, expression.toSql());
            PushDownTriplet pushDownTriplet = new PushDownTriplet(expression, alias.toSlot(), alias);
            return Optional.of(pushDownTriplet);
        }
    }
}
