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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.NormalizeToSlot;
import org.apache.doris.nereids.rules.rewrite.NormalizeToSlot.NormalizeToSlotContext;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * normalize aggregate's group keys and AggregateFunction's child to SlotReference
 * and generate a LogicalProject top on LogicalAggregate to hold to order of aggregate output,
 * since aggregate output's order could change when we do translate.
 * <p>
 * Apply this rule could simplify the processing of enforce and translate.
 * <pre>
 * Original Plan:
 * Aggregate(
 *   keys:[k1#1, K2#2 + 1],
 *   outputs:[k1#1, Alias(K2# + 1)#4, Alias(k1#1 + 1)#5, Alias(SUM(v1#3))#6,
 *            Alias(SUM(v1#3 + 1))#7, Alias(SUM(v1#3) + 1)#8])
 * </pre>
 * After rule:
 * <pre>
 * Project(k1#1, Alias(SR#9)#4, Alias(k1#1 + 1)#5, Alias(SR#10))#6, Alias(SR#11))#7, Alias(SR#10 + 1)#8)
 * +-- Aggregate(keys:[k1#1, SR#9], outputs:[k1#1, SR#9, Alias(SUM(v1#3))#10, Alias(SUM(v1#3 + 1))#11])
 *   +-- Project(k1#1, Alias(K2#2 + 1)#9, v1#3)
 * </pre>
 * Note: window function will be moved to upper project
 * all agg functions except the top agg should be pushed to Aggregate node.
 * example 1:
 * <pre>
 *    select min(x), sum(x) over () ...
 * the 'sum(x)' is top agg of window function, it should be moved to upper project
 * plan:
 *    project(sum(x) over())
 *        Aggregate(min(x), x)
 * </pre>
 * example 2:
 * <pre>
 *    select min(x), avg(sum(x)) over() ...
 * the 'sum(x)' should be moved to Aggregate
 * plan:
 *    project(avg(y) over())
 *         Aggregate(min(x), sum(x) as y)
 * </pre>
 * example 3:
 * <pre>
 *    select sum(x+1), x+1, sum(x+1) over() ...
 * window function should use x instead of x+1
 * plan:
 *    project(sum(x+1) over())
 *        Agg(sum(y), x)
 *            project(x+1 as y)
 * </pre>
 * More example could get from UT {NormalizeAggregateTest}
 */
public class NormalizeAggregate extends OneRewriteRuleFactory implements NormalizeToSlot {
    @Override
    public Rule build() {
        return logicalAggregate().whenNot(LogicalAggregate::isNormalized).then(aggregate -> {

            List<NamedExpression> aggregateOutput = aggregate.getOutputExpressions();
            Set<Alias> existsAlias = ExpressionUtils.mutableCollect(aggregateOutput, Alias.class::isInstance);

            List<AggregateFunction> aggFuncs = Lists.newArrayList();
            aggregateOutput.forEach(o -> o.accept(CollectNonWindowedAggFuncs.INSTANCE, aggFuncs));

            // we need push down subquery exprs inside non-window and non-distinct agg functions
            Set<SubqueryExpr> subqueryExprs = ExpressionUtils.mutableCollect(aggFuncs.stream()
                    .filter(aggFunc -> !aggFunc.isDistinct()).collect(Collectors.toList()),
                    SubqueryExpr.class::isInstance);
            Set<Expression> groupingByExprs = ImmutableSet.copyOf(aggregate.getGroupByExpressions());
            NormalizeToSlotContext bottomSlotContext =
                    NormalizeToSlotContext.buildContext(existsAlias, Sets.union(groupingByExprs, subqueryExprs));
            Set<NamedExpression> bottomOutputs =
                    bottomSlotContext.pushDownToNamedExpression(Sets.union(groupingByExprs, subqueryExprs));

            // use group by context to normalize agg functions to process
            //   sql like: select sum(a + 1) from t group by a + 1
            //
            // before normalize:
            // agg(output: sum(a[#0] + 1)[#2], group_by: alias(a + 1)[#1])
            // +-- project(a[#0], (a[#0] + 1)[#1])
            //
            // after normalize:
            // agg(output: sum(alias(a + 1)[#1])[#2], group_by: alias(a + 1)[#1])
            // +-- project((a[#0] + 1)[#1])
            List<AggregateFunction> normalizedAggFuncs = bottomSlotContext.normalizeToUseSlotRef(aggFuncs);
            Set<NamedExpression> bottomProjects = Sets.newHashSet(bottomOutputs);
            // TODO: if we have distinct agg, we must push down its children,
            //   because need use it to generate distribution enforce
            // step 1: split agg functions into 2 parts: distinct and not distinct
            List<AggregateFunction> distinctAggFuncs = Lists.newArrayList();
            List<AggregateFunction> nonDistinctAggFuncs = Lists.newArrayList();
            for (AggregateFunction aggregateFunction : normalizedAggFuncs) {
                if (aggregateFunction.isDistinct()) {
                    distinctAggFuncs.add(aggregateFunction);
                } else {
                    nonDistinctAggFuncs.add(aggregateFunction);
                }
            }
            // step 2: if we only have one distinct agg function, we do push down for it
            if (!distinctAggFuncs.isEmpty()) {
                // process distinct normalize and put it back to normalizedAggFuncs
                List<AggregateFunction> newDistinctAggFuncs = Lists.newArrayList();
                Map<Expression, Expression> replaceMap = Maps.newHashMap();
                Map<Expression, NamedExpression> aliasCache = Maps.newHashMap();
                for (AggregateFunction distinctAggFunc : distinctAggFuncs) {
                    List<Expression> newChildren = Lists.newArrayList();
                    for (Expression child : distinctAggFunc.children()) {
                        if (child instanceof SlotReference || child instanceof Literal) {
                            newChildren.add(child);
                        } else {
                            NamedExpression alias;
                            if (aliasCache.containsKey(child)) {
                                alias = aliasCache.get(child);
                            } else {
                                alias = new Alias(child);
                                aliasCache.put(child, alias);
                            }
                            bottomProjects.add(alias);
                            newChildren.add(alias.toSlot());
                        }
                    }
                    AggregateFunction newDistinctAggFunc = distinctAggFunc.withChildren(newChildren);
                    replaceMap.put(distinctAggFunc, newDistinctAggFunc);
                    newDistinctAggFuncs.add(newDistinctAggFunc);
                }
                aggregateOutput = aggregateOutput.stream()
                        .map(e -> ExpressionUtils.replace(e, replaceMap))
                        .map(NamedExpression.class::cast)
                        .collect(Collectors.toList());
                distinctAggFuncs = newDistinctAggFuncs;
            }
            normalizedAggFuncs = Lists.newArrayList(nonDistinctAggFuncs);
            normalizedAggFuncs.addAll(distinctAggFuncs);
            // TODO: process redundant expressions in aggregate functions children
            // build normalized agg output
            NormalizeToSlotContext normalizedAggFuncsToSlotContext =
                    NormalizeToSlotContext.buildContext(existsAlias, normalizedAggFuncs);
            // agg output include 2 part, normalized group by slots and normalized agg functions
            List<NamedExpression> normalizedAggOutput = ImmutableList.<NamedExpression>builder()
                    .addAll(bottomOutputs.stream().map(NamedExpression::toSlot).iterator())
                    .addAll(normalizedAggFuncsToSlotContext.pushDownToNamedExpression(normalizedAggFuncs))
                    .build();
            // add normalized agg's input slots to bottom projects
            Set<Slot> bottomProjectSlots = bottomProjects.stream()
                    .map(NamedExpression::toSlot)
                    .collect(Collectors.toSet());
            Set<NamedExpression> aggInputSlots = normalizedAggFuncs.stream()
                    .map(Expression::getInputSlots)
                    .flatMap(Set::stream)
                    .filter(e -> !bottomProjectSlots.contains(e))
                    .collect(Collectors.toSet());
            bottomProjects.addAll(aggInputSlots);
            // build group by exprs
            List<Expression> normalizedGroupExprs = bottomSlotContext.normalizeToUseSlotRef(groupingByExprs);

            Plan bottomPlan;
            if (!bottomProjects.isEmpty()) {
                bottomPlan = new LogicalProject<>(ImmutableList.copyOf(bottomProjects), aggregate.child());
            } else {
                bottomPlan = aggregate.child();
            }

            List<NamedExpression> upperProjects = normalizeOutput(aggregateOutput,
                    bottomSlotContext, normalizedAggFuncsToSlotContext);

            return new LogicalProject<>(upperProjects,
                    aggregate.withNormalized(normalizedGroupExprs, normalizedAggOutput, bottomPlan));
        }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }

    private List<NamedExpression> normalizeOutput(List<NamedExpression> aggregateOutput,
            NormalizeToSlotContext groupByToSlotContext, NormalizeToSlotContext normalizedAggFuncsToSlotContext) {
        // build upper project, use two context to do pop up, because agg output maybe contain two part:
        //   group by keys and agg expressions
        List<NamedExpression> upperProjects = groupByToSlotContext
                .normalizeToUseSlotRefWithoutWindowFunction(aggregateOutput);
        upperProjects = normalizedAggFuncsToSlotContext.normalizeToUseSlotRefWithoutWindowFunction(upperProjects);

        Builder<NamedExpression> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < aggregateOutput.size(); i++) {
            NamedExpression e = upperProjects.get(i);
            // process Expression like Alias(SlotReference#0)#0
            if (e instanceof Alias && e.child(0) instanceof SlotReference) {
                SlotReference slotReference = (SlotReference) e.child(0);
                if (slotReference.getExprId().equals(e.getExprId())) {
                    e = slotReference;
                }
            }
            // Make the output ExprId unchanged
            if (!e.getExprId().equals(aggregateOutput.get(i).getExprId())) {
                e = new Alias(aggregateOutput.get(i).getExprId(), e, aggregateOutput.get(i).getName());
            }
            builder.add(e);
        }
        return builder.build();
    }

    private static class CollectNonWindowedAggFuncs extends DefaultExpressionVisitor<Void, List<AggregateFunction>> {

        private static final CollectNonWindowedAggFuncs INSTANCE = new CollectNonWindowedAggFuncs();

        @Override
        public Void visitWindow(WindowExpression windowExpression, List<AggregateFunction> context) {
            for (Expression child : windowExpression.getExpressionsInWindowSpec()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitAggregateFunction(AggregateFunction aggregateFunction, List<AggregateFunction> context) {
            context.add(aggregateFunction);
            return null;
        }
    }
}
