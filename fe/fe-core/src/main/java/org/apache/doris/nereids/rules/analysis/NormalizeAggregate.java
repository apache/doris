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
import com.google.common.collect.Sets;

import java.util.HashSet;
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
            // The LogicalAggregate node may contain window agg functions and usual agg functions
            // we call window agg functions as window-agg and usual agg functions as trival-agg for short
            // This rule simplify LogicalAggregate node by:
            // 1. Push down some exprs from old LogicalAggregate node to a new child LogicalProject Node,
            // 2. create a new LogicalAggregate with normalized group by exprs and trival-aggs
            // 3. Pull up normalized old LogicalAggregate's output exprs to a new parent LogicalProject Node
            // Push down exprs:
            // 1. all group by exprs
            // 2. child contains subquery expr in trival-agg
            // 3. child contains window expr in trival-agg
            // 4. all input slots of trival-agg
            // 5. expr(including subquery) in distinct trival-agg
            // Normalize LogicalAggregate's output.
            // 1. normalize group by exprs by outputs of bottom LogicalProject
            // 2. normalize trival-aggs by outputs of bottom LogicalProject
            // 3. build normalized agg outputs
            // Pull up exprs:
            // normalize all output exprs in old LogicalAggregate to build a parent project node, typically includes:
            // 1. simple slots
            // 2. aliases
            //    a. alias with no aggs child
            //    b. alias with trival-agg child
            //    c. alias with window-agg

            // Push down exprs:
            // collect group by exprs
            Set<Expression> groupingByExprs =
                    ImmutableSet.copyOf(aggregate.getGroupByExpressions());

            // collect all trival-agg
            List<NamedExpression> aggregateOutput = aggregate.getOutputExpressions();
            List<AggregateFunction> aggFuncs = Lists.newArrayList();
            aggregateOutput.forEach(o -> o.accept(CollectNonWindowedAggFuncs.INSTANCE, aggFuncs));

            // split non-distinct agg child as two part
            // TRUE part 1: need push down itself, if it contains subqury or window expression
            // FALSE part 2: need push down its input slots, if it DOES NOT contain subqury or window expression
            Map<Boolean, Set<Expression>> categorizedNoDistinctAggsChildren = aggFuncs.stream()
                    .filter(aggFunc -> !aggFunc.isDistinct())
                    .flatMap(agg -> agg.children().stream())
                    .collect(Collectors.groupingBy(
                            child -> child.containsType(SubqueryExpr.class, WindowExpression.class),
                            Collectors.toSet()));

            // split distinct agg child as two parts
            // TRUE part 1: need push down itself, if it is NOT SlotReference or Literal
            // FALSE part 2: need push down its input slots, if it is SlotReference or Literal
            Map<Boolean, Set<Expression>> categorizedDistinctAggsChildren = aggFuncs.stream()
                    .filter(aggFunc -> aggFunc.isDistinct()).flatMap(agg -> agg.children().stream())
                    .collect(Collectors.groupingBy(
                            child -> !(child instanceof SlotReference || child instanceof Literal),
                            Collectors.toSet()));

            Set<Expression> needPushSelf = Sets.union(
                    categorizedNoDistinctAggsChildren.getOrDefault(true, new HashSet<>()),
                    categorizedDistinctAggsChildren.getOrDefault(true, new HashSet<>()));
            Set<Slot> needPushInputSlots = ExpressionUtils.getInputSlotSet(Sets.union(
                    categorizedNoDistinctAggsChildren.getOrDefault(false, new HashSet<>()),
                    categorizedDistinctAggsChildren.getOrDefault(false, new HashSet<>())));

            Set<Alias> existsAlias =
                    ExpressionUtils.mutableCollect(aggregateOutput, Alias.class::isInstance);

            // push down 3 kinds of exprs, these pushed exprs will be used to normalize agg output later
            // 1. group by exprs
            // 2. trivalAgg children
            // 3. trivalAgg input slots
            Set<Expression> allPushDownExprs =
                    Sets.union(groupingByExprs, Sets.union(needPushSelf, needPushInputSlots));
            NormalizeToSlotContext bottomSlotContext =
                    NormalizeToSlotContext.buildContext(existsAlias, allPushDownExprs);
            Set<NamedExpression> pushedGroupByExprs =
                    bottomSlotContext.pushDownToNamedExpression(groupingByExprs);
            Set<NamedExpression> pushedTrivalAggChildren =
                    bottomSlotContext.pushDownToNamedExpression(needPushSelf);
            Set<NamedExpression> pushedTrivalAggInputSlots =
                    bottomSlotContext.pushDownToNamedExpression(needPushInputSlots);
            Set<NamedExpression> bottomProjects = Sets.union(pushedGroupByExprs,
                    Sets.union(pushedTrivalAggChildren, pushedTrivalAggInputSlots));

            // create bottom project
            Plan bottomPlan;
            if (!bottomProjects.isEmpty()) {
                bottomPlan = new LogicalProject<>(ImmutableList.copyOf(bottomProjects),
                        aggregate.child());
            } else {
                bottomPlan = aggregate.child();
            }

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

            // normalize group by exprs by bottomProjects
            List<Expression> normalizedGroupExprs =
                    bottomSlotContext.normalizeToUseSlotRef(groupingByExprs);

            // normalize trival-aggs by bottomProjects
            List<AggregateFunction> normalizedAggFuncs =
                    bottomSlotContext.normalizeToUseSlotRef(aggFuncs);

            // build normalized agg output
            NormalizeToSlotContext normalizedAggFuncsToSlotContext =
                    NormalizeToSlotContext.buildContext(existsAlias, normalizedAggFuncs);

            // agg output include 2 parts
            // pushedGroupByExprs and normalized agg functions
            List<NamedExpression> normalizedAggOutput = ImmutableList.<NamedExpression>builder()
                    .addAll(pushedGroupByExprs.stream().map(NamedExpression::toSlot).iterator())
                    .addAll(normalizedAggFuncsToSlotContext
                            .pushDownToNamedExpression(normalizedAggFuncs))
                    .build();

            // create new agg node
            LogicalAggregate newAggregate =
                    aggregate.withNormalized(normalizedGroupExprs, normalizedAggOutput, bottomPlan);

            // create upper projects by normalize all output exprs in old LogicalAggregate
            List<NamedExpression> upperProjects = normalizeOutput(aggregateOutput,
                    bottomSlotContext, normalizedAggFuncsToSlotContext);

            // create a parent project node
            return new LogicalProject<>(upperProjects, newAggregate);
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
