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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.pattern.ExpressionPatternRules;
import org.apache.doris.nereids.pattern.ExpressionPatternTraverseListeners;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * expression of plan rewrite rule.
 */
public class ExpressionRewrite implements RewriteRuleFactory {
    protected final ExpressionRuleExecutor rewriter;

    public ExpressionRewrite(ExpressionRewriteRule... rules) {
        this.rewriter = new ExpressionRuleExecutor(ImmutableList.copyOf(rules));
    }

    public ExpressionRewrite(ExpressionRuleExecutor rewriter) {
        this.rewriter = Objects.requireNonNull(rewriter, "rewriter is null");
    }

    public Expression rewrite(Expression expression, ExpressionRewriteContext expressionRewriteContext) {
        return rewriter.rewrite(expression, expressionRewriteContext);
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                new GenerateExpressionRewrite().build(),
                new OneRowRelationExpressionRewrite().build(),
                new ProjectExpressionRewrite().build(),
                new AggExpressionRewrite().build(),
                new FilterExpressionRewrite().build(),
                new JoinExpressionRewrite().build(),
                new SortExpressionRewrite().build(),
                new LogicalRepeatRewrite().build(),
                new HavingExpressionRewrite().build(),
                new LogicalPartitionTopNExpressionRewrite().build(),
                new LogicalTopNExpressionRewrite().build(),
                new LogicalSetOperationRewrite().build(),
                new LogicalWindowRewrite().build(),
                new LogicalCteConsumerRewrite().build(),
                new LogicalResultSinkRewrite().build(),
                new LogicalFileSinkRewrite().build(),
                new LogicalHiveTableSinkRewrite().build(),
                new LogicalIcebergTableSinkRewrite().build(),
                new LogicalJdbcTableSinkRewrite().build(),
                new LogicalOlapTableSinkRewrite().build(),
                new LogicalDeferMaterializeResultSinkRewrite().build());
    }

    private class GenerateExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalGenerate().thenApply(ctx -> {
                LogicalGenerate<Plan> generate = ctx.root;
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                List<Function> generators = generate.getGenerators();
                List<Function> newGenerators = generators.stream()
                        .map(func -> (Function) rewriter.rewrite(func, context))
                        .collect(ImmutableList.toImmutableList());
                if (generators.equals(newGenerators)) {
                    return generate;
                }
                return generate.withGenerators(newGenerators);
            }).toRule(RuleType.REWRITE_GENERATE_EXPRESSION);
        }
    }

    private class OneRowRelationExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalOneRowRelation().thenApply(ctx -> {
                LogicalOneRowRelation oneRowRelation = ctx.root;
                List<NamedExpression> projects = oneRowRelation.getProjects();
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);

                List<NamedExpression> newProjects = projects
                        .stream()
                        .map(expr -> (NamedExpression) rewriter.rewrite(expr, context))
                        .collect(ImmutableList.toImmutableList());
                if (projects.equals(newProjects)) {
                    return oneRowRelation;
                }
                return new LogicalOneRowRelation(oneRowRelation.getRelationId(), newProjects);
            }).toRule(RuleType.REWRITE_ONE_ROW_RELATION_EXPRESSION);
        }
    }

    private class ProjectExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalProject().thenApply(ctx -> {
                LogicalProject<Plan> project = ctx.root;
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                List<NamedExpression> projects = project.getProjects();
                List<NamedExpression> newProjects = rewriteAll(projects, rewriter, context);
                if (projects.equals(newProjects)) {
                    return project;
                }
                return project.withProjectsAndChild(newProjects, project.child());
            }).toRule(RuleType.REWRITE_PROJECT_EXPRESSION);
        }
    }

    private class FilterExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalFilter().thenApply(ctx -> {
                LogicalFilter<Plan> filter = ctx.root;
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                Set<Expression> newConjuncts = ImmutableSet.copyOf(ExpressionUtils.extractConjunction(
                        rewriter.rewrite(filter.getPredicate(), context)));
                if (newConjuncts.equals(filter.getConjuncts())) {
                    return filter;
                }
                return new LogicalFilter<>(newConjuncts, filter.child());
            }).toRule(RuleType.REWRITE_FILTER_EXPRESSION);
        }
    }

    private class AggExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalAggregate().thenApply(ctx -> {
                LogicalAggregate<Plan> agg = ctx.root;
                List<Expression> groupByExprs = agg.getGroupByExpressions();
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                List<Expression> newGroupByExprs = rewriter.rewrite(groupByExprs, context);

                List<NamedExpression> outputExpressions = agg.getOutputExpressions();
                List<NamedExpression> newOutputExpressions = rewriteAll(outputExpressions, rewriter, context);
                if (outputExpressions.equals(newOutputExpressions)) {
                    return agg;
                }
                return new LogicalAggregate<>(newGroupByExprs, newOutputExpressions,
                        agg.isNormalized(), agg.getSourceRepeat(), agg.child());
            }).toRule(RuleType.REWRITE_AGG_EXPRESSION);
        }
    }

    private class JoinExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalJoin().thenApply(ctx -> {
                LogicalJoin<Plan, Plan> join = ctx.root;
                List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts();
                List<Expression> otherJoinConjuncts = join.getOtherJoinConjuncts();
                List<Expression> markJoinConjuncts = join.getMarkJoinConjuncts();
                if (otherJoinConjuncts.isEmpty() && hashJoinConjuncts.isEmpty()
                        && markJoinConjuncts.isEmpty()) {
                    return join;
                }

                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                Pair<Boolean, List<Expression>> newHashJoinConjuncts = rewriteConjuncts(hashJoinConjuncts, context);
                Pair<Boolean, List<Expression>> newOtherJoinConjuncts = rewriteConjuncts(otherJoinConjuncts, context);
                Pair<Boolean, List<Expression>> newMarkJoinConjuncts = rewriteConjuncts(markJoinConjuncts, context);

                if (!newHashJoinConjuncts.first && !newOtherJoinConjuncts.first
                        && !newMarkJoinConjuncts.first) {
                    return join;
                }

                return new LogicalJoin<>(join.getJoinType(), newHashJoinConjuncts.second,
                        newOtherJoinConjuncts.second, newMarkJoinConjuncts.second,
                        join.getDistributeHint(), join.getMarkJoinSlotReference(), join.children(),
                        join.getJoinReorderContext());
            }).toRule(RuleType.REWRITE_JOIN_EXPRESSION);
        }

        private Pair<Boolean, List<Expression>> rewriteConjuncts(List<Expression> conjuncts,
                ExpressionRewriteContext context) {
            boolean isChanged = false;
            // some rules will append new conjunct, we need to distinct it
            // for example:
            //   pk = 2 or pk < 0
            // after AddMinMax rule:
            //   (pk = 2 or pk < 0) and pk <= 2
            //
            // if not distinct it, the pk <= 2 will generate repeat forever
            ImmutableSet.Builder<Expression> rewrittenConjuncts = new ImmutableSet.Builder<>();
            for (Expression expr : conjuncts) {
                Expression newExpr = rewriter.rewrite(expr, context);
                newExpr = newExpr.isNullLiteral() && expr instanceof EqualPredicate
                                ? expr.withChildren(rewriter.rewrite(expr.child(0), context),
                                        rewriter.rewrite(expr.child(1), context))
                                : newExpr;
                isChanged = isChanged || !newExpr.equals(expr);
                rewrittenConjuncts.addAll(ExpressionUtils.extractConjunction(newExpr));
            }
            ImmutableList<Expression> newConjuncts = Utils.fastToImmutableList(rewrittenConjuncts.build());
            return Pair.of(isChanged && !newConjuncts.equals(conjuncts), newConjuncts);
        }
    }

    private class SortExpressionRewrite extends OneRewriteRuleFactory {

        @Override
        public Rule build() {
            return logicalSort().thenApply(ctx -> {
                LogicalSort<Plan> sort = ctx.root;
                List<OrderKey> orderKeys = sort.getOrderKeys();
                ImmutableList.Builder<OrderKey> rewrittenOrderKeys
                        = ImmutableList.builderWithExpectedSize(orderKeys.size());
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                boolean changed = false;
                for (OrderKey k : orderKeys) {
                    Expression expression = rewriter.rewrite(k.getExpr(), context);
                    changed |= expression != k.getExpr();
                    rewrittenOrderKeys.add(new OrderKey(expression, k.isAsc(), k.isNullFirst()));
                }
                return changed ? sort.withOrderKeys(rewrittenOrderKeys.build()) : sort;
            }).toRule(RuleType.REWRITE_SORT_EXPRESSION);
        }
    }

    private class HavingExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalHaving().thenApply(ctx -> {
                LogicalHaving<Plan> having = ctx.root;
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                Set<Expression> newConjuncts = ImmutableSet.copyOf(ExpressionUtils.extractConjunction(
                        rewriter.rewrite(having.getPredicate(), context)));
                if (newConjuncts.equals(having.getConjuncts())) {
                    return having;
                }
                return having.withExpressions(newConjuncts);
            }).toRule(RuleType.REWRITE_HAVING_EXPRESSION);
        }
    }

    private class LogicalWindowRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalWindow().thenApply(ctx -> {
                LogicalWindow<Plan> window = ctx.root;
                List<NamedExpression> windowExpressions = window.getWindowExpressions();
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                List<NamedExpression> result = rewriteAll(windowExpressions, rewriter, context);
                return window.withExpressionsAndChild(result, window.child());
            })
            .toRule(RuleType.REWRITE_WINDOW_EXPRESSION);
        }
    }

    private class LogicalSetOperationRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalSetOperation().thenApply(ctx -> {
                LogicalSetOperation setOperation = ctx.root;
                List<List<SlotReference>> slotsList = setOperation.getRegularChildrenOutputs();
                List<List<SlotReference>> newSlotsList = new ArrayList<>();
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                for (List<SlotReference> slots : slotsList) {
                    List<SlotReference> result = rewriteAll(slots, rewriter, context);
                    newSlotsList.add(result);
                }
                return setOperation.withChildrenAndTheirOutputs(setOperation.children(), newSlotsList);
            })
            .toRule(RuleType.REWRITE_SET_OPERATION_EXPRESSION);
        }
    }

    private class LogicalTopNExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalTopN().thenApply(ctx -> {
                LogicalTopN<Plan> topN = ctx.root;
                List<OrderKey> orderKeys = topN.getOrderKeys();
                ImmutableList.Builder<OrderKey> rewrittenOrderKeys
                        = ImmutableList.builderWithExpectedSize(orderKeys.size());
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                boolean changed = false;
                for (OrderKey k : orderKeys) {
                    Expression expression = rewriter.rewrite(k.getExpr(), context);
                    changed |= expression != k.getExpr();
                    rewrittenOrderKeys.add(new OrderKey(expression, k.isAsc(), k.isNullFirst()));
                }
                return changed ? topN.withOrderKeys(rewrittenOrderKeys.build()) : topN;
            }).toRule(RuleType.REWRITE_TOPN_EXPRESSION);
        }
    }

    private class LogicalPartitionTopNExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalPartitionTopN().thenApply(ctx -> {
                LogicalPartitionTopN<Plan> partitionTopN = ctx.root;
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                List<OrderExpression> newOrderExpressions = new ArrayList<>();
                for (OrderExpression orderExpression : partitionTopN.getOrderKeys()) {
                    OrderKey orderKey = orderExpression.getOrderKey();
                    Expression expr = rewriter.rewrite(orderKey.getExpr(), context);
                    OrderKey newOrderKey = new OrderKey(expr, orderKey.isAsc(), orderKey.isNullFirst());
                    newOrderExpressions.add(new OrderExpression(newOrderKey));
                }
                List<Expression> result = rewriteAll(partitionTopN.getPartitionKeys(), rewriter, context);
                return partitionTopN.withPartitionKeysAndOrderKeys(result, newOrderExpressions);
            }).toRule(RuleType.REWRITE_PARTITION_TOPN_EXPRESSION);
        }
    }

    private class LogicalCteConsumerRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalCTEConsumer().thenApply(ctx -> {
                LogicalCTEConsumer consumer = ctx.root;
                boolean changed = false;
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                ImmutableMap.Builder<Slot, Slot> cToPBuilder = ImmutableMap.builder();
                ImmutableMultimap.Builder<Slot, Slot> pToCBuilder = ImmutableMultimap.builder();
                for (Map.Entry<Slot, Slot> entry : consumer.getConsumerToProducerOutputMap().entrySet()) {
                    Slot key = (Slot) rewriter.rewrite(entry.getKey(), context);
                    Slot value = (Slot) rewriter.rewrite(entry.getValue(), context);
                    cToPBuilder.put(key, value);
                    pToCBuilder.put(value, key);
                    if (!key.equals(entry.getKey()) || !value.equals(entry.getValue())) {
                        changed = true;
                    }
                }
                return changed ? consumer.withTwoMaps(cToPBuilder.build(), pToCBuilder.build()) : consumer;
            }).toRule(RuleType.REWRITE_TOPN_EXPRESSION);
        }
    }

    private class LogicalResultSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalResultSink().thenApply(ExpressionRewrite.this::applyRewriteToSink)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalFileSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalFileSink().thenApply(ExpressionRewrite.this::applyRewriteToSink)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalHiveTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalHiveTableSink().thenApply(ExpressionRewrite.this::applyRewriteToSink)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalIcebergTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalIcebergTableSink().thenApply(ExpressionRewrite.this::applyRewriteToSink)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalJdbcTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalJdbcTableSink().thenApply(ExpressionRewrite.this::applyRewriteToSink)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalOlapTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalOlapTableSink().thenApply(ExpressionRewrite.this::applyRewriteToSink)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalDeferMaterializeResultSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalDeferMaterializeResultSink().thenApply(ExpressionRewrite.this::applyRewriteToSink)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private LogicalSink<Plan> applyRewriteToSink(MatchingContext<? extends LogicalSink<Plan>> ctx) {
        LogicalSink<Plan> sink = ctx.root;
        ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
        List<NamedExpression> outputExprs = sink.getOutputExprs();
        List<NamedExpression> result = rewriteAll(outputExprs, rewriter, context);
        return sink.withOutputExprs(result);
    }

    /** LogicalRepeatRewrite */
    public class LogicalRepeatRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalRepeat().thenApply(ctx -> {
                LogicalRepeat<Plan> repeat = ctx.root;
                ImmutableList.Builder<List<Expression>> groupingExprs = ImmutableList.builder();
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                for (List<Expression> expressions : repeat.getGroupingSets()) {
                    groupingExprs.add(expressions.stream()
                            .map(expr -> rewriter.rewrite(expr, context))
                            .collect(ImmutableList.toImmutableList())
                    );
                }
                return repeat.withGroupSetsAndOutput(groupingExprs.build(),
                        repeat.getOutputExpressions().stream()
                                .map(output -> rewriter.rewrite(output, context))
                                .map(e -> (NamedExpression) e)
                                .collect(ImmutableList.toImmutableList()));
            }).toRule(RuleType.REWRITE_REPEAT_EXPRESSION);
        }
    }

    /** bottomUp */
    public static ExpressionBottomUpRewriter bottomUp(ExpressionPatternRuleFactory... ruleFactories) {
        ImmutableList.Builder<ExpressionPatternMatchRule> rules = ImmutableList.builder();
        ImmutableList.Builder<ExpressionTraverseListenerMapping> listeners = ImmutableList.builder();
        for (ExpressionPatternRuleFactory ruleFactory : ruleFactories) {
            if (ruleFactory instanceof ExpressionTraverseListenerFactory) {
                List<ExpressionListenerMatcher<? extends Expression>> listenersMatcher
                        = ((ExpressionTraverseListenerFactory) ruleFactory).buildListeners();
                for (ExpressionListenerMatcher<? extends Expression> listenerMatcher : listenersMatcher) {
                    listeners.add(new ExpressionTraverseListenerMapping(listenerMatcher));
                }
            }
            for (ExpressionPatternMatcher<? extends Expression> patternMatcher : ruleFactory.buildRules()) {
                rules.add(new ExpressionPatternMatchRule(patternMatcher));
            }
        }

        return new ExpressionBottomUpRewriter(
                new ExpressionPatternRules(rules.build()),
                new ExpressionPatternTraverseListeners(listeners.build())
        );
    }

    public static <E extends Expression> List<E> rewriteAll(
            Collection<E> exprs, ExpressionRuleExecutor rewriter, ExpressionRewriteContext context) {
        ImmutableList.Builder<E> result = ImmutableList.builderWithExpectedSize(exprs.size());
        for (E expr : exprs) {
            result.add((E) rewriter.rewrite(expr, context));
        }
        return result.build();
    }
}
