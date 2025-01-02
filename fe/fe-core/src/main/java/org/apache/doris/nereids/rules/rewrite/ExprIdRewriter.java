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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** replace SlotReference ExprId in logical plans */
public class ExprIdRewriter extends ExpressionRewrite {
    private final List<Rule> rules;
    private final JobContext jobContext;

    public ExprIdRewriter(ReplaceRule replaceRule, JobContext jobContext) {
        super(new ExpressionRuleExecutor(ImmutableList.of(bottomUp(replaceRule))));
        rules = buildRules();
        this.jobContext = jobContext;
    }

    @Override
    public List<Rule> buildRules() {
        ImmutableList.Builder<Rule> builder = ImmutableList.builder();
        builder.addAll(super.buildRules());
        builder.addAll(ImmutableList.of(
                new LogicalPartitionTopNExpressionRewrite().build(),
                new LogicalTopNExpressionRewrite().build(),
                new LogicalSetOperationRewrite().build(),
                new LogicalWindowRewrite().build(),
                new LogicalResultSinkRewrite().build(),
                new LogicalFileSinkRewrite().build(),
                new LogicalHiveTableSinkRewrite().build(),
                new LogicalIcebergTableSinkRewrite().build(),
                new LogicalJdbcTableSinkRewrite().build(),
                new LogicalOlapTableSinkRewrite().build(),
                new LogicalDeferMaterializeResultSinkRewrite().build()
                ));
        return builder.build();
    }

    /**rewriteExpr*/
    public Plan rewriteExpr(Plan plan, Map<ExprId, ExprId> replaceMap) {
        if (replaceMap.isEmpty()) {
            return plan;
        }
        for (Rule rule : rules) {
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, jobContext.getCascadesContext());
                Plan newPlan = newPlans.get(0);
                if (!newPlan.deepEquals(plan)) {
                    return newPlan;
                }
            }
        }
        return plan;
    }

    /**
     * Iteratively rewrites IDs using the replaceMap:
     * 1. For a given SlotReference with initial ID, retrieve the corresponding value ID from the replaceMap.
     * 2. If the value ID exists within the replaceMap, continue the lookup process using the value ID
     * until it no longer appears in the replaceMap.
     * 3. return SlotReference final value ID as the result of the rewrite.
     * e.g. replaceMap:{0:3, 1:6, 6:7}
     * SlotReference:a#0 -> a#3, a#1 -> a#7
     * */
    public static class ReplaceRule implements ExpressionPatternRuleFactory {
        private final Map<ExprId, ExprId> replaceMap;

        public ReplaceRule(Map<ExprId, ExprId> replaceMap) {
            this.replaceMap = replaceMap;
        }

        @Override
        public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
            return ImmutableList.of(
                    matchesType(SlotReference.class).thenApply(ctx -> {
                        Slot slot = ctx.expr;
                        if (replaceMap.containsKey(slot.getExprId())) {
                            ExprId newId = replaceMap.get(slot.getExprId());
                            while (replaceMap.containsKey(newId)) {
                                newId = replaceMap.get(newId);
                            }
                            return slot.withExprId(newId);
                        }
                        return slot;
                    })
            );
        }
    }

    private class LogicalResultSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalResultSink().thenApply(ExprIdRewriter.this::applyRewrite)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalFileSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalFileSink().thenApply(ExprIdRewriter.this::applyRewrite)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalHiveTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalHiveTableSink().thenApply(ExprIdRewriter.this::applyRewrite)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalIcebergTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalIcebergTableSink().thenApply(ExprIdRewriter.this::applyRewrite)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalJdbcTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalJdbcTableSink().thenApply(ExprIdRewriter.this::applyRewrite)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalOlapTableSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalOlapTableSink().thenApply(ExprIdRewriter.this::applyRewrite)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
        }
    }

    private class LogicalDeferMaterializeResultSinkRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalDeferMaterializeResultSink().thenApply(ExprIdRewriter.this::applyRewrite)
                    .toRule(RuleType.REWRITE_SINK_EXPRESSION);
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
                    List<SlotReference> newSlots = rewriteAll(slots, rewriter, context);
                    newSlotsList.add(newSlots);
                }
                if (newSlotsList.equals(slotsList)) {
                    return setOperation;
                }
                return setOperation.withChildrenAndTheirOutputs(setOperation.children(), newSlotsList);
            })
            .toRule(RuleType.REWRITE_SET_OPERATION_EXPRESSION);
        }
    }

    private class LogicalWindowRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalWindow().thenApply(ctx -> {
                LogicalWindow<Plan> window = ctx.root;
                List<NamedExpression> windowExpressions = window.getWindowExpressions();
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                List<NamedExpression> newWindowExpressions = rewriteAll(windowExpressions, rewriter, context);
                if (newWindowExpressions.equals(windowExpressions)) {
                    return window;
                }
                return window.withExpressionsAndChild(newWindowExpressions, window.child());
            })
            .toRule(RuleType.REWRITE_WINDOW_EXPRESSION);
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
                boolean changed = false;
                for (OrderExpression orderExpression : partitionTopN.getOrderKeys()) {
                    OrderKey orderKey = orderExpression.getOrderKey();
                    Expression expr = rewriter.rewrite(orderKey.getExpr(), context);
                    changed |= expr != orderKey.getExpr();
                    OrderKey newOrderKey = new OrderKey(expr, orderKey.isAsc(), orderKey.isNullFirst());
                    newOrderExpressions.add(new OrderExpression(newOrderKey));
                }
                List<Expression> newPartitionKeys = rewriteAll(partitionTopN.getPartitionKeys(), rewriter, context);
                if (!newPartitionKeys.equals(partitionTopN.getPartitionKeys())) {
                    changed = true;
                }
                if (!changed) {
                    return partitionTopN;
                }
                return partitionTopN.withPartitionKeysAndOrderKeys(newPartitionKeys, newOrderExpressions);
            }).toRule(RuleType.REWRITE_PARTITION_TOPN_EXPRESSION);
        }
    }

    private LogicalSink<Plan> applyRewrite(MatchingContext<? extends LogicalSink<Plan>> ctx) {
        LogicalSink<Plan> sink = ctx.root;
        ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
        List<NamedExpression> outputExprs = sink.getOutputExprs();
        List<NamedExpression> newOutputExprs = rewriteAll(outputExprs, rewriter, context);
        if (outputExprs.equals(newOutputExprs)) {
            return sink;
        }
        return sink.withOutputExprs(newOutputExprs);
    }
}
