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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**ExprIdReplacer*/
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
    public Plan rewriteExpr(Plan plan) {
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

    /**ReplaceRule*/
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
                                newId = replaceMap.get(slot.getExprId());
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
