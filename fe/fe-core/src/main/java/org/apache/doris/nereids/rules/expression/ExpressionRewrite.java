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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * expression of plan rewrite rule.
 */
public class ExpressionRewrite implements RewriteRuleFactory {
    private final ExpressionRuleExecutor rewriter;

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
                new HavingExpressionRewrite().build());
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
                List<NamedExpression> newProjects = projects.stream()
                        .map(expr -> (NamedExpression) rewriter.rewrite(expr, context))
                        .collect(ImmutableList.toImmutableList());
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
                List<NamedExpression> newOutputExpressions = outputExpressions.stream()
                        .map(expr -> (NamedExpression) rewriter.rewrite(expr, context))
                        .collect(ImmutableList.toImmutableList());
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
                if (otherJoinConjuncts.isEmpty() && hashJoinConjuncts.isEmpty()) {
                    return join;
                }
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                List<Expression> rewriteHashJoinConjuncts = Lists.newArrayList();
                boolean hashJoinConjunctsChanged = false;
                for (Expression expr : hashJoinConjuncts) {
                    Expression newExpr = rewriter.rewrite(expr, context);
                    hashJoinConjunctsChanged = hashJoinConjunctsChanged || !newExpr.equals(expr);
                    rewriteHashJoinConjuncts.addAll(ExpressionUtils.extractConjunction(newExpr));
                }

                List<Expression> rewriteOtherJoinConjuncts = Lists.newArrayList();
                boolean otherJoinConjunctsChanged = false;
                for (Expression expr : otherJoinConjuncts) {
                    Expression newExpr = rewriter.rewrite(expr, context);
                    otherJoinConjunctsChanged = otherJoinConjunctsChanged || !newExpr.equals(expr);
                    rewriteOtherJoinConjuncts.addAll(ExpressionUtils.extractConjunction(newExpr));
                }

                if (!hashJoinConjunctsChanged && !otherJoinConjunctsChanged) {
                    return join;
                }
                return new LogicalJoin<>(join.getJoinType(), rewriteHashJoinConjuncts,
                        rewriteOtherJoinConjuncts, join.getHint(), join.getMarkJoinSlotReference(),
                        join.children());
            }).toRule(RuleType.REWRITE_JOIN_EXPRESSION);
        }
    }

    private class SortExpressionRewrite extends OneRewriteRuleFactory {

        @Override
        public Rule build() {
            return logicalSort().thenApply(ctx -> {
                LogicalSort<Plan> sort = ctx.root;
                List<OrderKey> orderKeys = sort.getOrderKeys();
                List<OrderKey> rewrittenOrderKeys = new ArrayList<>();
                ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                for (OrderKey k : orderKeys) {
                    Expression expression = rewriter.rewrite(k.getExpr(), context);
                    rewrittenOrderKeys.add(new OrderKey(expression, k.isAsc(), k.isNullFirst()));
                }
                return sort.withOrderKeys(rewrittenOrderKeys);
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

    private class LogicalRepeatRewrite extends OneRewriteRuleFactory {
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
}
