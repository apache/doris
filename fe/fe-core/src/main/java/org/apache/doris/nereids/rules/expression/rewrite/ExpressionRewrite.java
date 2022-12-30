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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

    public Expression rewrite(Expression expression) {
        return rewriter.rewrite(expression);
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
            return logicalGenerate().then(generate -> {
                List<Function> generators = generate.getGenerators();
                List<Function> newGenerators = generators.stream()
                        .map(func -> (Function) rewriter.rewrite(func))
                        .collect(Collectors.toList());
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
            return logicalOneRowRelation().then(oneRowRelation -> {
                List<NamedExpression> projects = oneRowRelation.getProjects();
                List<NamedExpression> newProjects = projects
                        .stream()
                        .map(expr -> (NamedExpression) rewriter.rewrite(expr))
                        .collect(Collectors.toList());
                if (projects.equals(newProjects)) {
                    return oneRowRelation;
                }
                return new LogicalOneRowRelation(newProjects);
            }).toRule(RuleType.REWRITE_ONE_ROW_RELATION_EXPRESSION);
        }
    }

    private class ProjectExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalProject().then(project -> {
                List<NamedExpression> projects = project.getProjects();
                List<NamedExpression> newProjects = projects.stream()
                        .map(expr -> (NamedExpression) rewriter.rewrite(expr)).collect(Collectors.toList());
                if (projects.containsAll(newProjects)) {
                    return project;
                }
                return new LogicalProject<>(newProjects, project.child());
            }).toRule(RuleType.REWRITE_PROJECT_EXPRESSION);
        }
    }

    private class FilterExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalFilter().then(filter -> {
                Set<Expression> newConjuncts = ImmutableSet.copyOf(ExpressionUtils.extractConjunction(
                        rewriter.rewrite(filter.getPredicate())));
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
            return logicalAggregate().then(agg -> {
                List<Expression> groupByExprs = agg.getGroupByExpressions();
                List<Expression> newGroupByExprs = rewriter.rewrite(groupByExprs);

                List<NamedExpression> outputExpressions = agg.getOutputExpressions();
                List<NamedExpression> newOutputExpressions = outputExpressions.stream()
                        .map(expr -> (NamedExpression) rewriter.rewrite(expr)).collect(Collectors.toList());
                if (outputExpressions.containsAll(newOutputExpressions) && groupByExprs.containsAll(newGroupByExprs)) {
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
            return logicalJoin().then(join -> {
                List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts();
                List<Expression> otherJoinConjuncts = join.getOtherJoinConjuncts();
                if (otherJoinConjuncts.isEmpty() && hashJoinConjuncts.isEmpty()) {
                    return join;
                }
                List<Expression> rewriteHashJoinConjuncts = Lists.newArrayList();
                boolean hashJoinConjunctsChanged = false;
                for (Expression expr : hashJoinConjuncts) {
                    Expression newExpr = rewriter.rewrite(expr);
                    hashJoinConjunctsChanged = hashJoinConjunctsChanged || !newExpr.equals(expr);
                    rewriteHashJoinConjuncts.add(newExpr);
                }

                List<Expression> rewriteOtherJoinConjuncts = Lists.newArrayList();
                boolean otherJoinConjunctsChanged = false;
                for (Expression expr : otherJoinConjuncts) {
                    Expression newExpr = rewriter.rewrite(expr);
                    otherJoinConjunctsChanged = otherJoinConjunctsChanged || !newExpr.equals(expr);
                    rewriteOtherJoinConjuncts.add(newExpr);
                }

                if (!hashJoinConjunctsChanged && !otherJoinConjunctsChanged) {
                    return join;
                }
                return new LogicalJoin<>(join.getJoinType(), rewriteHashJoinConjuncts,
                        rewriteOtherJoinConjuncts, join.getHint(), join.left(), join.right());
            }).toRule(RuleType.REWRITE_JOIN_EXPRESSION);
        }
    }

    private class SortExpressionRewrite extends OneRewriteRuleFactory {

        @Override
        public Rule build() {
            return logicalSort().then(sort -> {
                List<OrderKey> orderKeys = sort.getOrderKeys();
                List<OrderKey> rewrittenOrderKeys = new ArrayList<>();
                for (OrderKey k : orderKeys) {
                    Expression expression = rewriter.rewrite(k.getExpr());
                    rewrittenOrderKeys.add(new OrderKey(expression, k.isAsc(), k.isNullFirst()));
                }
                return sort.withOrderByKey(rewrittenOrderKeys);
            }).toRule(RuleType.REWRITE_SORT_EXPRESSION);
        }
    }

    private class HavingExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalHaving().then(having -> {
                Set<Expression> rewrittenExpr = new HashSet<>();
                for (Expression e : having.getExpressions()) {
                    rewrittenExpr.add(rewriter.rewrite(e));
                }
                return having.withExpressions(rewrittenExpr);
            }).toRule(RuleType.REWRITE_HAVING_EXPRESSSION);
        }
    }

    private class LogicalRepeatRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalRepeat().then(r -> {
                List<List<Expression>> groupingExprs = new ArrayList<>();
                for (List<Expression> expressions : r.getGroupingSets()) {
                    groupingExprs.add(expressions.stream().map(rewriter::rewrite).collect(Collectors.toList()));
                }
                return r.withGroupSetsAndOutput(groupingExprs,
                        r.getOutputExpressions().stream().map(rewriter::rewrite).map(e -> (NamedExpression) e)
                                .collect(Collectors.toList()));
            }).toRule(RuleType.REWRITE_REPEAT_EXPRESSSION);
        }
    }
}
