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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
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

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                new OneRowRelationExpressionRewrite().build(),
                new ProjectExpressionRewrite().build(),
                new AggExpressionRewrite().build(),
                new FilterExpressionRewrite().build(),
                new JoinExpressionRewrite().build());
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
                // TODO:
                // trick logic: currently XxxRelation in GroupExpression always difference to each other,
                // so this rule must check the expression whether is changed to prevent dead loop because
                // new LogicalOneRowRelation can hit this rule too. we would remove code until the pr
                // (@wangshuo128) mark the id in XxxRelation, then we can compare XxxRelation in
                // GroupExpression by id
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
                Expression newExpr = rewriter.rewrite(filter.getPredicates());
                if (newExpr.equals(filter.getPredicates())) {
                    return filter;
                }
                return new LogicalFilter<>(newExpr, filter.child());
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
                if (outputExpressions.containsAll(newOutputExpressions)) {
                    return agg;
                }
                return new LogicalAggregate<>(newGroupByExprs, newOutputExpressions,
                        agg.isDisassembled(), agg.isNormalized(), agg.isFinalPhase(), agg.getAggPhase(), agg.child());
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
                        rewriteOtherJoinConjuncts, join.left(), join.right());
            }).toRule(RuleType.REWRITE_JOIN_EXPRESSION);
        }
    }
}
