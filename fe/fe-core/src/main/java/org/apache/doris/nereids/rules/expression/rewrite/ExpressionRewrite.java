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
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * expression of plan rewrite rule.
 */
public class ExpressionRewrite implements RewriteRuleFactory {
    private final ExpressionRuleExecutor rewriter;

    public ExpressionRewrite(ExpressionRuleExecutor rewriter) {
        this.rewriter = Objects.requireNonNull(rewriter, "rewriter is null");
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                new ProjectExpressionRewrite().build(),
                new AggExpressionRewrite().build(),
                new FilterExpressionRewrite().build(),
                new JoinExpressionRewrite().build());
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
                        agg.isDisassembled(), agg.isNormalized(), agg.getAggPhase(), agg.child());
            }).toRule(RuleType.REWRITE_AGG_EXPRESSION);
        }
    }

    private class JoinExpressionRewrite extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return logicalJoin().then(join -> {
                Optional<Expression> condition = join.getCondition();
                if (!condition.isPresent()) {
                    return join;
                }
                Expression newCondition = rewriter.rewrite(condition.get());
                if (newCondition.equals(condition.get())) {
                    return join;
                }
                return new LogicalJoin<>(join.getJoinType(), Optional.of(newCondition), join.left(), join.right());
            }).toRule(RuleType.REWRITE_JOIN_EXPRESSION);
        }
    }
}
