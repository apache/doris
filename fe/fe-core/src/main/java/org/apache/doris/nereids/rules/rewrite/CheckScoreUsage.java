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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

/**
 * Tie every score() call to the MATCH and SEARCH predicates it scores.
 *
 * <p>score() is the relevance of a row for the MATCH and SEARCH predicates in the WHERE clause of
 * its own SELECT. This rewriter runs first in rewrite, while subqueries, views and CTEs still have
 * their own plan nodes. It gives every SELECT that calls score() a new score id and marks with it
 * the score() calls of the SELECT and the MATCH and SEARCH predicates of its filters.
 * BindScoreToOlapScan later binds the calls to the scan that those predicates filter.
 *
 * <p>A MATCH or SEARCH without a score id only filters rows: it adds nothing to the score of its
 * scan, even when a filter of an outer SELECT is pushed down onto that scan.
 */
public class CheckScoreUsage implements CustomRewriter {
    public static final String NO_SCORING_PREDICATE_ERROR =
            "score() requires a MATCH or SEARCH predicate in the WHERE clause of the same SELECT";
    public static final String UNSUPPORTED_CLAUSE_ERROR = "score() can only be used in the SELECT list "
            + "and in the WHERE, GROUP BY, HAVING, QUALIFY and ORDER BY clauses";

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (!containsScore(plan)) {
            return plan;
        }
        return tagSelect(plan);
    }

    /** Whether any plan node of the tree calls score() in its own expressions. */
    public static boolean containsScore(Plan plan) {
        return plan.anyMatch(node -> ((Plan) node).getExpressions().stream()
                .anyMatch(expression -> expression.containsType(Score.class)));
    }

    // A SELECT consists of the plan nodes from its root down to the roots of its subqueries, views,
    // CTE producers and set operation inputs, which are SELECTs of their own.
    private static boolean startsSelect(Plan parent, int childIndex) {
        return parent instanceof LogicalSubQueryAlias || parent instanceof LogicalView
                || parent instanceof LogicalCTEProducer || parent instanceof LogicalSetOperation
                || parent instanceof LogicalRecursiveUnionAnchor || parent instanceof LogicalRecursiveUnionProducer
                || parent instanceof LogicalApply && childIndex == 1;
    }

    private Plan tagSelect(Plan root) {
        SelectSummary select = new SelectSummary();
        summarize(root, select);
        if (!select.callsScore) {
            return tag(root, Optional.empty());
        }
        if (!select.hasScoringPredicate) {
            throw new AnalysisException(NO_SCORING_PREDICATE_ERROR);
        }
        if (select.unsupportedClause) {
            throw new AnalysisException(UNSUPPORTED_CLAUSE_ERROR);
        }
        return tag(root, Optional.of(StatementScopeIdGenerator.newExprId()));
    }

    private void summarize(Plan plan, SelectSummary select) {
        if (plan.getExpressions().stream().anyMatch(expression -> expression.containsType(Score.class))) {
            select.callsScore = true;
            select.unsupportedClause |= !canCallScore(plan);
        }
        if (plan instanceof LogicalFilter && ((LogicalFilter<?>) plan).getConjuncts().stream()
                .anyMatch(conjunct -> conjunct.containsType(Match.class, Search.class))) {
            select.hasScoringPredicate = true;
        }
        for (int i = 0; i < plan.arity(); i++) {
            if (!startsSelect(plan, i)) {
                summarize(plan.child(i), select);
            }
        }
    }

    // HAVING and QUALIFY are filters by now. Join conditions are rejected by CheckAnalysis.
    private static boolean canCallScore(Plan plan) {
        return plan instanceof LogicalProject || plan instanceof LogicalFilter
                || plan instanceof LogicalAggregate || plan instanceof LogicalSort;
    }

    private Plan tag(Plan plan, Optional<ExprId> scoreId) {
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (int i = 0; i < plan.arity(); i++) {
            Plan child = plan.child(i);
            Plan newChild = startsSelect(plan, i) ? tagSelect(child) : tag(child, scoreId);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        Plan newPlan = changed ? plan.withChildren(newChildren.build()) : plan;
        return scoreId.isPresent() ? tagExpressions(newPlan, scoreId.get()) : newPlan;
    }

    private Plan tagExpressions(Plan plan, ExprId scoreId) {
        if (plan.getExpressions().stream()
                .noneMatch(expression -> expression.containsType(Score.class, Match.class, Search.class))) {
            return plan;
        }
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            return filter.withConjuncts(filter.getConjuncts().stream()
                    .map(conjunct -> tagScoreAndPredicates(conjunct, scoreId))
                    .collect(ImmutableSet.toImmutableSet()));
        }
        if (plan instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) plan;
            return project.withProjects(tagScore(project.getProjects(), scoreId));
        }
        if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> aggregate = (LogicalAggregate<?>) plan;
            List<Expression> groupBy = aggregate.getGroupByExpressions().stream()
                    .map(expression -> tagScore(expression, scoreId))
                    .collect(ImmutableList.toImmutableList());
            return aggregate.withGroupByAndOutput(groupBy, tagScore(aggregate.getOutputExpressions(), scoreId));
        }
        if (plan instanceof LogicalSort) {
            LogicalSort<?> sort = (LogicalSort<?>) plan;
            List<OrderKey> orderKeys = sort.getOrderKeys().stream()
                    .map(orderKey -> orderKey.withExpression(tagScore(orderKey.getExpr(), scoreId)))
                    .collect(ImmutableList.toImmutableList());
            return sort.withOrderKeys(orderKeys);
        }
        // A MATCH or SEARCH outside a filter, e.g. in a join condition, only filters rows.
        return plan;
    }

    private static List<NamedExpression> tagScore(List<NamedExpression> expressions, ExprId scoreId) {
        return expressions.stream()
                .map(expression -> (NamedExpression) tagScore(expression, scoreId))
                .collect(ImmutableList.toImmutableList());
    }

    private static Expression tagScore(Expression expression, ExprId scoreId) {
        return expression.rewriteDownShortCircuit(
                node -> node instanceof Score ? ((Score) node).withScoreId(scoreId) : node);
    }

    // The MATCH and SEARCH predicates of a filter define the score of the SELECT.
    private static Expression tagScoreAndPredicates(Expression conjunct, ExprId scoreId) {
        return conjunct.rewriteDownShortCircuit(node -> {
            if (node instanceof Score) {
                return ((Score) node).withScoreId(scoreId);
            } else if (node instanceof Match) {
                return ((Match) node).withScoreId(scoreId);
            } else if (node instanceof Search) {
                return ((Search) node).withScoreId(scoreId);
            }
            return node;
        });
    }

    private static class SelectSummary {
        private boolean callsScore;
        private boolean hasScoringPredicate;
        private boolean unsupportedClause;
    }
}
