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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Use the visitor to iterate sub expression.
 */
class SubExprAnalyzer<T> extends DefaultExpressionRewriter<T> {
    private final Scope scope;
    private final CascadesContext cascadesContext;

    public SubExprAnalyzer(Scope scope, CascadesContext cascadesContext) {
        this.scope = scope;
        this.cascadesContext = cascadesContext;
    }

    @Override
    public Expression visitNot(Not not, T context) {
        Expression child = not.child();
        if (child instanceof Exists) {
            return visitExistsSubquery(
                    new Exists(((Exists) child).getQueryPlan(), true), context);
        } else if (child instanceof InSubquery) {
            return visitInSubquery(new InSubquery(((InSubquery) child).getCompareExpr(),
                    ((InSubquery) child).getListQuery(), true), context);
        }
        return visit(not, context);
    }

    @Override
    public Expression visitExistsSubquery(Exists exists, T context) {
        LogicalPlan queryPlan = exists.getQueryPlan();
        // distinct is useless, remove it
        if (queryPlan instanceof LogicalProject && ((LogicalProject) queryPlan).isDistinct()) {
            exists = exists.withSubquery(((LogicalProject) queryPlan).withDistinct(false));
        }
        AnalyzedResult analyzedResult = analyzeSubquery(exists);
        if (analyzedResult.rootIsLimitZero()) {
            return BooleanLiteral.of(exists.isNot());
        }
        if (analyzedResult.isCorrelated() && analyzedResult.rootIsLimitWithOffset()) {
            throw new AnalysisException("Unsupported correlated subquery with a LIMIT clause with offset > 0 "
                    + analyzedResult.getLogicalPlan());
        }
        return new Exists(analyzedResult.getLogicalPlan(),
                analyzedResult.getCorrelatedSlots(), exists.isNot());
    }

    @Override
    public Expression visitInSubquery(InSubquery expr, T context) {
        LogicalPlan queryPlan = expr.getQueryPlan();
        // distinct is useless, remove it
        if (queryPlan instanceof LogicalProject && ((LogicalProject) queryPlan).isDistinct()) {
            expr = expr.withSubquery(((LogicalProject) queryPlan).withDistinct(false));
        }
        AnalyzedResult analyzedResult = analyzeSubquery(expr);

        checkOutputColumn(analyzedResult.getLogicalPlan());
        checkNoCorrelatedSlotsUnderAgg(analyzedResult);
        checkRootIsLimit(analyzedResult);

        return new InSubquery(
                expr.getCompareExpr().accept(this, context),
                new ListQuery(analyzedResult.getLogicalPlan()),
                analyzedResult.getCorrelatedSlots(), expr.isNot());
    }

    @Override
    public Expression visitScalarSubquery(ScalarSubquery scalar, T context) {
        AnalyzedResult analyzedResult = analyzeSubquery(scalar);

        checkOutputColumn(analyzedResult.getLogicalPlan());
        checkHasAgg(analyzedResult);
        checkHasNoGroupBy(analyzedResult);

        // if scalar subquery is like select '2024-02-02 00:00:00'
        // we can just return the constant expr '2024-02-02 00:00:00'
        if (analyzedResult.getLogicalPlan() instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) analyzedResult.getLogicalPlan();
            if (project.child() instanceof LogicalOneRowRelation
                    && project.getProjects().size() == 1
                    && project.getProjects().get(0) instanceof Alias) {
                Alias alias = (Alias) project.getProjects().get(0);
                if (alias.isConstant()) {
                    return alias.child();
                }
            }
        }

        return new ScalarSubquery(analyzedResult.getLogicalPlan(), analyzedResult.getCorrelatedSlots());
    }

    private void checkOutputColumn(LogicalPlan plan) {
        if (plan.getOutput().size() != 1) {
            throw new AnalysisException("Multiple columns returned by subquery are not yet supported. Found "
                    + plan.getOutput().size());
        }
    }

    private void checkHasAgg(AnalyzedResult analyzedResult) {
        if (!analyzedResult.isCorrelated()) {
            return;
        }
        if (!analyzedResult.hasAgg()) {
            throw new AnalysisException("The select item in correlated subquery of binary predicate "
                    + "should only be sum, min, max, avg and count. Current subquery: "
                    + analyzedResult.getLogicalPlan());
        }
    }

    private void checkHasNoGroupBy(AnalyzedResult analyzedResult) {
        if (!analyzedResult.isCorrelated()) {
            return;
        }
        if (analyzedResult.hasGroupBy()) {
            throw new AnalysisException("Unsupported correlated subquery with grouping and/or aggregation "
                    + analyzedResult.getLogicalPlan());
        }
    }

    private void checkNoCorrelatedSlotsUnderAgg(AnalyzedResult analyzedResult) {
        if (analyzedResult.hasCorrelatedSlotsUnderAgg()) {
            throw new AnalysisException(
                    "Unsupported correlated subquery with grouping and/or aggregation "
                            + analyzedResult.getLogicalPlan());
        }
    }

    private void checkRootIsLimit(AnalyzedResult analyzedResult) {
        if (!analyzedResult.isCorrelated()) {
            return;
        }
        if (analyzedResult.rootIsLimit()) {
            throw new AnalysisException("Unsupported correlated subquery with a LIMIT clause "
                    + analyzedResult.getLogicalPlan());
        }
    }

    private AnalyzedResult analyzeSubquery(SubqueryExpr expr) {
        if (cascadesContext == null) {
            throw new IllegalStateException("Missing CascadesContext");
        }
        CascadesContext subqueryContext = CascadesContext.newContextWithCteContext(
                cascadesContext, expr.getQueryPlan(), cascadesContext.getCteContext());
        Scope subqueryScope = genScopeWithSubquery(expr);
        subqueryContext.setOuterScope(subqueryScope);
        subqueryContext.newAnalyzer().analyze();
        return new AnalyzedResult((LogicalPlan) subqueryContext.getRewritePlan(),
                subqueryScope.getCorrelatedSlots());
    }

    private Scope genScopeWithSubquery(SubqueryExpr expr) {
        return new Scope(getScope().getOuterScope(),
                getScope().getSlots(),
                Optional.ofNullable(expr));
    }

    public Scope getScope() {
        return scope;
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    private static class AnalyzedResult {
        private final LogicalPlan logicalPlan;
        private final List<Slot> correlatedSlots;

        public AnalyzedResult(LogicalPlan logicalPlan, Collection<Slot> correlatedSlots) {
            this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan can not be null");
            this.correlatedSlots = correlatedSlots == null ? new ArrayList<>() : ImmutableList.copyOf(correlatedSlots);
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public List<Slot> getCorrelatedSlots() {
            return correlatedSlots;
        }

        public boolean isCorrelated() {
            return !correlatedSlots.isEmpty();
        }

        public boolean hasAgg() {
            return logicalPlan.anyMatch(LogicalAggregate.class::isInstance);
        }

        public boolean hasGroupBy() {
            if (hasAgg()) {
                return !((LogicalAggregate)
                        ((ImmutableSet) logicalPlan.collect(LogicalAggregate.class::isInstance)).asList().get(0))
                        .getGroupByExpressions().isEmpty();
            }
            return false;
        }

        public boolean hasCorrelatedSlotsUnderAgg() {
            return correlatedSlots.isEmpty() ? false
                    : findAggContainsCorrelatedSlots(logicalPlan, ImmutableSet.copyOf(correlatedSlots));
        }

        private boolean findAggContainsCorrelatedSlots(Plan rootPlan, ImmutableSet<Slot> slots) {
            ArrayDeque<Plan> planQueue = new ArrayDeque<>();
            planQueue.add(rootPlan);
            while (!planQueue.isEmpty()) {
                Plan plan = planQueue.poll();
                if (plan instanceof LogicalAggregate) {
                    if (plan.containsSlots(slots)) {
                        return true;
                    }
                } else {
                    for (Plan child : plan.children()) {
                        planQueue.add(child);
                    }
                }
            }
            return false;
        }

        public boolean rootIsLimit() {
            return logicalPlan instanceof LogicalLimit;
        }

        public boolean rootIsLimitWithOffset() {
            return logicalPlan instanceof LogicalLimit && ((LogicalLimit<?>) logicalPlan).getOffset() != 0;
        }

        public boolean rootIsLimitZero() {
            return logicalPlan instanceof LogicalLimit && ((LogicalLimit<?>) logicalPlan).getLimit() == 0;
        }
    }
}
