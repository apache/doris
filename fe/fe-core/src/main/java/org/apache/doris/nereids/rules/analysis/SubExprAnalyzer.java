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
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
        boolean isCorrelated = analyzedResult.isCorrelated();
        LogicalPlan analyzedSubqueryPlan = analyzedResult.logicalPlan;
        checkOutputColumn(analyzedSubqueryPlan);
        if (isCorrelated) {
            if (analyzedSubqueryPlan instanceof LogicalLimit) {
                LogicalLimit limit = (LogicalLimit) analyzedSubqueryPlan;
                if (limit.getOffset() == 0 && limit.getLimit() == 1) {
                    // skip useless limit node
                    analyzedResult = new AnalyzedResult((LogicalPlan) analyzedSubqueryPlan.child(0),
                            analyzedResult.correlatedSlots);
                } else {
                    throw new AnalysisException("limit is not supported in correlated subquery "
                            + analyzedResult.getLogicalPlan());
                }
            }
            if (analyzedSubqueryPlan instanceof LogicalSort) {
                // skip useless sort node
                analyzedResult = new AnalyzedResult((LogicalPlan) analyzedSubqueryPlan.child(0),
                        analyzedResult.correlatedSlots);
            }
            CorrelatedSlotsValidator validator =
                    new CorrelatedSlotsValidator(ImmutableSet.copyOf(analyzedResult.correlatedSlots));
            List<PlanNodeCorrelatedInfo> nodeInfoList = new ArrayList<>(16);
            Set<LogicalAggregate> topAgg = new HashSet<>();
            validateSubquery(analyzedResult.logicalPlan, validator, nodeInfoList, topAgg);
        }

        if (analyzedResult.getLogicalPlan() instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) analyzedResult.getLogicalPlan();
            if (project.child() instanceof LogicalOneRowRelation
                    && project.getProjects().size() == 1
                    && project.getProjects().get(0) instanceof Alias) {
                // if scalar subquery is like select '2024-02-02 00:00:00'
                // we can just return the constant expr '2024-02-02 00:00:00'
                Alias alias = (Alias) project.getProjects().get(0);
                if (alias.isConstant()) {
                    return alias.child();
                }
            } else if (isCorrelated) {
                Set<Slot> correlatedSlots = new HashSet<>(analyzedResult.getCorrelatedSlots());
                if (!Sets.intersection(ExpressionUtils.getInputSlotSet(project.getProjects()),
                        correlatedSlots).isEmpty()) {
                    throw new AnalysisException(
                            "outer query's column is not supported in subquery's output "
                                    + analyzedResult.getLogicalPlan());
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
        // don't use `getScope()` because we only need `getScope().getOuterScope()` and `getScope().getSlots()`
        // otherwise unexpected errors may occur
        Scope subqueryScope = new Scope(getScope().getOuterScope(), getScope().getSlots());
        subqueryContext.setOuterScope(subqueryScope);
        subqueryContext.newAnalyzer().analyze();
        return new AnalyzedResult((LogicalPlan) subqueryContext.getRewritePlan(),
                subqueryScope.getCorrelatedSlots());
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

        public boolean hasCorrelatedSlotsUnderAgg() {
            return correlatedSlots.isEmpty() ? false
                    : hasCorrelatedSlotsUnderNode(logicalPlan,
                            ImmutableSet.copyOf(correlatedSlots), LogicalAggregate.class);
        }

        private static <T> boolean hasCorrelatedSlotsUnderNode(Plan rootPlan,
                                                               ImmutableSet<Slot> slots, Class<T> clazz) {
            ArrayDeque<Plan> planQueue = new ArrayDeque<>();
            planQueue.add(rootPlan);
            while (!planQueue.isEmpty()) {
                Plan plan = planQueue.poll();
                if (plan.getClass().equals(clazz)) {
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

    private static class PlanNodeCorrelatedInfo {
        private PlanType planType;
        private boolean containCorrelatedSlots;
        private boolean hasGroupBy;
        private LogicalAggregate aggregate;

        public PlanNodeCorrelatedInfo(PlanType planType, boolean containCorrelatedSlots) {
            this(planType, containCorrelatedSlots, null);
        }

        public PlanNodeCorrelatedInfo(PlanType planType, boolean containCorrelatedSlots,
                LogicalAggregate aggregate) {
            this.planType = planType;
            this.containCorrelatedSlots = containCorrelatedSlots;
            this.aggregate = aggregate;
            this.hasGroupBy = aggregate != null ? !aggregate.getGroupByExpressions().isEmpty() : false;
        }
    }

    private static class CorrelatedSlotsValidator
            extends PlanVisitor<PlanNodeCorrelatedInfo, Void> {
        private final ImmutableSet<Slot> correlatedSlots;

        public CorrelatedSlotsValidator(ImmutableSet<Slot> correlatedSlots) {
            this.correlatedSlots = correlatedSlots;
        }

        @Override
        public PlanNodeCorrelatedInfo visit(Plan plan, Void context) {
            return new PlanNodeCorrelatedInfo(plan.getType(), findCorrelatedSlots(plan));
        }

        public PlanNodeCorrelatedInfo visitLogicalProject(LogicalProject plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in project is not supported",
                                correlatedSlots));
            } else {
                PlanType planType = ExpressionUtils.containsWindowExpression(
                        ((LogicalProject<?>) plan).getProjects()) ? PlanType.LOGICAL_WINDOW : plan.getType();
                return new PlanNodeCorrelatedInfo(planType, false);
            }
        }

        public PlanNodeCorrelatedInfo visitLogicalAggregate(LogicalAggregate plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in aggregate is not supported",
                                correlatedSlots, plan));
            } else {
                return new PlanNodeCorrelatedInfo(plan.getType(), false, plan);
            }
        }

        public PlanNodeCorrelatedInfo visitLogicalJoin(LogicalJoin plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in join is not supported",
                                correlatedSlots, plan));
            } else {
                return new PlanNodeCorrelatedInfo(plan.getType(), false);
            }
        }

        public PlanNodeCorrelatedInfo visitLogicalSort(LogicalSort plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in order by is not supported",
                                correlatedSlots, plan));
            } else {
                return new PlanNodeCorrelatedInfo(plan.getType(), false);
            }
        }

        private boolean findCorrelatedSlots(Plan plan) {
            return plan.getExpressions().stream().anyMatch(expression -> !Sets
                    .intersection(correlatedSlots, expression.getInputSlots()).isEmpty());
        }
    }

    private LogicalAggregate validateNodeInfoList(List<PlanNodeCorrelatedInfo> nodeInfoList) {
        LogicalAggregate topAggregate = null;
        int size = nodeInfoList.size();
        if (size > 0) {
            List<PlanNodeCorrelatedInfo> correlatedNodes = new ArrayList<>(4);
            boolean checkNodeTypeAfterCorrelatedNode = false;
            boolean checkAfterAggNode = false;
            for (int i = size - 1; i >= 0; --i) {
                PlanNodeCorrelatedInfo nodeInfo = nodeInfoList.get(i);
                if (checkNodeTypeAfterCorrelatedNode) {
                    switch (nodeInfo.planType) {
                        case LOGICAL_LIMIT:
                            throw new AnalysisException(
                                    "limit is not supported in correlated subquery");
                        case LOGICAL_GENERATE:
                            throw new AnalysisException(
                                    "access outer query's column before lateral view is not supported");
                        case LOGICAL_AGGREGATE:
                            if (checkAfterAggNode) {
                                throw new AnalysisException(
                                        "access outer query's column before two agg nodes is not supported");
                            }
                            if (nodeInfo.hasGroupBy) {
                                // TODO support later
                                throw new AnalysisException(
                                        "access outer query's column before agg with group by is not supported");
                            }
                            checkAfterAggNode = true;
                            topAggregate = nodeInfo.aggregate;
                            break;
                        case LOGICAL_WINDOW:
                            throw new AnalysisException(
                                    "access outer query's column before window function is not supported");
                        case LOGICAL_JOIN:
                            throw new AnalysisException(
                                    "access outer query's column before join is not supported");
                        case LOGICAL_SORT:
                            // allow any sort node, the sort node will be removed by ELIMINATE_ORDER_BY_UNDER_SUBQUERY
                            break;
                        case LOGICAL_PROJECT:
                            // allow any project node
                            break;
                        case LOGICAL_SUBQUERY_ALIAS:
                            // allow any subquery alias
                            break;
                        default:
                            if (checkAfterAggNode) {
                                throw new AnalysisException(
                                        "only project, sort and subquery alias node is allowed after agg node");
                            }
                            break;
                    }
                }
                if (nodeInfo.containCorrelatedSlots) {
                    correlatedNodes.add(nodeInfo);
                    checkNodeTypeAfterCorrelatedNode = true;
                }
            }

            // only support 1 correlated node for now
            if (correlatedNodes.size() > 1) {
                throw new AnalysisException(
                        "access outer query's column in two places is not supported");
            }
        }
        return topAggregate;
    }

    private void validateSubquery(Plan plan, CorrelatedSlotsValidator validator,
            List<PlanNodeCorrelatedInfo> nodeInfoList, Set<LogicalAggregate> topAgg) {
        nodeInfoList.add(plan.accept(validator, null));
        for (Plan child : plan.children()) {
            validateSubquery(child, validator, nodeInfoList, topAgg);
        }
        if (plan.children().isEmpty()) {
            LogicalAggregate topAggNode = validateNodeInfoList(nodeInfoList);
            if (topAggNode != null) {
                topAgg.add(topAggNode);
            }
        }
        nodeInfoList.remove(nodeInfoList.size() - 1);
    }
}
