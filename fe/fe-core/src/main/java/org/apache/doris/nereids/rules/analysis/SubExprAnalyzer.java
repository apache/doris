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
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
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
                    ((InSubquery) child).getQueryPlan(), true), context);
        }
        return visit(not, context);
    }

    @Override
    public Expression visitExistsSubquery(Exists exists, T context) {
        if (!exists.getCorrelateSlots().isEmpty()) {
            return exists;
        }
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
        // EXISTS over a top-level scalar aggregate (no GROUP BY) always returns
        // exactly one row.  Fold to TRUE / FALSE immediately; this also avoids
        // rejecting a valid query that happens to contain a set-operation
        // underneath the aggregate, e.g.
        //   WHERE EXISTS (SELECT COUNT(*) FROM (... UNION ALL ...) u)
        // because SubqueryToApply would have constant-folded it anyway.
        if (hasTopLevelScalarAgg(analyzedResult)) {
            return BooleanLiteral.of(!exists.isNot());
        }
        checkNoCorrelatedSlotsUnderSetOp(analyzedResult);
        if (analyzedResult.isCorrelated() && containsARepeatAboveTheCorrelatedPredicate(
                analyzedResult.getLogicalPlan(), ImmutableSet.copyOf(analyzedResult.correlatedSlots))) {
            // The rewrite of a correlated EXISTS subquery reads the aggregation of the domain of an
            // outer row, and a repeat above the correlated predicate duplicates the rows of every
            // correlation key together (see containsARepeatAboveTheCorrelatedPredicate): report the
            // subquery instead of evaluating its grouping sets once for all of them.
            throw new AnalysisException(
                    "access outer query's column before grouping sets is not supported "
                            + analyzedResult.getLogicalPlan());
        }
        if (analyzedResult.isCorrelated() && containsAJoinAboveTheCorrelatedPredicate(
                analyzedResult.getLogicalPlan(), ImmutableSet.copyOf(analyzedResult.correlatedSlots))) {
            // The join interleaves the rows of the domain of an outer row with the rows of its other
            // side, and the rewrite reads the aggregation of that domain from below the join: the
            // join would be evaluated once for the rows of every correlation key together (see
            // containsAJoinAboveTheCorrelatedPredicate), so the subquery is reported instead of
            // reporting the outer rows which the domain of another correlation key decides on.
            throw new AnalysisException(
                    "access outer query's column before join is not supported "
                            + analyzedResult.getLogicalPlan());
        }
        return new Exists(analyzedResult.getLogicalPlan(), analyzedResult.getCorrelatedSlots(), exists.isNot());
    }

    @Override
    public Expression visitInSubquery(InSubquery expr, T context) {
        if (!expr.getCorrelateSlots().isEmpty()) {
            return expr;
        }
        LogicalPlan queryPlan = expr.getQueryPlan();
        // distinct is useless, remove it
        if (queryPlan instanceof LogicalProject && ((LogicalProject) queryPlan).isDistinct()) {
            expr = expr.withSubquery(((LogicalProject) queryPlan).withDistinct(false));
        }
        AnalyzedResult analyzedResult = analyzeSubquery(expr);

        checkOutputColumn(analyzedResult.getLogicalPlan());
        // the correlated predicate of an IN subquery may sit below the aggregation of the subquery:
        // the rewrite which unnests it (UnCorrelatedApplyAggregateFilter) computes the aggregation
        // of the domain of every outer row, the empty correlated domain included, so that the value
        // which the IN compares exists for every outer row
        if (analyzedResult.isCorrelated()) {
            // The rewrite only carries the outer slots through the filters of the subquery: it keeps
            // the aggregation of the domain as it is (the outer predicate becomes the condition
            // which pairs the outer row with the rows of the domain) and it reads the value which
            // the IN compares from the aggregation itself. An outer slot which the subquery reads
            // from its aggregation, its projections or its joins is therefore rejected here, the way
            // the scalar subquery path rejects it (see visitScalarSubquery): the subquery of
            //
            //     select k from o where k in (select sum(i.v + o.k) from i)
            //
            // cannot be unnested, because the aggregation of the domain of an outer row would have
            // to aggregate the value of the outer row as well, and the plan of the rewrite would
            // read that value from a scan which does not produce it.
            validateTheNodesOfTheSubqueryReadTheOuterSlotsThroughFilters(analyzedResult.getLogicalPlan(),
                    new CorrelatedSlotsValidator(ImmutableSet.copyOf(analyzedResult.correlatedSlots)));
            if (containsAWindowAboveTheCorrelatedPredicate(analyzedResult.getLogicalPlan(),
                    ImmutableSet.copyOf(analyzedResult.correlatedSlots))) {
                // The rewrite reads the value which the IN compares from the aggregation of the domain
                // of an outer row (the aggregation of the rewrite groups the rows of one correlation
                // key), so the nodes of the subquery which sit above the correlated predicate are
                // evaluated on the rows of one domain. A window is evaluated on the rows of the node
                // it sits in, so a window above the correlated predicate of the rewrite is evaluated
                // over the rows of every correlation key together, while that window of the subquery
                // of the query is evaluated over the rows of one domain: the subquery of
                //
                //     select k from o where k in (
                //         select sum(i.g) over () from i where i.k = o.k group by i.g)
                //
                // is reported as unsupported for that reason. A window below the correlated
                // predicate is evaluated before that predicate selects the rows of the domain in the
                // plan of the query as well, so the rewrite leaves its evaluation domain unchanged
                // and the subquery of
                //
                //     select k from o where k in (
                //         select rn from (select k, row_number() over (order by k) as rn from i) x
                //         where x.k = o.k)
                //
                // is accepted.
                throw new AnalysisException(
                        "access outer query's column before window function is not supported "
                                + analyzedResult.getLogicalPlan());
            }
        }
        checkNoCorrelatedSlotsUnderSetOp(analyzedResult);
        checkRootIsLimit(analyzedResult);
        if (analyzedResult.isCorrelated()) {
            // The nodes above the correlated predicate which the rewrites cannot rebuild per
            // correlation key are not reported by checkRootIsLimit (it reads the root of the plan
            // alone) nor by the validator (it validates the nodes which read the outer slots):
            // report them here, the plan of the rewrite would read the columns of the outer query
            // from the rows of another correlation key.
            rejectTheWrappersWhichTheRewriteCannotRebuild(analyzedResult.getLogicalPlan(),
                    ImmutableSet.copyOf(analyzedResult.correlatedSlots));
            if (containsARepeatAboveTheCorrelatedPredicate(analyzedResult.getLogicalPlan(),
                    ImmutableSet.copyOf(analyzedResult.correlatedSlots))) {
                throw new AnalysisException(
                        "access outer query's column before grouping sets is not supported "
                                + analyzedResult.getLogicalPlan());
            }
            if (containsAJoinAboveTheCorrelatedPredicate(analyzedResult.getLogicalPlan(),
                    ImmutableSet.copyOf(analyzedResult.correlatedSlots))) {
                // The join interleaves the rows of the domain of an outer row with the rows of its
                // other side, and the rewrite reads the aggregation of that domain from below the
                // join: the join would be evaluated once for the rows of every correlation key
                // together (see containsAJoinAboveTheCorrelatedPredicate), so the subquery is
                // reported instead of comparing the outer rows with the rows of another key.
                throw new AnalysisException(
                        "access outer query's column before join is not supported "
                                + analyzedResult.getLogicalPlan());
            }
        }

        return new InSubquery(
                expr.getCompareExpr().accept(this, context),
                analyzedResult.getLogicalPlan(),
                analyzedResult.getCorrelatedSlots(), expr.isNot());
    }

    @Override
    public Expression visitScalarSubquery(ScalarSubquery scalar, T context) {
        if (!scalar.getCorrelateSlots().isEmpty()) {
            return scalar;
        }
        AnalyzedResult analyzedResult = analyzeSubquery(scalar);
        boolean isCorrelated = analyzedResult.isCorrelated();
        LogicalPlan analyzedSubqueryPlan = analyzedResult.logicalPlan;
        checkOutputColumn(analyzedSubqueryPlan);
        // use limitOneIsEliminated to indicate if subquery has limit 1 clause
        // because limit 1 clause will ensure subquery output at most 1 row
        // we eliminate limit 1 clause and pass this info to later SubqueryToApply rule
        // so when creating LogicalApply node, we don't need to add AssertTrue function
        boolean limitOneIsEliminated = false;
        if (isCorrelated) {
            if (analyzedSubqueryPlan instanceof LogicalLimit) {
                Plan child = ((LogicalLimit<?>) analyzedSubqueryPlan).child();
                LogicalLimit<?> limit = (LogicalLimit<?>) analyzedSubqueryPlan;
                // after analysis, if project not contains sort key, FILL_UP_SORT_PROJECT will add a project upper sort
                // so we must find sort under project here.
                while (child instanceof LogicalProject) {
                    child = ((LogicalProject<?>) child).child();
                }
                // order by c1 limit 1 is not acceptable
                if (!(child instanceof LogicalSort)
                        && limit.getOffset() == 0 && limit.getLimit() == 1) {
                    // skip useless limit node
                    analyzedResult = new AnalyzedResult((LogicalPlan) analyzedSubqueryPlan.child(0),
                            analyzedResult.correlatedSlots);
                    limitOneIsEliminated = true;
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
            // A lateral view which sits above the correlated predicate is reported by the walk above
            // (see validateNodeInfoList), and a generator which reads an outer slot is reported here:
            // the generator of the lateral view of an outer row explodes the arrays of the rows of
            // the domain of that row, while the rewrite of the subquery moves the predicate of the
            // outer row into the join and evaluates the nodes below it once, where the outer column
            // has no row to read.
            rejectTheLateralViewsWhichReadTheOuterSlots(analyzedResult.logicalPlan,
                    ImmutableSet.copyOf(analyzedResult.correlatedSlots));
        }

        if (analyzedResult.getLogicalPlan() instanceof LogicalOneRowRelation) {
            LogicalOneRowRelation oneRowRelation = (LogicalOneRowRelation) analyzedResult.getLogicalPlan();
            if (oneRowRelation.getProjects().size() == 1 && oneRowRelation.getProjects().get(0) instanceof Alias) {
                // if scalar subquery is like select '2024-02-02 00:00:00'
                // we can just return the constant expr '2024-02-02 00:00:00'
                Alias alias = (Alias) oneRowRelation.getProjects().get(0);
                if (alias.isConstant()) {
                    return alias.child();
                }
            }
        } else if (analyzedResult.getLogicalPlan() instanceof LogicalProject) {
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

        return new ScalarSubquery(analyzedResult.getLogicalPlan(), analyzedResult.getCorrelatedSlots(),
                limitOneIsEliminated);
    }

    private void checkOutputColumn(LogicalPlan plan) {
        if (plan.getOutput().size() != 1) {
            throw new AnalysisException("Multiple columns returned by subquery are not yet supported. Found "
                    + plan.getOutput().size());
        }
    }

    private void checkNoCorrelatedSlotsUnderSetOp(AnalyzedResult analyzedResult) {
        if (analyzedResult.hasCorrelatedSlotsUnderSetOp()) {
            throw new AnalysisException(
                    "Unsupported correlated subquery with set operation "
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

    /**
     * Check whether the analyzed subquery plan has a top-level scalar aggregate
     * (aggregate without GROUP BY).  Such an aggregate is guaranteed to return
     * exactly one row regardless of its input, so EXISTS over it is always TRUE
     * and NOT EXISTS is always FALSE.  Sorting the single row cannot change
     * EXISTS semantics, so we also strip leading LogicalSort and
     * LogicalSubQueryAlias wrappers (the latter appears during analysis before
     * LogicalSubQueryAliasToLogicalProject is applied).
     */
    private boolean hasTopLevelScalarAgg(AnalyzedResult analyzedResult) {
        LogicalPlan plan = analyzedResult.getLogicalPlan();
        // Strip leading projects, sorts, and subquery-alias wrappers —
        // analysis may wrap the aggregate in any of these.
        while (plan instanceof LogicalProject || plan instanceof LogicalSort
                || plan instanceof LogicalSubQueryAlias) {
            plan = (LogicalPlan) plan.child(0);
        }
        if (plan instanceof LogicalAggregate) {
            return ((LogicalAggregate<?>) plan).getGroupByExpressions().isEmpty();
        }
        return false;
    }

    private AnalyzedResult analyzeSubquery(SubqueryExpr expr) {
        if (cascadesContext == null) {
            throw new IllegalStateException("Missing CascadesContext");
        }
        CascadesContext subqueryContext = CascadesContext.newContextWithCteContext(
                cascadesContext, expr.getQueryPlan(), cascadesContext.getCteContext(), null);
        // don't use `getScope()` because we only need `getScope().getOuterScope()` and `getScope().getSlots()`
        // otherwise unexpected errors may occur
        Scope subqueryScope = new Scope(getScope().getOuterScope(),
                getScope().getSlots(), getScope().getAsteriskSlots());
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

        public boolean hasCorrelatedSlotsUnderSetOp() {
            return correlatedSlots.isEmpty() ? false
                    : hasCorrelatedSlotsUnderNode(logicalPlan,
                            ImmutableSet.copyOf(correlatedSlots), LogicalSetOperation.class);
        }

        private static <T> boolean hasCorrelatedSlotsUnderNode(Plan rootPlan,
                                                               ImmutableSet<Slot> slots, Class<T> clazz) {
            ArrayDeque<Plan> planQueue = new ArrayDeque<>();
            planQueue.add(rootPlan);
            while (!planQueue.isEmpty()) {
                Plan plan = planQueue.poll();
                if (clazz.isInstance(plan)) {
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

        public PlanNodeCorrelatedInfo visitLogicalOneRowRelation(LogicalOneRowRelation plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in project is not supported",
                                correlatedSlots));
            } else {
                return new PlanNodeCorrelatedInfo(plan.getType(), false);
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
                        case LOGICAL_REPEAT:
                            // The aggregation above a repeat node computes the grouping sets of the
                            // subquery (GROUP BY GROUPING SETS ...), and the rewrite which unnests the
                            // subquery reads the aggregation of the domain below the repeat (see
                            // locateAggregate of UnCorrelatedApplyAggregateFilter): a repeat above the
                            // correlated predicate belongs to the grouping sets of that aggregation,
                            // whose groups the rewrite would compute for the rows of every correlation
                            // key together, so the subquery of
                            //
                            //     select t1.id, (select count(*) from t2 where t2.id = t1.id
                            //         group by grouping sets ((t2.score), ())) from t1
                            //
                            // is reported instead of building a plan whose correlation predicate no
                            // aggregation below it can carry (see the walk of validateNodeInfoList).
                            throw new AnalysisException(
                                    "access outer query's column before grouping sets is not supported");
                        case LOGICAL_AGGREGATE:
                            if (checkAfterAggNode) {
                                throw new AnalysisException(
                                        "access outer query's column before two agg nodes is not supported");
                            }
                            // the aggregation of the subquery may group the inner rows and it may
                            // filter them with a HAVING clause: the rewrite which unnests the
                            // subquery (UnCorrelatedApplyAggregateFilter) groups the aggregation of
                            // every outer row by the correlation key of that row, so that the groups
                            // of the aggregation of one outer row are the rows of the subquery for
                            // that row
                            checkAfterAggNode = true;
                            topAggregate = nodeInfo.aggregate;
                            break;
                        case LOGICAL_WINDOW:
                            throw new AnalysisException(
                                    "access outer query's column before window function is not supported");
                        case LOGICAL_JOIN:
                            throw new AnalysisException(
                                    "access outer query's column before join is not supported");
                        case LOGICAL_UNION:
                        case LOGICAL_INTERSECT:
                        case LOGICAL_EXCEPT:
                            throw new AnalysisException(
                                    "access outer query's column before set operation is not supported");
                        case LOGICAL_SORT:
                            // allow any sort node, the sort node will be removed by ELIMINATE_ORDER_BY_UNDER_SUBQUERY
                            break;
                        case LOGICAL_PROJECT:
                            // allow any project node
                            break;
                        case LOGICAL_FILTER:
                            // allow any filter node: the filters above the aggregation of the
                            // subquery are the predicates of its HAVING clause, which the rewrite
                            // evaluates on the aggregation of every outer row (and which it keeps
                            // where filter pushdown placed them)
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

    /**
     * Whether every node of the plan of the subquery reads the outer slots the way the rewrites of a
     * correlated subquery can carry them: the validator rejects the outer slots of an aggregation, a
     * projection, a join or a sort (the filters of the subquery may read them wherever they are, see
     * CorrelatedSlotsValidator). The scalar subquery path checks the order of the nodes of the
     * subquery as well (see validateNodeInfoList, which the caller of the validator of that path
     * runs), because only one aggregation may sit below the correlated predicate there; the rewrites
     * of an IN subquery read the correlated predicate below every aggregation of the chain, so only
     * the nodes which read the outer slots are validated here.
     */
    private void validateTheNodesOfTheSubqueryReadTheOuterSlotsThroughFilters(
            Plan plan, CorrelatedSlotsValidator validator) {
        plan.accept(validator, null);
        for (Plan child : plan.children()) {
            validateTheNodesOfTheSubqueryReadTheOuterSlotsThroughFilters(child, validator);
        }
    }

    /**
     * Whether a window of the subtree is evaluated on the rows of the correlated domain of one outer
     * row (see visitInSubquery): that is the case for a window which sits above the correlated
     * predicate, whose rows the predicate selects below it. A window below the correlated predicate
     * is evaluated on the rows of the node it sits in before the predicate selects the rows of the
     * domain of an outer row, and the rewrite keeps that node as it is.
     */
    private static boolean containsAWindowAboveTheCorrelatedPredicate(Plan plan, Set<Slot> correlatedSlots) {
        if (computesAWindow(plan) && plan.children().stream()
                .anyMatch(child -> subtreeReadsTheCorrelatedSlots(child, correlatedSlots))) {
            return true;
        }
        return plan.children().stream()
                .anyMatch(child -> containsAWindowAboveTheCorrelatedPredicate(child, correlatedSlots));
    }

    /** whether a node of the plan computes a window (a window node or a projection over a window) */
    private static boolean computesAWindow(Plan plan) {
        return plan instanceof LogicalWindow || plan.getExpressions().stream()
                .anyMatch(expression -> expression.containsType(WindowExpression.class));
    }

    /** whether a node of the subtree reads a slot of the outer query */
    private static boolean subtreeReadsTheCorrelatedSlots(Plan plan, Set<Slot> correlatedSlots) {
        if (plan.getInputSlots().stream().anyMatch(correlatedSlots::contains)) {
            return true;
        }
        return plan.children().stream()
                .anyMatch(child -> subtreeReadsTheCorrelatedSlots(child, correlatedSlots));
    }

    /**
     * Whether a repeat of the subtree computes the grouping sets of the rows of the correlated domain
     * of one outer row (see visitInSubquery): that is the case for a repeat which sits above the
     * correlated predicate, whose rows the predicate selects below it. The rewrite which unnests a
     * correlated subquery reads the aggregation of the domain of an outer row from below the repeat
     * (see locateAggregate of UnCorrelatedApplyAggregateFilter), because the repeat of the subquery
     * duplicates the rows of its child into the grouping sets which the aggregation above it
     * aggregates: a repeat above the correlated predicate would duplicate the rows of every
     * correlation key together, so the subquery of
     *
     *     select k from o where k in (
     *         select count(*) from i where i.k = o.k group by grouping sets ((i.g), ()))
     *
     * is reported as unsupported for that reason. A repeat below the correlated predicate computes the
     * rows which that predicate selects, so the rewrite keeps its evaluation domain unchanged and the
     * subquery of
     *
     *     select k from o where k in (
     *         select count(*) from (select k, g from i group by grouping sets ((k, g), ())) x
     *         where x.k = o.k)
     *
     * is accepted.
     */
    private static boolean containsARepeatAboveTheCorrelatedPredicate(Plan plan, Set<Slot> correlatedSlots) {
        if (plan instanceof LogicalRepeat && plan.children().stream()
                .anyMatch(child -> subtreeReadsTheCorrelatedSlots(child, correlatedSlots))) {
            return true;
        }
        return plan.children().stream()
                .anyMatch(child -> containsARepeatAboveTheCorrelatedPredicate(child, correlatedSlots));
    }

    /**
     * Whether a join of the subtree combines the rows of the correlated domain of one outer row with
     * the other side of the join (see visitInSubquery): that is the case for a join which sits above
     * the correlated predicate, whose rows the predicate selects below it. The rewrite which unnests a
     * correlated subquery reads the aggregation of the domain of an outer row from below the join (see
     * locateAggregate of UnCorrelatedApplyAggregateFilter), because the join interleaves the rows of
     * the domain of an outer row with the rows of another relation: a join above the correlated
     * predicate is evaluated once for the rows of every correlation key together when the rewrite
     * groups them, so the subquery of
     *
     *     select k from o where k in (
     *         select count(*) from (select i.id, i.k from i where i.k = o.k) x
     *             join j on x.id = j.id)
     *
     * is reported as unsupported for that reason. The plan of the query is
     * Apply(IN) -> Aggregate -> Join -> Project -> Filter(i.k = o.k): the walk which validates the
     * nodes above the correlated predicate (see rejectTheWrappersWhichTheRewriteCannotRebuild)
     * reaches the join before the filter, and the join has no aggregation below it which could carry
     * the keys of the correlation. A join below the correlated predicate is part of the rows which
     * that predicate selects (the domain of an outer row), so the rewrite keeps it as it is and the
     * subquery of
     *
     *     select k from o where k in (
     *         select count(*) from i join j on i.id = j.id where i.k = o.k)
     *
     * is accepted.
     */
    private static boolean containsAJoinAboveTheCorrelatedPredicate(Plan plan, Set<Slot> correlatedSlots) {
        if (plan instanceof LogicalJoin && plan.children().stream()
                .anyMatch(child -> subtreeReadsTheCorrelatedSlots(child, correlatedSlots))) {
            return true;
        }
        return plan.children().stream()
                .anyMatch(child -> containsAJoinAboveTheCorrelatedPredicate(child, correlatedSlots));
    }

    /**
     * Reject the LIMIT, the TOP-N, the LATERAL VIEW and the JOIN nodes which sit above the correlated
     * predicate of the subquery: the LIMIT and the LATERAL VIEW of the subquery of an outer row decide
     * on the rows of the domain of that row (the LIMIT keeps one row of the derived table of the domain,
     * the LATERAL VIEW explodes the arrays of the rows of the domain), and the JOIN combines those rows
     * with the rows of its other side, while the rewrite which unnests a correlated subquery reads the
     * value which the subquery exposes from the aggregation of the domain of the outer row: the LIMIT of
     * that rewrite reads the domains of every correlation key together, and the LATERAL VIEW and the
     * JOIN are evaluated once for all of them. Neither rewrite can rebuild those nodes per correlation
     * key, so the subquery of
     *
     *     select k from o where k in (
     *         select max(c) from (select count(*) c from i where i.k = o.k group by i.g limit 1) x)
     *
     * (where the limit keeps one row of the derived table of the domain of every outer row) is
     * reported instead of building a plan which reads the rows of the outer query from the wrong
     * correlation key. Only the nodes above the correlated predicate are checked: the nodes below it
     * are the rows of the domain of an outer row, which the rewrite keeps as they are.
     */
    private static void rejectTheWrappersWhichTheRewriteCannotRebuild(Plan plan,
            ImmutableSet<Slot> correlatedSlots) {
        rejectTheLateralViewsWhichReadTheOuterSlots(plan, correlatedSlots);
        for (Plan node = plan; node != null; node = theChildWhichHoldsTheOuterSlots(node, correlatedSlots)) {
            if (node instanceof LogicalLimit || node instanceof LogicalTopN) {
                throw new AnalysisException("access outer query's column before limit is not supported "
                        + plan);
            }
            if (node instanceof LogicalGenerate) {
                throw new AnalysisException(
                        "access outer query's column before lateral view is not supported " + plan);
            }
            if (node instanceof LogicalJoin) {
                // The join combines the rows of the domain of an outer row with the rows of its other
                // side, and the rewrite reads the aggregation of that domain from below the join (see
                // containsAJoinAboveTheCorrelatedPredicate): the keys which it adds to the group by of
                // the aggregation would be the keys of one branch of the join alone, so the join would
                // be evaluated once for the rows of every correlation key together.
                throw new AnalysisException(
                        "access outer query's column before join is not supported " + plan);
            }
            if (readsAnOuterSlot(node, correlatedSlots)) {
                // the predicate of the outer query itself: the nodes below it hold the rows of the
                // domain of an outer row, which the rewrite keeps as they are
                return;
            }
        }
    }

    /**
     * Reject the lateral views whose generator reads an outer slot: the generator of the lateral view
     * of an outer row is evaluated on the rows of the domain of that row (the LATERAL VIEW explodes
     * arrays which the value of the outer row may be a part of), while the rewrite of the subquery
     * evaluates the nodes below the correlated predicate once, with the predicate of the outer row
     * moved into the join: the outer column of the generator has no row to read there, and the plan of
     * the rewrite dangles.
     */
    private static void rejectTheLateralViewsWhichReadTheOuterSlots(Plan plan,
            ImmutableSet<Slot> correlatedSlots) {
        if (plan instanceof LogicalGenerate && readsAnOuterSlot(plan, correlatedSlots)) {
            throw new AnalysisException(
                    "access outer query's column in lateral view is not supported " + plan);
        }
        plan.children().forEach(child -> rejectTheLateralViewsWhichReadTheOuterSlots(child, correlatedSlots));
    }

    /** whether a node of the plan reads one of the outer slots (see CorrelatedSlotsValidator) */
    private static boolean readsAnOuterSlot(Plan plan, ImmutableSet<Slot> correlatedSlots) {
        return plan.getExpressions().stream().anyMatch(expression -> !Sets
                .intersection(correlatedSlots, expression.getInputSlots()).isEmpty());
    }

    /** the child of the node which holds the predicate of the outer query, or null when none holds it */
    private static Plan theChildWhichHoldsTheOuterSlots(Plan node, ImmutableSet<Slot> correlatedSlots) {
        for (Plan child : node.children()) {
            if (containsAnOuterSlot(child, correlatedSlots)) {
                return child;
            }
        }
        return null;
    }

    /** whether the plan or one of the nodes below it reads one of the outer slots */
    private static boolean containsAnOuterSlot(Plan plan, ImmutableSet<Slot> correlatedSlots) {
        if (readsAnOuterSlot(plan, correlatedSlots)) {
            return true;
        }
        return plan.children().stream().anyMatch(child -> containsAnOuterSlot(child, correlatedSlots));
    }
}
