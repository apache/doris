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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.window.SupportWindowAnalytic;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * change the plan:
 * logicalFilter(logicalApply(any(), logicalAggregate()))
 * to
 * logicalProject(logicalFilter(logicalWindow(logicalFilter(any()))))
 * <p>
 * refer paper: WinMagic - Subquery Elimination Using Window Aggregation
 * <p>
 * TODO: use materialized view pattern match to do outer and inner tree match.
 */

public class AggScalarSubQueryToWindowFunction extends DefaultPlanRewriter<JobContext> implements CustomRewriter {

    private static final Set<Class<? extends LogicalPlan>> OUTER_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalJoin.class,
            LogicalFilter.class,
            LogicalProject.class,
            LogicalRelation.class,
            LogicalSubQueryAlias.class
    );

    private static final Set<Class<? extends LogicalPlan>> INNER_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalAggregate.class,
            LogicalFilter.class,
            LogicalJoin.class,
            LogicalProject.class,
            LogicalRelation.class,
            LogicalSubQueryAlias.class
    );

    private final List<LogicalPlan> outerPlans = Lists.newArrayList();
    private final List<LogicalPlan> innerPlans = Lists.newArrayList();
    private final List<AggregateFunction> functions = Lists.newArrayList();
    private final Map<Expression, Expression> innerOuterSlotMap = Maps.newHashMap();
    // Outer conjuncts that were matched against inner subquery filter conjuncts
    // by checkFilter().  These must stay BELOW the window because they are part
    // of the inner aggregate's filter, not extra outer-only predicates.
    private final Set<Expression> matchedInnerFilterConjuncts = Sets.newHashSet();
    // The sole table that appears in the outer plan but not the inner plan.
    // Identified by checkRelation() and reused by checkUniqueCorrelatedTable()
    // and rewrite().
    private CatalogRelation outerOnlyTable = null;

    /**
     * the entrance of this rule. we only override one visitor: visitLogicalFilter
     * because we need to process the filter of outer plan. It is on the top of Apply.
     */
    @Override
    public Plan rewriteRoot(Plan plan, JobContext context) {
        if (!plan.containsType(Filter.class, LogicalApply.class, LogicalAggregate.class)) {
            return plan;
        }
        return plan.accept(this, context);
    }

    /**
     * we need to process Filter and Apply, but sometimes there are project between Filter and Apply.
     * According to {@link org.apache.doris.nereids.rules.analysis.SubqueryToApply} rule. The project
     * is used to project apply output to original output, it is not affect this rule at all. so we ignore it.
     */
    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> plan, JobContext context) {
        LogicalFilter<? extends Plan> filter = visitChildren(this, plan, context);
        return findApply(filter)
                .filter(a -> check(filter, a))
                .map(a -> rewrite(filter, a))
                .orElse(filter);
    }

    private Optional<LogicalApply<Plan, Plan>> findApply(LogicalFilter<? extends Plan> filter) {
        return Optional.of(filter.child())
                .map(p -> p instanceof LogicalProject ? p.child(0) : p)
                .filter(LogicalApply.class::isInstance)
                .map(p -> (LogicalApply<Plan, Plan>) p);
    }

    private boolean check(LogicalFilter<? extends Plan> outerFilter, LogicalApply<Plan, Plan> apply) {
        // Clear per-candidate state: the same rule instance visits every
        // LogicalFilter in one rewriteRoot() call, and earlier rejected
        // candidates must not leak into later ones.
        outerPlans.clear();
        innerPlans.clear();
        functions.clear();
        innerOuterSlotMap.clear();
        matchedInnerFilterConjuncts.clear();
        outerOnlyTable = null;

        outerPlans.addAll(apply.child(0).collect(LogicalPlan.class::isInstance));
        innerPlans.addAll(apply.child(1).collect(LogicalPlan.class::isInstance));

        return checkPlanType()
                && checkApply(apply)
                && checkAggregate()
                && checkJoin()
                && checkProject()
                && checkRelation(apply.getCorrelationSlot())
                && checkFilter(outerFilter)
                && checkUniqueCorrelatedTable(apply.getCorrelationSlot());
    }

    // check children's nodes because query process will be changed
    private boolean checkPlanType() {
        return outerPlans.stream().allMatch(p -> OUTER_SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)))
                && innerPlans.stream().allMatch(p -> INNER_SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    /**
     * Apply should be
     *   1. scalar
     *   2. is not mark join
     *   3. is correlated
     *   4. correlated conjunct should be {@link ComparisonPredicate}
     *   5. the top plan of Apply inner should be {@link LogicalAggregate}
     */
    private boolean checkApply(LogicalApply<Plan, Plan> apply) {
        return apply.isScalar()
                && !apply.isMarkJoin()
                && apply.right() instanceof LogicalAggregate
                && apply.isCorrelated();
    }

    /**
     * check aggregation of inner scope, it should be only one Aggregate and only one AggregateFunction in it
     */
    private boolean checkAggregate() {
        List<LogicalAggregate<Plan>> aggSet = innerPlans.stream().filter(LogicalAggregate.class::isInstance)
                .map(p -> (LogicalAggregate<Plan>) p)
                .collect(Collectors.toList());
        if (aggSet.size() != 1) {
            // window functions don't support nesting.
            return false;
        }
        LogicalAggregate<Plan> aggOp = aggSet.get(0);
        functions.addAll(ExpressionUtils.collectAll(
                aggOp.getOutputExpressions(), AggregateFunction.class::isInstance));
        if (functions.size() != 1) {
            return false;
        }
        return functions.stream().allMatch(f -> f instanceof SupportWindowAnalytic && !f.isDistinct());
    }

    /**
     * check inner filter conjuncts are a sub collection of outer filter.
     * Also records which outer conjuncts were matched against inner conjuncts so that rewrite()
     * can force them below the window — they are part of the inner aggregate's filter, not extra
     * outer-only predicates.
     * <p>
     * PushDownFilterThroughProject can split a single LogicalFilter into multiple filters
     * (correlated predicates that reference slots not in a project's output stay above,
     * while non-correlated predicates can be pushed below).  We collect conjuncts from ALL
     * inner filters rather than requiring exactly one.
     */
    private boolean checkFilter(LogicalFilter<? extends Plan> outerFilter) {
        List<LogicalFilter<Plan>> innerFilters = innerPlans.stream()
                .filter(LogicalFilter.class::isInstance)
                .map(p -> (LogicalFilter<Plan>) p).collect(Collectors.toList());
        matchedInnerFilterConjuncts.clear();
        // An inner plan with zero filters cannot prove its shape
        // through checkFilter — reject early to avoid bypassing the
        // relation/identity/uniqueness proofs that depend on filter
        // conjunct matching.
        if (innerFilters.isEmpty()) {
            return false;
        }
        Set<Expression> outerConjunctSet = Sets.newHashSet(outerFilter.getConjuncts());
        // Collect conjuncts from ALL inner filter nodes — PushDownFilterThroughProject
        // may split correlated and non-correlated predicates into separate filters.
        Set<Expression> innerConjunctSet = innerFilters.stream()
                .flatMap(f -> f.getConjuncts().stream())
                .map(e -> ExpressionUtils.replace(e, innerOuterSlotMap))
                .collect(Collectors.toSet());
        Iterator<Expression> innerIterator = innerConjunctSet.iterator();
        // inner predicate should be the sub-set of outer predicate.
        while (innerIterator.hasNext()) {
            Expression innerExpr = innerIterator.next();
            Iterator<Expression> outerIterator = outerConjunctSet.iterator();
            while (outerIterator.hasNext()) {
                Expression outerExpr = outerIterator.next();
                if (ExpressionIdenticalChecker.INSTANCE.check(innerExpr, outerExpr)) {
                    // Volatile and NoneMovableFunction predicates must never
                    // be matched as inner-filter conjuncts: two syntactically
                    // identical calls are independent evaluations.
                    if (isVolatileOrNoneMovable(innerExpr)
                            || isVolatileOrNoneMovable(outerExpr)) {
                        continue;
                    }
                    innerIterator.remove();
                    outerIterator.remove();
                    // Remember this outer conjunct came from the inner subquery
                    // filter — it must go below the window, not above.
                    matchedInnerFilterConjuncts.add(outerExpr);
                    // Each inner conjunct matches at most one outer conjunct.
                    // Break to the next inner predicate to avoid matching the
                    // same inner conjunct against a second outer conjunct
                    // (which would call innerIterator.remove() twice and
                    // throw IllegalStateException).
                    break;
                }
            }
        }
        // now the expressions are all like 'expr op literal' or flipped, and whose expr is not correlated.
        return innerConjunctSet.isEmpty();
    }

    /**
     * Reject joins that are unsafe for the WinMagic window rewrite.
     * <p>
     * Only {@link JoinType#INNER_JOIN} and {@link JoinType#CROSS_JOIN} are
     * accepted.  Semi/anti joins output only one side and would cause the
     * window to reference slots not produced by its child; outer joins
     * introduce null-extended rows that corrupt {@code COUNT(*)} and other
     * aggregates (a null-padded outer-join row makes {@code COUNT(*) OVER (…)}
     * return 1 where the original scalar subquery would return 0).
     * <p>
     * Additionally, any ON clause condition is forbidden because the rule
     * cannot accurately pattern-match predicates between outer and inner scope
     * when the join carries its own condition.
     */
    private boolean checkJoin() {
        return outerPlans.stream()
                .filter(LogicalJoin.class::isInstance)
                .map(p -> (LogicalJoin<Plan, Plan>) p)
                .allMatch(j -> j.getJoinType().isInnerOrCrossJoin()
                        && !j.getOnClauseCondition().isPresent())
                && innerPlans.stream()
                .filter(LogicalJoin.class::isInstance)
                .map(p -> (LogicalJoin<Plan, Plan>) p)
                .allMatch(j -> j.getJoinType().isInnerOrCrossJoin()
                        && !j.getOnClauseCondition().isPresent());
    }

    /**
     * check inner and outer project to ensure no project except column pruning
     */
    private boolean checkProject() {
        return outerPlans.stream()
                .filter(LogicalProject.class::isInstance)
                .map(p -> (LogicalProject<Plan>) p)
                .allMatch(p -> p.getExpressions().stream().allMatch(SlotReference.class::isInstance))
                && innerPlans.stream()
                .filter(LogicalProject.class::isInstance)
                .map(p -> (LogicalProject<Plan>) p)
                .allMatch(p -> p.getExpressions().stream().allMatch(SlotReference.class::isInstance));
    }

    /**
     * check inner and outer relation
     * 1. outer table size - inner table size must equal to 1
     * 2. outer table list - inner table list should only remain 1 table
     * 3. the remaining table in step 2 should be correlated table for inner plan
     */
    private boolean checkRelation(List<Slot> correlatedSlots) {
        List<CatalogRelation> outerTables = outerPlans.stream().filter(CatalogRelation.class::isInstance)
                .map(CatalogRelation.class::cast)
                .collect(Collectors.toList());
        List<CatalogRelation> innerTables = innerPlans.stream().filter(CatalogRelation.class::isInstance)
                .map(CatalogRelation.class::cast)
                .collect(Collectors.toList());

        List<Long> outerIds = outerTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toList());
        List<Long> innerIds = innerTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toList());
        if (Sets.newHashSet(outerIds).size() != outerIds.size()
                || Sets.newHashSet(innerIds).size() != innerIds.size()) {
            return false;
        }
        if (outerIds.size() - innerIds.size() != 1) {
            return false;
        }
        innerIds.forEach(outerIds::remove);
        if (outerIds.size() != 1) {
            return false;
        }

        createSlotMapping(outerTables, innerTables);

        // Shared-table scans must have equivalent row-domain semantics.
        // Table.getId() equality alone is insufficient: wrappers
        // (RowBinlogTableWrapper, OlapTableStreamWrapper) share the base
        // table ID but read different rows.  Likewise, two scans of the
        // same base table can differ by @incr params, partition/tablet
        // selection, index, snapshot, or sample.
        if (!isSameScanDomain(outerTables, innerTables)) {
            return false;
        }

        // Identify and stash the sole outer-only table for downstream checks
        // and for rewrite().  checkRelation() already validated that exactly
        // one outer-only table exists.
        outerOnlyTable = outerTables.stream()
                .filter(node -> outerIds.contains(node.getTable().getId()))
                .findFirst().get();
        Preconditions.checkState(outerOnlyTable != null,
                "outerOnlyTable must be non-null after checkRelation validation");

        Set<ExprId> correlatedRelationOutput = outerTables.stream()
                .filter(node -> outerIds.contains(node.getTable().getId()))
                .map(LogicalRelation.class::cast)
                .map(LogicalRelation::getOutputExprIdSet).flatMap(Collection::stream).collect(Collectors.toSet());
        return correlatedSlots.stream().allMatch(e -> correlatedRelationOutput.contains(e.getExprId()));
    }

    /**
     * The correlated columns of the outer-only table must form a unique, non-null
     * key for the WinMagic window-function rewrite to be correct. Without unique
     * and non-null keys, the window function may aggregate over duplicated or
     * null-grouped outer rows, producing wrong results for aggregates like SUM
     * and COUNT.
     * <p>
     * In particular, nullable correlated keys are unsafe even with a uniqueness
     * guarantee: PARTITION BY groups all null-key outer rows into a single
     * partition.  Under a null-safe equality ({@code <=>}) correlation, those
     * rows can join the same null-key inner rows and multiply the window
     * aggregate, whereas the original scalar subquery is evaluated per outer row.
     * <p>
     * Uses {@link DataTrait#isUniqueAndNotNull(Set)} which covers OLAP key
     * metadata (PRIMARY_KEYS / UNIQUE_KEYS), declared constraints
     * (PRIMARY KEY / UNIQUE), and rejects nullable slots.
     */
    private boolean checkUniqueCorrelatedTable(List<Slot> correlatedSlots) {
        // outerOnlyTable was identified and stashed by checkRelation() which
        // runs before this method in the check() && chain, so it is guaranteed
        // non-null here.
        Preconditions.checkState(outerOnlyTable != null,
                "checkRelation() must run and set outerOnlyTable before "
                + "checkUniqueCorrelatedTable()");

        // Check uniqueness and non-nullability via DataTrait on the correlated (outer-only) table.
        // Must use isUniqueAndNotNull: nullable unique keys are unsafe for window-rewrite
        // because PARTITION BY groups all null-key rows together.
        DataTrait dataTrait = outerOnlyTable.getLogicalProperties().getTrait();
        return dataTrait.isUniqueAndNotNull(Sets.newHashSet(correlatedSlots));
    }

    private void createSlotMapping(List<CatalogRelation> outerTables, List<CatalogRelation> innerTables) {
        for (CatalogRelation outerTable : outerTables) {
            for (CatalogRelation innerTable : innerTables) {
                if (innerTable.getTable().getId() == outerTable.getTable().getId()) {
                    for (Slot innerSlot : innerTable.getOutput()) {
                        for (Slot outerSlot : outerTable.getOutput()) {
                            if (innerSlot.getName().equals(outerSlot.getName())) {
                                innerOuterSlotMap.put(innerSlot, outerSlot);
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    /**
     * Verify that every pair of outer/inner scans sharing the same table ID
     * has equivalent row-domain semantics.  Table.getId() equality alone is
     * insufficient — wrappers (RowBinlogTableWrapper, OlapTableStreamWrapper)
     * share the base table ID but read different rows, and even two scans of
     * the same base table can differ by {@code @incr} params, partition/tablet
     * selection, index, snapshot, or table sample.
     */
    private static boolean isSameScanDomain(List<CatalogRelation> outerTables,
                                            List<CatalogRelation> innerTables) {
        for (CatalogRelation outerTable : outerTables) {
            for (CatalogRelation innerTable : innerTables) {
                if (innerTable.getTable().getId() != outerTable.getTable().getId()) {
                    continue;
                }
                // Different table objects with the same ID means one is a
                // wrapper (RowBinlogTableWrapper, OlapTableStreamWrapper, …)
                // and the other is the base table.  They read different rows.
                if (innerTable.getTable() != outerTable.getTable()) {
                    return false;
                }
                // If both are LogicalOlapScan, compare scan-domain properties.
                if (innerTable instanceof LogicalOlapScan
                        && outerTable instanceof LogicalOlapScan) {
                    LogicalOlapScan innerScan = (LogicalOlapScan) innerTable;
                    LogicalOlapScan outerScan = (LogicalOlapScan) outerTable;
                    // Different selected index → different row set.
                    if (innerScan.getSelectedIndexId() != outerScan.getSelectedIndexId()) {
                        return false;
                    }
                    // Different scan params (e.g. @incr) → different row set.
                    if (!Objects.equals(
                            innerScan.getScanParams(), outerScan.getScanParams())) {
                        return false;
                    }
                    // Different table sample → different row set.
                    if (!Objects.equals(
                            innerScan.getTableSample(), outerScan.getTableSample())) {
                        return false;
                    }
                    // Different partition selection → different row set.
                    if (!innerScan.getSelectedPartitionIds()
                            .equals(outerScan.getSelectedPartitionIds())) {
                        return false;
                    }
                    // Different tablet selection → different row set.
                    if (!innerScan.getSelectedTabletIds()
                            .equals(outerScan.getSelectedTabletIds())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Rewrite a correlated scalar subquery into a Window function.
     *
     * <h3>Input Plan Shape</h3>
     * <pre>
     * Filter(pred_shared + pred_outer-only + pred_correlated)
     *   Apply(correlation: outer-only.col)
     *     Join / CrossJoin
     *       Scan(shared_tbl)   -- appears in both outer & inner
     *       Scan(outer-only_tbl)  -- only in outer, correlated table
     *     Aggregate(agg_func)
     *       Filter(inner_correlated_pred)
     *         Scan(shared_tbl)
     * </pre>
     *
     * <h3>Output Plan Shape</h3>
     * <pre>
     * Filter(pred_shared + window_comparison)  -- shared-only preds stay ABOVE window
     *   Window(agg_func OVER (PARTITION BY outer-only.cols))
     *     Filter(pred_outer-only + join_cond)  -- outer-only preds go BELOW window
     *       Join / CrossJoin
     *         Scan(shared_tbl)
     *         Scan(outer-only_tbl)
     * </pre>
     *
     * <h3>Key Correctness Rule</h3>
     * Predicates that reference ONLY shared-table columns (tables appearing in both
     * outer and inner plans) and were NOT matched against inner subquery filter
     * conjuncts MUST stay above the Window. Otherwise the window function would
     * see fewer rows than the original scalar subquery.
     * <p>
     * Conversely, predicates that were matched against inner subquery filter
     * conjuncts (tracked by {@link #checkFilter}) MUST stay below the Window, even
     * if they reference only shared-table columns.  These predicates are
     * semantically part of the inner aggregate's filter — placing them above the
     * window would let the window aggregate over rows that the original scalar
     * subquery excluded.
     *
     * <p>Example: Given fact(f) as shared table and dim(d) as outer-only table,
     * with the query:
     * <pre>
     *   SELECT ... FROM fact f, dim d
     *   WHERE f.k = d.k AND f.v > 6
     *     AND f.v * 2 > (SELECT SUM(f2.v) FROM fact f2 WHERE f2.k = d.k)
     * </pre>
     *
     * The predicate {@code f.v > 6} references only shared-table columns, so it
     * must stay above the window. The window computes SUM over ALL fact rows
     * per d.k, matching the original scalar subquery semantics.
     */
    private Plan rewrite(LogicalFilter<? extends Plan> filter, LogicalApply<Plan, Plan> apply) {
        Preconditions.checkArgument(apply.right() instanceof LogicalAggregate,
                "right child of Apply should be LogicalAggregate");
        LogicalAggregate<Plan> agg = (LogicalAggregate<Plan>) apply.right();

        // transform algorithm
        // first: find the slot in outer scope corresponding to the slot in aggregate function in inner scope.
        // second: find the aggregation function in inner scope, and replace it to window function, and the aggregate
        // slot is the slot in outer scope in the first step.
        // third: the expression containing aggregation function in inner scope will be the child of an alias,
        // so in the predicate between outer and inner, we change the alias to expression which is the alias's child,
        // and change the aggregation function to the alias of window function.

        // for example, in tpc-h Q17
        // window filter conjuncts is
        // cast(l_quantity#id1 as decimal(27, 9)) < `0.2 * avg(l_quantity)`#id2
        // and
        // 0.2 * avg(l_quantity#id3) as `0.2 * l_quantity`#id2
        // is aggregate's output expression
        // we change it to
        // cast(l_quantity#id1 as decimal(27, 9)) < 0.2 * `avg(l_quantity#id1) over(window)`#id4
        // and
        // avg(l_quantity#id1) over(window) as `avg(l_quantity#id1) over(window)`#id4

        // it's a simple case, but we may meet some complex cases in ut.
        // TODO: support compound predicate and multi apply node.

        Map<Boolean, Set<Expression>> conjuncts = filter.getConjuncts().stream()
                .collect(Collectors.groupingBy(conjunct -> Sets
                        .intersection(conjunct.getInputSlotExprIds(), agg.getOutputExprIdSet())
                        .isEmpty(), Collectors.toSet()));
        Set<Expression> correlatedConjuncts = conjuncts.get(false);
        if (correlatedConjuncts == null || correlatedConjuncts.size() != 1
                || !(correlatedConjuncts.iterator().next() instanceof ComparisonPredicate)) {
            //TODO: only support simple comparison predicate now
            return filter;
        }
        Expression windowFilterConjunct = correlatedConjuncts.iterator().next();
        windowFilterConjunct = PlanUtils.maybeCommuteComparisonPredicate(
                (ComparisonPredicate) windowFilterConjunct, apply.left());

        AggregateFunction function = functions.get(0);
        if (function instanceof NullableAggregateFunction) {
            // adjust agg function's nullable.
            function = ((NullableAggregateFunction) function).withAlwaysNullable(false);
        }

        WindowExpression windowFunction = createWindowFunction(apply.getCorrelationSlot(),
                (AggregateFunction) ExpressionUtils.replace(function, innerOuterSlotMap));
        NamedExpression windowFunctionAlias = new Alias(windowFunction);

        // build filter conjunct, get the alias of the agg output and extract its child.
        // then replace the agg to window function, then build conjunct
        // we ensure aggOut is Alias.
        NamedExpression aggOut = agg.getOutputExpressions().get(0);
        Expression aggOutExpr = aggOut.child(0);
        // change the agg function to window function alias.
        aggOutExpr = ExpressionUtils.replace(aggOutExpr, ImmutableMap
                .of(functions.get(0), windowFunctionAlias.toSlot()));

        windowFilterConjunct = ExpressionUtils.replace(windowFilterConjunct,
                ImmutableMap.of(aggOut.toSlot(), aggOutExpr));

        // Split uncorrelated conjuncts: predicates that reference ONLY shared
        // relation slots (tables appearing in both outer and inner plans) must
        // stay ABOVE the window. Otherwise the window function would see a
        // different set of rows than the original scalar subquery.
        //
        // For example, with fact(f) as shared table and dim(d) as outer-only:
        //   f.v > 6        → shared-only → must stay above the window
        //   d.tag > 0      → outer-only  → safe below the window
        //   f.k = d.k      → join cond   → needed below the window
        //
        // We find shared tables by comparing table IDs that appear in both
        // outer and inner plans, then collect ALL output slots of those
        // tables (not just columns referenced in the inner query).
        // outerOnlyTable is already identified by checkRelation().
        List<CatalogRelation> outerRels = outerPlans.stream()
                .filter(CatalogRelation.class::isInstance)
                .map(CatalogRelation.class::cast)
                .collect(Collectors.toList());
        Set<Long> innerTableIds = innerPlans.stream()
                .filter(CatalogRelation.class::isInstance)
                .map(r -> ((CatalogRelation) r).getTable().getId())
                .collect(Collectors.toSet());
        Set<ExprId> sharedOuterExprIds = outerRels.stream()
                .filter(r -> innerTableIds.contains(r.getTable().getId()))
                .flatMap(r -> r.getOutput().stream())
                .map(Slot::getExprId)
                .collect(Collectors.toSet());
        Set<Expression> uncorrelatedConjuncts = conjuncts.get(true);
        Set<Expression> belowWindowConjuncts = Sets.newHashSet();
        Set<Expression> aboveWindowConjuncts = Sets.newHashSet();
        if (uncorrelatedConjuncts != null) {
            for (Expression conj : uncorrelatedConjuncts) {
                // Conjuncts that were matched against inner subquery filter
                // conjuncts (tracked by checkFilter) must stay BELOW the
                // window.  They are semantically part of the inner aggregate's
                // filter, not extra outer-only predicates.  Placing them above
                // the window would let the window see more rows than the
                // original scalar subquery, producing wrong aggregate results.
                if (matchedInnerFilterConjuncts.contains(conj)) {
                    belowWindowConjuncts.add(conj);
                    continue;
                }
                // Volatile and NoneMovableFunction predicates must stay
                // ABOVE the window — pushing them below would change
                // evaluation frequency or move a side-effecting call.
                if (isVolatileOrNoneMovable(conj)) {
                    aboveWindowConjuncts.add(conj);
                    continue;
                }
                // Predicates referencing shared-table columns must stay
                // ABOVE the window; otherwise the window sees fewer rows
                // than the original scalar subquery.
                if (referencesSharedTable(conj, sharedOuterExprIds)) {
                    aboveWindowConjuncts.add(conj);
                } else {
                    belowWindowConjuncts.add(conj);
                }
            }
        }

        // Extract and classify conjuncts from any LogicalFilter nodes nested
        // inside the outer child (apply.left()).  Nested shared-table filters
        // (e.g. f.v > 6 under a CrossJoin) must be hoisted ABOVE the window;
        // otherwise the window would aggregate over a filtered subset of rows
        // while the original scalar subquery sees all rows for the key.
        //
        // Nested outer-only filters (e.g. d.tag > 0) are safe below and can
        // stay in place after the filters are stripped.
        //
        // Use a barrier-aware collector: filters below a retained unsafe
        // filter are NOT collected, matching stripOuterFilters() semantics.
        // Otherwise descendant predicates would be reinserted above the join
        // while the originals remain below the unsafe filter — evaluated
        // twice per joined row.
        //
        // Collecting stops at unsafe barriers (collectStrippableFilters line 773);
        // stripping also stops at unsafe barriers (stripOuterFilters line 638).
        // This barrier symmetry is intentional: safe filters underneath an
        // unsafe filter stay in place — they are neither hoisted nor removed.
        List<LogicalFilter<Plan>> nestedOuterFilters = collectStrippableFilters(apply.left());
        Set<ExprId> extractedConjunctExprIds = Sets.newHashSet();
        for (LogicalFilter<Plan> nf : nestedOuterFilters) {
            for (Expression conj : nf.getConjuncts()) {
                // Matched inner-filter conjuncts always go below.
                if (matchedInnerFilterConjuncts.contains(conj)) {
                    belowWindowConjuncts.add(conj);
                    extractedConjunctExprIds.addAll(conj.getInputSlotExprIds());
                    continue;
                }
                // Volatile / NoneMovable on a shared table would restrict
                // the window's input — reject the rewrite.
                if (isVolatileOrNoneMovable(conj)) {
                    if (nf.collect(CatalogRelation.class::isInstance).stream()
                            .map(r -> ((CatalogRelation) r).getTable().getId())
                            .anyMatch(innerTableIds::contains)) {
                        return filter;
                    }
                    continue;
                }
                if (referencesSharedTable(conj, sharedOuterExprIds)) {
                    aboveWindowConjuncts.add(conj);
                } else {
                    belowWindowConjuncts.add(conj);
                }
                extractedConjunctExprIds.addAll(conj.getInputSlotExprIds());
            }
        }
        // Strip all nested LogicalFilter nodes from the outer child so the
        // window operates on the unfiltered scan/join.
        Plan strippedOuterChild = stripOuterFilters(apply.left());

        // The window function's aggregate references shared-table slots
        // (e.g. f.v for SUM(f.v) after slot replacement).  A pruning
        // project inside apply.left() may have dropped those slots even
        // when there are no nested filters to extract.  Collect the
        // replaced aggregate's input slot ExprIds and include them in
        // the project-expansion set so ensureProjectOutput carries them
        // through.
        Set<ExprId> allNeededExprIds = Sets.newHashSet(extractedConjunctExprIds);
        allNeededExprIds.addAll(ExpressionUtils.replace(function, innerOuterSlotMap)
                .getInputSlotExprIds());
        strippedOuterChild = ensureProjectOutput(strippedOuterChild,
                allNeededExprIds);

        LogicalFilter<Plan> newFilter = filter.withConjunctsAndChild(
                belowWindowConjuncts, strippedOuterChild);
        LogicalWindow<Plan> newWindow = new LogicalWindow<>(ImmutableList.of(windowFunctionAlias), newFilter);

        // Combine shared-table predicates with the window comparison predicate above the window
        Set<Expression> topConjuncts = Sets.newHashSet(windowFilterConjunct);
        topConjuncts.addAll(aboveWindowConjuncts);
        LogicalFilter<Plan> windowFilter = new LogicalFilter<>(ImmutableSet.copyOf(topConjuncts), newWindow);
        return windowFilter;
    }

    /**
     * Ensure that every LogicalProject in the plan tree outputs all slots
     * referenced by extracted conjuncts (ExprIds in {@code neededExprIds}).
     * If a project prunes away a needed slot, the reinserted filter
     * predicates would have dangling references.
     *
     * <p>Recursively walks the tree depth-first; expands each project to
     * include any missing slots as simple identity projections (SlotReference →
     * SlotReference).  Recursing into the child before computing the current
     * project's {@code childOutput} ensures that stacked pruning projects
     * (e.g. {@code Project(k) → Project(k) → Scan(k, v)}) are expanded
     * bottom-up — the inner project is expanded first, and then the outer
     * project can see the expanded slots in its child output.
     */
    private Plan ensureProjectOutput(Plan plan, Set<ExprId> neededExprIds) {
        if (neededExprIds.isEmpty()) {
            return plan;
        }
        if (plan instanceof LogicalProject) {
            LogicalProject<Plan> project = (LogicalProject<Plan>) plan;
            // Recurse into the child first so stacked pruning projects
            // are expanded bottom-up.  The child's output after expansion
            // determines which slots the current project can pull through.
            Plan newChild = ensureProjectOutput(project.child(), neededExprIds);
            Set<ExprId> projectOutputExprIds = project.getOutputExprIdSet();
            Set<Slot> childOutput = newChild.getOutputSet();
            List<NamedExpression> newProjects = Lists.newArrayList(project.getProjects());
            boolean expanded = false;
            for (ExprId id : neededExprIds) {
                if (!projectOutputExprIds.contains(id)) {
                    for (Slot slot : childOutput) {
                        if (slot.getExprId().equals(id)) {
                            newProjects.add(slot);
                            expanded = true;
                            break;
                        }
                    }
                }
            }
            if (expanded) {
                return project.withProjectsAndChild(newProjects, newChild);
            }
            if (newChild != project.child()) {
                return project.withChildren(ImmutableList.of(newChild));
            }
            return plan;
        }
        if (plan.children().isEmpty()) {
            return plan;
        }
        List<Plan> newChildren = plan.children().stream()
                .map(c -> ensureProjectOutput(c, neededExprIds))
                .collect(Collectors.toList());
        return plan.withChildren(newChildren);
    }

    private WindowExpression createWindowFunction(List<Slot> correlatedSlots, AggregateFunction function) {
        // partition by clause is set by all the correlated slots.
        return new WindowExpression(function, ImmutableList.copyOf(correlatedSlots), Collections.emptyList());
    }

    /** Recursively strip deterministic, movable LogicalFilter nodes from the
     *  plan tree.  Filters containing volatile predicates (e.g. random()) or
     *  NoneMovableFunction predicates (e.g. assert_true()) are left in place —
     *  moving such predicates across a join/window changes their evaluation
     *  context and can alter query results. */
    private Plan stripOuterFilters(Plan plan) {
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            // Separate unsafe-to-move conjuncts from safe ones.
            // Volatile and NoneMovableFunction predicates stay at their
            // original position; deterministic movable predicates have been
            // extracted and can be stripped.
            Set<Expression> keepConjuncts = filter.getConjuncts().stream()
                    .filter(AggScalarSubQueryToWindowFunction::isVolatileOrNoneMovable)
                    .collect(Collectors.toSet());
            if (keepConjuncts.isEmpty()) {
                // All conjuncts are safe to strip.
                return stripOuterFilters(filter.child(0));
            }
            // An unsafe filter is a subtree movement barrier: stripping
            // filters from below it would change which rows reach the
            // side-effecting predicate and alter its evaluation domain.
            return new LogicalFilter<>(ImmutableSet.copyOf(keepConjuncts), filter.child(0));
        }
        if (plan.children().isEmpty()) {
            return plan;
        }
        return plan.withChildren(
                plan.children().stream().map(this::stripOuterFilters).collect(Collectors.toList()));
    }

    // ---- helper methods ----------------------------------------------------

    /** An expression that must not be moved or pruned by plan rewrites. */
    private static boolean isVolatileOrNoneMovable(Expression expr) {
        return expr.containsVolatileExpression()
                || expr.containsType(NoneMovableFunction.class);
    }

    /** Collect LogicalFilter nodes that are safe to strip, stopping
     *  recursion at unsafe-filter barriers (matching the semantics of
     *  {@link #stripOuterFilters}). */
    private static List<LogicalFilter<Plan>> collectStrippableFilters(Plan plan) {
        List<LogicalFilter<Plan>> result = Lists.newArrayList();
        collectStrippableFilters(plan, result);
        return result;
    }

    private static void collectStrippableFilters(Plan plan, List<LogicalFilter<Plan>> result) {
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            result.add((LogicalFilter<Plan>) plan);
            if (filter.getConjuncts().stream().anyMatch(
                    AggScalarSubQueryToWindowFunction::isVolatileOrNoneMovable)) {
                return; // unsafe filter — stop descending but still collected
            }
        }
        for (Plan child : plan.children()) {
            collectStrippableFilters(child, result);
        }
    }

    /** True when {@code expr} references any column of a shared table
     *  (a table appearing in both outer and inner plans). */
    private static boolean referencesSharedTable(Expression expr, Set<ExprId> sharedOuterExprIds) {
        for (ExprId id : expr.getInputSlotExprIds()) {
            if (sharedOuterExprIds.contains(id)) {
                return true;
            }
        }
        return false;
    }

    // ---- ExpressionIdenticalChecker ----------------------------------------

    /**
     * Structural-equality checker used by {@link #checkFilter} to decide
     * whether an inner-filter conjunct (after slot replacement) matches an
     * outer-filter conjunct.  The generic path delegates to
     * {@link Expression#equals(Object)} which includes all semantic
     * attributes (target type for Cast/TryCast, analyzer for Match, function
     * name for BoundFunction, etc.).  The only exception is
     * {@link ComparisonPredicate}, where we additionally accept the commuted
     * form ({@code a = b} ↔ {@code b = a}).
     */
    private static class ExpressionIdenticalChecker extends DefaultExpressionVisitor<Boolean, Expression> {
        public static final ExpressionIdenticalChecker INSTANCE = new ExpressionIdenticalChecker();

        public boolean check(Expression expression, Expression expression1) {
            return expression.accept(this, expression1);
        }

        @Override
        public Boolean visit(Expression expression, Expression expression1) {
            return expression.equals(expression1);
        }

        @Override
        public Boolean visitComparisonPredicate(ComparisonPredicate cp, Expression other) {
            return cp.equals(other) || cp.commute().equals(other);
        }
    }
}
