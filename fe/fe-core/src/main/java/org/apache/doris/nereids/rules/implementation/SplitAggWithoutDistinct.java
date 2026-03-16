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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBucketedHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**SplitAgg
 * only process agg without distinct function, split Agg into 2 phase: local agg and global agg
 * */
public class SplitAggWithoutDistinct extends OneImplementationRuleFactory {
    public static final SplitAggWithoutDistinct INSTANCE = new SplitAggWithoutDistinct();

    @Override
    public Rule build() {
        return logicalAggregate()
                .whenNot(Aggregate::hasDistinctFunc)
                .thenApplyMulti(ctx -> rewrite(ctx.root, ctx.connectContext))
                .toRule(RuleType.SPLIT_AGG_WITHOUT_DISTINCT);
    }

    private List<Plan> rewrite(LogicalAggregate<? extends Plan> aggregate, ConnectContext ctx) {
        ImmutableList.Builder<Plan> candidates = ImmutableList.builder();
        switch (ctx.getSessionVariable().aggPhase) {
            case 1:
                candidates.addAll(implementOnePhase(aggregate));
                break;
            case 2:
                candidates.addAll(splitTwoPhase(aggregate));
                break;
            default:
                candidates.addAll(implementOnePhase(aggregate));
                candidates.addAll(splitTwoPhase(aggregate));
                break;
        }
        // Add bucketed agg candidate when enabled and on single BE with GROUP BY keys
        candidates.addAll(implementBucketedPhase(aggregate, ctx));
        return candidates.build();
    }

    /**
     * select sum(a) from t group by b;
     * LogicalAggregate(group by b, outputExpr: sum(a), b)
     * ->
     * PhysicalHashAggregate(group by b, outputExpr: sum(a), b; AGG_PHASE:GLOBAL)
     * */
    private List<Plan> implementOnePhase(LogicalAggregate<? extends Plan> logicalAgg) {
        if (!logicalAgg.supportAggregatePhase(AggregatePhase.ONE)) {
            return ImmutableList.of();
        }
        List<NamedExpression> aggOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), expr -> {
                    if (!(expr instanceof AggregateFunction)) {
                        return expr;
                    }
                    return new AggregateExpression((AggregateFunction) expr, AggregateParam.GLOBAL_RESULT);
                }
        );
        AggregateParam param = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT, !skipRegulator(logicalAgg));
        return ImmutableList.of(new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), aggOutput,
                logicalAgg.getPartitionExpressions(), param,
                AggregateUtils.maybeUsingStreamAgg(logicalAgg.getGroupByExpressions(), param),
                null, logicalAgg.child()));
    }

    /**
     * select sum(a) from t group by b;
     * LogicalAggregate(group by b, outputExpr: sum(a), b)
     * ->
     * PhysicalHashAggregate(group by b, outputExpr: sum(a), b; AGG_PHASE:GLOBAL)
     *   +--PhysicalHashAggregate(group by b, outputExpr: partial_sum(a), b; AGG_PHASE:LOCAL)
     * */
    private List<Plan> splitTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        if (!aggregate.supportAggregatePhase(AggregatePhase.TWO)) {
            return ImmutableList.of();
        }
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> aggFunctionToAlias = new HashMap<>();
        for (Expression expr : aggregate.getOutputExpressions()) {
            expr.accept(new DefaultExpressionVisitor<Void, Map<String, String>>() {
                @Override
                public Void visitAggregateFunction(AggregateFunction expr, Map<String, String> sessionVars) {
                    AggregateExpression localAggFunc = new AggregateExpression(expr, inputToBufferParam);
                    if (sessionVars != null) {
                        aggFunctionToAlias.put(expr, new Alias(new SessionVarGuardExpr(localAggFunc, sessionVars)));
                    } else {
                        aggFunctionToAlias.put(expr, new Alias(localAggFunc));
                    }
                    return null;
                }

                @Override
                public Void visitSessionVarGuardExpr(SessionVarGuardExpr expr, Map<String, String> sessionVars) {
                    super.visit(expr, expr.getSessionVars());
                    return null;
                }
            }, null);
        }
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List) aggregate.getGroupByExpressions())
                .addAll(aggFunctionToAlias.values())
                .build();

        PhysicalHashAggregate<? extends Plan> localAgg = new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(),
                localAggOutput, inputToBufferParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), inputToBufferParam),
                null, aggregate.child());

        // global agg
        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalAggOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (!(expr instanceof AggregateFunction)) {
                        return expr;
                    }
                    Alias alias = aggFunctionToAlias.get(expr);
                    if (alias == null) {
                        return expr;
                    }
                    AggregateFunction aggFunc = (AggregateFunction) expr;
                    return new AggregateExpression(aggFunc, bufferToResultParam, alias.toSlot());
                });
        return ImmutableList.of(new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(),
                globalAggOutput, aggregate.getPartitionExpressions(), bufferToResultParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), bufferToResultParam),
                aggregate.getLogicalProperties(), localAgg));
    }

    /**
     * Implements bucketed hash aggregation for single-BE deployments.
     * Fuses two-phase aggregation into a single PhysicalBucketedHashAggregate operator,
     * eliminating exchange overhead and serialization/deserialization costs.
     *
     * Only generated when:
     * 1. enable_bucketed_hash_agg session variable is true
     * 2. Cluster has exactly one alive BE
     * 3. Aggregate has GROUP BY keys (no without-key aggregation)
     * 4. Aggregate functions support two-phase execution
     * 5. Data volume checks pass (min input rows, max group keys)
     */
    private List<Plan> implementBucketedPhase(LogicalAggregate<? extends Plan> aggregate, ConnectContext ctx) {
        if (!ctx.getSessionVariable().enableBucketedHashAgg) {
            return ImmutableList.of();
        }
        // Only for single-BE deployments
        int beNumber = Math.max(1, ctx.getEnv().getClusterInfo().getBackendsNumber(true));
        if (beNumber != 1) {
            return ImmutableList.of();
        }
        // Without-key aggregation not supported in initial version
        if (aggregate.getGroupByExpressions().isEmpty()) {
            return ImmutableList.of();
        }
        // Must support two-phase execution (same check as splitTwoPhase)
        if (!aggregate.supportAggregatePhase(AggregatePhase.TWO)) {
            return ImmutableList.of();
        }
        // Skip aggregates with no aggregate functions (pure GROUP BY dedup).
        // These are produced by DistinctAggregateRewriter as the bottom dedup phase.
        if (aggregate.getAggregateFunctions().isEmpty()) {
            return ImmutableList.of();
        }
        // Skip aggregates whose child group contains a LogicalAggregate.
        // This detects the top aggregate in a DISTINCT decomposition (e.g.,
        // COUNT(DISTINCT a) GROUP BY b is rewritten to COUNT(a) GROUP BY b
        // on top of GROUP BY a,b dedup). Bucketed agg does not support
        // DISTINCT aggregation in the initial version.
        if (childGroupContainsAggregate(aggregate)) {
            return ImmutableList.of();
        }
        // Skip when data is already distributed by the GROUP BY keys
        // (e.g., table bucketed by UserID, query GROUP BY UserID).
        // In this case the two-phase plan needs no exchange and is strictly
        // better than bucketed agg (no 256-bucket overhead, no merge phase).
        if (groupByKeysSatisfyDistribution(aggregate)) {
            return ImmutableList.of();
        }
        // Data-volume-based checks: control bucketed agg eligibility based on
        // estimated data scale, similar to ClickHouse's group_by_two_level_threshold
        // and group_by_two_level_threshold_bytes. This reduces reliance on
        // column-level statistics which may be inaccurate or missing.
        //
        // When statistics are unavailable (groupExpression absent or childStats null),
        // conservatively skip bucketed agg — without data volume information we cannot
        // make an informed decision, and the risk of choosing bucketed agg in a
        // high-cardinality scenario outweighs the potential benefit.
        if (!aggregate.getGroupExpression().isPresent()) {
            return ImmutableList.of();
        }
        GroupExpression ge = aggregate.getGroupExpression().get();
        Statistics childStats = ge.childStatistics(0);
        if (childStats == null) {
            return ImmutableList.of();
        }
        double rows = childStats.getRowCount();
        long minInputRows = ctx.getSessionVariable().bucketedAggMinInputRows;
        long maxGroupKeys = ctx.getSessionVariable().bucketedAggMaxGroupKeys;

        // Gate: minimum input rows.
        // When input data is too small, the overhead of initializing 256
        // per-bucket hash tables and the pipelined merge phase outweighs
        // the benefit of eliminating exchange. Skip bucketed agg.
        if (minInputRows > 0 && rows < minInputRows) {
            return ImmutableList.of();
        }

        // Gate: maximum estimated group keys (similar to ClickHouse's
        // group_by_two_level_threshold). When the number of distinct groups
        // is too large, the source-side merge must combine too many keys
        // across instances, and the merge cost dominates. Skip bucketed agg.
        Statistics aggStats = ge.getOwnerGroup().getStatistics();
        if (maxGroupKeys > 0 && aggStats != null && aggStats.getRowCount() > maxGroupKeys) {
            return ImmutableList.of();
        }

        // High-cardinality ratio checks (existing logic).
        // These complement the absolute thresholds above with relative checks:
        // 1. Single-column NDV check: if ANY GROUP BY key's NDV > rows * threshold,
        //    the combined NDV is at least that high.
        // 2. Aggregation ratio check: if estimated output rows > rows * threshold,
        //    merge cost dominates.
        double highCardThreshold = 0.3;
        for (Expression groupByKey : aggregate.getGroupByExpressions()) {
            ColumnStatistic colStat = childStats.findColumnStatistics(groupByKey);
            if (colStat != null && !colStat.isUnKnown() && colStat.ndv > rows * highCardThreshold) {
                return ImmutableList.of();
            }
        }
        if (aggStats != null && aggStats.getRowCount() > rows * highCardThreshold) {
            return ImmutableList.of();
        }
        // Build output expressions: rewrite AggregateFunction -> AggregateExpression with GLOBAL_RESULT param
        // (same as one-phase aggregation — raw input directly produces final result).
        List<NamedExpression> aggOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (!(expr instanceof AggregateFunction)) {
                        return expr;
                    }
                    return new AggregateExpression((AggregateFunction) expr, AggregateParam.GLOBAL_RESULT);
                }
        );
        return ImmutableList.of(new PhysicalBucketedHashAggregate<>(
                aggregate.getGroupByExpressions(), aggOutput,
                aggregate.getLogicalProperties(), aggregate.child()));
    }

    /**
     * Check if the child group of this aggregate contains a LogicalAggregate.
     * This is used to detect aggregates produced by DISTINCT decomposition rewrites
     * (e.g., DistinctAggregateRewriter, SplitMultiDistinctStrategy), where the original
     * DISTINCT aggregate is split into a top non-distinct aggregate over a bottom dedup aggregate.
     */
    private boolean childGroupContainsAggregate(LogicalAggregate<? extends Plan> aggregate) {
        if (!aggregate.getGroupExpression().isPresent()) {
            return false;
        }
        GroupExpression groupExpr = aggregate.getGroupExpression().get();
        if (groupExpr.arity() == 0) {
            return false;
        }
        Group childGroup = groupExpr.child(0);
        for (GroupExpression childGroupExpr : childGroup.getLogicalExpressions()) {
            if (childGroupExpr.getPlan() instanceof LogicalAggregate) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the GROUP BY keys of this aggregate are a superset of (or equal to)
     * the underlying OlapTable's hash distribution columns. When this is true,
     * the data is already correctly partitioned for the aggregation, so the
     * two-phase plan (local + global) requires no exchange and is strictly better
     * than bucketed agg (no 256-bucket overhead, no merge phase).
     *
     * Traverses the child group in the Memo to find a LogicalOlapScan,
     * walking through LogicalProject and LogicalFilter transparently.
     */
    private boolean groupByKeysSatisfyDistribution(LogicalAggregate<? extends Plan> aggregate) {
        if (!aggregate.getGroupExpression().isPresent()) {
            return false;
        }
        GroupExpression groupExpr = aggregate.getGroupExpression().get();
        if (groupExpr.arity() == 0) {
            return false;
        }
        OlapTable table = findOlapTableInGroup(groupExpr.child(0), 5);
        if (table == null) {
            return false;
        }
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        if (!(distributionInfo instanceof HashDistributionInfo)) {
            return false;
        }
        List<Column> distributionColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
        if (distributionColumns.isEmpty()) {
            return false;
        }
        // Collect GROUP BY column names (only direct SlotReference with original column info)
        Set<String> groupByColumnNames = new HashSet<>();
        for (Expression expr : aggregate.getGroupByExpressions()) {
            if (expr instanceof SlotReference) {
                SlotReference slot = (SlotReference) expr;
                if (slot.getOriginalColumn().isPresent()) {
                    groupByColumnNames.add(slot.getOriginalColumn().get().getName().toLowerCase());
                }
            }
        }
        // All distribution columns must appear in the GROUP BY keys
        for (Column column : distributionColumns) {
            if (!groupByColumnNames.contains(column.getName().toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Recursively search through a Memo Group to find a LogicalOlapScan,
     * walking through LogicalProject and LogicalFilter nodes.
     * Returns the OlapTable if found, null otherwise.
     * maxDepth prevents infinite recursion.
     */
    private OlapTable findOlapTableInGroup(Group group, int maxDepth) {
        if (maxDepth <= 0) {
            return null;
        }
        for (GroupExpression ge : group.getLogicalExpressions()) {
            Plan plan = ge.getPlan();
            if (plan instanceof LogicalOlapScan) {
                return ((LogicalOlapScan) plan).getTable();
            }
            if ((plan instanceof LogicalProject || plan instanceof LogicalFilter) && ge.arity() > 0) {
                OlapTable result = findOlapTableInGroup(ge.child(0), maxDepth - 1);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    private boolean shouldUseLocalAgg(LogicalAggregate<? extends Plan> aggregate) {
        Statistics aggStats = aggregate.getGroupExpression().get().getOwnerGroup().getStatistics();
        Statistics aggChildStats = aggregate.getGroupExpression().get().childStatistics(0);
        // if gbyNdv is high, should not use local agg
        double rows = aggChildStats.getRowCount();
        double gbyNdv = aggStats.getRowCount();
        return gbyNdv * 10 < rows;
    }

    private boolean skipRegulator(LogicalAggregate<? extends Plan> aggregate) {
        for (AggregateFunction aggregateFunction : aggregate.getAggregateFunctions()) {
            if (aggregateFunction.forceSkipRegulator(AggregatePhase.ONE)) {
                return true;
            }
        }
        return false;
    }
}
