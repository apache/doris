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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.SupportMultiDistinct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Utils for aggregate
 */
public class AggregateUtils {
    public static final double LOW_CARDINALITY_THRESHOLD = 0.001;
    public static final double MID_CARDINALITY_THRESHOLD = 0.01;
    public static final double HIGH_CARDINALITY_THRESHOLD = 0.1;
    public static final int LOW_NDV_THRESHOLD = 1024;
    public static final int NDV_INSTANCE_BALANCE_MULTIPLIER = 512;

    public static AggregateFunction tryConvertToMultiDistinct(AggregateFunction function) {
        if (function instanceof SupportMultiDistinct && function.isDistinct()) {
            return ((SupportMultiDistinct) function).convertToMultiDistinct();
        }
        return function;
    }

    /**countDistinctMultiExprToCountIf*/
    public static Expression countDistinctMultiExprToCountIf(Count count) {
        Set<Expression> arguments = ImmutableSet.copyOf(count.getArguments());
        Expression countExpr = count.getArgument(arguments.size() - 1);
        for (int i = arguments.size() - 2; i >= 0; --i) {
            Expression argument = count.getArgument(i);
            If ifNull = new If(new IsNull(argument), NullLiteral.INSTANCE, countExpr);
            countExpr = assignNullType(ifNull);
        }
        return new Count(countExpr);
    }

    private static If assignNullType(If ifExpr) {
        If ifWithCoercion = (If) TypeCoercionUtils.processBoundFunction(ifExpr);
        Expression trueValue = ifWithCoercion.getArgument(1);
        if (trueValue instanceof Cast && trueValue.child(0) instanceof NullLiteral) {
            List<Expression> newArgs = Lists.newArrayList(ifWithCoercion.getArguments());
            // backend don't support null type, so we should set the type
            newArgs.set(1, new NullLiteral(((Cast) trueValue).getDataType()));
            return ifWithCoercion.withChildren(newArgs);
        }
        return ifWithCoercion;
    }

    public static boolean maybeUsingStreamAgg(List<Expression> groupExpressions, AggregateParam param) {
        ConnectContext ctx = ConnectContext.get();
        return ctx != null && !ctx.getSessionVariable().disableStreamPreaggregations
                && !groupExpressions.isEmpty()
                && param.aggPhase.isLocal();
    }

    /**
     * Check whether any expression in the collection has unknown statistics.
     * Statistics are considered unknown if they are null, isUnKnown(), or cannot be estimated.
     * Note: when returning false, hotValue may still be unknown; use hasUnknownStatistics(..., true)
     * if hot value presence is required.
     *
     * @param expressions expressions to check (e.g. group-by expressions)
     * @param inputStatistics input statistics
     * @return true if any expression has unknown statistics
     */
    public static boolean hasUnknownStatistics(Collection<Expression> expressions, Statistics inputStatistics) {
        return hasUnknownStatistics(expressions, inputStatistics, false);
    }

    /**
     * Check whether any expression has unknown statistics, optionally requiring hot values.
     * When requireHotValues is true, expressions without hotValues are also treated as unknown.
     *
     * @param expressions expressions to check
     * @param inputStatistics input statistics
     * @param requireHotValues if true, treat missing hotValues as unknown
     * @return true if any expression has unknown statistics (or missing hot values when requireHotValues)
     */
    public static boolean hasUnknownStatistics(Collection<Expression> expressions,
            Statistics inputStatistics, boolean requireHotValues) {
        for (Expression gbyExpr : expressions) {
            ColumnStatistic colStats = inputStatistics.findColumnStatistics(gbyExpr);
            if (colStats == null) {
                colStats = ExpressionEstimation.estimate(gbyExpr, inputStatistics);
            }
            if (colStats == null || colStats.isUnKnown()) {
                return true;
            }
            if (requireHotValues && colStats.hotValues == null) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsCountDistinctMultiExpr(LogicalAggregate<? extends Plan> aggregate) {
        return ExpressionUtils.deapAnyMatch(aggregate.getOutputExpressions(), expr ->
                expr instanceof Count && ((Count) expr).isDistinct() && expr.arity() > 1);
    }

    /**getAllKeySet*/
    public static Set<NamedExpression> getAllKeySet(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> distinctArguments = getDistinctNamedExpr(aggregate);
        Set<NamedExpression> groupBySet = getGroupBySetNamedExpr(aggregate);
        return ImmutableSet.<NamedExpression>builder()
                .addAll(groupBySet)
                .addAll(distinctArguments)
                .build();
    }

    /**getGroupBySetNamedExpr*/
    public static Set<NamedExpression> getGroupBySetNamedExpr(LogicalAggregate<? extends Plan> aggregate) {
        return aggregate.getGroupByExpressions().stream()
                .filter(NamedExpression.class::isInstance)
                .map(NamedExpression.class::cast)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**getDistinctNamedExpr*/
    public static Set<NamedExpression> getDistinctNamedExpr(LogicalAggregate<? extends Plan> aggregate) {
        return aggregate.getAggregateFunctions().stream()
                .filter(AggregateFunction::isDistinct)
                .flatMap(aggFunc -> aggFunc.getArguments().stream())
                .filter(NamedExpression.class::isInstance)
                .map(NamedExpression.class::cast)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Check if order keys are identical to group-by keys (1-1 mapping, same order).
     * Shared utility used by both PushTopnToAgg and SplitAggWithoutDistinct.
     */
    public static boolean isOrderKeysMatchGroupKeys(List<OrderKey> orderKeys,
            List<Expression> groupByKeys) {
        if (orderKeys.size() != groupByKeys.size()) {
            return false;
        }
        for (int i = 0; i < groupByKeys.size(); i++) {
            if (!groupByKeys.get(i).equals(orderKeys.get(i).getExpr())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check the basic environmental conditions for bucketed hash aggregation.
     * This is the environment part of the shared eligibility gate; the physical
     * plan shape part is {@link #isBucketedHashAggFusible(PhysicalHashAggregate)} and
     * {@link #isBucketedHashAggFusible(PhysicalHashAggregate, DistributionSpec)},
     * which ChildrenPropertiesRegulator (to allow the one-phase-GLOBAL+distribute
     * pattern), ChildOutputPropertyDeriver, CostModel (for the cost discount) and
     * PhysicalPlanTranslator (for fusion into BucketedAggregationNode) all use.
     *
     * @return true if the session variable is enabled, there is exactly one alive BE,
     *         spill and the query cache are disabled, no smooth upgrade is in progress,
     *         the aggregate has GROUP BY keys and contains no user-defined aggregate function.
     */
    public static boolean isBucketedHashAggEnabled(Aggregate<? extends Plan> aggregate) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return false;
        }
        if (!ctx.getSessionVariable().enableBucketedHashAgg) {
            return false;
        }
        // Must have GROUP BY keys (without-key aggregation not supported)
        if (aggregate.getGroupByExpressions().isEmpty()) {
            return false;
        }
        // Bucketed agg has no spill support. Keep the regular (spillable) aggregation
        // when spill is enabled, otherwise a high-cardinality GROUP BY could hit the
        // memory limit instead of spilling.
        if (ctx.getSessionVariable().enableSpill || ctx.getSessionVariable().enableForceSpill) {
            return false;
        }
        // The query cache is built on the LOCAL AggregationNode above the scan, and neither
        // the FE normalizer nor the BE cache operators know BucketedAggregationNode. Keep the
        // regular aggregation when the query cache is enabled, otherwise the query would
        // silently lose its cache point.
        if (ctx.getSessionVariable().getEnableQueryCache()) {
            return false;
        }
        // be_number_for_test can only disable bucketed agg (to test the multi-BE plan),
        // never bypass the single-BE gate below.
        int beNumberForTest = ctx.getSessionVariable().getBeNumberForTest();
        if (beNumberForTest > 0 && beNumberForTest != 1) {
            return false;
        }
        // Correctness gate: single-BE only (cross-BE in-memory merge is impossible).
        if (getBucketedHashAggBackend(ctx) == null) {
            return false;
        }
        // Bucketed agg merges the live states built by different sink instances
        // directly, without serializing them. Java / Python UDAFs can only merge a
        // state that was deserialized by the merging evaluator (the Java UDAF
        // executor place and the Python UDAF serialized buffer are only set up on
        // that path), so they must stay on the regular aggregation path.
        if (aggregate.getAggregateFunctions().stream().anyMatch(Udf.class::isInstance)) {
            return false;
        }
        return true;
    }

    /**
     * Returns the backend that a bucketed hash aggregation of this query would run on:
     * the only alive backend of the current cluster. Returns null when the number of
     * alive backends is not exactly one, or when that backend is a smooth-upgrade source
     * (its old BE process does not know BUCKETED_AGGREGATION_NODE).
     * <p>
     * Scan ranges always go to the real alive backends, so they are counted directly
     * (getBackendsNumber() would return be_number_for_test), and zero backends must not
     * be clamped to one. PhysicalPlanTranslator reads the backend again when it fuses
     * an aggregate and pins the scan of the fused fragment to it, so a backend that
     * becomes alive after this check cannot receive tablets of that fragment.
     */
    public static Backend getBucketedHashAggBackend(ConnectContext ctx) {
        SystemInfoService clusterInfo = ctx.getEnv().getClusterInfo();
        List<Long> aliveBackendIds = clusterInfo.getAllBackendByCurrentCluster(true);
        if (aliveBackendIds.size() != 1) {
            return null;
        }
        Backend be = clusterInfo.getBackend(aliveBackendIds.get(0));
        if (be == null || be.isSmoothUpgradeSrc()) {
            return null;
        }
        return be;
    }

    /**
     * Check whether a physical hash aggregate has the shape that PhysicalPlanTranslator
     * fuses into BucketedAggregationNode, as far as the aggregate itself is concerned:
     * the environment gate above passes, it is the one-phase GLOBAL INPUT_TO_RESULT
     * aggregate, none of its functions produces a partial buffer, all of them support
     * two-phase execution and no TopN was pushed into it. The translator additionally
     * requires a distribute child on exactly the GROUP BY keys over a unary single-scan
     * pipeline, which is not visible here.
     * <p>
     * The regulator, the output property deriver and the cost model must use this
     * gate rather than {@link #isBucketedHashAggEnabled(Aggregate)} alone: an aggregate
     * that passes the environment gate but not the shape gate (for example the GLOBAL
     * INPUT_TO_RESULT dedup aggregate of a mixed DISTINCT / non-DISTINCT query, whose
     * non-distinct functions run in INPUT_TO_BUFFER mode) is translated into a regular
     * AggregationNode that keeps the exchange, so it must not receive the bucketed
     * cost discount or the one-phase-with-distribute exemption. Callers that know the
     * distribution of the aggregate's child use
     * {@link #isBucketedHashAggFusible(PhysicalHashAggregate, DistributionSpec)}.
     */
    public static boolean isBucketedHashAggFusible(PhysicalHashAggregate<? extends Plan> aggregate) {
        // Must be one-phase: GLOBAL + INPUT_TO_RESULT. Checked before the environment
        // gate, which asks the cluster for its alive backends, because every aggregate
        // alternative in the memo passes through here.
        if (aggregate.getAggPhase() != AggPhase.GLOBAL
                || aggregate.getAggMode() != AggMode.INPUT_TO_RESULT) {
            return false;
        }
        if (!isBucketedHashAggEnabled(aggregate)) {
            return false;
        }
        // BucketedAggregationNode always finalizes into the output tuple slot
        // types (need_finalize=true, isPartial=false), so fusing an aggregate
        // whose functions produce buffers (partial) would fail the BE
        // result-type check: the slot type of a buffer-producing function is
        // Varchar while the function's final return type (e.g. DOUBLE for
        // stddev) is what insert_result_into writes. The one-phase GLOBAL
        // dedup aggregate of a 3-phase DISTINCT plan has exactly this shape —
        // the node itself is INPUT_TO_RESULT but its non-distinct functions
        // run in INPUT_TO_BUFFER mode — and must stay on the regular
        // AggregationNode path, which serializes when isPartial.
        if (containsPartialAggFunction(aggregate)) {
            return false;
        }
        // Exclude one-phase-only aggregates (e.g. GROUP_CONCAT with ORDER BY).
        // BucketedAggregationNode has no sort-info field, so fusing would drop
        // the aggregate ORDER BY contract. Only aggregates supporting two-phase
        // execution can be safely fused.
        if (!supportsTwoPhaseAgg(aggregate)) {
            return false;
        }
        // BucketedAggregationNode does not support sortByGroupKey (PushTopnToAgg
        // optimization). Regular AggregationNode fills sort info; fusing would drop it.
        return aggregate.getTopnPushInfo() == null;
    }

    /**
     * {@link #isBucketedHashAggFusible(PhysicalHashAggregate)} plus the translator's
     * condition on the aggregate's child: it is hash-distributed by exactly the GROUP BY
     * keys. With agg_shuffle_use_parent_key the aggregate can also ask its child for the
     * parent's keys, a strict subset of the GROUP BY keys. The parent then consumes the
     * aggregate without an exchange and relies on that distribution, which a fused
     * aggregate does not preserve, so that alternative stays a regular aggregate over a
     * raw-row exchange and must not get the one-phase-with-distribute exemption.
     *
     * @param childDistribution the distribution of the aggregate's child
     */
    public static boolean isBucketedHashAggFusible(PhysicalHashAggregate<? extends Plan> aggregate,
            DistributionSpec childDistribution) {
        if (!(childDistribution instanceof DistributionSpecHash)) {
            return false;
        }
        List<ExprId> distributeKeys = ((DistributionSpecHash) childDistribution).getOrderedShuffledColumns();
        List<ExprId> groupByKeys = aggregate.getGroupByExpressions().stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .map(SlotReference::getExprId)
                .collect(Collectors.toList());
        return distributeKeys.equals(groupByKeys) && isBucketedHashAggFusible(aggregate);
    }

    /**
     * Check whether all aggregate functions in this physical hash aggregate
     * support two-phase execution. One-phase-only aggregates (e.g. GROUP_CONCAT
     * with ORDER BY) cannot be bucketed because BucketedAggregationNode does not
     * carry sort-info metadata (aggSortInfos); fusing them would drop the
     * aggregate ORDER BY contract and produce unordered results.
     */
    public static boolean supportsTwoPhaseAgg(PhysicalHashAggregate<? extends Plan> aggregate) {
        for (NamedExpression o : aggregate.getOutputExpressions()) {
            AtomicBoolean foundOnePhaseOnly = new AtomicBoolean(false);
            o.foreach(c -> {
                if (c instanceof OrderExpression) {
                    // Any aggregate function with an internal ORDER BY
                    // (e.g. GROUP_CONCAT(... ORDER BY ...)) needs sort-info
                    // metadata, which BucketedAggregationNode does not carry.
                    foundOnePhaseOnly.set(true);
                    return true;
                }
                if (c instanceof AggregateExpression) {
                    AggregateFunction func = ((AggregateExpression) c).getFunction();
                    if (!func.supportAggregatePhase(AggregatePhase.TWO)) {
                        foundOnePhaseOnly.set(true);
                        return true;
                    }
                }
                return false;
            });
            if (foundOnePhaseOnly.get()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether the aggregate's output contains any buffer-producing
     * (partial) aggregate function, i.e. an AggregateExpression in a mode with
     * productAggregateBuffer=true. BucketedAggregationNode cannot carry such
     * functions: it always finalizes into the output tuple slot types, while a
     * buffer-producing function's slot type is the serialized Varchar type
     * (AggregateExpression.getDataType) — writing the final result (e.g. DOUBLE
     * for stddev) into that String column fails the BE result-type check.
     */
    public static boolean containsPartialAggFunction(PhysicalHashAggregate<? extends Plan> aggregate) {
        for (NamedExpression o : aggregate.getOutputExpressions()) {
            AtomicBoolean foundPartial = new AtomicBoolean(false);
            o.foreach(c -> {
                if (c instanceof AggregateExpression) {
                    if (((AggregateExpression) c).getAggregateParam().aggMode.productAggregateBuffer) {
                        foundPartial.set(true);
                    }
                    return true;
                }
                return false;
            });
            if (foundPartial.get()) {
                return true;
            }
        }
        return false;
    }
}
