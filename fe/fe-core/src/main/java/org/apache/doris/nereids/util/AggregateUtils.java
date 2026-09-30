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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.SupportMultiDistinct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
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
     * This is the shared eligibility gate used by ChildrenPropertiesRegulator
     * (to allow the one-phase-GLOBAL+distribute pattern), CostModel (for cost
     * discount), and PhysicalPlanTranslator (for fusion into BucketedAggregationNode).
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
        // Scan ranges always go to the real alive backends, so count them directly
        // (getBackendsNumber() would return be_number_for_test).
        // Note: do not clamp to 1 — with zero backends bucketed agg must not be enabled.
        SystemInfoService clusterInfo = ctx.getEnv().getClusterInfo();
        List<Long> aliveBackendIds = clusterInfo.getAllBackendByCurrentCluster(true);
        if (aliveBackendIds.size() != 1) {
            return false;
        }
        // Smooth upgrade safety net: old BE processes do not recognize
        // BUCKETED_AGGREGATION_NODE plan node type
        for (Long beId : aliveBackendIds) {
            Backend be = clusterInfo.getBackend(beId);
            if (be != null && be.isSmoothUpgradeSrc()) {
                return false;
            }
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
}
