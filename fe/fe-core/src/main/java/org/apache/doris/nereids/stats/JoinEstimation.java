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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.model.Bucket;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Histogram;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.statistics.model.StatisticsBuilder;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Estimate hash join stats.
 * TODO: Update other props in the ColumnStats properly.
 */
public class JoinEstimation {
    private static double DEFAULT_ANTI_JOIN_SELECTIVITY_COEFFICIENT = 0.3;
    private static double UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND = 0.5;
    private static double TRUSTABLE_CONDITION_SELECTIVITY_POW_FACTOR = 2.0;
    private static double UNTRUSTABLE_CONDITION_SELECTIVITY_LINEAR_FACTOR = 0.9;
    private static double TRUSTABLE_UNIQ_THRESHOLD = 0.9;
    private static double OUTER_JOIN_NULL_SUPPLELMENT_RATIO = 0.1;

    private static final double MIN_JOIN_KEY_SELECTIVITY = 1e-12;

    private static boolean shouldDecayRemainingUntrustConditions() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext == null || connectContext.getSessionVariable() == null
                || connectContext.getSessionVariable().isEnableLowConfidenceEqJoinRemainingConditionDecay();
    }

    private static boolean isMcvJoinEstimationEnabled() {
        ConnectContext ctx = ConnectContext.get();
        return ctx != null && ctx.getSessionVariable() != null && ctx.getSessionVariable().isEnableMcvJoinEstimation();
    }

    private static boolean isHistogramJoinEstimationEnabled() {
        ConnectContext ctx = ConnectContext.get();
        return ctx != null && ctx.getSessionVariable() != null
                && ctx.getSessionVariable().isEnableHistogramJoinEstimation();
    }

    private static void normalizeColumnStatistics(Statistics outputStats, Statistics inputStats) {
        outputStats.normalizeColumnStatistics(inputStats.getRowCount(), false);
    }

    /**
     * Equi-join key selectivity as sum_v p_L(v) * p_R(v) over hot values and histogram buckets.
     * A side without a histogram is treated as one unbounded bucket. When {@code joinedKeyStats}
     * is non-null, also fill the join-key column stats for the output.
     */
    private static double estimateJoinKeySelectivity(ColumnStatistic leftColStats, ColumnStatistic rightColStats,
            ColumnStatisticBuilder joinedKeyStats) {
        Histogram leftHistogram = getJoinHistogram(leftColStats);
        Histogram rightHistogram = getJoinHistogram(rightColStats);
        double leftNdv = Math.max(1, leftColStats.ndv);
        double rightNdv = Math.max(1, rightColStats.ndv);
        // Missing histogram: one unbounded bucket; selectivity falls back to 1/max(ndv).
        Map<Literal, Float> leftHotValues = leftHistogram == null ? Collections.emptyMap() : leftHistogram.mcv;
        Map<Literal, Float> rightHotValues = rightHistogram == null ? Collections.emptyMap() : rightHistogram.mcv;
        List<Bucket> leftBuckets = leftHistogram == null ? Collections.singletonList(
                new Bucket(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1, 0, leftNdv))
                : leftHistogram.hasMcv() ? leftHistogram.mcvBuckets : leftHistogram.buckets;
        List<Bucket> rightBuckets = rightHistogram == null ? Collections.singletonList(
                new Bucket(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1, 0, rightNdv))
                : rightHistogram.hasMcv() ? rightHistogram.mcvBuckets : rightHistogram.buckets;
        double leftCountToRatio = Math.max(0, 1 - leftHotValues.values().stream().mapToDouble(r -> r).sum())
                / StatsMathUtil.nonZeroDivisor(leftBuckets.stream().mapToDouble(b -> b.count).sum());
        double rightCountToRatio = Math.max(0, 1 - rightHotValues.values().stream().mapToDouble(r -> r).sum())
                / StatsMathUtil.nonZeroDivisor(rightBuckets.stream().mapToDouble(b -> b.count).sum());

        Map<Literal, Double> outputHotValues = new LinkedHashMap<>();
        double selectivity = 0;

        for (Map.Entry<Literal, Float> entry : leftHotValues.entrySet()) {
            Literal rightKey = StatisticsUtil.findHotValueKey(rightHotValues, entry.getKey());
            double rightRatio = rightKey != null ? rightHotValues.get(rightKey)
                    : rightHistogram != null ? getBucketValueRatio(rightBuckets, entry.getKey(), rightCountToRatio)
                    : getNdvValueRatio(rightColStats, entry.getKey(), Math.max(leftNdv, rightNdv));
            if (rightRatio > 0) {
                double ratio = entry.getValue() * rightRatio;
                outputHotValues.put(entry.getKey(), ratio);
                selectivity += ratio;
            }
        }
        for (Map.Entry<Literal, Float> entry : rightHotValues.entrySet()) {
            if (StatisticsUtil.findHotValueKey(leftHotValues, entry.getKey()) != null) {
                continue;
            }
            double leftRatio = leftHistogram != null
                    ? getBucketValueRatio(leftBuckets, entry.getKey(), leftCountToRatio)
                    : getNdvValueRatio(leftColStats, entry.getKey(), Math.max(leftNdv, rightNdv));
            if (leftRatio > 0) {
                double ratio = entry.getValue() * leftRatio;
                outputHotValues.put(entry.getKey(), ratio);
                selectivity += ratio;
            }
        }
        // Bucket-bucket overlap (sorted merge).
        List<Bucket> outputBuckets = Lists.newArrayList();
        DataType leftType = leftHistogram == null ? NullType.INSTANCE : leftHistogram.getDataType();
        DataType rightType = rightHistogram == null ? NullType.INSTANCE : rightHistogram.getDataType();
        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < leftBuckets.size() && rightIndex < rightBuckets.size()) {
            Bucket leftBucket = leftBuckets.get(leftIndex);
            Bucket rightBucket = rightBuckets.get(rightIndex);
            double overlapLower = Math.max(leftBucket.lower, rightBucket.lower);
            double overlapUpper = Math.min(leftBucket.upper, rightBucket.upper);
            if (overlapLower <= overlapUpper) {
                double leftFraction = leftHistogram == null ? Math.min(1, rightBucket.ndv / rightNdv)
                        : leftBucket.coveredFraction(overlapLower, overlapUpper, leftType);
                double rightFraction = rightHistogram == null ? Math.min(1, leftBucket.ndv / leftNdv)
                        : rightBucket.coveredFraction(overlapLower, overlapUpper, rightType);
                double leftRatio = leftBucket.count * leftCountToRatio * leftFraction;
                double rightRatio = rightBucket.count * rightCountToRatio * rightFraction;
                double leftBucketNdv = Math.max(1, leftBucket.ndv * leftFraction);
                double rightBucketNdv = Math.max(1, rightBucket.ndv * rightFraction);
                double ratio = leftRatio * rightRatio / Math.max(leftBucketNdv, rightBucketNdv);
                selectivity += ratio;
                outputBuckets.add(new Bucket(overlapLower, overlapUpper, ratio, 0,
                        Math.min(leftBucketNdv, rightBucketNdv)));
            }
            if (leftBucket.upper <= rightBucket.upper) {
                leftIndex++;
            }
            if (rightBucket.upper <= leftBucket.upper) {
                rightIndex++;
            }
        }
        selectivity = Math.min(1.0, Math.max(MIN_JOIN_KEY_SELECTIVITY, selectivity));
        if (joinedKeyStats == null) {
            return selectivity;
        }

        Map<Literal, Float> hotValues = Maps.newLinkedHashMap();
        for (Map.Entry<Literal, Double> entry : outputHotValues.entrySet()) {
            hotValues.put(entry.getKey(), (float) (entry.getValue() / selectivity));
        }
        Histogram joinedHistogram = null;
        if ((leftHistogram != null || rightHistogram != null) && !(outputBuckets.isEmpty() && hotValues.isEmpty())) {
            Type dataType = leftHistogram != null ? leftHistogram.dataType : rightHistogram.dataType;
            joinedHistogram = hotValues.isEmpty() ? new Histogram(dataType, 0, outputBuckets.size(), outputBuckets)
                    : new Histogram(dataType, 0, 0, Collections.emptyList(), hotValues, outputBuckets);
        }
        double joinedNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
        if (joinedHistogram != null) {
            joinedNdv = Math.min(joinedNdv, joinedHistogram.getNdv());
        }
        joinedKeyStats.setNdv(Math.max(1, joinedNdv))
                .setHotValues(hotValues.isEmpty() ? null : hotValues)
                .setHistogram(joinedHistogram);
        return selectivity;
    }

    /** Histogram and/or MCV used for join estimation; null if neither is available. */
    private static Histogram getJoinHistogram(ColumnStatistic colStats) {
        Histogram histogram = isHistogramJoinEstimationEnabled() ? colStats.histogram : null;
        if (isMcvJoinEstimationEnabled() && (histogram == null || !histogram.hasMcv())) {
            Histogram hotValueHistogram = Histogram.fromHotValues(colStats);
            histogram = hotValueHistogram != null ? hotValueHistogram : histogram;
        }
        return histogram;
    }

    private static double getNdvValueRatio(ColumnStatistic colStats, Literal value, double maxNdv) {
        double doubleValue = value.getDouble();
        return doubleValue < colStats.minValue || doubleValue > colStats.maxValue ? 0 : 1 / maxNdv;
    }

    private static double getBucketValueRatio(List<Bucket> buckets, Literal value, double countToRatio) {
        double doubleValue = value.getDouble();
        for (Bucket bucket : buckets) {
            if (doubleValue >= bucket.lower && doubleValue <= bucket.upper) {
                return bucket.count * countToRatio / Math.max(1, bucket.ndv);
            }
        }
        return 0;
    }

    private static double estimateEqualConditionSelectivity(EqualPredicate condition, Statistics leftStats,
            Statistics rightStats) {
        EqualPredicate equal = normalizeEqualPredJoinCondition(condition, rightStats);
        ColumnStatistic leftColStats = ExpressionEstimation.estimate(equal.left(), leftStats);
        ColumnStatistic rightColStats = ExpressionEstimation.estimate(equal.right(), rightStats);
        double leftNullRatio = getNullRatio(leftColStats, leftStats);
        double rightNullRatio = getNullRatio(rightColStats, rightStats);
        double selectivity = estimateJoinKeySelectivity(leftColStats, rightColStats, null)
                * (1 - leftNullRatio) * (1 - rightNullRatio);
        if (condition instanceof NullSafeEqual) {
            selectivity += leftNullRatio * rightNullRatio;
        }
        return selectivity;
    }

    private static double getNullRatio(ColumnStatistic colStats, Statistics stats) {
        return Statistics.getValidSelectivity(colStats.numNulls / StatsMathUtil.nonZeroDivisor(stats.getRowCount()));
    }

    private static EqualPredicate normalizeEqualPredJoinCondition(EqualPredicate equal, Statistics rightStats) {
        boolean changeOrder = equal.left().getInputSlots().stream()
                .anyMatch(slot -> rightStats.findColumnStatistics(slot) != null);
        if (changeOrder) {
            return equal.commute();
        } else {
            return equal;
        }
    }

    /**
     * Check whether any equal-join predicate has high-confidence column statistics
     * on at least one side, i.e. {@code ndv / rowCount > TRUSTABLE_UNIQ_THRESHOLD (0.9)}.
     *
     * A "trustable" equality means one side of the join key is nearly unique, so the
     * join selectivity estimation is reliable.  This is used by
     * {@code MemoStatsAndCostRecomputer.isTrustJoin()} to score join candidates for
     * the {@code trust_join_count} row-count aggregation policy, and by
     * {@code estimateInnerJoinWithEqualPredicate()} to separate high-confidence
     * equalities from low-confidence ones.
     *
     * Unknown column stats ({@code ColumnStatistic.UNKNOWN}) are rejected before the
     * NDV check because they carry {@code ndv=1} which could falsely pass the ratio
     * test on small tables.
     */
    static boolean hasTrustableEqualCondition(Statistics leftStats, Statistics rightStats, Join join) {
        if (join.getEqualPredicates().isEmpty()) {
            return false;
        }
        double rightStatsRowCount = StatsMathUtil.nonZeroDivisor(rightStats.getRowCount());
        double leftStatsRowCount = StatsMathUtil.nonZeroDivisor(leftStats.getRowCount());
        return join.getEqualPredicates().stream()
                .map(expression -> normalizeEqualPredJoinCondition((EqualPredicate) expression, rightStats))
                .anyMatch(equal -> {
                    ColumnStatistic eqLeftColStats = ExpressionEstimation.estimate(equal.left(), leftStats);
                    ColumnStatistic eqRightColStats = ExpressionEstimation.estimate(equal.right(), rightStats);
                    // Reject unknown column stats: ExpressionEstimation.visitSlotReference()
                    // returns ColumnStatistic.UNKNOWN (ndv=1, isUnKnown=true) when a slot
                    // has no stats.  An unknown column with ndv=1 could satisfy the NDV-ratio
                    // check for small row counts, but the equality should not be treated as
                    // trustable.
                    if (eqLeftColStats.isUnKnown || eqRightColStats.isUnKnown) {
                        return false;
                    }
                    return eqRightColStats.ndv / rightStatsRowCount > TRUSTABLE_UNIQ_THRESHOLD
                            || eqLeftColStats.ndv / leftStatsRowCount > TRUSTABLE_UNIQ_THRESHOLD;
                });
    }

    private static boolean joinConditionContainsUnknownColumnStats(Statistics leftStats,
            Statistics rightStats, Join join) {
        for (Expression expr : join.getEqualPredicates()) {
            for (Slot slot : expr.getInputSlots()) {
                ColumnStatistic colStats = leftStats.findColumnStatistics(slot);
                if (colStats == null) {
                    colStats = rightStats.findColumnStatistics(slot);
                }
                if (colStats == null || colStats.isUnKnown) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Statistics estimateInnerJoinWithEqualPredicate(Statistics leftStats,
            Statistics rightStats, Join join) {
        /*
         * When we estimate filter A=B,
         * if any side of equation, A or B, is almost unique, the confidence level of estimation is high.
         * But is both sides are not unique, the confidence level is very low.
         * The equations, whose confidence level is low, are called unTrustEquation.
         * In order to avoid error propagation, for unTrustEquations, we only use the biggest selectivity.
         */
        List<EqualPredicate> unTrustableCondition = Lists.newArrayList();
        double rightStatsRowCount = StatsMathUtil.nonZeroDivisor(rightStats.getRowCount());
        double leftStatsRowCount = StatsMathUtil.nonZeroDivisor(leftStats.getRowCount());
        List<EqualPredicate> trustableConditions = join.getEqualPredicates().stream()
                .map(expression -> (EqualPredicate) expression)
                .filter(
                        expression -> {
                            // since ndv is not accurate, if ndv/rowcount < TRUSTABLE_UNIQ_THRESHOLD,
                            // this column is regarded as unique.
                            EqualPredicate equal = normalizeEqualPredJoinCondition(expression, rightStats);
                            ColumnStatistic eqLeftColStats = ExpressionEstimation.estimate(equal.left(), leftStats);
                            ColumnStatistic eqRightColStats = ExpressionEstimation.estimate(equal.right(), rightStats);
                            boolean trustable = eqRightColStats.ndv / rightStatsRowCount > TRUSTABLE_UNIQ_THRESHOLD
                                    || eqLeftColStats.ndv / leftStatsRowCount > TRUSTABLE_UNIQ_THRESHOLD;
                            if (!trustable) {
                                unTrustableCondition.add(equal);
                            }
                            return trustable;
                        }
                ).collect(Collectors.toList());

        Statistics innerJoinStats;
        Statistics crossJoinStats = new StatisticsBuilder()
                .setRowCount(Math.max(1, leftStats.getRowCount()) * Math.max(1, rightStats.getRowCount()))
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();

        double outputRowCount;
        if (!trustableConditions.isEmpty()) {
            // TODO: strict pk-fk can use one-side stats instead of crossJoinStats
            // in estimateEqualConditionSelectivity, to get more accurate estimation.
            List<Double> joinConditionSels = trustableConditions.stream()
                    .map(expression -> estimateEqualConditionSelectivity(expression, leftStats, rightStats))
                    .sorted()
                    .collect(Collectors.toList());

            double sel = 1.0;
            double denominator = 1.0;
            for (Double joinConditionSel : joinConditionSels) {
                sel *= Math.pow(joinConditionSel, 1 / denominator);
                denominator *= TRUSTABLE_CONDITION_SELECTIVITY_POW_FACTOR;
            }
            outputRowCount = Math.max(1, crossJoinStats.getRowCount() * sel);
            outputRowCount = outputRowCount * Math.pow(UNTRUSTABLE_CONDITION_SELECTIVITY_LINEAR_FACTOR,
                    unTrustableCondition.size());
        } else {
            // Untrustable: take the most selective condition only.
            Optional<Double> mostSelective = unTrustableCondition.stream()
                    .map(cond -> estimateEqualConditionSelectivity(cond, leftStats, rightStats))
                    .min(Double::compareTo);
            if (mostSelective.isPresent()) {
                outputRowCount = Math.max(1, crossJoinStats.getRowCount() * mostSelective.get());
                if (shouldDecayRemainingUntrustConditions()) {
                    outputRowCount = Math.max(1, outputRowCount * Math.pow(
                            UNTRUSTABLE_CONDITION_SELECTIVITY_LINEAR_FACTOR,
                            Math.max(0, unTrustableCondition.size() - 1)));
                }
            } else {
                outputRowCount = Math.max(leftStats.getRowCount(), rightStats.getRowCount());
            }
        }
        innerJoinStats = crossJoinStats.withRowCountAndEnforceValid(outputRowCount);
        return innerJoinStats;
    }

    private static Statistics estimateInnerJoinWithoutEqualPredicate(Statistics leftStats,
            Statistics rightStats, Join join) {
        if (joinConditionContainsUnknownColumnStats(leftStats, rightStats, join)) {
            double rowCount = (leftStats.getRowCount() + rightStats.getRowCount());
            // We do more like the nested loop join with one rows than inner join
            if (leftStats.getRowCount() == 1 || rightStats.getRowCount() == 1) {
                rowCount *= 0.99;
            } else {
                rowCount *= 1.01;
            }
            rowCount = Math.max(1, rowCount);
            return new StatisticsBuilder()
                    .setRowCount(rowCount)
                    .putColumnStatistics(leftStats.columnStatistics())
                    .putColumnStatistics(rightStats.columnStatistics())
                    .build();
        }
        return new StatisticsBuilder()
                .setRowCount(Math.max(1, leftStats.getRowCount() * rightStats.getRowCount()))
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();
    }

    private static double computeSelectivityForBuildSideWhenColStatsUnknown(Statistics buildStats, Join join) {
        double sel = 1.0;
        for (Expression cond : join.getEqualPredicates()) {
            if (cond instanceof EqualTo) {
                EqualTo equal = (EqualTo) cond;
                if (equal.left() instanceof Slot && equal.right() instanceof Slot) {
                    ColumnStatistic buildColStats = buildStats.findColumnStatistics(equal.left());
                    if (buildColStats == null) {
                        buildColStats = buildStats.findColumnStatistics(equal.right());
                    }
                    if (buildColStats != null) {
                        if (buildColStats.count == 0) {
                            sel = 1;
                        } else {
                            double buildSel = Math.min(buildStats.getRowCount() / buildColStats.count, 1.0);
                            buildSel = Math.max(buildSel, UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND);
                            sel = Math.min(sel, buildSel);
                        }
                    }
                }
            }
        }
        return sel;
    }

    private static Statistics estimateInnerJoin(Statistics leftStats, Statistics rightStats, Join join) {
        if (joinConditionContainsUnknownColumnStats(leftStats, rightStats, join)) {
            double rowCount = Math.max(leftStats.getRowCount(), rightStats.getRowCount());
            rowCount = Math.max(1, rowCount);
            return new StatisticsBuilder()
                .setRowCount(rowCount)
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();
        }

        Statistics innerJoinStats;
        if (join.getEqualPredicates().isEmpty()) {
            innerJoinStats = estimateInnerJoinWithoutEqualPredicate(leftStats, rightStats, join);
        } else {
            innerJoinStats = estimateInnerJoinWithEqualPredicate(leftStats, rightStats, join);
        }

        if (!join.getOtherJoinConjuncts().isEmpty()) {
            FilterEstimation filterEstimation = new FilterEstimation();
            innerJoinStats = filterEstimation.estimate(
                    ExpressionUtils.and(join.getOtherJoinConjuncts()), innerJoinStats);
            if (innerJoinStats.getRowCount() <= 0) {
                innerJoinStats = new StatisticsBuilder(innerJoinStats).setRowCount(1).build();
            }
        }
        return innerJoinStats;
    }

    private static double estimateSemiOrAntiRowCountBySlotsEqual(Statistics leftStats,
            Statistics rightStats, Join join, EqualPredicate equalTo) {
        Expression eqLeft = equalTo.left();
        Expression eqRight = equalTo.right();
        ColumnStatistic probColStats = leftStats.findColumnStatistics(eqLeft);
        ColumnStatistic buildColStats;
        if (probColStats == null) {
            probColStats = leftStats.findColumnStatistics(eqRight);
            buildColStats = rightStats.findColumnStatistics(eqLeft);
        } else {
            buildColStats = rightStats.findColumnStatistics(eqRight);
        }
        if (probColStats == null || buildColStats == null) {
            return Double.POSITIVE_INFINITY;
        }

        double rowCount;
        if (join.getJoinType().isLeftSemiOrAntiJoin()) {
            double semiRowCount = StatsMathUtil.divide(leftStats.getRowCount() * buildColStats.ndv,
                    buildColStats.getOriginalNdv());
            if (join.getJoinType().isSemiJoin()) {
                rowCount = semiRowCount;
            } else {
                rowCount = Math.max(leftStats.getRowCount() - semiRowCount,
                        leftStats.getRowCount() * DEFAULT_ANTI_JOIN_SELECTIVITY_COEFFICIENT);
            }
        } else {
            //right semi or anti
            double semiRowCount = StatsMathUtil.divide(rightStats.getRowCount() * probColStats.ndv,
                    probColStats.getOriginalNdv());
            if (join.getJoinType().isSemiJoin()) {
                rowCount = semiRowCount;
            } else {
                rowCount = Math.max(rightStats.getRowCount() - semiRowCount,
                        rightStats.getRowCount() * DEFAULT_ANTI_JOIN_SELECTIVITY_COEFFICIENT);
            }
        }
        return Math.max(1, rowCount);
    }

    private static Statistics estimateSemiOrAnti(Statistics leftStats, Statistics rightStats,
            Statistics innerJoinStats, Join join) {
        if (joinConditionContainsUnknownColumnStats(leftStats, rightStats, join) || join.isMarkJoin()) {
            double sel = join.isMarkJoin() ? 1.0 : computeSelectivityForBuildSideWhenColStatsUnknown(rightStats, join);
            Statistics result;
            if (join.getJoinType().isLeftSemiOrAntiJoin()) {
                result = new StatisticsBuilder().setRowCount(leftStats.getRowCount() * sel)
                        .putColumnStatistics(leftStats.columnStatistics())
                        .putColumnStatistics(rightStats.columnStatistics())
                        .build();
            } else {
                //right semi or anti
                result = new StatisticsBuilder().setRowCount(rightStats.getRowCount() * sel)
                        .putColumnStatistics(leftStats.columnStatistics())
                        .putColumnStatistics(rightStats.columnStatistics())
                        .build();
            }
            normalizeColumnStatistics(result,
                    join.getJoinType().isLeftSemiOrAntiJoin() ? leftStats : rightStats);
            return result;
        }
        double rowCount = Double.POSITIVE_INFINITY;
        for (Expression conjunct : join.getEqualPredicates()) {
            double eqRowCount = estimateSemiOrAntiRowCountBySlotsEqual(leftStats, rightStats,
                    join, (EqualPredicate) conjunct);
            if (rowCount > eqRowCount) {
                rowCount = eqRowCount;
            }
        }
        if (Double.isInfinite(rowCount)) {
            //slotsEqual estimation failed, fall back to original algorithm
            double baseRowCount =
                    join.getJoinType().isLeftSemiOrAntiJoin() ? leftStats.getRowCount() : rightStats.getRowCount();
            rowCount = Math.min(innerJoinStats.getRowCount(), baseRowCount);
            return innerJoinStats.withRowCountAndEnforceValid(rowCount);
        } else {
            // TODO: tuning the new semi/anti estimation method
            /*double crossRowCount = Math.max(1, leftStats.getRowCount()) * Math.max(1, rightStats.getRowCount());
            double selectivity = innerJoinStats.getRowCount() / crossRowCount;
            selectivity = Statistics.getValidSelectivity(selectivity);
            double outputRowCount;
            StatisticsBuilder builder;

            if (join.getJoinType().isLeftSemiOrAntiJoin()) {
                outputRowCount = leftStats.getRowCount();
                builder = new StatisticsBuilder(leftStats);
            } else {
                outputRowCount = rightStats.getRowCount();
                builder = new StatisticsBuilder(rightStats);
            }
            if (join.getJoinType().isLeftSemiJoin() || join.getJoinType().isRightSemiJoin()) {
                outputRowCount *= selectivity;
            } else {
                outputRowCount *= 1 - selectivity;
                if (join.getJoinType().isLeftAntiJoin() && rightStats.getRowCount() < 1) {
                    outputRowCount = leftStats.getRowCount();
                } else if (join.getJoinType().isRightAntiJoin() && leftStats.getRowCount() < 1) {
                    outputRowCount = rightStats.getRowCount();
                } else {
                    outputRowCount = StatsMathUtil.normalizeRowCountOrNdv(outputRowCount);
                }
            }
            builder.setRowCount(outputRowCount);
            Statistics outputStats = builder.build();
            outputStats.normalizeColumnStatistics();
            return outputStats;*/
            StatisticsBuilder builder;
            if (join.getJoinType().isLeftSemiOrAntiJoin()) {
                builder = new StatisticsBuilder(leftStats);
                builder.setRowCount(rowCount);
            } else {
                //right semi or anti
                builder = new StatisticsBuilder(rightStats);
                builder.setRowCount(rowCount);
            }
            Statistics outputStats = builder.build();
            normalizeColumnStatistics(outputStats,
                    join.getJoinType().isLeftSemiOrAntiJoin() ? leftStats : rightStats);
            return outputStats;
        }
    }

    private static Statistics estimateAsofInnerJoin(Statistics leftStats, Statistics rightStats,
                                                 Statistics innerJoinStats, Join join) {
        if (joinConditionContainsUnknownColumnStats(leftStats, rightStats, join)) {
            double sel = computeSelectivityForBuildSideWhenColStatsUnknown(rightStats, join);
            Statistics result;
            if (join.getJoinType().isAsofLeftInnerJoin()) {
                result = new StatisticsBuilder().setRowCount(leftStats.getRowCount() * sel)
                        .putColumnStatistics(leftStats.columnStatistics())
                        .putColumnStatistics(rightStats.columnStatistics())
                        .build();
            } else {
                //asof right inner join
                result = new StatisticsBuilder().setRowCount(rightStats.getRowCount() * sel)
                        .putColumnStatistics(leftStats.columnStatistics())
                        .putColumnStatistics(rightStats.columnStatistics())
                        .build();
            }
            normalizeColumnStatistics(result,
                    join.getJoinType().isAsofLeftInnerJoin() ? leftStats : rightStats);
            return result;
        }
        double rowCount = Double.POSITIVE_INFINITY;
        for (Expression conjunct : join.getEqualPredicates()) {
            double eqRowCount = estimateAsofInnerJoinCountBySlotsEqual(leftStats, rightStats,
                    join, (EqualPredicate) conjunct);
            if (rowCount > eqRowCount) {
                rowCount = eqRowCount;
            }
        }
        if (Double.isInfinite(rowCount)) {
            //slotsEqual estimation failed, fall back to original algorithm
            double baseRowCount =
                    join.getJoinType().isAsofLeftInnerJoin() ? leftStats.getRowCount() : rightStats.getRowCount();
            rowCount = Math.min(innerJoinStats.getRowCount(), baseRowCount);
            return innerJoinStats.withRowCountAndEnforceValid(rowCount);
        } else {
            StatisticsBuilder builder;
            if (join.getJoinType().isAsofLeftInnerJoin()) {
                builder = new StatisticsBuilder(leftStats);
            } else {
                //asof right inner join
                builder = new StatisticsBuilder(rightStats);
            }
            builder.setRowCount(rowCount);
            Statistics outputStats = builder.build();
            normalizeColumnStatistics(outputStats,
                    join.getJoinType().isAsofLeftInnerJoin() ? leftStats : rightStats);
            return outputStats;
        }
    }

    private static double estimateAsofInnerJoinCountBySlotsEqual(Statistics leftStats,
            Statistics rightStats, Join join, EqualPredicate equalTo) {
        Expression eqLeft = equalTo.left();
        Expression eqRight = equalTo.right();
        ColumnStatistic probColStats = leftStats.findColumnStatistics(eqLeft);
        ColumnStatistic buildColStats;
        if (probColStats == null) {
            probColStats = leftStats.findColumnStatistics(eqRight);
            buildColStats = rightStats.findColumnStatistics(eqLeft);
        } else {
            buildColStats = rightStats.findColumnStatistics(eqRight);
        }
        if (probColStats == null || buildColStats == null) {
            return Double.POSITIVE_INFINITY;
        }

        double rowCount;
        if (join.getJoinType().isAsofLeftInnerJoin()) {
            rowCount = StatsMathUtil.divide(leftStats.getRowCount() * buildColStats.ndv,
                    buildColStats.getOriginalNdv());
        } else {
            // asof right inner join
            rowCount = StatsMathUtil.divide(rightStats.getRowCount() * probColStats.ndv,
                    probColStats.getOriginalNdv());
        }
        return Math.max(1, rowCount);
    }

    /**
     * outer join generates nulls.
     * for example, T1 left outer join T2,
     * in join results, columns from T2 contain nulls.
     * we estimate the numNulls as max(T1.row - inner_join_rows,  T1.row * 0.1)
     */
    private static void updateNumNullsForOuterJoin(Statistics crossJoinStats, Statistics innerJoinStats,
            Statistics probeStats, Statistics buildStats, double estJoinRowCount) {
        for (Map.Entry<Expression, ColumnStatistic> entry : buildStats.columnStatistics().entrySet()) {
            double numNulls = Math.max(probeStats.getRowCount() - innerJoinStats.getRowCount(),
                    probeStats.getRowCount() * OUTER_JOIN_NULL_SUPPLELMENT_RATIO);
            if (!entry.getValue().isUnKnown()) {
                if (entry.getValue().numNulls > 0) {
                    numNulls += entry.getValue().numNulls / buildStats.getRowCount() * estJoinRowCount;
                    numNulls = Math.max(1, numNulls);
                }
                ColumnStatistic colStats = new ColumnStatisticBuilder(entry.getValue())
                        .setNumNulls(numNulls)
                        .build();
                crossJoinStats.addColumnStats(entry.getKey(), colStats);
            }
        }
    }

    /**
     * estimate join
     */
    public static Statistics estimate(Statistics leftStats, Statistics rightStats, Join join) {
        JoinType joinType = join.getJoinType();
        Statistics crossJoinStats = new StatisticsBuilder()
                .setRowCount(Math.max(1, leftStats.getRowCount()) * Math.max(1, rightStats.getRowCount()))
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();
        Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
        if (joinType.isSemiOrAntiJoin()) {
            Statistics outputStats = estimateSemiOrAnti(leftStats, rightStats, innerJoinStats, join);
            updateJoinConditionColumnStatistics(outputStats, join);
            return outputStats;
        } else if (joinType == JoinType.INNER_JOIN) {
            updateJoinConditionColumnStatistics(innerJoinStats, join);
            return innerJoinStats;
        } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
            double rowCount = Math.max(leftStats.getRowCount(), innerJoinStats.getRowCount());
            updateNumNullsForOuterJoin(crossJoinStats, innerJoinStats, leftStats, rightStats, rowCount);
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
            double rowCount = Math.max(rightStats.getRowCount(), innerJoinStats.getRowCount());
            updateNumNullsForOuterJoin(crossJoinStats, innerJoinStats, rightStats, leftStats, rowCount);
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.FULL_OUTER_JOIN) {
            double rowCount = Math.max(leftStats.getRowCount(), innerJoinStats.getRowCount());
            rowCount = Math.max(rightStats.getRowCount(), rowCount);
            updateNumNullsForOuterJoin(crossJoinStats, innerJoinStats, leftStats, rightStats, rowCount);
            updateNumNullsForOuterJoin(crossJoinStats, innerJoinStats, rightStats, leftStats, rowCount);
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.ASOF_LEFT_OUTER_JOIN) {
            double rowCount = Math.max(leftStats.getRowCount(), 1);
            updateNumNullsForOuterJoin(crossJoinStats, innerJoinStats, leftStats, rightStats, rowCount);
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.ASOF_RIGHT_OUTER_JOIN) {
            double rowCount = Math.max(rightStats.getRowCount(), 1);
            updateNumNullsForOuterJoin(crossJoinStats, innerJoinStats, rightStats, leftStats, rowCount);
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType.isAsofInnerJoin()) {
            Statistics outputStats = estimateAsofInnerJoin(leftStats, rightStats, innerJoinStats, join);
            updateJoinConditionColumnStatistics(outputStats, join);
            return outputStats;
        } else if (joinType == JoinType.CROSS_JOIN) {
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats;
        }
        throw new AnalysisException("join type not supported: " + join.getJoinType());
    }

    /**
     * Merge join-key column stats into {@code builder}.
     * When MCV/histogram join estimation is on, reuse {@link #estimateJoinKeySelectivity};
     * otherwise keep min(ndv) and min hot-value ratios.
     */
    private static void mergeJoinKeyStatistics(ColumnStatistic leftColStats, ColumnStatistic rightColStats,
            ColumnStatisticBuilder builder) {
        if (isMcvJoinEstimationEnabled() || isHistogramJoinEstimationEnabled()) {
            estimateJoinKeySelectivity(leftColStats, rightColStats, builder);
            return;
        }
        builder.setNdv(Math.min(leftColStats.ndv, rightColStats.ndv));
        if (leftColStats.getHotValues() == null || rightColStats.getHotValues() == null) {
            return;
        }
        Map<Literal, Float> newHotValues = Maps.newHashMap();
        for (Map.Entry<Literal, Float> entry : leftColStats.getHotValues().entrySet()) {
            Float rightRatio = rightColStats.getHotValues().get(entry.getKey());
            if (rightRatio != null) {
                newHotValues.put(entry.getKey(), Math.min(entry.getValue(), rightRatio));
            }
        }
        builder.setHotValues(newHotValues.isEmpty() ? null : newHotValues);
    }

    /**
     * Update column stats of join keys for the given join type.
     */
    private static void updateJoinConditionColumnStatistics(Statistics inputStats, Join join) {
        Map<Expression, ColumnStatistic> updatedCols = new HashMap<>();
        JoinType joinType = join.getJoinType();
        for (Expression expr : join.getEqualPredicates()) {
            EqualPredicate equalTo = (EqualPredicate) expr;
            ColumnStatistic leftColStats = ExpressionEstimation.estimate(equalTo.left(), inputStats);
            ColumnStatistic rightColStats = ExpressionEstimation.estimate(equalTo.right(), inputStats);
            Expression eqLeft = equalTo.left();
            if (eqLeft instanceof Cast) {
                eqLeft = eqLeft.child(0);
            }
            Expression eqRight = equalTo.right();
            if (eqRight instanceof Cast) {
                eqRight = eqRight.child(0);
            }
            if (joinType.isInnerJoin() || joinType.isAsofInnerJoin()) {
                ColumnStatisticBuilder builder = new ColumnStatisticBuilder(leftColStats);
                mergeJoinKeyStatistics(leftColStats, rightColStats, builder);
                ColumnStatistic merged = builder.build();
                updatedCols.put(eqLeft, merged);
                updatedCols.put(eqRight, merged);
            } else if (joinType.isLeftOuterJoin() || joinType.isAsofLeftOuterJoin()) {
                ColumnStatisticBuilder rightBuilder = new ColumnStatisticBuilder(rightColStats);
                mergeJoinKeyStatistics(leftColStats, rightColStats, rightBuilder);
                updatedCols.put(eqRight, rightBuilder.build());
            } else if (joinType.isLeftSemiOrAntiJoin()) {
                ColumnStatisticBuilder leftBuilder = new ColumnStatisticBuilder(leftColStats);
                leftBuilder.setNdv(Math.min(leftColStats.ndv, rightColStats.ndv));
                updatedCols.put(eqLeft, leftBuilder.build());
            } else if (joinType.isRightOuterJoin() || joinType.isAsofRightOuterJoin()) {
                ColumnStatisticBuilder leftBuilder = new ColumnStatisticBuilder(leftColStats);
                mergeJoinKeyStatistics(leftColStats, rightColStats, leftBuilder);
                updatedCols.put(eqLeft, leftBuilder.build());
            } else if (joinType.isRightSemiOrAntiJoin()) {
                ColumnStatisticBuilder rightBuilder = new ColumnStatisticBuilder(rightColStats);
                rightBuilder.setNdv(Math.min(leftColStats.ndv, rightColStats.ndv));
                updatedCols.put(eqRight, rightBuilder.build());
            } else if (joinType.isFullOuterJoin() || joinType.isCrossJoin()) {
                // ignore
            }

        }
        updatedCols.entrySet().stream().forEach(
                entry -> inputStats.addColumnStats(entry.getKey(), entry.getValue())
        );
    }

}
