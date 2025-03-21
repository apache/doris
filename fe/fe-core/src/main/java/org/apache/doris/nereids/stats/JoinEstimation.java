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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.Lists;

import java.util.HashMap;
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

    private static EqualPredicate normalizeEqualPredJoinCondition(EqualPredicate equal, Statistics rightStats) {
        boolean changeOrder = equal.left().getInputSlots().stream()
                .anyMatch(slot -> rightStats.findColumnStatistics(slot) != null);
        if (changeOrder) {
            return equal.commute();
        } else {
            return equal;
        }
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
        List<Double> unTrustEqualRatio = Lists.newArrayList();
        List<EqualPredicate> unTrustableCondition = Lists.newArrayList();
        boolean leftBigger = leftStats.getRowCount() > rightStats.getRowCount();
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
                                double rNdv = StatsMathUtil.nonZeroDivisor(eqRightColStats.ndv);
                                double lNdv = StatsMathUtil.nonZeroDivisor(eqLeftColStats.ndv);
                                if (leftBigger) {
                                    unTrustEqualRatio.add((rightStatsRowCount / rNdv)
                                            * Math.min(eqLeftColStats.ndv, eqRightColStats.ndv) / lNdv);
                                } else {
                                    unTrustEqualRatio.add((leftStatsRowCount / lNdv)
                                            * Math.min(eqLeftColStats.ndv, eqRightColStats.ndv) / rNdv);
                                }
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
            // in estimateJoinConditionSel, to get more accurate estimation.
            List<Double> joinConditionSels = trustableConditions.stream()
                    .map(expression -> estimateJoinConditionSel(crossJoinStats, expression))
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
            outputRowCount = Math.max(leftStats.getRowCount(), rightStats.getRowCount());
            Optional<Double> ratio = unTrustEqualRatio.stream().min(Double::compareTo);
            if (ratio.isPresent()) {
                outputRowCount = Math.max(1, outputRowCount * ratio.get());
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
                        double buildSel = Math.min(buildStats.getRowCount() / buildColStats.count, 1.0);
                        buildSel = Math.max(buildSel, UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND);
                        sel = Math.min(sel, buildSel);
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

    private static double estimateJoinConditionSel(Statistics crossJoinStats, Expression joinCond) {
        Statistics statistics = new FilterEstimation().estimate(joinCond, crossJoinStats);
        return statistics.getRowCount() / crossJoinStats.getRowCount();
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
            result.normalizeColumnStatistics();
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
            outputStats.normalizeColumnStatistics();
            return outputStats;
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
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
            double rowCount = Math.max(rightStats.getRowCount(), innerJoinStats.getRowCount());
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.FULL_OUTER_JOIN) {
            double rowCount = Math.max(leftStats.getRowCount(), innerJoinStats.getRowCount());
            rowCount = Math.max(rightStats.getRowCount(), rowCount);
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.CROSS_JOIN) {
            updateJoinConditionColumnStatistics(crossJoinStats, join);
            return crossJoinStats;
        }
        throw new AnalysisException("join type not supported: " + join.getJoinType());
    }

    /**
     * update join condition columns' ColumnStatistics, based on different join type.
     */
    private static void updateJoinConditionColumnStatistics(Statistics inputStats, Join join) {
        Map<Expression, ColumnStatistic> updatedCols = new HashMap<>();
        JoinType joinType = join.getJoinType();
        for (Expression expr : join.getEqualPredicates()) {
            EqualPredicate equalTo = (EqualPredicate) expr;
            ColumnStatistic leftColStats = ExpressionEstimation.estimate(equalTo.left(), inputStats);
            ColumnStatistic rightColStats = ExpressionEstimation.estimate(equalTo.right(), inputStats);
            double leftNdv = 1.0;
            double rightNdv = 1.0;
            boolean updateLeft = false;
            boolean updateRight = false;
            Expression eqLeft = equalTo.left();
            if (eqLeft instanceof Cast) {
                eqLeft = eqLeft.child(0);
            }
            Expression eqRight = equalTo.right();
            if (eqRight instanceof Cast) {
                eqRight = eqRight.child(0);
            }
            if (joinType == JoinType.INNER_JOIN) {
                leftNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
                rightNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
                updateLeft = true;
                updateRight = true;
            } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
                leftNdv = leftColStats.ndv;
                rightNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
                updateLeft = true;
                updateRight = true;
            } else if (joinType == JoinType.LEFT_SEMI_JOIN
                    || joinType == JoinType.LEFT_ANTI_JOIN
                    || joinType == JoinType.NULL_AWARE_LEFT_ANTI_JOIN) {
                leftNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
                updateLeft = true;
            } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
                leftNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
                rightNdv = rightColStats.ndv;
            } else if (joinType == JoinType.RIGHT_SEMI_JOIN
                    || joinType == JoinType.RIGHT_ANTI_JOIN) {
                rightNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
                updateRight = true;
            } else if (joinType == JoinType.FULL_OUTER_JOIN || joinType == JoinType.CROSS_JOIN) {
                leftNdv = leftColStats.ndv;
                rightNdv = rightColStats.ndv;
                updateLeft = true;
                updateRight = true;
            }

            if (updateLeft) {
                leftColStats = new ColumnStatisticBuilder(leftColStats).setNdv(leftNdv).build();
                updatedCols.put(eqLeft, leftColStats);
            }
            if (updateRight) {
                rightColStats = new ColumnStatisticBuilder(rightColStats).setNdv(rightNdv).build();
                updatedCols.put(eqRight, rightColStats);
            }
        }
        updatedCols.entrySet().stream().forEach(
                entry -> inputStats.addColumnStats(entry.getKey(), entry.getValue())
        );
    }

}
