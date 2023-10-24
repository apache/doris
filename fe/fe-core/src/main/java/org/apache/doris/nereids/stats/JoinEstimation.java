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

    private static EqualTo normalizeHashJoinCondition(EqualTo equalTo, Statistics leftStats, Statistics rightStats) {
        boolean changeOrder = equalTo.left().getInputSlots().stream().anyMatch(
                slot -> rightStats.findColumnStatistics(slot) != null
        );
        if (changeOrder) {
            return new EqualTo(equalTo.right(), equalTo.left());
        } else {
            return equalTo;
        }
    }

    private static boolean hashJoinConditionContainsUnknownColumnStats(Statistics leftStats,
            Statistics rightStats, Join join) {
        for (Expression expr : join.getHashJoinConjuncts()) {
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

    private static Statistics estimateHashJoin(Statistics leftStats, Statistics rightStats, Join join) {
        /*
         * When we estimate filter A=B,
         * if any side of equation, A or B, is almost unique, the confidence level of estimation is high.
         * But is both sides are not unique, the confidence level is very low.
         * The equations, whose confidence level is low, are called unTrustEquation.
         * In order to avoid error propagation, for unTrustEquations, we only use the biggest selectivity.
         */
        List<Double> unTrustEqualRatio = Lists.newArrayList();
        List<EqualTo> unTrustableCondition = Lists.newArrayList();
        boolean leftBigger = leftStats.getRowCount() > rightStats.getRowCount();
        double rightStatsRowCount = StatsMathUtil.nonZeroDivisor(rightStats.getRowCount());
        double leftStatsRowCount = StatsMathUtil.nonZeroDivisor(leftStats.getRowCount());
        List<EqualTo> trustableConditions = join.getHashJoinConjuncts().stream()
                .map(expression -> (EqualTo) expression)
                .filter(
                        expression -> {
                            // since ndv is not accurate, if ndv/rowcount < almostUniqueThreshold,
                            // this column is regarded as unique.
                            double almostUniqueThreshold = 0.9;
                            EqualTo equal = normalizeHashJoinCondition(expression, leftStats, rightStats);
                            ColumnStatistic eqLeftColStats = ExpressionEstimation.estimate(equal.left(), leftStats);
                            ColumnStatistic eqRightColStats = ExpressionEstimation.estimate(equal.right(), rightStats);
                            boolean trustable = eqRightColStats.ndv / rightStatsRowCount > almostUniqueThreshold
                                    || eqLeftColStats.ndv / leftStatsRowCount > almostUniqueThreshold;
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
            List<Double> joinConditionSels = trustableConditions.stream()
                    .map(expression -> estimateJoinConditionSel(crossJoinStats, expression))
                    .sorted()
                    .collect(Collectors.toList());

            double sel = 1.0;
            double denominator = 1.0;
            for (Double joinConditionSel : joinConditionSels) {
                sel *= Math.pow(joinConditionSel, 1 / denominator);
                denominator *= 2;
            }
            outputRowCount = Math.max(1, crossJoinStats.getRowCount() * sel);
            outputRowCount = outputRowCount * Math.pow(0.9, unTrustableCondition.size());
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

    private static Statistics estimateNestLoopJoin(Statistics leftStats, Statistics rightStats, Join join) {
        return new StatisticsBuilder()
                .setRowCount(Math.max(1, leftStats.getRowCount() * rightStats.getRowCount()))
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();
    }

    private static Statistics estimateInnerJoin(Statistics leftStats, Statistics rightStats, Join join) {
        if (hashJoinConditionContainsUnknownColumnStats(leftStats, rightStats, join)) {
            double rowCount = Math.max(leftStats.getRowCount(), rightStats.getRowCount());
            rowCount = Math.max(1, rowCount);
            return new StatisticsBuilder()
                    .setRowCount(rowCount)
                    .putColumnStatistics(leftStats.columnStatistics())
                    .putColumnStatistics(rightStats.columnStatistics())
                    .build();
        }

        Statistics innerJoinStats;
        if (join.getHashJoinConjuncts().isEmpty()) {
            innerJoinStats = estimateNestLoopJoin(leftStats, rightStats, join);
        } else {
            innerJoinStats = estimateHashJoin(leftStats, rightStats, join);
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
            Statistics rightStats, Join join, EqualTo equalTo) {
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

    private static Statistics estimateSemiOrAnti(Statistics leftStats, Statistics rightStats, Join join) {
        if (hashJoinConditionContainsUnknownColumnStats(leftStats, rightStats, join)) {
            if (join.getJoinType().isLeftSemiOrAntiJoin()) {
                return new StatisticsBuilder().setRowCount(leftStats.getRowCount())
                        .putColumnStatistics(leftStats.columnStatistics())
                        .putColumnStatistics(rightStats.columnStatistics())
                        .build();
            } else {
                //right semi or anti
                return new StatisticsBuilder().setRowCount(rightStats.getRowCount())
                        .putColumnStatistics(leftStats.columnStatistics())
                        .putColumnStatistics(rightStats.columnStatistics())
                        .build();
            }
        }
        double rowCount = Double.POSITIVE_INFINITY;
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            double eqRowCount = estimateSemiOrAntiRowCountBySlotsEqual(leftStats, rightStats,
                    join, (EqualTo) conjunct);
            if (rowCount > eqRowCount) {
                rowCount = eqRowCount;
            }
        }
        if (Double.isInfinite(rowCount)) {
            //slotsEqual estimation failed, fall back to original algorithm
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            double baseRowCount =
                    join.getJoinType().isLeftSemiOrAntiJoin() ? leftStats.getRowCount() : rightStats.getRowCount();
            rowCount = Math.min(innerJoinStats.getRowCount(), baseRowCount);
            return innerJoinStats.withRowCountAndEnforceValid(rowCount);
        } else {
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
            outputStats.enforceValid();
            return outputStats;
        }
    }

    /**
     * estimate join
     */
    public static Statistics estimate(Statistics leftStats, Statistics rightStats, Join join) {
        JoinType joinType = join.getJoinType();
        if (joinType.isSemiOrAntiJoin()) {
            return estimateSemiOrAnti(leftStats, rightStats, join);
        } else if (joinType == JoinType.INNER_JOIN) {
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            innerJoinStats = updateJoinResultStatsByHashJoinCondition(innerJoinStats, join);
            return innerJoinStats;
        } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            double rowCount = Math.max(leftStats.getRowCount(), innerJoinStats.getRowCount());
            rowCount = Math.max(leftStats.getRowCount(), rowCount);
            return innerJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            double rowCount = Math.max(rightStats.getRowCount(), innerJoinStats.getRowCount());
            rowCount = Math.max(rowCount, rightStats.getRowCount());
            return innerJoinStats.withRowCountAndEnforceValid(rowCount);
        } else if (joinType == JoinType.FULL_OUTER_JOIN) {
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            return innerJoinStats.withRowCountAndEnforceValid(leftStats.getRowCount()
                    + rightStats.getRowCount() + innerJoinStats.getRowCount());
        } else if (joinType == JoinType.CROSS_JOIN) {
            return new StatisticsBuilder()
                    .setRowCount(leftStats.getRowCount() * rightStats.getRowCount())
                    .putColumnStatistics(leftStats.columnStatistics())
                    .putColumnStatistics(rightStats.columnStatistics())
                    .build();
        }
        throw new AnalysisException("join type not supported: " + join.getJoinType());
    }

    /**
     * L join R on a = b
     * after join, a.ndv and b.ndv should be equal to min(a.ndv, b.ndv)
     */
    private static Statistics updateJoinResultStatsByHashJoinCondition(Statistics innerStats, Join join) {
        Map<Expression, ColumnStatistic> updatedCols = new HashMap<>();
        for (Expression expr : join.getHashJoinConjuncts()) {
            EqualTo equalTo = (EqualTo) expr;
            ColumnStatistic leftColStats = ExpressionEstimation.estimate(equalTo.left(), innerStats);
            ColumnStatistic rightColStats = ExpressionEstimation.estimate(equalTo.right(), innerStats);
            double minNdv = Math.min(leftColStats.ndv, rightColStats.ndv);
            leftColStats = new ColumnStatisticBuilder(leftColStats).setNdv(minNdv).build();
            rightColStats = new ColumnStatisticBuilder(rightColStats).setNdv(minNdv).build();
            Expression eqLeft = equalTo.left();
            if (eqLeft instanceof Cast) {
                eqLeft = eqLeft.child(0);
            }
            Expression eqRight = equalTo.right();
            if (eqRight instanceof Cast) {
                eqRight = eqRight.child(0);
            }
            updatedCols.put(eqLeft, leftColStats);
            updatedCols.put(eqRight, rightColStats);
        }
        updatedCols.entrySet().stream().forEach(
                entry -> innerStats.addColumnStats(entry.getKey(), entry.getValue())
        );
        return innerStats;
    }

}
