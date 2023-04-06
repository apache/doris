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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Estimate hash join stats.
 * TODO: Update other props in the ColumnStats properly.
 */
public class JoinEstimation {

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

    private static Statistics estimateInnerJoin(Statistics leftStats, Statistics rightStats, Join join) {
        /*
         * When we estimate filter A=B,
         * if any side of equation, A or B, is almost unique, the confidence level of estimation is high.
         * But is both sides are not unique, the confidence level is very low.
         * The equations, whose confidence level is low, are called unTrustEquation.
         * In order to avoid error propagation, for unTrustEquations, we only use the biggest selectivity.
         */
        List<Double> unTrustEqualRatio = Lists.newArrayList();
        boolean leftBigger = leftStats.getRowCount() > rightStats.getRowCount();
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
                        boolean trustable = eqRightColStats.ndv / rightStats.getRowCount() > almostUniqueThreshold
                                || eqLeftColStats.ndv / leftStats.getRowCount() > almostUniqueThreshold;
                        if (!trustable) {
                            if (leftBigger) {
                                unTrustEqualRatio.add((rightStats.getRowCount() / eqRightColStats.ndv)
                                        * Math.min(eqLeftColStats.ndv, eqRightColStats.ndv) / eqLeftColStats.ndv);
                            } else {
                                unTrustEqualRatio.add((leftStats.getRowCount() / eqLeftColStats.ndv)
                                        * Math.min(eqLeftColStats.ndv, eqRightColStats.ndv) / eqRightColStats.ndv);
                            }
                        }
                        return trustable;
                    }
                ).collect(Collectors.toList());

        Statistics innerJoinStats;
        Statistics crossJoinStats = new StatisticsBuilder()
                .setRowCount(leftStats.getRowCount() * rightStats.getRowCount())
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();
        if (!trustableConditions.isEmpty()) {
            List<Pair<Expression, Double>> sortedJoinConditions = join.getHashJoinConjuncts().stream()
                    .map(expression -> Pair.of(expression, estimateJoinConditionSel(crossJoinStats, expression)))
                    .sorted((a, b) -> {
                        double sub = a.second - b.second;
                        if (sub > 0) {
                            return 1;
                        } else if (sub < 0) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }).collect(Collectors.toList());

            double sel = 1.0;
            for (int i = 0; i < sortedJoinConditions.size(); i++) {
                sel *= Math.pow(sortedJoinConditions.get(i).second, 1 / Math.pow(2, i));
            }
            innerJoinStats = crossJoinStats.updateRowCountOnly(crossJoinStats.getRowCount() * sel);
        } else {
            double outputRowCount = Math.max(leftStats.getRowCount(), rightStats.getRowCount());
            Optional<Double> ratio = unTrustEqualRatio.stream().max(Double::compareTo);
            if (ratio.isPresent()) {
                outputRowCount = outputRowCount * ratio.get();
            }
            innerJoinStats = crossJoinStats.updateRowCountOnly(outputRowCount);
        }

        if (!join.getOtherJoinConjuncts().isEmpty()) {
            FilterEstimation filterEstimation = new FilterEstimation();
            innerJoinStats = filterEstimation.estimate(
                    ExpressionUtils.and(join.getOtherJoinConjuncts()), innerJoinStats);
        }
        innerJoinStats.setWidth(leftStats.getWidth() + rightStats.getWidth());
        innerJoinStats.setPenalty(0);
        return innerJoinStats;
    }

    private static double estimateJoinConditionSel(Statistics crossJoinStats, Expression joinCond) {
        Statistics statistics = new FilterEstimation().estimate(joinCond, crossJoinStats);
        return statistics.getRowCount() / crossJoinStats.getRowCount();
    }

    private static double adjustSemiOrAntiByOtherJoinConditions(Join join) {
        int otherConditionCount = join.getOtherJoinConjuncts().size();
        double sel = 1.0;
        for (int i = 0; i < otherConditionCount; i++) {
            sel *= Math.pow(FilterEstimation.DEFAULT_INEQUALITY_COEFFICIENT, 1 / Math.pow(2, i));
        }
        return sel;
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
            rowCount = StatsMathUtil.divide(leftStats.getRowCount() * buildColStats.ndv, buildColStats.originalNdv);
        } else {
            //right semi or anti
            rowCount = StatsMathUtil.divide(rightStats.getRowCount() * probColStats.ndv, probColStats.originalNdv);
        }
        return rowCount;
    }

    private static Statistics estimateSemiOrAnti(Statistics leftStats, Statistics rightStats, Join join) {
        double rowCount = Double.POSITIVE_INFINITY;
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            double eqRowCount = estimateSemiOrAntiRowCountBySlotsEqual(leftStats, rightStats, join, (EqualTo) conjunct);
            if (rowCount > eqRowCount) {
                rowCount = eqRowCount;
            }
        }
        if (Double.isInfinite(rowCount)) {
            //slotsEqual estimation failed, estimate by innerJoin
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            double baseRowCount =
                    join.getJoinType().isLeftSemiOrAntiJoin() ? leftStats.getRowCount() : rightStats.getRowCount();
            rowCount = Math.min(innerJoinStats.getRowCount(), baseRowCount);
            return innerJoinStats.withRowCount(rowCount);
        } else {
            rowCount = rowCount * adjustSemiOrAntiByOtherJoinConditions(join);
            StatisticsBuilder builder;
            double originalRowCount = leftStats.getRowCount();
            if (join.getJoinType().isLeftSemiOrAntiJoin()) {
                builder = new StatisticsBuilder(leftStats);
                builder.setRowCount(rowCount);
            } else {
                //right semi or anti
                builder = new StatisticsBuilder(rightStats);
                builder.setRowCount(rowCount);
                originalRowCount = rightStats.getRowCount();
            }
            Statistics outputStats = builder.build();
            outputStats.fix(rowCount, originalRowCount);
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
            return innerJoinStats;
        } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            double rowCount = Math.max(leftStats.getRowCount(), innerJoinStats.getRowCount());
            return innerJoinStats.withRowCount(rowCount);
        } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            double rowCount = Math.max(rightStats.getRowCount(), innerJoinStats.getRowCount());
            return innerJoinStats.withRowCount(rowCount);
        } else if (joinType == JoinType.FULL_OUTER_JOIN) {
            Statistics innerJoinStats = estimateInnerJoin(leftStats, rightStats, join);
            return innerJoinStats.withRowCount(leftStats.getRowCount()
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

}
