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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Estimate hash join stats.
 * TODO: Update other props in the ColumnStats properly.
 */
public class JoinEstimation {

    private static Statistics estimateInnerJoin(Statistics crossJoinStats, Join join) {

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
        Statistics innerJoinStats = crossJoinStats.updateRowCountOnly(crossJoinStats.getRowCount() * sel);

        if (!join.getOtherJoinConjuncts().isEmpty()) {
            FilterEstimation filterEstimation = new FilterEstimation();
            innerJoinStats = filterEstimation.estimate(
                    ExpressionUtils.and(join.getOtherJoinConjuncts()), innerJoinStats);
        }
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

    private static double estimateSemiOrAntiRowCountByEqual(Statistics leftStats,
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
            double eqRowCount = estimateSemiOrAntiRowCountByEqual(leftStats, rightStats, join, (EqualTo) conjunct);
            if (rowCount > eqRowCount) {
                rowCount = eqRowCount;
            }
        }
        if (Double.isInfinite(rowCount)) {
            //fall back to original alg.
            return null;
        }
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

    /**
     * estimate join
     */
    public static Statistics estimate(Statistics leftStats, Statistics rightStats, Join join) {
        JoinType joinType = join.getJoinType();
        if (joinType.isSemiOrAntiJoin()) {
            Statistics estStats = estimateSemiOrAnti(leftStats, rightStats, join);
            //if estStats is null, fall back to original alg.
            if (estStats != null) {
                return estStats;
            }
        }
        Statistics crossJoinStats = new StatisticsBuilder()
                .setRowCount(leftStats.getRowCount() * rightStats.getRowCount())
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();
        Statistics innerJoinStats = estimateInnerJoin(crossJoinStats, join);
        innerJoinStats.setWidth(leftStats.getWidth() + rightStats.getWidth());
        innerJoinStats.setPenalty(0);
        double rowCount;
        if (joinType.isLeftSemiOrAntiJoin()) {
            rowCount = Math.min(innerJoinStats.getRowCount(), leftStats.getRowCount());
            return innerJoinStats.withRowCount(rowCount);
        } else if (joinType.isRightSemiOrAntiJoin()) {
            rowCount = Math.min(innerJoinStats.getRowCount(), rightStats.getRowCount());
            return innerJoinStats.withRowCount(rowCount);
        } else if (joinType == JoinType.INNER_JOIN) {
            return innerJoinStats;
        } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
            rowCount = Math.max(leftStats.getRowCount(), innerJoinStats.getRowCount());
            return innerJoinStats.withRowCount(rowCount);
        } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
            rowCount = Math.max(rightStats.getRowCount(), innerJoinStats.getRowCount());
            return innerJoinStats.withRowCount(rowCount);
        } else if (joinType == JoinType.CROSS_JOIN) {
            return crossJoinStats;
        } else if (joinType == JoinType.FULL_OUTER_JOIN) {
            return innerJoinStats.withRowCount(leftStats.getRowCount()
                    + rightStats.getRowCount() + innerJoinStats.getRowCount());
        }
        return crossJoinStats;
    }

}
