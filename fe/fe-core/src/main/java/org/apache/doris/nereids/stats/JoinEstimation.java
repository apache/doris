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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Estimate hash join stats.
 * TODO: Update other props in the ColumnStats properly.
 */
public class JoinEstimation {
    private static Statistics estimateInnerJoin(Statistics crossJoinStats, List<Expression> joinConditions) {
        List<Pair<Expression, Double>> sortedJoinConditions = joinConditions.stream()
                .map(expression -> Pair.of(expression, estimateJoinConditionSel(crossJoinStats, expression)))
                .sorted((a, b) -> {
                    double sub = a.second - b.second;
                    if (sub > 0) {
                        return -1;
                    } else if (sub < 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }).collect(Collectors.toList());

        double sel = 1.0;
        for (int i = 0; i < sortedJoinConditions.size(); i++) {
            sel *= Math.pow(sortedJoinConditions.get(i).second, 1 / Math.pow(2, i));
        }
        return crossJoinStats.withSel(sel);
    }

    private static double estimateJoinConditionSel(Statistics crossJoinStats, Expression joinCond) {
        Statistics statistics = new FilterEstimation().estimate(joinCond, crossJoinStats);
        return statistics.getRowCount() / crossJoinStats.getRowCount();
    }

    /**
     * estimate join
     */
    public static Statistics estimate(Statistics leftStats, Statistics rightStats, Join join) {
        JoinType joinType = join.getJoinType();
        Statistics crossJoinStats = new StatisticsBuilder()
                .setRowCount(leftStats.getRowCount() * rightStats.getRowCount())
                .putColumnStatistics(leftStats.columnStatistics())
                .putColumnStatistics(rightStats.columnStatistics())
                .build();
        List<Expression> joinConditions = join.getHashJoinConjuncts();
        Statistics innerJoinStats = estimateInnerJoin(crossJoinStats, joinConditions);
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
