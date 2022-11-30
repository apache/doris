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

import org.apache.doris.common.CheckedMath;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Estimate hash join stats.
 * TODO: Update other props in the ColumnStats properly.
 */
public class JoinEstimation {
    private static final Logger LOG = LogManager.getLogger(JoinEstimation.class);

    private static double estimateInnerJoin(Join join, EqualTo equalto,
            StatsDeriveResult leftStats, StatsDeriveResult rightStats) {
        SlotReference eqRight = (SlotReference) equalto.child(1).getInputSlots().toArray()[0];

        ColumnStatistic rColumnStats = rightStats.getSlotIdToColumnStats().get(eqRight.getExprId());
        SlotReference eqLeft = (SlotReference) equalto.child(0).getInputSlots().toArray()[0];

        if (rColumnStats == null) {
            rColumnStats = rightStats.getSlotIdToColumnStats().get(eqLeft.getExprId());
        }
        if (rColumnStats == null) {
            LOG.info("estimate inner join failed, column stats not found: %s", eqRight);
            throw new RuntimeException("estimateInnerJoin cannot find columnStats: " + eqRight);
        }

        double rowCount = 0;

        if (rColumnStats.isUnKnown || rColumnStats.ndv == 0) {
            rowCount = Math.max(leftStats.getRowCount(), rightStats.getRowCount());
        } else {
            //TODO range is not considered
            rowCount = (leftStats.getRowCount()
                    * rightStats.getRowCount()
                    * rColumnStats.selectivity
                    / rColumnStats.ndv);
        }
        rowCount = Math.max(1, Math.ceil(rowCount));
        return rowCount;
    }

    private static double estimateLeftSemiJoin(double leftCount, double rightCount) {
        //TODO the estimation of semi and anti join is not proper, just for tpch q21
        return leftCount - leftCount / Math.max(2, rightCount);
    }

    /**
     * estimate join
     */
    public static StatsDeriveResult estimate(StatsDeriveResult leftStats, StatsDeriveResult rightStats, Join join) {
        JoinType joinType = join.getJoinType();
        double rowCount = Double.MAX_VALUE;
        if (joinType == JoinType.LEFT_SEMI_JOIN || joinType == JoinType.LEFT_ANTI_JOIN) {
            double rightCount = rightStats.getRowCount();
            double leftCount = leftStats.getRowCount();
            if (join.getHashJoinConjuncts().isEmpty()) {
                rowCount = joinType == JoinType.LEFT_SEMI_JOIN ? leftCount : 0;
            } else {
                rowCount = estimateLeftSemiJoin(leftCount, rightCount);
            }
        } else if (joinType == JoinType.RIGHT_SEMI_JOIN || joinType == JoinType.RIGHT_ANTI_JOIN) {
            double rightCount = rightStats.getRowCount();
            double leftCount = leftStats.getRowCount();
            if (join.getHashJoinConjuncts().isEmpty()) {
                rowCount = joinType == JoinType.RIGHT_SEMI_JOIN ? rightCount : 0;
            } else {
                rowCount = estimateLeftSemiJoin(rightCount, leftCount);
            }
        } else if (joinType == JoinType.INNER_JOIN) {
            if (join.getHashJoinConjuncts().isEmpty()) {
                rowCount = leftStats.getRowCount() * rightStats.getRowCount();
            } else {
                for (Expression joinConjunct : join.getHashJoinConjuncts()) {
                    double tmpRowCount = estimateInnerJoin(join,
                            (EqualTo) joinConjunct, leftStats, rightStats);
                    rowCount = Math.min(rowCount, tmpRowCount);
                }
            }
        } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
            rowCount = leftStats.getRowCount();
        } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
            rowCount = rightStats.getRowCount();
        } else if (joinType == JoinType.CROSS_JOIN) {
            rowCount = CheckedMath.checkedMultiply(leftStats.getRowCount(),
                    rightStats.getRowCount());
        } else {
            LOG.warn("join type is not supported: " + joinType);
            throw new RuntimeException("joinType is not supported");
        }

        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(rowCount,
                rightStats.getWidth() + leftStats.getWidth(), 0, Maps.newHashMap());
        if (joinType.isRemainLeftJoin()) {
            statsDeriveResult.merge(leftStats);
        }
        if (joinType.isRemainRightJoin()) {
            statsDeriveResult.merge(rightStats);
        }
        //TODO: consider other join conjuncts
        return statsDeriveResult;
    }

}
