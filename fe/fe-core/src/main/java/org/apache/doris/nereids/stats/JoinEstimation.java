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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.statistics.ColumnStats;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * Estimate hash join stats.
 * TODO: Update other props in the ColumnStats properly.
 */
public class JoinEstimation {

    /**
     * Do estimate.
     */
    public static StatsDeriveResult estimate(StatsDeriveResult leftStats, StatsDeriveResult rightStats, Join join) {
        JoinType joinType = join.getJoinType();
        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(leftStats);
        statsDeriveResult.merge(rightStats);
        List<Expression> eqConjunctList = join.getHashJoinConjuncts();
        long rowCount = -1;
        if (joinType.isSemiOrAntiJoin()) {
            rowCount = getSemiJoinRowCount(leftStats, rightStats, eqConjunctList, joinType);
        } else if (joinType.isInnerJoin() || joinType.isOuterJoin()) {
            rowCount = getJoinRowCount(leftStats, rightStats, eqConjunctList, joinType);
        } else if (joinType.isCrossJoin()) {
            rowCount = CheckedMath.checkedMultiply(leftStats.getRowCount(),
                    rightStats.getRowCount());
        } else {
            throw new RuntimeException("joinType is not supported");
        }
        statsDeriveResult.setRowCount(rowCount);
        return statsDeriveResult;
    }

    // TODO: If the condition of Join Plan could any expression in addition to EqualTo type,
    //       we should handle that properly.
    private static long getSemiJoinRowCount(StatsDeriveResult leftStats, StatsDeriveResult rightStats,
            List<Expression> eqConjunctList, JoinType joinType) {
        long rowCount;
        if (JoinType.RIGHT_SEMI_JOIN.equals(joinType) || JoinType.RIGHT_ANTI_JOIN.equals(joinType)) {
            if (rightStats.getRowCount() == -1) {
                return -1;
            }
            rowCount = rightStats.getRowCount();
        } else {
            if (leftStats.getRowCount() == -1) {
                return -1;
            }
            rowCount = leftStats.getRowCount();
        }
        Map<Slot, ColumnStats> leftSlotToColStats = leftStats.getSlotToColumnStats();
        Map<Slot, ColumnStats> rightSlotToColStats = rightStats.getSlotToColumnStats();
        double minSelectivity = 1.0;
        for (Expression eqJoinPredicate : eqConjunctList) {
            long lhsNdv = leftSlotToColStats.get(eqJoinPredicate.child(0)).getNdv();
            lhsNdv = Math.min(lhsNdv, leftStats.getRowCount());
            long rhsNdv = rightSlotToColStats.get(eqJoinPredicate.child(1)).getNdv();
            rhsNdv = Math.min(rhsNdv, rightStats.getRowCount());
            // Skip conjuncts with unknown NDV on either side.
            if (lhsNdv == -1 || rhsNdv == -1) {
                continue;
            }
            // TODO: Do we need NULL_AWARE_LEFT_ANTI_JOIN type as stale optimizer?
            double selectivity = 1.0;
            switch (joinType) {
                case LEFT_SEMI_JOIN: {
                    selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (lhsNdv);
                    break;
                }
                case RIGHT_SEMI_JOIN: {
                    selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (rhsNdv);
                    break;
                }
                case LEFT_ANTI_JOIN:
                    selectivity = (double) (lhsNdv > rhsNdv ? (lhsNdv - rhsNdv) : lhsNdv) / (double) lhsNdv;
                    break;
                case RIGHT_ANTI_JOIN: {
                    selectivity = (double) (rhsNdv > lhsNdv ? (rhsNdv - lhsNdv) : rhsNdv) / (double) rhsNdv;
                    break;
                }
                default:
                    throw new RuntimeException("joinType is not supported");
            }
            minSelectivity = Math.min(minSelectivity, selectivity);
        }
        Preconditions.checkState(rowCount != -1);
        return Math.round(rowCount * minSelectivity);
    }

    private static long getJoinRowCount(StatsDeriveResult leftStats, StatsDeriveResult rightStats,
            List<Expression> eqConjunctList, JoinType joinType) {
        long lhsCard = leftStats.getRowCount();
        long rhsCard = rightStats.getRowCount();
        Map<Slot, ColumnStats> leftSlotToColumnStats = leftStats.getSlotToColumnStats();
        Map<Slot, ColumnStats> rightSlotToColumnStats = rightStats.getSlotToColumnStats();
        if (lhsCard == -1 || rhsCard == -1) {
            return lhsCard;
        }

        long result = -1;
        for (Expression eqJoinConjunct : eqConjunctList) {
            Expression left = eqJoinConjunct.child(0);
            if (!(left instanceof SlotReference)) {
                continue;
            }
            Expression right = eqJoinConjunct.child(1);
            if (!(right instanceof SlotReference)) {
                continue;
            }
            SlotReference leftSlot = (SlotReference) left;
            ColumnStats leftColStats = leftSlotToColumnStats.get(leftSlot);
            if (leftColStats == null) {
                continue;
            }
            SlotReference rightSlot = (SlotReference) right;
            ColumnStats rightColStats = rightSlotToColumnStats.get(rightSlot);
            if (rightColStats == null) {
                continue;
            }
            double leftSideNdv = leftColStats.getNdv();
            double rightSideNdv = rightColStats.getNdv();
            long tmpNdv = (long) Math.max(1, Math.max(leftSideNdv, rightSideNdv));
            long joinCard = tmpNdv == rhsCard ? lhsCard : CheckedMath.checkedMultiply(
                    Math.round((lhsCard / Math.max(1, Math.max(leftSideNdv, rightSideNdv)))), rhsCard);
            if (result == -1) {
                result = joinCard;
            } else {
                result = Math.min(result, joinCard);
            }
        }

        return result;
    }
}
