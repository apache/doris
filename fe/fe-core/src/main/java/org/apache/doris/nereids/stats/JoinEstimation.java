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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.CheckedMath;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStats;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Estimate hash join stats.
 * TODO: Update other props in the ColumnStats properly.
 */
public class JoinEstimation {
    private static final Logger LOG = LogManager.getLogger(JoinEstimation.class);

    private static class JoinEstimationResult {
        public boolean forbiddenReducePropagation = false;
        public boolean isReducedByHashJoin = false;
        public long rowCount = 0;
    }

    private static JoinEstimationResult computeInner(PhysicalHashJoin join, EqualTo equalto,
            StatsDeriveResult leftStats, StatsDeriveResult rightStats) {
        JoinEstimationResult result = new JoinEstimationResult();

        SlotReference eqLeft = (SlotReference) equalto.child(0);
        SlotReference eqRight = (SlotReference) equalto.child(1);
        if ((rightStats.level >= 2 && !rightStats.isReduced) || rightStats.level > 3) {
            //right deep tree is not good for parallel building hash table,
            //penalty too right deep tree by multiply level
            result.forbiddenReducePropagation = true;
            result.rowCount = rightStats.level * (leftStats.getRowCount() + 2 * rightStats.getRowCount());
        } else if (eqLeft.getColumn().isPresent() || eqRight.getColumn().isPresent()) {
            Set<Slot> rightSlots = ((PhysicalHashJoin<?, ?>) join).child(1).getOutputSet();
            if ((rightSlots.contains(eqRight)
                    && eqRight.getColumn().isPresent()
                    && eqRight.getColumn().get().isKey()
                    && !compoundKey(eqRight))
                    || (rightSlots.contains(eqLeft)
                    && eqLeft.getColumn().isPresent()
                    && eqLeft.getColumn().get().isKey()
                    && !compoundKey(eqLeft))) {
                //fact table JOIN dimension table
                if (rightStats.isReduced) {
                    //dimension table is reduced
                    result.isReducedByHashJoin = true;
                    result.rowCount = leftStats.getRowCount() / 2;
                } else {
                    //dimension table is not reduced, the join result tuple number equals to
                    // the tuple number of fact table.
                    result.rowCount = leftStats.getRowCount();
                }
            } else {
                //dimension table JOIN fact table
                result.rowCount = leftStats.getRowCount() + 2 * rightStats.getRowCount();
            }
        } else {
            LOG.debug("HashJoin cost calculation: slot.column is null, star-schema support failed.");
            result.rowCount = Math.max(leftStats.getRowCount() + rightStats.getRowCount(),
                    leftStats.getRowCount() * 2);
        }
        return result;
    }

    /**
     * Do estimate.
     * // TODO: since we have no column stats here. just use a fix ratio to compute the row count.
     */
    public static StatsDeriveResult estimate(StatsDeriveResult leftStats, StatsDeriveResult rightStats, Join join) {
        JoinType joinType = join.getJoinType();
        // TODO: normalize join hashConjuncts.
        // List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts();
        // List<Expression> normalizedConjuncts = hashJoinConjuncts.stream().map(EqualTo.class::cast)
        //         .map(e -> JoinUtils.swapEqualToForChildrenOrder(e, leftStats.getSlotToColumnStats().keySet()))
        //         .collect(Collectors.toList());
        boolean isReducedByHashJoin = false;
        boolean forbiddenReducePropagation = false;
        long rowCount;
        if (joinType == JoinType.LEFT_SEMI_JOIN || joinType == JoinType.LEFT_ANTI_JOIN) {
            if (rightStats.isReduced && rightStats.level < 3) {
                rowCount = leftStats.getRowCount() / 2;
            } else {
                rowCount = leftStats.getRowCount() + 1;
            }
        } else if (joinType == JoinType.RIGHT_SEMI_JOIN || joinType == JoinType.RIGHT_ANTI_JOIN) {
            rowCount = rightStats.getRowCount();
        } else if (joinType == JoinType.INNER_JOIN) {
            if (!join.getHashJoinConjuncts().isEmpty() && join instanceof PhysicalHashJoin
                    && ConnectContext.get().getSessionVariable().isNereidsStarSchemaSupport()) {
                /*
                 * Doris does not support primary key and foreign key. But the data may satisfy pk and fk constraints.
                 * This fact is indicated by session variable `support_star_schema_nereids`.
                 * If `support_star_schema_nereids` is true, we have the following implications:
                 * 1. the duplicate key, the unique key and the aggregate key are primary key.
                 * 2. the inner join is between fact table and dimension table
                 * 3. if the dimension table is not filtered, after join, the output tuple number is the tuple
                 * number of fact table.
                 * 4. if we choose fact table as right table (building hash table), the output tuple number is 4 times
                 * of the tuple number in dimension table. The magic number `4` is from tpch, in which every
                 * o_orderkey is corresponding to 1-7 l_orderkey.
                  */
                //TODO:
                // 1.currently, we only consider single primary key table. The idea could be expanded to table
                //   with compond keys, like tpch lineitem/partsupp.
                // 2.after aggregation, group key is unique. we could use it as primary key.
                // 3.isReduced should be replaced by selectivity. and we need to refine the propagation of selectivity.
                JoinEstimationResult better = new JoinEstimationResult();
                better.rowCount = Long.MAX_VALUE;

                for (Expression equalto : join.getHashJoinConjuncts()) {
                    JoinEstimationResult result = computeInner((PhysicalHashJoin) join,
                            (EqualTo) equalto, leftStats, rightStats);
                    if (result.rowCount < better.rowCount) {
                        better = result;
                    }
                }
                rowCount = better.rowCount;
                forbiddenReducePropagation = better.forbiddenReducePropagation;
                isReducedByHashJoin = better.isReducedByHashJoin;
            } else {
                long childRowCount = Math.max(leftStats.getRowCount(), rightStats.getRowCount());
                rowCount = childRowCount;
            }
        } else if (joinType == JoinType.LEFT_OUTER_JOIN) {
            rowCount = leftStats.getRowCount();
        } else if (joinType == JoinType.RIGHT_OUTER_JOIN) {
            rowCount = rightStats.getRowCount();
        } else if (joinType == JoinType.CROSS_JOIN) {
            rowCount = CheckedMath.checkedMultiply(leftStats.getRowCount(),
                    rightStats.getRowCount());
        } else {
            throw new RuntimeException("joinType is not supported");
        }

        StatsDeriveResult statsDeriveResult = new StatsDeriveResult(rowCount, Maps.newHashMap());
        if (joinType.isRemainLeftJoin()) {
            statsDeriveResult.merge(leftStats);
        }
        if (joinType.isRemainRightJoin()) {
            statsDeriveResult.merge(rightStats);
        }
        statsDeriveResult.setRowCount(rowCount);
        statsDeriveResult.isReduced = !forbiddenReducePropagation && (isReducedByHashJoin || leftStats.isReduced);
        statsDeriveResult.level = rightStats.level + leftStats.level;
        return statsDeriveResult;
    }

    private static boolean compoundKey(SlotReference slotReference) {
        Optional<Database> db = Env.getCurrentEnv().getInternalCatalog().getDb(slotReference.getQualifier().get(0));
        if (db.isPresent()) {
            Optional<Table> table = db.get().getTable(slotReference.getQualifier().get(1));
            if (table.isPresent()) {
                return table.get().isHasCompoundKey();
            }
        }
        return false;
    }

    private static Expression removeCast(Expression parent) {
        if (parent instanceof Cast) {
            return removeCast(((Cast) parent).child());
        }
        return parent;
    }

    // TODO: If the condition of Join Plan could any expression in addition to EqualTo type,
    //       we should handle that properly.
    private static long getSemiJoinRowCount(StatsDeriveResult leftStats, StatsDeriveResult rightStats,
            List<Expression> hashConjuncts, JoinType joinType) {
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
        for (Expression hashConjunct : hashConjuncts) {
            // TODO: since we have no column stats here. just use a fix ratio to compute the row count.
            long lhsNdv = leftSlotToColStats.get(removeCast(hashConjunct.child(0))).getNdv();
            lhsNdv = Math.min(lhsNdv, leftStats.getRowCount());
            long rhsNdv = rightSlotToColStats.get(removeCast(hashConjunct.child(1))).getNdv();
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
