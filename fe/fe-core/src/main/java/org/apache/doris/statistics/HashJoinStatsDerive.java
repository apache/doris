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

package org.apache.doris.statistics;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.ColumnStats;
import org.apache.doris.common.CheckedMath;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.HashJoinNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Derive HashJoinNode statistics.
 */
public class HashJoinStatsDerive extends BaseStatsDerive {

    private static final Logger LOG = LogManager.getLogger(HashJoinStatsDerive.class);

    private JoinOperator joinOp;
    private List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();

    @Override
    public void init(PlanStats node) throws UserException {
        Preconditions.checkState(node instanceof HashJoinNode);
        super.init(node);
        joinOp = ((HashJoinNode) node).getJoinOp();
        eqJoinConjuncts.addAll(((HashJoinNode) node).getEqJoinConjuncts());
    }

    @Override
    public StatsDeriveResult deriveStats() {
        return new StatsDeriveResult(deriveRowCount(), deriveColumnToDataSize(), deriveColumnToNdv());
    }

    @Override
    protected long deriveRowCount() {
        if (joinOp.isSemiAntiJoin()) {
            rowCount = getSemiJoinrowCount();
        } else if (joinOp.isInnerJoin() || joinOp.isOuterJoin()) {
            rowCount = getJoinrowCount();
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("joinOp:{} is not supported for HashJoinStatsDerive", joinOp);
            }
        }
        capRowCountAtLimit();
        return rowCount;
    }

    /**
     * Returns the estimated rowCount of a semi join node.
     * For a left semi join between child(0) and child(1), we look for equality join
     * conditions "L.c = R.d" (with L being from child(0) and R from child(1)) and use as
     * the rowCount estimate the minimum of
     * |child(0)| * Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
     * over all suitable join conditions. The reasoning is that:
     * -each row in child(0) is returned at most once
     * -the probability of a row in child(0) having a match in R is
     * Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
     *
     *<p>
     *     For a left anti join we estimate the rowCount as the minimum of:
     *     |L| * Max(NDV(L.c) - NDV(R.d), NDV(L.c)) / NDV(L.c)
     *     over all suitable join conditions. The reasoning is that:
     *     - each row in child(0) is returned at most once
     *     - if NDV(L.c) > NDV(R.d) then the probability of row in L having a match
     *     in child(1) is (NDV(L.c) - NDV(R.d)) / NDV(L.c)
     *     - otherwise, we conservatively use |L| to avoid underestimation
     *</p>
     *
     *<p>
     * We analogously estimate the rowCount for right semi/anti joins, and treat the
     * null-aware anti join like a regular anti join
     *</p>
     */
    private long getSemiJoinrowCount() {
        Preconditions.checkState(joinOp.isSemiJoin());

        // Return -1 if the rowCount of the returned side is unknown.
        double rowCount;
        if (joinOp == JoinOperator.RIGHT_SEMI_JOIN
                || joinOp == JoinOperator.RIGHT_ANTI_JOIN) {
            if (childrenStatsResult.get(1).getRowCount() == -1) {
                return -1;
            }
            rowCount = childrenStatsResult.get(1).getRowCount();
        } else {
            if (childrenStatsResult.get(0).getRowCount() == -1) {
                return -1;
            }
            rowCount = childrenStatsResult.get(0).getRowCount();
        }
        double minSelectivity = 1.0;
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            double lhsNdv = getNdv(eqJoinPredicate.getChild(0));
            lhsNdv = Math.min(lhsNdv, childrenStatsResult.get(0).getRowCount());
            double rhsNdv = getNdv(eqJoinPredicate.getChild(1));
            rhsNdv = Math.min(rhsNdv, childrenStatsResult.get(1).getRowCount());

            // Skip conjuncts with unknown NDV on either side.
            if (lhsNdv == -1 || rhsNdv == -1) {
                continue;
            }

            double selectivity = 1.0;
            switch (joinOp) {
                case LEFT_SEMI_JOIN: {
                    selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (lhsNdv);
                    break;
                }
                case RIGHT_SEMI_JOIN: {
                    selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (rhsNdv);
                    break;
                }
                case LEFT_ANTI_JOIN:
                case NULL_AWARE_LEFT_ANTI_JOIN: {
                    selectivity = (double) (lhsNdv > rhsNdv ? (lhsNdv - rhsNdv) : lhsNdv) / (double) lhsNdv;
                    break;
                }
                case RIGHT_ANTI_JOIN: {
                    selectivity = (double) (rhsNdv > lhsNdv ? (rhsNdv - lhsNdv) : rhsNdv) / (double) rhsNdv;
                    break;
                }
                default:
                    Preconditions.checkState(false);
            }
            minSelectivity = Math.min(minSelectivity, selectivity);
        }

        Preconditions.checkState(rowCount != -1);
        return Math.round(rowCount * minSelectivity);
    }

    /**
     * Unwraps the SlotRef in expr and returns the NDVs of it.
     * Returns -1 if the NDVs are unknown or if expr is not a SlotRef.
     */
    private long getNdv(Expr expr) {
        SlotRef slotRef = expr.unwrapSlotRef(false);
        if (slotRef == null) {
            return -1;
        }
        SlotDescriptor slotDesc = slotRef.getDesc();
        if (slotDesc == null) {
            return -1;
        }
        ColumnStats stats = slotDesc.getStats();
        if (!stats.hasNumDistinctValues()) {
            return -1;
        }
        return stats.getNumDistinctValues();
    }

    private long getJoinrowCount() {
        Preconditions.checkState(joinOp.isInnerJoin() || joinOp.isOuterJoin());
        Preconditions.checkState(childrenStatsResult.size() == 2);

        long lhsCard = (long) childrenStatsResult.get(0).getRowCount();
        long rhsCard = (long) childrenStatsResult.get(1).getRowCount();
        if (lhsCard == -1 || rhsCard == -1) {
            return lhsCard;
        }

        // Collect join conjuncts that are eligible to participate in rowCount estimation.
        List<HashJoinNode.EqJoinConjunctScanSlots> eqJoinConjunctSlots = new ArrayList<>();
        for (Expr eqJoinConjunct : eqJoinConjuncts) {
            HashJoinNode.EqJoinConjunctScanSlots slots = HashJoinNode.EqJoinConjunctScanSlots.create(eqJoinConjunct);
            if (slots != null) {
                eqJoinConjunctSlots.add(slots);
            }
        }

        if (eqJoinConjunctSlots.isEmpty()) {
            // There are no eligible equi-join conjuncts.
            return lhsCard;
        }

        return getGenericJoinrowCount(eqJoinConjunctSlots, lhsCard, rhsCard);
    }

    /**
     * Returns the estimated join rowCount of a generic N:M inner or outer join based
     * on the given list of equi-join conjunct slots and the join input cardinalities.
     * The returned result is >= 0.
     * The list of join conjuncts must be non-empty and the cardinalities must be >= 0.
     *
     * <p>
     * Generic estimation:
     * rowCount = |child(0)| * |child(1)| / max(NDV(L.c), NDV(R.d))
     * - case A: NDV(L.c) <= NDV(R.d)
     * every row from child(0) joins with |child(1)| / NDV(R.d) rows
     * - case B: NDV(L.c) > NDV(R.d)
     * every row from child(1) joins with |child(0)| / NDV(L.c) rows
     * - we adjust the NDVs from both sides to account for predicates that may
     * might have reduce the rowCount and NDVs
     *</p>
     */
    private long getGenericJoinrowCount(List<HashJoinNode.EqJoinConjunctScanSlots> eqJoinConjunctSlots,
                                        long lhsCard,
                                        long rhsCard) {
        Preconditions.checkState(joinOp.isInnerJoin() || joinOp.isOuterJoin());
        Preconditions.checkState(!eqJoinConjunctSlots.isEmpty());
        Preconditions.checkState(lhsCard >= 0 && rhsCard >= 0);

        long result = -1;
        for (HashJoinNode.EqJoinConjunctScanSlots slots : eqJoinConjunctSlots) {
            // Adjust the NDVs on both sides to account for predicates. Intuitively, the NDVs
            // should only decrease. We ignore adjustments that would lead to an increase.
            double lhsAdjNdv = slots.lhsNdv();
            if (slots.lhsNumRows() > lhsCard) {
                lhsAdjNdv *= lhsCard / slots.lhsNumRows();
            }
            double rhsAdjNdv = slots.rhsNdv();
            if (slots.rhsNumRows() > rhsCard) {
                rhsAdjNdv *= rhsCard / slots.rhsNumRows();
            }
            // A lower limit of 1 on the max Adjusted Ndv ensures we don't estimate
            // rowCount more than the max possible.
            long tmpNdv = Double.doubleToLongBits(Math.max(1, Math.max(lhsAdjNdv, rhsAdjNdv)));
            long joinCard = tmpNdv == rhsCard
                    ? lhsCard
                    : CheckedMath.checkedMultiply(
                    Math.round((lhsCard / Math.max(1, Math.max(lhsAdjNdv, rhsAdjNdv)))), rhsCard);
            if (result == -1) {
                result = joinCard;
            } else {
                result = Math.min(result, joinCard);
            }
        }
        Preconditions.checkState(result >= 0);
        return result;
    }
}
