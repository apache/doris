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

package org.apache.doris.nereids.properties;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**ShuffleKeyPruneUtils*/
public class ShuffleKeyPruneUtils {
    public static final double shuffleKeyHotValueThreshold = 0.05;

    private static Optional<List<Expression>> toOptionalIfChanged(
            List<? extends Expression> originalKeys, List<Expression> optimizedKeys) {
        if (optimizedKeys.equals(originalKeys)) {
            return Optional.empty();
        }
        return Optional.of(optimizedKeys);
    }

    private static Optional<Pair<List<ExprId>, List<ExprId>>> toOptionalIfChanged(
            Pair<List<ExprId>, List<ExprId>> originalKeys, Pair<List<ExprId>, List<ExprId>> optimizedKeys) {
        if (originalKeys.first.size() == optimizedKeys.first.size()) {
            return Optional.empty();
        }
        return Optional.of(optimizedKeys);
    }

    /**
     * Scenario 4: When partition expressions are set by rule, optionally reduce shuffle keys.
     * Strategy: 1) Try single key (isBalanced); 2) Try numeric+date keys (remove strings);
     * 3) Fall back to full partitionExprs.
     * Returns the list of expressions to use as shuffle keys, or empty to use full partitionExprs.
     */
    public static Optional<List<Expression>> selectBestShuffleKeyForAgg(
            PhysicalHashAggregate<? extends Plan> agg, List<Expression> partitionExprs, Statistics childStats,
            ConnectContext context) {
        double rowCount = childStats.getRowCount();
        int instanceNum = context.getTotalInstanceNum();
        return selectOptimalShuffleKeys(partitionExprs, childStats, rowCount, instanceNum);
    }

    /**
     * Select optimal shuffle keys with three-step strategy:
     * 1. Try single key: sort by type (numeric/date first, string sorted by avg_size), pick first isBalanced key.
     * 2. Try remove strings: filter numeric+date keys, if combinedNDV > instanceNum*512 return that list.
     * 3. Fall back: return empty (caller uses full partitionExprs).
     */
    private static Optional<List<Expression>> selectOptimalShuffleKeys(List<Expression> partitionExprs,
            Statistics childStats, double rowCount, int instanceNum) {
        List<SlotReference> slotRefs = partitionExprs.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .collect(Collectors.toList());
        if (slotRefs.isEmpty()) {
            return Optional.empty();
        }
        // If any partition slot lacks column stats, skip optimization and use original partitionExprs.
        for (SlotReference slotRef : slotRefs) {
            ColumnStatistic columnStatistic = childStats.findColumnStatistics(slotRef);
            if (columnStatistic == null || columnStatistic.isUnKnown) {
                return Optional.empty();
            }
            if (columnStatistic.hotValues == null) {
                return Optional.empty();
            }
        }

        // Step 1: Try single key - sort by type priority, pick first isBalanced
        List<SlotReference> sortedByType = sortShuffleKeysByTypePriority(slotRefs, childStats);
        for (SlotReference slotRef : sortedByType) {
            ColumnStatistic colStats = childStats.findColumnStatistics(slotRef);
            if (StatisticsUtil.isBalanced(colStats, instanceNum, shuffleKeyHotValueThreshold, rowCount)) {
                return toOptionalIfChanged(partitionExprs, ImmutableList.of(slotRef));
            }
        }

        // Step 2: Try remove string types - filter numeric+date, check combined NDV
        List<Expression> numericAndDateExprs = slotRefs.stream()
                .filter(s -> s.getDataType().isNumericType() || s.getDataType().isDateLikeType())
                .collect(Collectors.toList());
        if (!numericAndDateExprs.isEmpty()) {
            double combinedNdv = StatsCalculator.estimateGroupByRowCount(numericAndDateExprs, childStats);
            long ndvThreshold = (long) instanceNum * AggregateUtils.NDV_INSTANCE_BALANCE_MULTIPLIER;
            if (combinedNdv > ndvThreshold) {
                return toOptionalIfChanged(partitionExprs, ImmutableList.copyOf(numericAndDateExprs));
            }
        }

        // Step 3: Fall back - return empty, caller uses full partitionExprs
        return Optional.empty();
    }

    /**
     * Sort shuffle keys: numeric and date first, then string types.
     * String types are sorted by column statistics avg size (avgSizeByte) ascending.
     */
    private static List<SlotReference> sortShuffleKeysByTypePriority(List<SlotReference> slotRefs,
            Statistics childStats) {
        List<SlotReference> result = new ArrayList<>(slotRefs);
        result.sort(Comparator
                .comparingInt((SlotReference s) -> getTypeSortPriority(s.getDataType()))
                .thenComparingDouble((SlotReference s) -> getStringAvgSizeForSort(s, childStats)));
        return result;
    }

    /** 0=numeric/date first, 1=string last. */
    private static int getTypeSortPriority(DataType dataType) {
        if (dataType.isNumericType() || dataType.isDateLikeType()) {
            return 0;
        }
        return 1;
    }

    /** For string types return avg size from stats; for others return 0 (no secondary sort). */
    private static double getStringAvgSizeForSort(Slot slotRef, Statistics childStats) {
        DataType dataType = slotRef.getDataType();
        if (dataType instanceof CharacterType) {
            ColumnStatistic colStats = childStats.findColumnStatistics(slotRef);
            if (colStats != null && !colStats.isUnKnown && colStats.avgSizeByte > 0) {
                return colStats.avgSizeByte;
            }
            return ((CharacterType) dataType).getLen();
        }
        return 0;
    }

    /**
     * Pick optimal shuffle keys for a hash join.
     * Uses the same three-step strategy as agg shuffle-key pruning:
     * 1) Try single key (isBalanced); 2) Try numeric+date keys (remove strings);
     * 3) Fall back (empty).
     */
    public static Optional<Pair<List<ExprId>, List<ExprId>>> tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
            ConnectContext context, List<Slot> leftOrderedShuffledColumns, List<Slot> rightOrderedShuffledColumns,
            List<ExprId> leftOrderedShuffledColumnId, List<ExprId> rightOrderedShuffledColumnId,
            Statistics leftStats, Statistics rightStats) {
        if (leftStats == null || rightStats == null) {
            return Optional.empty();
        }
        if (leftOrderedShuffledColumns.size() != rightOrderedShuffledColumns.size()) {
            return Optional.empty();
        }
        if (leftOrderedShuffledColumnId.size() != rightOrderedShuffledColumnId.size()) {
            return Optional.empty();
        }
        double leftRows = leftStats.getRowCount();
        double rightRows = rightStats.getRowCount();
        int instanceNum = context.getTotalInstanceNum();
        List<Pair<Slot, Slot>> validPairs = new ArrayList<>();
        for (int i = 0; i < leftOrderedShuffledColumns.size(); ++i) {
            validPairs.add(Pair.of(leftOrderedShuffledColumns.get(i), rightOrderedShuffledColumns.get(i)));
        }
        return selectOptimalJoinShuffleKeysFromPairs(validPairs,
                Pair.of(leftOrderedShuffledColumnId, rightOrderedShuffledColumnId),
                leftStats, rightStats, leftRows, rightRows,
                instanceNum);
    }

    /**
     * Three-step join shuffle optimization; compares result to {@code baselineForChange}.
     */
    private static Optional<Pair<List<ExprId>, List<ExprId>>> selectOptimalJoinShuffleKeysFromPairs(
            List<Pair<Slot, Slot>> validPairs,
            Pair<List<ExprId>, List<ExprId>> baselineForChange,
            Statistics leftStats, Statistics rightStats,
            double leftRows, double rightRows, int instanceNum) {
        for (Pair<Slot, Slot> pair : validPairs) {
            ColumnStatistic firstStats = leftStats.findColumnStatistics(pair.first);
            ColumnStatistic secondStats = rightStats.findColumnStatistics(pair.second);
            if (firstStats == null || secondStats == null || firstStats.isUnKnown || secondStats.isUnKnown
                    || firstStats.hotValues == null || secondStats.hotValues == null) {
                return Optional.empty();
            }
        }

        // Step 1: Try single key - sort by type, pick first where both isBalanced
        List<Pair<Slot, Slot>> sortedPairs =
                sortJoinKeyPairsByTypePriority(validPairs, leftStats, rightStats);
        for (Pair<Slot, Slot> pair : sortedPairs) {
            Slot leftSlotRef = pair.first;
            Slot rightSlotRef = pair.second;
            ColumnStatistic leftColStats = leftStats.findColumnStatistics(leftSlotRef);
            ColumnStatistic rightColStats = rightStats.findColumnStatistics(rightSlotRef);
            if (StatisticsUtil.isBalanced(leftColStats, instanceNum, shuffleKeyHotValueThreshold, leftRows)
                    && StatisticsUtil.isBalanced(rightColStats, instanceNum, shuffleKeyHotValueThreshold, rightRows)) {
                return toOptionalIfChanged(baselineForChange, Pair.of(
                        ImmutableList.of(leftSlotRef.getExprId()),
                        ImmutableList.of(rightSlotRef.getExprId())));
            }
        }

        // Step 2: Try remove string types - filter numeric+date pairs, check combined NDV
        List<Slot> numericDateLeftSlots = new ArrayList<>();
        List<Slot> numericDateRightSlots = new ArrayList<>();
        for (Pair<Slot, Slot> pair : validPairs) {
            if ((pair.first.getDataType().isNumericType() || pair.first.getDataType().isDateLikeType())
                    && (pair.second.getDataType().isNumericType() || pair.second.getDataType().isDateLikeType())) {
                numericDateLeftSlots.add(pair.first);
                numericDateRightSlots.add(pair.second);
            }
        }
        if (!numericDateLeftSlots.isEmpty()) {
            double leftCombinedNdv = StatsCalculator.estimateGroupByRowCount(
                    new ArrayList<>(numericDateLeftSlots), leftStats);
            double rightCombinedNdv = StatsCalculator.estimateGroupByRowCount(
                    new ArrayList<>(numericDateRightSlots), rightStats);
            long ndvThreshold = (long) instanceNum * AggregateUtils.NDV_INSTANCE_BALANCE_MULTIPLIER;
            if (leftCombinedNdv > ndvThreshold && rightCombinedNdv > ndvThreshold) {
                List<ExprId> leftIds = numericDateLeftSlots.stream()
                        .map(Slot::getExprId)
                        .collect(Collectors.toList());
                List<ExprId> rightIds = numericDateRightSlots.stream()
                        .map(Slot::getExprId)
                        .collect(Collectors.toList());
                return toOptionalIfChanged(baselineForChange, Pair.of(leftIds, rightIds));
            }
        }

        // Step 3: Fall back
        return Optional.empty();
    }

    /** Sort join key pairs by type priority (numeric/date first, string by avg_size). */
    private static List<Pair<Slot, Slot>> sortJoinKeyPairsByTypePriority(
            List<Pair<Slot, Slot>> pairs, Statistics leftStats, Statistics rightStats) {
        List<Pair<Slot, Slot>> result = new ArrayList<>(pairs);
        result.sort(Comparator
                .comparingInt((Pair<Slot, Slot> p) ->
                        getTypeSortPriority(p.first.getDataType()))
                .thenComparingDouble((Pair<Slot, Slot> p) ->
                        getJoinPairStringAvgSizeForSort(p, leftStats, rightStats)));
        return result;
    }

    /** For string join-key pairs, use avg size of both sides for sorting; for others return 0. */
    private static double getJoinPairStringAvgSizeForSort(Pair<Slot, Slot> pair,
            Statistics leftStats, Statistics rightStats) {
        if (pair.first.getDataType() instanceof CharacterType && pair.second.getDataType() instanceof CharacterType) {
            return (getStringAvgSizeForSort(pair.first, leftStats) + getStringAvgSizeForSort(pair.second, rightStats));
        }
        return 0;
    }
}
