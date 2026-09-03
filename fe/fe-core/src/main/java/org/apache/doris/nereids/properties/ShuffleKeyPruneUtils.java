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
import org.apache.doris.nereids.stats.ExpressionEstimation;
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
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**ShuffleKeyPruneUtils*/
public class ShuffleKeyPruneUtils {
    public static final double shuffleKeyHotValueThreshold = 0.05;
    private static final double SHUFFLE_BUCKET_SKEW_MULTIPLIER = 10;
    // Analyze stores hot-value ratios with ROUND(..., 2), so the true ratio can be up to 0.005 higher.
    private static final double HOT_VALUE_RATIO_ROUNDING_ERROR = 0.005;

    private enum ShuffleKeySafetyPolicy {
        PARENT_REUSE,
        PRUNING
    }

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
     * Merge transitive overlaps in a hash spec's equivalence sets. Hash-join property derivation can
     * produce sets such as {a, x} and {b, x}; they represent one independent shuffle dimension.
     * The result keeps the first ordered position of every merged dimension.
     */
    public static List<Set<ExprId>> getIndependentShuffleDimensions(DistributionSpecHash hashSpec) {
        List<Set<ExprId>> dimensions = new ArrayList<>();
        for (Set<ExprId> equivalenceSet : hashSpec.getEquivalenceExprIds()) {
            int firstOverlappingDimension = -1;
            for (int i = 0; i < dimensions.size(); i++) {
                Set<ExprId> dimension = dimensions.get(i);
                if (Collections.disjoint(dimension, equivalenceSet)) {
                    continue;
                }
                if (firstOverlappingDimension < 0) {
                    firstOverlappingDimension = i;
                    dimension.addAll(equivalenceSet);
                } else {
                    dimensions.get(firstOverlappingDimension).addAll(dimension);
                    dimensions.remove(i--);
                }
            }
            if (firstOverlappingDimension < 0) {
                dimensions.add(new HashSet<>(equivalenceSet));
            }
        }
        return dimensions;
    }

    /** Whether configured hot values or an instance-hot NULL bucket prove one-phase aggregate skew. */
    static boolean hasKnownSkewForOnePhaseAgg(
            List<Expression> shuffleKeys, Statistics inputStatistics, int instanceNum) {
        List<Expression> uniqueShuffleKeys = distinctShuffleKeys(shuffleKeys);
        List<ColumnStatistic> columnStatistics = new ArrayList<>(uniqueShuffleKeys.size());
        for (Expression shuffleKey : uniqueShuffleKeys) {
            ColumnStatistic columnStatistic = findColumnStatistic(shuffleKey, inputStatistics);
            if (columnStatistic == null || columnStatistic.isUnKnown) {
                return false;
            }
            columnStatistics.add(columnStatistic);
        }

        double rowCount = inputStatistics.getRowCount();
        for (int i = 0; i < uniqueShuffleKeys.size(); i++) {
            List<Expression> otherShuffleKeys = new ArrayList<>(uniqueShuffleKeys);
            otherShuffleKeys.remove(i);
            double maxOtherCombinationCount = maxDistinctCombinationCount(
                    otherShuffleKeys, inputStatistics);
            ColumnStatistic columnStatistic = columnStatistics.get(i);
            if (hasConfiguredKnownHotValue(columnStatistic, maxOtherCombinationCount)) {
                return true;
            }
            double dispersion = Math.max(1, maxOtherCombinationCount);
            if (columnStatistic.numNulls > 0
                    && isHotShuffleBucket(columnStatistic.numNulls / rowCount / dispersion, instanceNum)) {
                return true;
            }
        }
        return false;
    }

    /** Upper bound of combinations from marginal NDVs; a nullable column can add one more hash value. */
    private static double maxDistinctCombinationCount(
            List<Expression> expressions, Statistics inputStatistics) {
        double maxCombinationCount = 1;
        for (Expression expression : expressions) {
            ColumnStatistic columnStatistic = findColumnStatistic(expression, inputStatistics);
            double distinctValueCount = columnStatistic.ndv + (columnStatistic.numNulls > 0 ? 1 : 0);
            maxCombinationCount *= Math.max(1, distinctValueCount);
            if (maxCombinationCount >= inputStatistics.getRowCount()) {
                return inputStatistics.getRowCount();
            }
        }
        return maxCombinationCount;
    }

    /** Whether parent-required shuffle keys are safe to reuse before aggregate reduction. */
    public static boolean isSafeForParentShuffleReuse(
            List<Expression> shuffleKeys, Statistics inputStatistics, int instanceNum) {
        List<Expression> uniqueShuffleKeys = distinctShuffleKeys(shuffleKeys);
        if (uniqueShuffleKeys.size() == 1) {
            ColumnStatistic columnStatistic = findColumnStatistic(uniqueShuffleKeys.get(0), inputStatistics);
            return columnStatistic != null && !columnStatistic.isUnKnown
                    && StatisticsUtil.isBalancedAllowUnknownHotValues(
                            columnStatistic, instanceNum, shuffleKeyHotValueThreshold,
                            inputStatistics.getRowCount());
        }
        return isSafeForShuffleKeys(
                uniqueShuffleKeys, inputStatistics, instanceNum, ShuffleKeySafetyPolicy.PARENT_REUSE);
    }

    /** Whether actively pruning to the specified shuffle keys is safe. */
    public static boolean isSafeForShuffleKeyPruning(
            List<Expression> shuffleKeys, Statistics inputStatistics, int instanceNum) {
        return isSafeForShuffleKeys(
                shuffleKeys, inputStatistics, instanceNum, ShuffleKeySafetyPolicy.PRUNING);
    }

    private static boolean isSafeForShuffleKeys(List<Expression> shuffleKeys,
            Statistics inputStatistics, int instanceNum, ShuffleKeySafetyPolicy safetyPolicy) {
        List<Expression> uniqueShuffleKeys = distinctShuffleKeys(shuffleKeys);
        List<List<Expression>> shuffleDimensions = new ArrayList<>(uniqueShuffleKeys.size());
        for (Expression shuffleKey : uniqueShuffleKeys) {
            shuffleDimensions.add(ImmutableList.of(shuffleKey));
        }
        return isSafeForShuffleDimensions(
                shuffleDimensions, inputStatistics, instanceNum, safetyPolicy);
    }

    /** Whether actual shuffle dimensions can satisfy an approved parent-key reuse request. */
    static boolean isSafeForParentShuffleDimensions(
            List<? extends List<? extends Expression>> shuffleDimensions,
            Statistics inputStatistics, int instanceNum) {
        return isSafeForShuffleDimensions(
                shuffleDimensions, inputStatistics, instanceNum, ShuffleKeySafetyPolicy.PARENT_REUSE);
    }

    /** Whether actively pruning to the specified shuffle dimensions is safe. */
    static boolean isSafeForShuffleDimensionsPruning(
            List<? extends List<? extends Expression>> shuffleDimensions,
            Statistics inputStatistics, int instanceNum) {
        return isSafeForShuffleDimensions(
                shuffleDimensions, inputStatistics, instanceNum, ShuffleKeySafetyPolicy.PRUNING);
    }

    /**
     * Every inner list contains output expressions known to be equal. Consume all member statistics
     * conservatively while counting the dimension once in the combined-NDV estimate.
     */
    private static boolean isSafeForShuffleDimensions(
            List<? extends List<? extends Expression>> shuffleDimensions,
            Statistics inputStatistics, int instanceNum, ShuffleKeySafetyPolicy safetyPolicy) {
        if (shuffleDimensions.isEmpty()) {
            return false;
        }

        StatisticsBuilder conservativeStatistics = new StatisticsBuilder(inputStatistics);
        List<Expression> representatives = new ArrayList<>(shuffleDimensions.size());
        double rowCount = inputStatistics.getRowCount();
        for (List<? extends Expression> shuffleDimension : shuffleDimensions) {
            List<Expression> members = distinctShuffleKeys(shuffleDimension);
            Expression representative = members.get(0);
            ColumnStatistic representativeStatistic = findColumnStatistic(representative, inputStatistics);
            if (isUnsafeShuffleKey(
                    representativeStatistic, rowCount, instanceNum, safetyPolicy)) {
                return false;
            }
            double minNdv = representativeStatistic.ndv;
            for (int i = 1; i < members.size(); i++) {
                ColumnStatistic memberStatistic = findColumnStatistic(members.get(i), inputStatistics);
                if (isUnsafeShuffleKey(memberStatistic, rowCount, instanceNum, safetyPolicy)) {
                    return false;
                }
                minNdv = Math.min(minNdv, memberStatistic.ndv);
            }
            conservativeStatistics.putColumnStatistics(representative,
                    new ColumnStatisticBuilder(representativeStatistic).setNdv(minNdv).build());
            representatives.add(representative);
        }

        double combinedNdv = StatsCalculator.estimateGroupByRowCount(
                representatives, conservativeStatistics.build());
        long ndvThreshold = safetyPolicy == ShuffleKeySafetyPolicy.PARENT_REUSE
                ? (long) instanceNum * AggregateUtils.NDV_INSTANCE_BALANCE_MULTIPLIER
                : getBalancedNdvThreshold(instanceNum);
        return combinedNdv > ndvThreshold;
    }

    private static boolean isUnsafeShuffleKey(ColumnStatistic columnStatistic,
            double rowCount, int instanceNum, ShuffleKeySafetyPolicy safetyPolicy) {
        if (columnStatistic == null || columnStatistic.isUnKnown) {
            return true;
        }
        if (safetyPolicy == ShuffleKeySafetyPolicy.PARENT_REUSE) {
            return StatisticsUtil.hasSignificantHotValues(
                    columnStatistic, shuffleKeyHotValueThreshold, rowCount, false);
        }
        return hasPotentialSkew(columnStatistic, rowCount, instanceNum);
    }

    private static List<Expression> distinctShuffleKeys(List<? extends Expression> shuffleKeys) {
        return new ArrayList<>(new LinkedHashSet<>(shuffleKeys));
    }

    private static boolean hasConfiguredKnownHotValue(
            ColumnStatistic columnStatistic, double maxOtherCombinationCount) {
        if (maxOtherCombinationCount > AggregateUtils.LOW_NDV_THRESHOLD
                || columnStatistic.getHotValues() == null) {
            return false;
        }
        double hotValueThreshold = SessionVariable.getHotValueThreshold();
        double skewValueThreshold = SessionVariable.getSkewValueThreshold();
        for (double collectedRatio : columnStatistic.getHotValues().values()) {
            double ratioLowerBound = Math.max(0, collectedRatio - HOT_VALUE_RATIO_ROUNDING_ERROR);
            if (ratioLowerBound >= hotValueThreshold
                    || ratioLowerBound * Math.max(1, columnStatistic.ndv) >= skewValueThreshold) {
                return true;
            }
        }
        return false;
    }

    private static ColumnStatistic findColumnStatistic(Expression expression, Statistics inputStatistics) {
        ColumnStatistic columnStatistic = inputStatistics.findColumnStatistics(expression);
        return columnStatistic == null
                ? ExpressionEstimation.estimate(expression, inputStatistics)
                : columnStatistic;
    }

    private static boolean hasPotentialSkew(
            ColumnStatistic columnStatistic, double rowCount, int instanceNum) {
        if (columnStatistic == null || columnStatistic.isUnKnown
                || columnStatistic.getHotValues() == null) {
            return true;
        }
        if (columnStatistic.numNulls > 0
                && isHotShuffleBucket(columnStatistic.numNulls / rowCount, instanceNum)) {
            return true;
        }
        return columnStatistic.getHotValues().values().stream()
                .anyMatch(ratio -> isPotentialCollectedHotValueBucket(ratio, 1, instanceNum));
    }

    private static boolean isHotShuffleBucket(double ratio, int instanceNum) {
        return ratio >= shuffleKeyHotValueThreshold
                || ratio * instanceNum >= SHUFFLE_BUCKET_SKEW_MULTIPLIER;
    }

    private static boolean isPotentialCollectedHotValueBucket(
            double collectedRatio, double dispersion, int instanceNum) {
        double ratioUpperBound = Math.min(1, collectedRatio + HOT_VALUE_RATIO_ROUNDING_ERROR);
        return isHotShuffleBucket(ratioUpperBound / dispersion, instanceNum);
    }

    private static long getBalancedNdvThreshold(int instanceNum) {
        return Math.max(AggregateUtils.LOW_NDV_THRESHOLD,
                (long) instanceNum * AggregateUtils.NDV_INSTANCE_BALANCE_MULTIPLIER);
    }

    /**
     * Scenario 4: When partition expressions are set by rule, optionally reduce shuffle keys.
     * Strategy: 1) Try a safe single key; 2) Try a safe numeric+date key set (remove strings);
     * 3) Fall back to full partitionExprs.
     * Returns the list of expressions to use as shuffle keys, or empty to use full partitionExprs.
     */
    public static Optional<List<Expression>> selectBestShuffleKeyForAgg(
            PhysicalHashAggregate<? extends Plan> agg, List<Expression> partitionExprs, Statistics childStats,
            ConnectContext context) {
        int instanceNum = AggregateUtils.estimateExecutionInstanceNum(context);
        return selectOptimalShuffleKeys(partitionExprs, childStats, instanceNum);
    }

    /**
     * Select optimal shuffle keys with three-step strategy:
     * 1. Try single key: sort by type (numeric/date first, string sorted by avg_size), pick the first safe key.
     * 2. Try remove strings: use numeric+date keys when that reduced set is safe.
     * 3. Fall back: return empty (caller uses full partitionExprs).
     */
    private static Optional<List<Expression>> selectOptimalShuffleKeys(List<Expression> partitionExprs,
            Statistics childStats, int instanceNum) {
        List<SlotReference> slotRefs = partitionExprs.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .distinct()
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

        // Step 1: Try single key - sort by type priority, pick the first safe key.
        List<SlotReference> sortedByType = sortShuffleKeysByTypePriority(slotRefs, childStats);
        for (SlotReference slotRef : sortedByType) {
            List<Expression> candidate = ImmutableList.of(slotRef);
            if (isSafeForShuffleKeyPruning(candidate, childStats, instanceNum)) {
                return toOptionalIfChanged(partitionExprs, candidate);
            }
        }

        // Step 2: Try remove string types when the remaining key set is safe.
        List<Expression> numericAndDateExprs = slotRefs.stream()
                .filter(s -> s.getDataType().isNumericType() || s.getDataType().isDateLikeType())
                .collect(Collectors.toList());
        if (!numericAndDateExprs.isEmpty()
                && isSafeForShuffleKeyPruning(numericAndDateExprs, childStats, instanceNum)) {
            return toOptionalIfChanged(partitionExprs, ImmutableList.copyOf(numericAndDateExprs));
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
        int instanceNum = AggregateUtils.estimateExecutionInstanceNum(context);
        List<Pair<Slot, Slot>> validPairs = new ArrayList<>();
        for (int i = 0; i < leftOrderedShuffledColumns.size(); ++i) {
            validPairs.add(Pair.of(leftOrderedShuffledColumns.get(i), rightOrderedShuffledColumns.get(i)));
        }
        return selectOptimalJoinShuffleKeysFromPairs(validPairs,
                Pair.of(leftOrderedShuffledColumnId, rightOrderedShuffledColumnId),
                leftStats, rightStats, instanceNum);
    }

    /**
     * Three-step join shuffle optimization; compares result to {@code baselineForChange}.
     */
    private static Optional<Pair<List<ExprId>, List<ExprId>>> selectOptimalJoinShuffleKeysFromPairs(
            List<Pair<Slot, Slot>> validPairs,
            Pair<List<ExprId>, List<ExprId>> baselineForChange,
            Statistics leftStats, Statistics rightStats, int instanceNum) {
        for (Pair<Slot, Slot> pair : validPairs) {
            ColumnStatistic firstStats = leftStats.findColumnStatistics(pair.first);
            ColumnStatistic secondStats = rightStats.findColumnStatistics(pair.second);
            if (firstStats == null || secondStats == null || firstStats.isUnKnown || secondStats.isUnKnown
                    || firstStats.hotValues == null || secondStats.hotValues == null) {
                return Optional.empty();
            }
        }

        // Step 1: Try single key - sort by type, pick the first safe pair.
        List<Pair<Slot, Slot>> sortedPairs =
                sortJoinKeyPairsByTypePriority(validPairs, leftStats, rightStats);
        for (Pair<Slot, Slot> pair : sortedPairs) {
            Slot leftSlotRef = pair.first;
            Slot rightSlotRef = pair.second;
            if (isSafeForShuffleKeyPruning(ImmutableList.of(leftSlotRef), leftStats, instanceNum)
                    && isSafeForShuffleKeyPruning(ImmutableList.of(rightSlotRef), rightStats, instanceNum)) {
                return toOptionalIfChanged(baselineForChange, Pair.of(
                        ImmutableList.of(leftSlotRef.getExprId()),
                        ImmutableList.of(rightSlotRef.getExprId())));
            }
        }

        // Step 2: Try remove string types when both remaining key sets are safe.
        List<Slot> numericDateLeftSlots = new ArrayList<>();
        List<Slot> numericDateRightSlots = new ArrayList<>();
        for (Pair<Slot, Slot> pair : validPairs) {
            if ((pair.first.getDataType().isNumericType() || pair.first.getDataType().isDateLikeType())
                    && (pair.second.getDataType().isNumericType() || pair.second.getDataType().isDateLikeType())) {
                numericDateLeftSlots.add(pair.first);
                numericDateRightSlots.add(pair.second);
            }
        }
        if (!numericDateLeftSlots.isEmpty()
                && isSafeForShuffleKeyPruning(new ArrayList<Expression>(numericDateLeftSlots), leftStats, instanceNum)
                && isSafeForShuffleKeyPruning(
                        new ArrayList<Expression>(numericDateRightSlots), rightStats, instanceNum)) {
            List<ExprId> leftIds = numericDateLeftSlots.stream()
                    .map(Slot::getExprId)
                    .collect(Collectors.toList());
            List<ExprId> rightIds = numericDateRightSlots.stream()
                    .map(Slot::getExprId)
                    .collect(Collectors.toList());
            return toOptionalIfChanged(baselineForChange, Pair.of(leftIds, rightIds));
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
