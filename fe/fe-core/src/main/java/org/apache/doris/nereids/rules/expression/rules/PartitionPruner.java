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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndId;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndRange;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * PartitionPruner
 */
public class PartitionPruner extends DefaultExpressionRewriter<Void> {
    private final List<OnePartitionEvaluator<?>> partitions;
    private final Expression partitionPredicate;

    /** Different type of table may have different partition prune behavior. */
    public enum PartitionTableType {
        OLAP,
        EXTERNAL
    }

    private PartitionPruner(List<OnePartitionEvaluator<?>> partitions, Expression partitionPredicate) {
        this.partitions = Objects.requireNonNull(partitions, "partitions cannot be null");
        this.partitionPredicate = Objects.requireNonNull(partitionPredicate.accept(this, null),
                "partitionPredicate cannot be null");
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, Void context) {
        // Date cp Date is not supported in BE storage engine. So cast to DateTime in SimplifyComparisonPredicate
        // for easy process partition prune, we convert back to date compare date here
        // see more info in SimplifyComparisonPredicate
        Expression left = cp.left();
        Expression right = cp.right();
        if (left.getDataType() != DateTimeType.INSTANCE || right.getDataType() != DateTimeType.INSTANCE) {
            return cp;
        }
        if (!(left instanceof DateTimeLiteral) && !(right instanceof DateTimeLiteral)) {
            return cp;
        }
        if (left instanceof DateTimeLiteral && ((DateTimeLiteral) left).isMidnight()
                && right instanceof Cast
                && ((Cast) right).child() instanceof SlotReference
                && ((Cast) right).child().getDataType().isDateType()) {
            DateTimeLiteral dt = (DateTimeLiteral) left;
            Cast cast = (Cast) right;
            return cp.withChildren(
                    ImmutableList.of(new DateLiteral(dt.getYear(), dt.getMonth(), dt.getDay()), cast.child())
            );
        } else if (right instanceof DateTimeLiteral && ((DateTimeLiteral) right).isMidnight()
                && left instanceof Cast
                && ((Cast) left).child() instanceof SlotReference
                && ((Cast) left).child().getDataType().isDateType()) {
            DateTimeLiteral dt = (DateTimeLiteral) right;
            Cast cast = (Cast) left;
            return cp.withChildren(ImmutableList.of(
                    cast.child(),
                    new DateLiteral(dt.getYear(), dt.getMonth(), dt.getDay()))
            );
        } else {
            return cp;
        }
    }

    /** prune */
    public <K extends Comparable<K>> Pair<List<K>, Boolean> prune() {
        Builder<K> scanPartitionIdents = ImmutableList.builder();
        boolean canPredicatePruned = true;
        for (OnePartitionEvaluator partition : partitions) {
            Pair<Boolean, Boolean> res = canBePrunedOut(partitionPredicate, partition);
            if (!res.first) {
                canPredicatePruned = canPredicatePruned && res.second;
                scanPartitionIdents.add((K) partition.getPartitionIdent());
            }
        }
        return Pair.of(scanPartitionIdents.build(), canPredicatePruned);
    }

    public static <K extends Comparable<K>> Pair<List<K>, Optional<Expression>> prune(List<Slot> partitionSlots,
            Expression partitionPredicate,
            Map<K, PartitionItem> idToPartitions, CascadesContext cascadesContext,
            PartitionTableType partitionTableType) {
        return prune(partitionSlots, partitionPredicate, idToPartitions,
                cascadesContext, partitionTableType, Optional.empty());
    }

    /**
     * prune partition with `idToPartitions` as parameter.
     */
    public static <K extends Comparable<K>> Pair<List<K>, Optional<Expression>> prune(List<Slot> partitionSlots,
            Expression partitionPredicate,
            Map<K, PartitionItem> idToPartitions, CascadesContext cascadesContext,
            PartitionTableType partitionTableType, Optional<SortedPartitionRanges<K>> sortedPartitionRanges) {
        long startAt = System.currentTimeMillis();
        try {
            return pruneInternal(partitionSlots, partitionPredicate, idToPartitions, cascadesContext,
                    partitionTableType,
                    sortedPartitionRanges);
        } finally {
            SummaryProfile profile = SummaryProfile.getSummaryProfile(cascadesContext.getConnectContext());
            if (profile != null) {
                profile.addNereidsPartitiionPruneTime(System.currentTimeMillis() - startAt);
            }
        }
    }

    private static <K extends Comparable<K>> Pair<List<K>, Optional<Expression>> pruneInternal(
            List<Slot> partitionSlots,
            Expression partitionPredicate,
            Map<K, PartitionItem> idToPartitions, CascadesContext cascadesContext,
            PartitionTableType partitionTableType, Optional<SortedPartitionRanges<K>> sortedPartitionRanges) {
        partitionPredicate = PartitionPruneExpressionExtractor.extract(
                partitionPredicate, ImmutableSet.copyOf(partitionSlots), cascadesContext);
        Expression originalPartitionPredicate = partitionPredicate;
        partitionPredicate = PredicateRewriteForPartitionPrune.rewrite(partitionPredicate, cascadesContext);
        int expandThreshold = cascadesContext.getAndCacheSessionVariable(
                "partitionPruningExpandThreshold",
                10, sessionVariable -> sessionVariable.partitionPruningExpandThreshold);

        partitionPredicate = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(
                partitionPredicate, new ExpressionRewriteContext(cascadesContext));
        if (BooleanLiteral.TRUE.equals(partitionPredicate)) {
            // The partition column predicate is always true and can be deleted, the partition cannot be pruned
            return Pair.of(Utils.fastToImmutableList(idToPartitions.keySet()), Optional.of(originalPartitionPredicate));
        } else if (BooleanLiteral.FALSE.equals(partitionPredicate) || partitionPredicate.isNullLiteral()) {
            // The partition column predicate is always false, and all partitions can be pruned.
            return Pair.of(ImmutableList.of(), Optional.empty());
        }

        if (sortedPartitionRanges.isPresent()) {
            RangeSet<MultiColumnBound> predicateRanges = partitionPredicate.accept(
                    new PartitionPredicateToRange(partitionSlots), null);
            if (predicateRanges != null) {
                Pair<List<K>, Boolean> res = binarySearchFiltering(
                        sortedPartitionRanges.get(), partitionSlots, partitionPredicate, cascadesContext,
                        expandThreshold, predicateRanges
                );
                if (res.second) {
                    return Pair.of(res.first, Optional.of(originalPartitionPredicate));
                } else {
                    return Pair.of(res.first, Optional.empty());
                }
            }
        }

        Pair<List<K>, Boolean> res = sequentialFiltering(
                idToPartitions, partitionSlots, partitionPredicate, cascadesContext, expandThreshold
        );
        if (res.second) {
            return Pair.of(res.first, Optional.of(originalPartitionPredicate));
        } else {
            return Pair.of(res.first, Optional.empty());
        }
    }

    /**
     * convert partition item to partition evaluator
     */
    public static <K> OnePartitionEvaluator<K> toPartitionEvaluator(K id, PartitionItem partitionItem,
            List<Slot> partitionSlots, CascadesContext cascadesContext, int expandThreshold) {
        if (partitionItem instanceof ListPartitionItem) {
            return new OneListPartitionEvaluator<>(
                    id, partitionSlots, (ListPartitionItem) partitionItem, cascadesContext);
        } else if (partitionItem instanceof RangePartitionItem) {
            return new OneRangePartitionEvaluator<>(
                    id, partitionSlots, (RangePartitionItem) partitionItem, cascadesContext, expandThreshold);
        } else {
            return new UnknownPartitionEvaluator<>(id, partitionItem);
        }
    }

    private static <K extends Comparable<K>> Pair<List<K>, Boolean> binarySearchFiltering(
            SortedPartitionRanges<K> sortedPartitionRanges, List<Slot> partitionSlots,
            Expression partitionPredicate, CascadesContext cascadesContext, int expandThreshold,
            RangeSet<MultiColumnBound> predicateRanges) {
        List<PartitionItemAndRange<K>> sortedPartitions = sortedPartitionRanges.sortedPartitions;

        Set<K> selectedIdSets = Sets.newTreeSet();
        int leftIndex = 0;
        boolean canPredicatePruned = true;
        for (Range<MultiColumnBound> predicateRange : predicateRanges.asRanges()) {
            int rightIndex = sortedPartitions.size();
            if (leftIndex >= rightIndex) {
                break;
            }

            int midIndex;
            MultiColumnBound predicateUpperBound = predicateRange.upperEndpoint();
            MultiColumnBound predicateLowerBound = predicateRange.lowerEndpoint();

            while (leftIndex + 1 < rightIndex) {
                midIndex = (leftIndex + rightIndex) / 2;
                PartitionItemAndRange<K> partition = sortedPartitions.get(midIndex);
                Range<MultiColumnBound> partitionSpan = partition.range;

                if (predicateUpperBound.compareTo(partitionSpan.lowerEndpoint()) < 0) {
                    rightIndex = midIndex;
                } else if (predicateLowerBound.compareTo(partitionSpan.upperEndpoint()) > 0) {
                    leftIndex = midIndex;
                } else {
                    break;
                }
            }

            for (; leftIndex < sortedPartitions.size(); leftIndex++) {
                PartitionItemAndRange<K> partition = sortedPartitions.get(leftIndex);

                K partitionId = partition.id;
                // list partition will expand to multiple PartitionItemAndRange, we should skip evaluate it again
                if (selectedIdSets.contains(partitionId)) {
                    continue;
                }

                Range<MultiColumnBound> partitionSpan = partition.range;
                if (predicateUpperBound.compareTo(partitionSpan.lowerEndpoint()) < 0) {
                    break;
                }

                OnePartitionEvaluator<K> partitionEvaluator = toPartitionEvaluator(
                        partitionId, partition.partitionItem, partitionSlots, cascadesContext, expandThreshold);
                Pair<Boolean, Boolean> res = canBePrunedOut(partitionPredicate, partitionEvaluator);
                if (!res.first) {
                    selectedIdSets.add(partitionId);
                    canPredicatePruned = canPredicatePruned && res.second;
                }
            }
        }

        for (PartitionItemAndId<K> defaultPartition : sortedPartitionRanges.defaultPartitions) {
            K partitionId = defaultPartition.id;
            OnePartitionEvaluator<K> partitionEvaluator = toPartitionEvaluator(
                    partitionId, defaultPartition.partitionItem, partitionSlots, cascadesContext, expandThreshold);
            Pair<Boolean, Boolean> res = canBePrunedOut(partitionPredicate, partitionEvaluator);
            if (!res.first) {
                selectedIdSets.add(partitionId);
                canPredicatePruned = canPredicatePruned && res.second;
            }
        }

        return Pair.of(Utils.fastToImmutableList(selectedIdSets), canPredicatePruned);
    }

    private static <K extends Comparable<K>> Pair<List<K>, Boolean> sequentialFiltering(
            Map<K, PartitionItem> idToPartitions, List<Slot> partitionSlots,
            Expression partitionPredicate, CascadesContext cascadesContext, int expandThreshold) {
        List<OnePartitionEvaluator<?>> evaluators = Lists.newArrayListWithCapacity(idToPartitions.size());
        for (Entry<K, PartitionItem> kv : idToPartitions.entrySet()) {
            evaluators.add(toPartitionEvaluator(
                    kv.getKey(), kv.getValue(), partitionSlots, cascadesContext, expandThreshold));
        }
        PartitionPruner partitionPruner = new PartitionPruner(evaluators, partitionPredicate);
        //TODO: we keep default partition because it's too hard to prune it, we return false in canPrune().
        return partitionPruner.prune();
    }

    /**
     * return Pair
     *     pair.first is true if partition can be pruned
     *     pair.second is true if partitionPredicate is always true in this partition
     */
    private static <K> Pair<Boolean, Boolean> canBePrunedOut(Expression partitionPredicate,
            OnePartitionEvaluator<K> evaluator) {
        List<Map<Slot, PartitionSlotInput>> onePartitionInputs = evaluator.getOnePartitionInputs();
        if (evaluator instanceof OneListPartitionEvaluator) {
            // if a table has default partition, the predicate should not be pruned,
            // because evaluateWithDefaultPartition always return true in default partition
            // e.g. PARTITION BY LIST(k1) (
            //     PARTITION p1 VALUES IN ("1","2","3","4"),
            //     PARTITION p2 VALUES IN ("5","6","7","8"),
            //     PARTITION p3 )  p3 is default partition
            boolean notDefaultPartition = !evaluator.isDefaultPartition();
            Pair<Boolean, Boolean> res = Pair.of(notDefaultPartition, notDefaultPartition);
            for (Map<Slot, PartitionSlotInput> currentInputs : onePartitionInputs) {
                // evaluate whether there's possible for this partition to accept this predicate
                Expression result = evaluator.evaluateWithDefaultPartition(partitionPredicate, currentInputs);
                if (result.equals(BooleanLiteral.FALSE) || (result instanceof NullLiteral)) {
                    // Indicates that there is a partition value that does not satisfy the predicate
                    res.second = false;
                } else if (result.equals(BooleanLiteral.TRUE)) {
                    // Indicates that there is a partition value that satisfies the predicate
                    res.first = false;
                } else {
                    // Indicates that this partition value may or may not satisfy the predicate
                    res.second = false;
                    res.first = false;
                }
                if (!res.first && !res.second) {
                    break;
                }
            }
            return res;
        } else {
            // only prune partition predicates in list partition, therefore set pair.second always be false,
            // meaning not to prune partition predicates in range partition
            for (Map<Slot, PartitionSlotInput> currentInputs : onePartitionInputs) {
                Expression result = evaluator.evaluateWithDefaultPartition(partitionPredicate, currentInputs);
                if (!result.equals(BooleanLiteral.FALSE) && !(result instanceof NullLiteral)) {
                    return Pair.of(false, false);
                }
            }
            // only have false result: Can be pruned out. have other exprs: CanNot be pruned out
            return Pair.of(true, false);
        }
    }

    /** remove predicates that are always true*/
    public static Plan prunePredicate(boolean skipPrunePredicate, Optional<Expression> prunedPredicates,
            LogicalFilter<? extends Plan> filter, LogicalRelation scan) {
        if (!skipPrunePredicate && prunedPredicates.isPresent()) {
            Set<Expression> conjuncts = new LinkedHashSet<>(filter.getConjuncts());
            Expression deletedPredicate = prunedPredicates.get();
            Set<Expression> deletedPredicateSet = ExpressionUtils.extractConjunctionToSet(deletedPredicate);
            conjuncts.removeAll(deletedPredicateSet);
            if (conjuncts.isEmpty()) {
                return scan;
            } else {
                return filter.withConjunctsAndChild(conjuncts, scan);
            }
        } else {
            return filter.withChildren(ImmutableList.of(scan));
        }
    }
}
