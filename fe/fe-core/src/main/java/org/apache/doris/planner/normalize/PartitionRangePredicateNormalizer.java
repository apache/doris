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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlanFragment.java
// and modified by Doris

package org.apache.doris.planner.normalize;

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.Pair;
import org.apache.doris.planner.OlapScanNode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** PartitionRangePredicateNormalizer */
public class PartitionRangePredicateNormalizer {
    private final Normalizer normalizer;
    private final OlapScanNode olapScanNode;

    public PartitionRangePredicateNormalizer(Normalizer normalizer, OlapScanNode olapScanNode) {
        this.normalizer = Objects.requireNonNull(normalizer, "normalizer can not be null");
        this.olapScanNode = Objects.requireNonNull(olapScanNode, "olapScanNode can not be null");
    }

    public List<Expr> normalize() {
        NormalizedPartitionPredicates predicates = normalizePredicates();
        normalizer.setNormalizedPartitionPredicates(olapScanNode, predicates);
        return predicates.remainedPredicates;
    }

    private NormalizedPartitionPredicates normalizePredicates() {
        OlapTable olapTable = olapScanNode.getOlapTable();
        List<Column> partitionColumns = olapTable.getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            return cannotIntersectPartitionRange();
        }

        if (partitionColumns.size() > 1) {
            return cannotIntersectPartitionRange();
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() != PartitionType.RANGE) {
            return cannotIntersectPartitionRange();
        }

        return normalizeSingleRangePartitionColumnPredicates(
                partitionColumns.get(0), (RangePartitionInfo) partitionInfo);
    }

    private NormalizedPartitionPredicates normalizeSingleRangePartitionColumnPredicates(
            Column partitionColumn, RangePartitionInfo rangePartitionInfo) {

        List<Pair<Long, RangeSet<PartitionKey>>> partitionItemRanges
                = rangePartitionInfo.getPartitionItems(olapScanNode.getSelectedPartitionIds())
                .entrySet()
                .stream()
                .map(entry -> {
                    RangeSet<PartitionKey> rangeSet = TreeRangeSet.create();
                    rangeSet.add(entry.getValue().getItems());
                    return Pair.of(entry.getKey(), rangeSet);
                })
                .collect(Collectors.toList());

        List<Expr> conjuncts = extractConjuncts(olapScanNode.getConjuncts());

        ToRangePredicatesExtractor extractor = ToRangePredicatesExtractor.extract(
                conjuncts, olapScanNode, partitionColumn);

        PredicateToRange predicateToRange = new PredicateToRange(partitionColumn);

        for (Expr partitionPredicate : extractor.supportedToRangePredicates) {
            RangeSet<PartitionKey> predicateRanges = predicateToRange.exprToRange(partitionPredicate);
            partitionItemRanges = partitionItemRanges.stream()
                    .map(kv -> {
                        RangeSet<PartitionKey> partitionRangeSet = kv.second;
                        RangeSet<PartitionKey> intersect = TreeRangeSet.create();
                        for (Range<PartitionKey> predicateRange : predicateRanges.asRanges()) {
                            intersect.addAll(partitionRangeSet.subRangeSet(predicateRange));
                        }
                        return Pair.of(kv.first, intersect);
                    }).filter(kv -> !kv.second.isEmpty())
                    .collect(Collectors.toList());
        }

        Map<Long, String> partitionToIntersectRange = partitionItemRanges.stream()
                .map(pair -> Pair.of(pair.first, normalizeRangeSet(pair.second).toString()))
                .collect(Collectors.toMap(Pair::key, Pair::value));

        return new NormalizedPartitionPredicates(extractor.notSupportedToRangePredicates, partitionToIntersectRange);
    }

    private RangeSet<PartitionKey> normalizeRangeSet(RangeSet<PartitionKey> rangeSet) {
        // normalize range to closeOpened range, the between predicate and less than predicate
        // maybe reuse the same cache to save memory
        RangeSet<PartitionKey> normalized = TreeRangeSet.create();
        for (Range<PartitionKey> range : rangeSet.asRanges()) {
            PartitionKey lowerEndpoint = range.lowerEndpoint();
            PartitionKey upperEndpoint = range.upperEndpoint();

            try {
                if (!lowerEndpoint.isMinValue() && !range.contains(lowerEndpoint)) {
                    lowerEndpoint = lowerEndpoint.successor();
                }
                if (!upperEndpoint.isMaxValue() && range.contains(upperEndpoint)) {
                    upperEndpoint = upperEndpoint.successor();
                }
                normalized.add(Range.closedOpen(lowerEndpoint, upperEndpoint));
            } catch (Throwable t) {
                throw new IllegalStateException("Can not normalize range: " + t.getMessage(), t);
            }
        }
        return normalized;
    }

    private NormalizedPartitionPredicates cannotIntersectPartitionRange() {
        return new NormalizedPartitionPredicates(
                // conjuncts will be used as the part of the digest
                olapScanNode.getConjuncts(),
                // can not compute intersect range
                ImmutableMap.of(olapScanNode.getSelectedPartitionIds().iterator().next(), "")
        );
    }

    private List<Expr> extractConjuncts(List<Expr> conjuncts) {
        List<Expr> flattenedConjuncts = Lists.newArrayListWithCapacity(conjuncts.size());
        for (Expr conjunct : conjuncts) {
            boolean findChildren = true;
            conjunct.foreachDown(expr -> {
                if (expr instanceof CompoundPredicate && ((CompoundPredicate) expr).getOp() == Operator.AND) {
                    return findChildren;
                } else {
                    flattenedConjuncts.add((Expr) expr);
                    return !findChildren;
                }
            });
        }
        return flattenedConjuncts;
    }

    private static class ToRangePredicatesExtractor {
        public final List<Expr> supportedToRangePredicates;
        public final List<Expr> notSupportedToRangePredicates;

        private ToRangePredicatesExtractor(
                List<Expr> simplePartitionPredicates, List<Expr> notSupportedToRangePredicates) {
            this.supportedToRangePredicates = simplePartitionPredicates;
            this.notSupportedToRangePredicates = notSupportedToRangePredicates;
        }

        public static ToRangePredicatesExtractor extract(
                List<Expr> conjuncts, OlapScanNode olapScanNode, Column partitionColumn) {
            List<Expr> supportedPartitionPredicates = Lists.newArrayList();
            List<Expr> otherPredicates = Lists.newArrayList();

            Optional<SlotId> optPartitionId = findPartitionColumnSlotId(olapScanNode, partitionColumn);

            for (Expr conjunct : conjuncts) {
                if (optPartitionId.isPresent()
                        && conjunct.isBound(optPartitionId.get())
                        && PredicateToRange.supportedToRange(conjunct)) {
                    supportedPartitionPredicates.add(conjunct);
                } else {
                    otherPredicates.add(conjunct);
                }
            }

            return new ToRangePredicatesExtractor(supportedPartitionPredicates, otherPredicates);
        }

        private static Optional<SlotId> findPartitionColumnSlotId(OlapScanNode olapScanNode, Column partitionColumn) {
            if (partitionColumn == null) {
                return Optional.empty();
            }

            for (SlotDescriptor slot : olapScanNode.getTupleDesc().getSlots()) {
                Column column = slot.getColumn();
                if (column.getName().equalsIgnoreCase(partitionColumn.getName())) {
                    return Optional.of(slot.getId());
                }
            }
            return Optional.empty();
        }
    }
}
