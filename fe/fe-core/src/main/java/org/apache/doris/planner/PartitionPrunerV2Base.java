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

package org.apache.doris.planner;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;

public abstract class PartitionPrunerV2Base implements PartitionPruner {
    protected final Map<Long, PartitionItem> idToPartitionItem;
    protected final List<Column> partitionColumns;
    protected final Map<String, ColumnRange> columnNameToRange;

    public PartitionPrunerV2Base(Map<Long, PartitionItem> idToPartitionItem,
                                 List<Column> partitionColumns,
                                 Map<String, ColumnRange> columnNameToRange) {
        this.idToPartitionItem = idToPartitionItem;
        this.partitionColumns = partitionColumns;
        this.columnNameToRange = columnNameToRange;
    }

    @Override
    public Collection<Long> prune() throws AnalysisException {
        Map<Column, FinalFilters> columnToFilters = Maps.newHashMap();
        for (Column column : partitionColumns) {
            ColumnRange columnRange = columnNameToRange.get(column.getName());
            if (columnRange == null) {
                columnToFilters.put(column, FinalFilters.noFilters());
            } else {
                columnToFilters.put(column, getFinalFilters(columnRange, column));
            }
        }

        if (partitionColumns.size() == 1) {
            return pruneSingleColumnPartition(columnToFilters);
        } else if (partitionColumns.size() > 1) {
            return pruneMultipleColumnPartition(columnToFilters);
        } else {
            return Lists.newArrayList();
        }
    }

    abstract RangeMap<ColumnBound, UniqueId> getCandidateRangeMap();

    /**
     * Handle conjunctive and disjunctive `is null` predicates.
     */
    abstract FinalFilters getFinalFilters(ColumnRange columnRange,
                                          Column column) throws AnalysisException;

    /**
     * It's a little complex to unify the logic of pruning multiple columns partition for both
     * list and range partitions.
     *
     * The key point is that the list partitions value are the explicit values of partition columns,
     * however, the range bound for a partition column in multiple columns partition is depended on
     * both other partition columns' range values and the range value itself.
     *
     * Let's say we have two partition columns k1, k2:
     * For partition [(1, 5), (1, 10)), the range for k2 is [5, 10).
     * For partition [(1, 5), (2, 10)), the range for k2 is (-∞, +∞).
     * For partition [(1, 10), (2, 5)), the range for k2 is (-∞, 5) union [10, +∞).
     *
     * We could try to compute the range bound of every column in multiple columns partition and
     * unify the logic like pruning multiple list columns partition for multiple range ones.
     */
    abstract Collection<Long> pruneMultipleColumnPartition(
        Map<Column, FinalFilters> columnToFilters) throws AnalysisException;

    /**
     * Now we could unify the logic of pruning single column partition for both list and range
     * partitions.
     */
    private Collection<Long> pruneSingleColumnPartition(Map<Column, FinalFilters> columnToFilters) {
        FinalFilters finalFilters = columnToFilters.get(partitionColumns.get(0));
        switch (finalFilters.type) {
            case CONSTANT_FALSE_FILTERS:
                return Collections.emptyList();
            case HAVE_FILTERS:
                RangeMap<ColumnBound, UniqueId> candidate = getCandidateRangeMap();
                return finalFilters.filters.stream()
                    .map(filter -> {
                        RangeMap<ColumnBound, UniqueId> filtered = candidate.subRangeMap(filter);
                        return filtered.asMapOfRanges().values().stream()
                            .map(UniqueId::getPartitionId)
                            .collect(Collectors.toSet());
                    })
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
            case NO_FILTERS:
            default:
                return idToPartitionItem.keySet();
        }
    }

    protected Range<ColumnBound> mapPartitionKeyRange(Range<PartitionKey> fromRange,
                                                      int columnIdx) {
        return mapRange(fromRange,
            partitionKey -> ColumnBound.of(partitionKey.getKeys().get(columnIdx)));
    }

    protected <TO extends Comparable, FROM extends Comparable>
    Range<TO> mapRange(Range<FROM> range, Function<FROM, TO> mapper) {
        TO lower = range.hasLowerBound() ? mapper.apply(range.lowerEndpoint()) : null;
        TO upper = range.hasUpperBound() ? mapper.apply(range.upperEndpoint()) : null;
        if (range.hasUpperBound()) {
            // has upper bound
            if (range.hasLowerBound()) {
                return Range.range(lower, range.lowerBoundType(), upper, range.upperBoundType());
            } else {
                if (range.upperBoundType() == BoundType.OPEN) {
                    return Range.lessThan(upper);
                } else {
                    return Range.atMost(upper);
                }
            }
        } else if (range.hasLowerBound()) {
            // has no upper bound, but has lower bound
            if (range.lowerBoundType() == BoundType.OPEN) {
                return Range.greaterThan(lower);
            } else {
                return Range.atLeast(lower);
            }
        } else {
            // has neither upper nor lower bound
            return Range.all();
        }
    }

    protected interface UniqueId {
        long getPartitionId();
    }

    protected static class FinalFilters {
        enum Type {
            // Have no filters, should just return all the partitions.
            NO_FILTERS,
            // Have filters.
            HAVE_FILTERS,
            // Filter predicates are folded to constant false, pruned partitions should be
            // an empty collection.
            CONSTANT_FALSE_FILTERS,
        }

        final Type type;
        final Set<Range<ColumnBound>> filters;

        private FinalFilters(Type type, Set<Range<ColumnBound>> filters) {
            this.type = type;
            this.filters = filters;
        }

        private static final FinalFilters NO_FILTERS = new FinalFilters(Type.NO_FILTERS, null);

        private static final FinalFilters CONSTANT_FALSE_FILTERS =
            new FinalFilters(Type.CONSTANT_FALSE_FILTERS, null);

        public static FinalFilters noFilters() {
            return NO_FILTERS;
        }

        public static FinalFilters constantFalseFilters() {
            return CONSTANT_FALSE_FILTERS;
        }

        public static FinalFilters create(Set<Range<ColumnBound>> filters) {
            return new FinalFilters(Type.HAVE_FILTERS, filters);
        }
    }
}
