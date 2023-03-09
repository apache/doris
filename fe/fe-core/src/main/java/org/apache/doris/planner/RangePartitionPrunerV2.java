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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RangePartitionPrunerV2 extends PartitionPrunerV2Base {
    public RangePartitionPrunerV2(Map<Long, PartitionItem> idToPartitionItem,
            List<Column> partitionColumns,
            Map<String, ColumnRange> columnNameToRange) {
        super(idToPartitionItem, partitionColumns, columnNameToRange);
    }

    @Override
    void genSingleColumnRangeMap() {
        if (singleColumnRangeMap == null) {
            singleColumnRangeMap = genSingleColumnRangeMap(idToPartitionItem);
        }
    }

    public static RangeMap<ColumnBound, UniqueId> genSingleColumnRangeMap(Map<Long, PartitionItem> idToPartitionItem) {
        RangeMap<ColumnBound, UniqueId> candidate = TreeRangeMap.create();
        idToPartitionItem.forEach((id, item) -> {
            Range<PartitionKey> range = item.getItems();
            candidate.put(mapPartitionKeyRange(range, 0), new RangePartitionUniqueId(id));
        });
        return candidate;
    }

    /**
     * This is just like the logic in v1 version, but we support disjunctive predicates here.
     */
    @Override
    Collection<Long> pruneMultipleColumnPartition(Map<Column, FinalFilters> columnToFilters) throws AnalysisException {
        PartitionKey minKey = new PartitionKey();
        PartitionKey maxKey = new PartitionKey();
        RangeMap<PartitionKey, Long> rangeMap = TreeRangeMap.create();
        idToPartitionItem.forEach((id, item) -> rangeMap.put(item.getItems(), id));
        return doPruneMulti(columnToFilters, rangeMap, 0, minKey, maxKey);
    }

    @Override
    FinalFilters getFinalFilters(ColumnRange columnRange,
                                 Column column) throws AnalysisException {
        if (!columnRange.hasFilter()) {
            return FinalFilters.noFilters();
        }

        Optional<RangeSet<ColumnBound>> rangeSetOpt = columnRange.getRangeSet();
        if (columnRange.hasConjunctiveIsNull()) {
            if (!rangeSetOpt.isPresent()) {
                // For Hive external table, partition column could be null.
                // In which case, the data will be put to a default partition __HIVE_DEFAULT_PARTITION__
                if (isHive) {
                    return FinalFilters.noFilters();
                }
                // Only has conjunctive `is null` predicate.
                return FinalFilters.create(Sets.newHashSet(getMinInfinityRange(column)));
            } else {
                // Has both conjunctive `is null` predicate and other predicates.
                return FinalFilters.constantFalseFilters();
            }
        } else {
            if (columnRange.hasDisjunctiveIsNull()) {
                if (rangeSetOpt.isPresent() && !rangeSetOpt.get().isEmpty()) {
                    RangeSet<ColumnBound> rangeSet = rangeSetOpt.get();
                    rangeSet.add(getMinInfinityRange(column));
                    return FinalFilters.create(rangeSet.asRanges());
                } else {
                    return FinalFilters.create(Sets.newHashSet(getMinInfinityRange(column)));
                }
            } else {
                if (rangeSetOpt.isPresent()) {
                    RangeSet<ColumnBound> rangeSet = rangeSetOpt.get();
                    if (rangeSet.isEmpty()) {
                        return FinalFilters.constantFalseFilters();
                    } else {
                        return FinalFilters.create(rangeSet.asRanges());
                    }
                } else {
                    return FinalFilters.noFilters();
                }
            }
        }
    }

    private Range<ColumnBound> getMinInfinityRange(Column column) throws AnalysisException {
        ColumnBound value = ColumnBound.of(
                LiteralExpr.createInfinity(Type.fromPrimitiveType(column.getDataType()), false));
        return Range.closed(value, value);
    }

    private Collection<Long> doPruneMulti(Map<Column, FinalFilters> columnToFilters,
                                          RangeMap<PartitionKey, Long> rangeMap,
                                          int columnIdx,
                                          PartitionKey minKey,
                                          PartitionKey maxKey) throws AnalysisException {

        // the last column in partition Key
        if (columnIdx == partitionColumns.size()) {
            try {
                return Lists.newArrayList(rangeMap.subRangeMap(Range.closed(minKey, maxKey))
                    .asMapOfRanges().values());
            } catch (IllegalArgumentException e) {
                return Lists.newArrayList();
            }
        }

        Column column = partitionColumns.get(columnIdx);
        FinalFilters finalFilters = columnToFilters.get(column);
        switch (finalFilters.type) {
            case HAVE_FILTERS:
                Set<Range<ColumnBound>> filters = finalFilters.filters;
                Set<Long> result = Sets.newHashSet();
                for (Range<ColumnBound> filter : filters) {
                    if (filter.hasLowerBound() && filter.lowerBoundType() == BoundType.CLOSED
                            && filter.hasUpperBound() && filter.upperBoundType() == BoundType.CLOSED
                            && filter.lowerEndpoint() == filter.upperEndpoint()) {
                        // Equal to predicate, e.g., col=1, the filter range is [1, 1].
                        minKey.pushColumn(filter.lowerEndpoint().getValue(), column.getDataType());
                        maxKey.pushColumn(filter.upperEndpoint().getValue(), column.getDataType());
                        result.addAll(doPruneMulti(columnToFilters, rangeMap, columnIdx + 1, minKey, maxKey));
                        minKey.popColumn();
                        maxKey.popColumn();
                    } else {
                        // Range that is not an equal to predicate.
                        int lastColumnId = partitionColumns.size() - 1;
                        int pushMinCount = 0;
                        int pushMaxCount = 0;
                        // lower bound
                        if (filter.hasLowerBound()) {
                            minKey.pushColumn(filter.lowerEndpoint().getValue(), column.getDataType());
                            pushMinCount++;
                            if (filter.lowerBoundType() == BoundType.CLOSED && columnIdx != lastColumnId) {
                                pushInfinity(minKey, columnIdx + 1, false);
                                pushMinCount++;
                            }
                        } else {
                            pushInfinity(minKey, columnIdx, false);
                            pushMinCount++;
                        }

                        // upper bound
                        if (filter.hasUpperBound()) {
                            maxKey.pushColumn(filter.upperEndpoint().getValue(), column.getDataType());
                            pushMaxCount++;
                            if (filter.upperBoundType() == BoundType.CLOSED && columnIdx != lastColumnId) {
                                pushInfinity(maxKey, columnIdx + 1, true);
                                pushMaxCount++;
                            }
                        } else {
                            pushInfinity(maxKey, columnIdx, true);
                            pushMaxCount++;
                        }

                        try {
                            BoundType lowerType = filter.hasLowerBound() && filter.lowerBoundType() == BoundType.CLOSED
                                    ? BoundType.CLOSED : BoundType.OPEN;
                            BoundType upperType = filter.hasUpperBound() && filter.upperBoundType() == BoundType.CLOSED
                                    ? BoundType.CLOSED : BoundType.OPEN;
                            result.addAll(rangeMap.subRangeMap(Range.range(minKey, lowerType, maxKey, upperType))
                                    .asMapOfRanges().values());
                        } catch (IllegalArgumentException e) {
                            // CHECKSTYLE IGNORE THIS LINE
                        }

                        for (; pushMinCount > 0; pushMinCount--) {
                            minKey.popColumn();
                        }
                        for (; pushMaxCount > 0; pushMaxCount--) {
                            maxKey.popColumn();
                        }
                    }
                }
                return result;
            case CONSTANT_FALSE_FILTERS:
                return Collections.emptyList();
            case NO_FILTERS:
            default:
                return noFiltersResult(minKey, maxKey, columnIdx, rangeMap);
        }
    }

    private void pushInfinity(PartitionKey key, int columnIdx,
                              boolean isMax) throws AnalysisException {
        Column column = partitionColumns.get(columnIdx);
        key.pushColumn(LiteralExpr.createInfinity(Type.fromPrimitiveType(column.getDataType()), isMax),
                column.getDataType());
    }

    private Collection<Long> noFiltersResult(PartitionKey minKey, PartitionKey maxKey,
                                             int columnIdx,
                                             RangeMap<PartitionKey, Long> rangeMap) throws AnalysisException {
        pushInfinity(minKey, columnIdx, false);
        pushInfinity(maxKey, columnIdx, true);
        Collection<Long> result;
        try {
            result = Lists.newArrayList(
                rangeMap.subRangeMap(Range.closed(minKey, maxKey)).asMapOfRanges().values());
        } catch (IllegalArgumentException e) {
            result = Lists.newArrayList();
        }
        minKey.popColumn();
        maxKey.popColumn();
        return result;
    }

    private static class RangePartitionUniqueId implements UniqueId {
        private final long partitionId;

        public RangePartitionUniqueId(long partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public long getPartitionId() {
            return partitionId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("partitionId", partitionId)
                .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RangePartitionUniqueId that = (RangePartitionUniqueId) o;
            return partitionId == that.partitionId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(partitionId);
        }
    }
}
