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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ListPartitionPrunerV2
 * @since 1.0
 */
@SuppressWarnings("UnstableApiUsage")
public class ListPartitionPrunerV2 extends PartitionPrunerV2Base {
    private final Map<UniqueId, Range<PartitionKey>> uidToPartitionRange;

    public ListPartitionPrunerV2(Map<Long, PartitionItem> idToPartitionItem,
                                 List<Column> partitionColumns,
                                 Map<String, ColumnRange> columnNameToRange) {
        super(idToPartitionItem, partitionColumns, columnNameToRange);
        this.uidToPartitionRange = Maps.newHashMap();
        if (partitionColumns.size() > 1) {
            // `uidToPartitionRange` is only used for multiple columns partition.
            idToPartitionItem.forEach((id, item) -> {
                List<PartitionKey> keys = item.getItems();
                List<Range<PartitionKey>> ranges = keys.stream()
                    .map(key -> Range.closed(key, key))
                    .collect(Collectors.toList());
                for (int i = 0; i < ranges.size(); i++) {
                    uidToPartitionRange.put(new ListPartitionUniqueId(id, i), ranges.get(i));
                }
            });
        }
    }

    @Override
    RangeMap<ColumnBound, UniqueId> getCandidateRangeMap() {
        RangeMap<ColumnBound, UniqueId> candidate = TreeRangeMap.create();
        idToPartitionItem.forEach((id, item) -> {
            List<PartitionKey> keys = item.getItems();
            List<Range<PartitionKey>> ranges = keys.stream()
                .map(key -> Range.closed(key, key))
                .collect(Collectors.toList());
            for (int i = 0; i < ranges.size(); i++) {
                candidate.put(mapPartitionKeyRange(ranges.get(i), 0),
                    new ListPartitionUniqueId(id, i));
            }
        });
        return candidate;
    }

    /**
     * List partitions don't have null value.
     */
    @Override
    FinalFilters getFinalFilters(ColumnRange columnRange,
                                 Column column) throws AnalysisException {
        if (!columnRange.hasFilter()) {
            return FinalFilters.noFilters();
        }

        Optional<RangeSet<ColumnBound>> rangeSetOpt = columnRange.getRangeSet();
        if (columnRange.hasConjunctiveIsNull() || !rangeSetOpt.isPresent()) {
            return FinalFilters.constantFalseFilters();
        } else {
            RangeSet<ColumnBound> rangeSet = rangeSetOpt.get();
            if (rangeSet.isEmpty()) {
                return FinalFilters.constantFalseFilters();
            } else {
                return FinalFilters.create(rangeSet.asRanges());
            }
        }
    }

    @Override
    Collection<Long> pruneMultipleColumnPartition(
        Map<Column, FinalFilters> columnToFilters) throws AnalysisException {
        Map<Range<PartitionKey>, UniqueId> rangeToId = Maps.newHashMap();
        uidToPartitionRange.forEach((uid, range) -> rangeToId.put(range, uid));
        return doPruneMultiple(columnToFilters, rangeToId, 0);
    }

    private Collection<Long> doPruneMultiple(Map<Column, FinalFilters> columnToFilters,
                                             Map<Range<PartitionKey>, UniqueId> partitionRangeToUid,
                                             int columnIdx) {
        // No more partition column.
        if (columnIdx == partitionColumns.size()) {
            return partitionRangeToUid.values().stream()
                .map(UniqueId::getPartitionId)
                .collect(Collectors.toSet());
        }

        FinalFilters finalFilters = columnToFilters.get(partitionColumns.get(columnIdx));
        switch (finalFilters.type) {
            case CONSTANT_FALSE_FILTERS:
                return Collections.emptyList();
            case HAVE_FILTERS:
                // Grouping partition ranges by the range of column value indexed by `columnIdx`,
                // so that to compare with the filters.
                Map<Range<ColumnBound>, List<UniqueId>> grouped =
                    partitionRangeToUid
                        .entrySet()
                        .stream()
                        .collect(Collectors.groupingBy(entry -> mapPartitionKeyRange(entry.getKey(), columnIdx),
                            Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

                // Convert the grouped map to a RangeMap.
                TreeRangeMap<ColumnBound, List<UniqueId>> candidateRangeMap = TreeRangeMap.create();
                grouped.forEach(candidateRangeMap::put);

                return finalFilters.filters.stream()
                    .map(filter -> {
                        RangeMap<ColumnBound, List<UniqueId>> filtered =
                            candidateRangeMap.subRangeMap(filter);
                        // Find PartitionKey ranges according to filtered UniqueIds.
                        Map<Range<PartitionKey>, UniqueId> filteredPartitionRange =
                            filtered.asMapOfRanges().values()
                                .stream()
                                .flatMap(List::stream)
                                .collect(Collectors.toMap(
                                    uidToPartitionRange::get, Function.identity()));
                        return doPruneMultiple(columnToFilters, filteredPartitionRange,
                            columnIdx + 1);
                    })
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            case NO_FILTERS:
            default:
                return doPruneMultiple(columnToFilters, partitionRangeToUid, columnIdx + 1);
        }
    }

    private static class ListPartitionUniqueId implements UniqueId {
        private final long partitionId;
        private final int partitionKeyIndex;

        public ListPartitionUniqueId(long partitionId, int partitionKeyIndex) {
            this.partitionId = partitionId;
            this.partitionKeyIndex = partitionKeyIndex;
        }

        @Override
        public long getPartitionId() {
            return partitionId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("partitionId", partitionId)
                .add("partitionKeyIndex", partitionKeyIndex)
                .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ListPartitionUniqueId that = (ListPartitionUniqueId) o;
            return partitionId == that.partitionId && partitionKeyIndex == that.partitionKeyIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(partitionId, partitionKeyIndex);
        }
    }
}
