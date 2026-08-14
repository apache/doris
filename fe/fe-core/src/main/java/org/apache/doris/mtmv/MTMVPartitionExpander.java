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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Expands query-used partitions to MV partition granularity without running
 * partition expressions for every base table partition.
 */
public class MTMVPartitionExpander {

    public static Set<String> expandToMvPartitionGranularity(
            Set<String> queryUsedBaseTablePartitions,
            Map<String, PartitionItem> mvPartitionItems,
            MTMVRelatedTableIf relatedTable) throws AnalysisException {
        Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(relatedTable);
        if (relatedTable.getPartitionType(snapshot) != PartitionType.RANGE) {
            return queryUsedBaseTablePartitions;
        }

        NavigableMap<PartitionKey, Range<PartitionKey>> mvRanges = new TreeMap<>();
        for (PartitionItem item : mvPartitionItems.values()) {
            Range<PartitionKey> range = ((RangePartitionItem) item).getItems();
            mvRanges.put(range.lowerEndpoint(), range);
        }

        Map<String, PartitionItem> basePartitionItems = relatedTable.getAndCopyPartitionItems(snapshot);
        NavigableMap<PartitionKey, Range<PartitionKey>> relevantMvRanges = new TreeMap<>();
        for (String queriedBasePartition : queryUsedBaseTablePartitions) {
            PartitionItem baseItem = basePartitionItems.get(queriedBasePartition);
            if (baseItem == null) {
                continue;
            }
            Range<PartitionKey> baseRange = ((RangePartitionItem) baseItem).getItems();
            Range<PartitionKey> mvRange = findEnclosingRange(mvRanges, baseRange);
            if (mvRange != null) {
                relevantMvRanges.put(mvRange.lowerEndpoint(), mvRange);
            }
        }

        Set<String> expandedPartitions = Sets.newHashSet();
        for (Entry<String, PartitionItem> baseEntry : basePartitionItems.entrySet()) {
            Range<PartitionKey> baseRange = ((RangePartitionItem) baseEntry.getValue()).getItems();
            if (findEnclosingRange(relevantMvRanges, baseRange) != null) {
                expandedPartitions.add(baseEntry.getKey());
            }
        }
        return expandedPartitions;
    }

    private static Range<PartitionKey> findEnclosingRange(
            NavigableMap<PartitionKey, Range<PartitionKey>> ranges, Range<PartitionKey> baseRange) {
        Entry<PartitionKey, Range<PartitionKey>> candidate = ranges.floorEntry(baseRange.lowerEndpoint());
        return candidate != null && candidate.getValue().encloses(baseRange) ? candidate.getValue() : null;
    }

    private MTMVPartitionExpander() {
    }
}
