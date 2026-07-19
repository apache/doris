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

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * Utility to expand query-used partition filters to MV partition granularity
 * using Range.encloses(), avoiding expensive dateTrunc / strToDate / dateIncrement
 * per-partition operations in the rollup pipeline.
 * Separated from MTMV to keep a lightweight dependency tree for testability —
 * loading this class does not trigger MTMV → OlapTable → CloudReplica class loading.
 */
public class MTMVPartitionExpander {

    /**
     * Expand queryUsedPartitions to MV partition granularity for RANGE base tables.
     * For example, if MV is monthly partitioned (date_trunc(month)) and base table is daily:
     * - Query uses p_20250115 (Jan 15)
     * - Find MV partition p_202501 that encloses [20250115, 20250116)
     * - Expand to ALL daily partitions within p_202501's range [20250101, 20250201)
     * - Result: {p_20250101, p_20250102, ..., p_20250131}
     */
    public static Map<List<String>, Set<String>> expandToMvPartitionGranularity(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap,
            Map<String, PartitionItem> mvPartitionItems,
            Set<MTMVRelatedTableIf> pctTables) throws AnalysisException {
        List<Range<PartitionKey>> mvRanges = new ArrayList<>(mvPartitionItems.size());
        for (PartitionItem item : mvPartitionItems.values()) {
            mvRanges.add(((RangePartitionItem) item).getItems());
        }

        Map<List<String>, Set<String>> expanded = Maps.newHashMap();
        for (MTMVRelatedTableIf pctTable : pctTables) {
            List<String> qualifiers = pctTable.getFullQualifiers();
            Set<String> queryUsedPartitions = queryUsedBaseTablePartitionMap.get(qualifiers);
            if (queryUsedPartitions == null) {
                continue;
            }

            Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(pctTable);
            if (pctTable.getPartitionType(snapshot) != PartitionType.RANGE) {
                expanded.put(qualifiers, queryUsedPartitions);
                continue;
            }

            Map<String, PartitionItem> basePartitionItems = pctTable.getAndCopyPartitionItems(snapshot);

            List<Range<PartitionKey>> relevantMvRanges = new ArrayList<>();
            for (String queriedBasePartition : queryUsedPartitions) {
                PartitionItem baseItem = basePartitionItems.get(queriedBasePartition);
                if (baseItem == null) {
                    continue;
                }
                Range<PartitionKey> baseRange = ((RangePartitionItem) baseItem).getItems();
                for (Range<PartitionKey> mvRange : mvRanges) {
                    if (mvRange.encloses(baseRange)) {
                        if (!relevantMvRanges.contains(mvRange)) {
                            relevantMvRanges.add(mvRange);
                        }
                        break;
                    }
                }
            }

            if (relevantMvRanges.isEmpty()) {
                expanded.put(qualifiers, Sets.newHashSet());
                continue;
            }

            Set<String> expandedPartitions = Sets.newHashSet();
            for (Entry<String, PartitionItem> baseEntry : basePartitionItems.entrySet()) {
                Range<PartitionKey> baseRange = ((RangePartitionItem) baseEntry.getValue()).getItems();
                for (Range<PartitionKey> mvRange : relevantMvRanges) {
                    if (mvRange.encloses(baseRange)) {
                        expandedPartitions.add(baseEntry.getKey());
                        break;
                    }
                }
            }

            expanded.put(qualifiers, expandedPartitions);
        }

        return expanded;
    }

    private MTMVPartitionExpander() {
    }
}
