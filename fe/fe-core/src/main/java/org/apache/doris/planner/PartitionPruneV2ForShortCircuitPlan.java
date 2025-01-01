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
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class PartitionPruneV2ForShortCircuitPlan extends PartitionPrunerV2Base {
    private static final Logger LOG = LogManager.getLogger(PartitionPruneV2ForShortCircuitPlan.class);
    // map to record literal range to find specific partition
    private RangeMap<ColumnBound, List<Long>> partitionRangeMap = TreeRangeMap.create();
    // last timestamp partitionRangeMapByLiteral updated
    private long lastPartitionRangeMapUpdateTimestampMs = 0;

    PartitionPruneV2ForShortCircuitPlan() {
        super();
    }

    public static <C extends Comparable<C>, V> Set<V>
            getOverlappingRangeValues(RangeMap<C, List<V>> partRangeMap, Set<Range<C>> ranges) {
        Set<V> partitionIds = Sets.newHashSet();
        for (Range<C> range : ranges) {
            Map<Range<C>, List<V>> overlappingRanges = partRangeMap.subRangeMap(range).asMapOfRanges();
            for (Map.Entry<Range<C>, List<V>> entry : overlappingRanges.entrySet()) {
                partitionIds.addAll(entry.getValue());
            }
        }
        return partitionIds;
    }

    public RangeMap<ColumnBound, List<Long>> getPartitionColValue2PartitionID() {
        return partitionRangeMap;
    }

    public boolean update(Map<Long, PartitionItem> keyItemMap) {
        // interval to update partitionRangeMapByLiteral
        long partitionRangeMapUpdateIntervalS = 10;
        if (System.currentTimeMillis() - lastPartitionRangeMapUpdateTimestampMs
                    > partitionRangeMapUpdateIntervalS * 1000) {
            partitionRangeMap = TreeRangeMap.create();
            // recalculate map
            for (Entry<Long, PartitionItem> entry : keyItemMap.entrySet()) {
                Range<PartitionKey> range = entry.getValue().getItems();
                LiteralExpr partitionLowerBound = (LiteralExpr) range.lowerEndpoint().getKeys().get(0);
                LiteralExpr partitionUpperBound = (LiteralExpr) range.upperEndpoint().getKeys().get(0);
                Range<ColumnBound> partitionRange =
                        Range.closedOpen(ColumnBound.of(partitionLowerBound), ColumnBound.of(partitionUpperBound));
                partitionRangeMap.put(partitionRange, Lists.newArrayList(entry.getKey()));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("update partitionRangeMapByLiteral");
            }
            this.lastPartitionRangeMapUpdateTimestampMs = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public Collection<Long> prune(Set<Range<ColumnBound>> partitionColumnRange,
                                  String partitionColName,
                                  Map<String, RangeMap<ColumnBound, List<Long>>> partitionCol2PartitionID) {
        Set<Long> overlappingRangeValues = getOverlappingRangeValues(partitionRangeMap, partitionColumnRange);
        partitionCol2PartitionID.putIfAbsent(
                        partitionColName, partitionRangeMap);
        return overlappingRangeValues;
    }

    @Override
    public Collection<Long> prune() throws AnalysisException {
        throw new AnalysisException("Not implemented");
    }

    @Override
    void genSingleColumnRangeMap() {
    }

    @Override
    FinalFilters getFinalFilters(ColumnRange columnRange,
            Column column) throws AnalysisException {
        throw new AnalysisException("Not implemented");
    }

    @Override
    Collection<Long> pruneMultipleColumnPartition(Map<Column, FinalFilters> columnToFilters) throws AnalysisException {
        throw new AnalysisException("Not implemented");
    }
}
