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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionKeyDesc.PartitionKeyValueType;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.RangeUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * transfer and valid
 */
public class MTMVRelatedPartitionDescTransferGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult, List<Column> partitionColumns,
                      Map<List<String>, Set<String>> queryUsedPartitionMap) throws AnalysisException {
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res =
                mergeOverlappingListDescs(lastResult.getDescs());
        if (mvPartitionInfo.getPctInfos().size() > 1) {
            checkIntersect(res.keySet(), partitionColumns);
        }
        lastResult.setRes(res);
    }

    /**
     * One MV partition per set of keys that meet, whichever table wrote them down: a partition of a list
     * partitioned base table can hold several keys of the MV's partition column, so two partitions -- of one
     * table or of two -- can describe keys that meet, and an MV's own partitions cannot overlap. The one
     * partition that holds a group's keys names every table's partitions of them, which is what a refresh
     * reads and records for those keys (see {@link MTMVPartitionUtil#mergedListDescs}).
     *
     * <p>Merging across tables, not within each of them, is what keeps the MV buildable: two tables of a
     * multi-table MV have to come out with the same descs, or one table's merged desc repeats a key another
     * table's desc holds and `checkIntersect` (or the partition creation itself) rejects the MV.
     */
    private Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> mergeOverlappingListDescs(
            Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> descs) {
        Set<PartitionKeyDesc> allDescs = Sets.newHashSet();
        for (Map<PartitionKeyDesc, Set<String>> onePctDescs : descs.values()) {
            allDescs.addAll(onePctDescs.keySet());
        }
        Map<PartitionKeyDesc, PartitionKeyDesc> mergedOfDesc = MTMVPartitionUtil.mergedListDescs(allDescs);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res = Maps.newHashMap();
        for (Entry<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> entry : descs.entrySet()) {
            for (Entry<PartitionKeyDesc, Set<String>> onePctEntry : entry.getValue().entrySet()) {
                PartitionKeyDesc desc = mergedOfDesc.getOrDefault(onePctEntry.getKey(), onePctEntry.getKey());
                res.computeIfAbsent(desc, k -> new HashMap<>())
                        .merge(entry.getKey(), Sets.newHashSet(onePctEntry.getValue()), (left, right) -> {
                            left.addAll(right);
                            return left;
                        });
            }
        }
        return res;
    }

    public void checkIntersect(Set<PartitionKeyDesc> partitionKeyDescs, List<Column> partitionColumns)
            throws AnalysisException {
        if (CollectionUtils.isEmpty(partitionKeyDescs)) {
            return;
        }
        if (partitionKeyDescs.iterator().next().getPartitionType().equals(PartitionKeyValueType.IN)) {
            checkIntersectForList(partitionKeyDescs, partitionColumns);
        } else {
            checkIntersectForRange(partitionKeyDescs, partitionColumns);
        }
    }

    public void checkIntersectForList(Set<PartitionKeyDesc> partitionKeyDescs, List<Column> partitionColumns)
            throws AnalysisException {
        Set<PartitionValue> allPartitionValues = Sets.newHashSet();
        for (PartitionKeyDesc partitionKeyDesc : partitionKeyDescs) {
            if (!partitionKeyDesc.hasInValues()) {
                throw new AnalysisException("must have in values");
            }
            for (List<PartitionValue> values : partitionKeyDesc.getInValues()) {
                for (PartitionValue partitionValue : values) {
                    if (allPartitionValues.contains(partitionValue)) {
                        throw new AnalysisException("PartitionValue is repeat: " + partitionValue.getStringValue());
                    } else {
                        allPartitionValues.add(partitionValue);
                    }
                }
            }
        }
    }

    public void checkIntersectForRange(Set<PartitionKeyDesc> partitionKeyDescs, List<Column> partitionColumns)
            throws AnalysisException {
        List<Range<PartitionKey>> sortedRanges = Lists.newArrayListWithCapacity(partitionKeyDescs.size());
        for (PartitionKeyDesc partitionKeyDesc : partitionKeyDescs) {
            if (partitionKeyDesc.hasInValues()) {
                throw new AnalysisException("only support range partition");
            }
            if (partitionKeyDesc.getPartitionType() != PartitionKeyDesc.PartitionKeyValueType.FIXED) {
                throw new AnalysisException("only support fixed partition");
            }
            PartitionKey lowKey = PartitionKey.createPartitionKey(partitionKeyDesc.getLowerValues(), partitionColumns);
            PartitionKey upperKey = PartitionKey.createPartitionKey(partitionKeyDesc.getUpperValues(),
                    partitionColumns);
            if (lowKey.compareTo(upperKey) >= 0) {
                throw new AnalysisException("The lower values must smaller than upper values");
            }
            Range<PartitionKey> range = Range.closedOpen(lowKey, upperKey);
            sortedRanges.add(range);
        }

        sortedRanges.sort((r1, r2) -> {
            return r1.lowerEndpoint().compareTo(r2.lowerEndpoint());
        });
        if (sortedRanges.size() < 2) {
            return;
        }
        for (int i = 0; i < sortedRanges.size() - 1; i++) {
            Range<PartitionKey> current = sortedRanges.get(i);
            Range<PartitionKey> next = sortedRanges.get(i + 1);
            if (RangeUtils.checkIsTwoRangesIntersect(current, next)) {
                throw new AnalysisException("Range " + current + " is intersected with range: " + next);
            }
        }
    }
}
