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
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> merged =
                mergeOverlappingListDescs(lastResult.getDescs());
        // Now that the keys are grouped -- across the tables of the MV, and before any query filter -- the
        // partitions the query does not read are dropped per table, and an MV partition no table of which
        // feeds the query is dropped whole: what the query reads is what is left.
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res = Maps.newHashMap();
        for (Entry<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> entry : merged.entrySet()) {
            Map<MTMVRelatedTableIf, Set<String>> onePartition = Maps.newHashMap();
            for (Entry<MTMVRelatedTableIf, Set<String>> tableEntry : entry.getValue().entrySet()) {
                Set<String> names = Sets.newHashSet(tableEntry.getValue());
                Set<String> queryUsed = queryUsedPartitionMap.get(tableEntry.getKey().getFullQualifiers());
                // The query decides which components it reads -- one none of whose partitions it reads feeds
                // nothing to it, and the MV partition is not offered -- but a component it does read keeps
                // every partition that was recorded for it. Narrowing the names to the ones the query reads
                // would leave a descriptor the MV holds and a name set short of it, and the freshness check
                // compares those names to the recorded ones: a query reading part of a merged LIST component
                // would find the MV partition freshly refreshed and still be told the two are not
                // synchronized, so the MV would answer none of it.
                if (queryUsed != null && names.stream().noneMatch(queryUsed::contains)) {
                    continue;
                }
                onePartition.put(tableEntry.getKey(), names);
            }
            if (!onePartition.isEmpty()) {
                res.put(entry.getKey(), onePartition);
            }
        }
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
     * reads and records for those keys (see {@link MTMVPartitionUtil#groupedListDescs}).
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
        Map<PartitionKeyDesc, Set<PartitionKeyDesc>> groups = MTMVPartitionUtil.groupedListDescs(allDescs);
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res = Maps.newHashMap();
        for (Entry<PartitionKeyDesc, Set<PartitionKeyDesc>> group : groups.entrySet()) {
            Map<MTMVRelatedTableIf, Set<String>> namesByTable = Maps.newHashMap();
            for (Entry<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> entry : descs.entrySet()) {
                for (PartitionKeyDesc source : group.getValue()) {
                    Set<String> names = entry.getValue().get(source);
                    if (names == null) {
                        continue;
                    }
                    namesByTable.computeIfAbsent(entry.getKey(), k -> Sets.newHashSet()).addAll(names);
                }
            }
            if (!namesByTable.isEmpty()) {
                res.put(group.getKey(), namesByTable);
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
