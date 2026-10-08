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

import java.util.Collections;
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
        Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> descs =
                mergeOverlappingListDescs(lastResult.getDescs());
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res = Maps.newHashMap();
        for (Entry<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> entry : descs.entrySet()) {
            MTMVRelatedTableIf pctTable = entry.getKey();
            Map<PartitionKeyDesc, Set<String>> onePctDescs = entry.getValue();
            for (Entry<PartitionKeyDesc, Set<String>> onePctEntry : onePctDescs.entrySet()) {
                PartitionKeyDesc partitionKeyDesc = onePctEntry.getKey();
                Set<String> partitionNames = onePctEntry.getValue();
                Map<MTMVRelatedTableIf, Set<String>> partitionKeyDescMap = res.computeIfAbsent(partitionKeyDesc,
                        k -> new HashMap<>());
                partitionKeyDescMap.merge(pctTable, partitionNames, (left, right) -> {
                    left.addAll(right);
                    return left;
                });
            }
        }
        if (mvPartitionInfo.getPctInfos().size() > 1) {
            checkIntersect(res.keySet(), partitionColumns);
        }
        lastResult.setRes(res);
    }

    /**
     * One MV partition per set of keys, not one per way of writing a set down. A partition of a list
     * partitioned base table can hold several keys of the MV's partition column, so two of them can describe
     * keys that meet: an expired partition holding one key of a retained partition's key list, for instance.
     * An MV's own partitions cannot overlap, so descs whose keys meet are one partition whose keys are the
     * union of theirs. Without this an MV over such a table cannot be built at all -- its partition items
     * would repeat a key -- which is the shape a default-partition table is left unwindowed into, and the
     * one it is recorded with changes with it: the merged partition names every base partition of the keys
     * it covers, which is what a refresh reads for it.
     *
     * <p>Descs whose keys are disjoint, one desc per table, and every desc that is not a list of keys are
     * left exactly as they were, so an MV whose base partitions do not meet keeps its partitions and their
     * names.
     */
    private Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> mergeOverlappingListDescs(
            Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> descs) {
        Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> res = Maps.newHashMap();
        for (Entry<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> entry : descs.entrySet()) {
            res.put(entry.getKey(), mergeOverlappingListDescsOfOneTable(entry.getValue()));
        }
        return res;
    }

    private Map<PartitionKeyDesc, Set<String>> mergeOverlappingListDescsOfOneTable(
            Map<PartitionKeyDesc, Set<String>> descs) {
        Map<PartitionKeyDesc, Set<String>> res = Maps.newHashMap();
        List<Set<List<PartitionValue>>> mergedKeys = Lists.newArrayList();
        List<Set<String>> mergedNames = Lists.newArrayList();
        List<PartitionKeyDesc> mergedDescs = Lists.newArrayList();
        for (Entry<PartitionKeyDesc, Set<String>> entry : descs.entrySet()) {
            if (!entry.getKey().hasInValues()) {
                res.put(entry.getKey(), entry.getValue());
                continue;
            }
            Set<List<PartitionValue>> keys = Sets.newHashSet(entry.getKey().getInValues());
            Set<String> names = Sets.newHashSet(entry.getValue());
            // The desc this group came from, kept while it is the only one, since a desc that was not
            // merged is left as it is rather than written out again in another key order.
            PartitionKeyDesc mergedDesc = entry.getKey();
            for (int i = mergedKeys.size() - 1; i >= 0; i--) {
                if (Collections.disjoint(mergedKeys.get(i), keys)) {
                    continue;
                }
                keys.addAll(mergedKeys.get(i));
                names.addAll(mergedNames.get(i));
                mergedDesc = null;
                mergedKeys.remove(i);
                mergedNames.remove(i);
                mergedDescs.remove(i);
            }
            mergedKeys.add(keys);
            mergedNames.add(names);
            mergedDescs.add(mergedDesc);
        }
        for (int i = 0; i < mergedKeys.size(); i++) {
            PartitionKeyDesc desc = mergedDescs.get(i) == null
                    ? PartitionKeyDesc.createIn(Lists.newArrayList(mergedKeys.get(i))) : mergedDescs.get(i);
            res.put(desc, mergedNames.get(i));
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
