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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Comparator;
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
     * One MV partition per set of keys that meet, whichever table wrote them down.
     *
     * <p>A partition of a list partitioned base table can hold several keys of the MV's partition column, so
     * two partitions -- of one table or of two -- can describe keys that meet: an expired partition holding a
     * key a retained partition also holds, for instance. An MV's own partitions cannot overlap, so descs
     * whose keys meet are one partition whose keys are the union of theirs, and it names the partitions of
     * every table whose keys are in it, which is what a refresh reads and records for those keys.
     *
     * <p>Merging across tables, not within each of them, is what keeps the MV buildable: two tables of a
     * multi-table MV have to come out with the same descs, or one table's merged desc repeats a key another
     * table's desc holds and `checkIntersect` (or the partition creation itself) rejects the MV.
     *
     * <p>Descs whose keys are disjoint stay as they are, and so does a desc that is the only one in its
     * group, so an MV whose base partitions do not meet keeps its partitions and their names.
     */
    private Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> mergeOverlappingListDescs(
            Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> descs) {
        // A union-find over the keys: two descs whose keys meet end up in one group, transitively, and each
        // key is looked up once -- walking the groups per desc would be quadratic in the number of partitions.
        Map<List<PartitionValue>, List<PartitionValue>> groupOfKey = Maps.newHashMap();
        for (Map<PartitionKeyDesc, Set<String>> onePctDescs : descs.values()) {
            for (PartitionKeyDesc desc : onePctDescs.keySet()) {
                if (!desc.hasInValues()) {
                    continue;
                }
                List<PartitionValue> first = desc.getInValues().iterator().next();
                groupOfKey.putIfAbsent(first, first);
                for (List<PartitionValue> key : desc.getInValues()) {
                    groupOfKey.putIfAbsent(key, key);
                    union(groupOfKey, first, key);
                }
            }
        }
        Map<List<PartitionValue>, Set<List<PartitionValue>>> keysOfGroup = Maps.newHashMap();
        for (List<PartitionValue> key : groupOfKey.keySet()) {
            keysOfGroup.computeIfAbsent(find(groupOfKey, key), k -> Sets.newHashSet()).add(key);
        }
        Map<PartitionKeyDesc, List<PartitionValue>> groupOfDesc = Maps.newHashMap();
        Map<List<PartitionValue>, PartitionKeyDesc> onlyDescOfGroup = Maps.newHashMap();
        for (Map<PartitionKeyDesc, Set<String>> onePctDescs : descs.values()) {
            for (PartitionKeyDesc desc : onePctDescs.keySet()) {
                if (!desc.hasInValues()) {
                    continue;
                }
                List<PartitionValue> group = find(groupOfKey, desc.getInValues().iterator().next());
                groupOfDesc.put(desc, group);
                if (onlyDescOfGroup.put(group, desc) != null) {
                    // A second desc in this group: its keys are the group's from here on.
                    onlyDescOfGroup.put(group, null);
                }
            }
        }
        Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res = Maps.newHashMap();
        for (Entry<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> entry : descs.entrySet()) {
            for (Entry<PartitionKeyDesc, Set<String>> onePctEntry : entry.getValue().entrySet()) {
                PartitionKeyDesc desc = onePctEntry.getKey();
                if (desc.hasInValues()) {
                    List<PartitionValue> group = groupOfDesc.get(desc);
                    PartitionKeyDesc only = onlyDescOfGroup.get(group);
                    desc = only == null
                            ? PartitionKeyDesc.createIn(sortedKeys(keysOfGroup.get(group))) : only;
                }
                res.computeIfAbsent(desc, k -> new HashMap<>())
                        .merge(entry.getKey(), Sets.newHashSet(onePctEntry.getValue()), (left, right) -> {
                            left.addAll(right);
                            return left;
                        });
            }
        }
        return res;
    }

    private void union(Map<List<PartitionValue>, List<PartitionValue>> groupOfKey, List<PartitionValue> left,
            List<PartitionValue> right) {
        List<PartitionValue> leftGroup = find(groupOfKey, left);
        List<PartitionValue> rightGroup = find(groupOfKey, right);
        if (leftGroup != rightGroup) {
            groupOfKey.put(rightGroup, leftGroup);
        }
    }

    private List<PartitionValue> find(Map<List<PartitionValue>, List<PartitionValue>> groupOfKey,
            List<PartitionValue> key) {
        List<PartitionValue> group = Preconditions.checkNotNull(groupOfKey.get(key),
                "a key is registered before it is looked up: %s", key);
        while (group != groupOfKey.get(group)) {
            group = groupOfKey.get(group);
        }
        List<PartitionValue> root = group;
        // Path compression, so that the walk is not repeated for the rest of this group's keys.
        group = groupOfKey.get(key);
        while (group != root) {
            List<PartitionValue> next = groupOfKey.get(group);
            groupOfKey.put(group, root);
            group = next;
        }
        return root;
    }

    /** The group's keys, in the order the base partition values sort in, so a partition name is stable. */
    private List<List<PartitionValue>> sortedKeys(Set<List<PartitionValue>> keys) {
        List<List<PartitionValue>> res = Lists.newArrayList(keys);
        res.sort(Comparator.comparing(key -> key.get(0).getStringValue()));
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
