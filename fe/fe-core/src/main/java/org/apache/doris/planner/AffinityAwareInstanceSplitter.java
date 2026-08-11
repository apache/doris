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

import org.apache.doris.common.util.ListUtil;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Splits work into instances without separating items that share an affinity key. */
public final class AffinityAwareInstanceSplitter {
    private AffinityAwareInstanceSplitter() {
    }

    /**
     * Group keyed items atomically, then greedily place the heaviest groups on the lightest instances.
     * Unkeyed items remain independently movable. If no item has an affinity key, preserve round-robin behavior.
     */
    public static <T, K> List<List<Integer>> split(
            List<T> items, int expectedInstanceNum, Function<T, Optional<K>> affinityKey,
            ToLongFunction<T> weight) {
        Preconditions.checkArgument(expectedInstanceNum > 0, "expectedInstanceNum must be positive");
        Preconditions.checkArgument(!items.isEmpty(), "items must not be empty");

        List<Optional<K>> affinityKeys = items.stream().map(affinityKey).collect(Collectors.toList());
        if (affinityKeys.stream().noneMatch(Optional::isPresent)) {
            List<Integer> indexes = IntStream.range(0, items.size()).boxed().collect(Collectors.toList());
            return ListUtil.splitBySize(indexes, expectedInstanceNum);
        }

        List<AffinityGroup> groups = new ArrayList<>();
        Map<K, AffinityGroup> affinityGroups = new LinkedHashMap<>();
        for (int index = 0; index < items.size(); index++) {
            T item = items.get(index);
            Optional<K> key = affinityKeys.get(index);
            AffinityGroup group;
            if (key.isPresent()) {
                group = affinityGroups.get(key.get());
                if (group == null) {
                    group = new AffinityGroup(groups.size());
                    affinityGroups.put(key.get(), group);
                    groups.add(group);
                }
            } else {
                group = new AffinityGroup(groups.size());
                groups.add(group);
            }
            group.add(index, weight.applyAsLong(item));
        }

        groups.sort(Comparator.comparingLong(AffinityGroup::getWeight).reversed()
                .thenComparingInt(AffinityGroup::getOrdinal));
        int instanceNum = Math.min(expectedInstanceNum, groups.size());
        List<List<Integer>> instanceToItemIndexes = new ArrayList<>(instanceNum);
        PriorityQueue<InstanceLoad> instanceLoads = new PriorityQueue<>(Comparator
                .comparingLong(InstanceLoad::getWeight)
                .thenComparingInt(InstanceLoad::getGroupCount)
                .thenComparingInt(InstanceLoad::getOrdinal));
        for (int i = 0; i < instanceNum; i++) {
            instanceToItemIndexes.add(new ArrayList<>());
            instanceLoads.add(new InstanceLoad(i));
        }
        for (AffinityGroup group : groups) {
            InstanceLoad lightestInstance = instanceLoads.remove();
            instanceToItemIndexes.get(lightestInstance.ordinal).addAll(group.itemIndexes);
            lightestInstance.add(group.weight);
            instanceLoads.add(lightestInstance);
        }
        instanceToItemIndexes.forEach(indexes -> indexes.sort(Integer::compareTo));
        return instanceToItemIndexes;
    }

    private static class AffinityGroup {
        private final int ordinal;
        private final List<Integer> itemIndexes = new ArrayList<>();
        private long weight;

        AffinityGroup(int ordinal) {
            this.ordinal = ordinal;
        }

        void add(int itemIndex, long itemWeight) {
            Preconditions.checkArgument(itemWeight >= 0, "itemWeight must not be negative");
            itemIndexes.add(itemIndex);
            weight = Math.addExact(weight, itemWeight);
        }

        long getWeight() {
            return weight;
        }

        int getOrdinal() {
            return ordinal;
        }
    }

    private static class InstanceLoad {
        private final int ordinal;
        private long weight;
        private int groupCount;

        InstanceLoad(int ordinal) {
            this.ordinal = ordinal;
        }

        void add(long groupWeight) {
            weight = Math.addExact(weight, groupWeight);
            groupCount++;
        }

        long getWeight() {
            return weight;
        }

        int getGroupCount() {
            return groupCount;
        }

        int getOrdinal() {
            return ordinal;
        }
    }
}
