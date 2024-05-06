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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Function dependence items Direct Graph
 */
public class FuncDepsDG {
    static class DGItem {
        Set<Slot> slots;
        int index;
        Set<Integer> parents;
        Set<Integer> children;

        public DGItem(Set<Slot> slots, int index) {
            this.index = index;
            this.slots = ImmutableSet.copyOf(slots);
            this.parents = new HashSet<>();
            this.children = new HashSet<>();
        }

        public DGItem(DGItem dgItem) {
            this.index = dgItem.index;
            this.slots = ImmutableSet.copyOf(dgItem.slots);
            this.parents = Sets.newHashSet(parents);
            this.children = Sets.newHashSet(children);
        }
    }

    private final Map<Set<Slot>, Integer> itemMap;
    private List<DGItem> dgItems;

    FuncDepsDG(Map<Set<Slot>, Integer> itemMap, List<DGItem> dgItems) {
        this.itemMap = ImmutableMap.copyOf(itemMap);
        this.dgItems = ImmutableList.copyOf(dgItems);
    }

    public boolean isEmpty() {
        return dgItems.isEmpty();
    }

    /**
     * find all func deps
     */
    public FuncDeps findValidFuncDeps(Set<Slot> validSlot) {
        FuncDeps res = new FuncDeps();
        for (Entry<Set<Slot>, Integer> entry : itemMap.entrySet()) {
            if (validSlot.containsAll(entry.getKey())) {
                Set<DGItem> visited = new HashSet<>();
                Set<DGItem> children = new HashSet<>();
                DGItem dgItem = dgItems.get(entry.getValue());
                visited.add(dgItem);
                collectAllChildren(validSlot, dgItem, visited, children);
                for (DGItem child : children) {
                    res.addFuncItems(dgItem.slots, child.slots);
                }
            }
        }
        return res;
    }

    private void collectAllChildren(Set<Slot> validSlot, DGItem root,
            Set<DGItem> visited, Set<DGItem> children) {
        for (int childIdx : root.children) {
            DGItem child = dgItems.get(childIdx);
            if (visited.contains(child)) {
                continue;
            }
            if (validSlot.containsAll(child.slots)) {
                children.add(child);
            }
            visited.add(child);
            collectAllChildren(validSlot, child, visited, children);
        }
    }

    static class Builder {
        private List<DGItem> dgItems;
        private Map<Set<Slot>, Integer> itemMap;

        public Builder() {
            dgItems = new ArrayList<>();
            itemMap = new HashMap<>();
        }

        public Builder(FuncDepsDG funcDepsDG) {
            itemMap = Maps.newHashMap(funcDepsDG.itemMap);
            dgItems = new ArrayList<>();
            for (DGItem dgItem : funcDepsDG.dgItems) {
                dgItems.add(new DGItem(dgItem));
            }
        }

        public FuncDepsDG build() {
            return new FuncDepsDG(ImmutableMap.copyOf(itemMap), ImmutableList.copyOf(dgItems));
        }

        public void addDeps(Set<Slot> dominant, Set<Slot> dependency) {
            DGItem dominateItem = getOrCreateNode(dominant);
            DGItem dependencyItem = getOrCreateNode(dependency);
            addEdge(dominateItem, dependencyItem);
        }

        public void addDeps(FuncDepsDG funcDepsDG) {
            for (DGItem dgItem : funcDepsDG.dgItems) {
                for (int i : dgItem.children) {
                    addDeps(dgItem.slots, funcDepsDG.dgItems.get(i).slots);
                }
            }
        }

        private DGItem getOrCreateNode(Set<Slot> slots) {
            if (!itemMap.containsKey(slots)) {
                itemMap.put(slots, dgItems.size());
                dgItems.add(new DGItem(slots, dgItems.size()));
            }
            return dgItems.get(itemMap.get(slots));
        }

        private void addEdge(DGItem from, DGItem to) {
            if (!from.children.contains(to.index)) {
                from.children.add(to.index);
                to.parents.add(from.index);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (DGItem item : dgItems) {
            for (int childIdx : item.children) {
                sb.append(item.slots);
                sb.append(" -> ");
                sb.append(dgItems.get(childIdx).slots);
                sb.append(", ");
            }
        }
        sb.append("} ");
        return sb.toString();
    }
}
