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
 * Function dependence items.
 */
public class FuncDepsDAG {
    static class DAGItem {
        Set<Slot> slots;
        int index;
        Set<Integer> parents;
        Set<Integer> children;

        public DAGItem(Set<Slot> slots, int index) {
            this.index = index;
            this.slots = ImmutableSet.copyOf(slots);
            this.parents = new HashSet<>();
            this.children = new HashSet<>();
        }

        public DAGItem(DAGItem dagItem) {
            this.index = dagItem.index;
            this.slots = ImmutableSet.copyOf(dagItem.slots);
            this.parents = Sets.newHashSet(parents);
            this.children = Sets.newHashSet(children);
        }
    }

    private final Map<Set<Slot>, Integer> itemMap;
    private List<DAGItem> dagItems;

    FuncDepsDAG(Map<Set<Slot>, Integer> itemMap, List<DAGItem> dagItems) {
        this.itemMap = ImmutableMap.copyOf(itemMap);
        this.dagItems = ImmutableList.copyOf(dagItems);
    }

    /**
     * find all func deps
     */
    public FuncDeps findValidFuncDeps(Set<Slot> validSlot) {
        FuncDeps res = new FuncDeps();
        for (Entry<Set<Slot>, Integer> entry : itemMap.entrySet()) {
            if (validSlot.containsAll(entry.getKey())) {
                Set<DAGItem> visited = new HashSet<>();
                Set<DAGItem> children = new HashSet<>();
                DAGItem dagItem = dagItems.get(entry.getValue());
                visited.add(dagItem);
                collectAllChildren(validSlot, dagItem, visited, children);
                for (DAGItem child : children) {
                    res.addFuncItems(dagItem.slots, child.slots);
                }
            }
        }
        return res;
    }

    private void collectAllChildren(Set<Slot> validSlot, DAGItem root,
            Set<DAGItem> visited, Set<DAGItem> children) {
        for (int childIdx : root.children) {
            DAGItem child = dagItems.get(childIdx);
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
        private List<DAGItem> dagItems;
        private Map<Set<Slot>, Integer> itemMap;

        public Builder() {
            dagItems = new ArrayList<>();
            itemMap = new HashMap<>();
        }

        public Builder(FuncDepsDAG funcDepsDAG) {
            itemMap = Maps.newHashMap(funcDepsDAG.itemMap);
            dagItems = new ArrayList<>();
            for (DAGItem dagItem : funcDepsDAG.dagItems) {
                dagItems.add(new DAGItem(dagItem));
            }
        }

        public FuncDepsDAG build() {
            return new FuncDepsDAG(ImmutableMap.copyOf(itemMap), ImmutableList.copyOf(dagItems));
        }

        public void addDeps(Set<Slot> dominant, Set<Slot> dependency) {
            DAGItem dominateItem = getOrCreateNode(dominant);
            DAGItem dependencyItem = getOrCreateNode(dependency);
            addEdge(dominateItem, dependencyItem);
        }

        public void addDeps(FuncDepsDAG funcDepsDAG) {
            for (DAGItem dagItem : funcDepsDAG.dagItems) {
                for (int i : dagItem.children) {
                    addDeps(dagItem.slots, funcDepsDAG.dagItems.get(i).slots);
                }
            }
        }

        private DAGItem getOrCreateNode(Set<Slot> slots) {
            if (!itemMap.containsKey(slots)) {
                itemMap.put(slots, dagItems.size());
                dagItems.add(new DAGItem(slots, dagItems.size()));
            }
            return dagItems.get(itemMap.get(slots));
        }

        private void addEdge(DAGItem from, DAGItem to) {
            if (!from.children.contains(to.index)) {
                from.children.add(to.index);
                to.parents.add(from.index);
            }
        }
    }
}
