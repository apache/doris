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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Represents a direct graph where each node corresponds to a set of slots in a table,
 * and each directed edge denotes a dependency where the child node depends on its parent node.
 * e.g.
 * if we have a = b, and we can get b -> a and b -> a. The DG is as follows:
 *      a → b
 *      ↑___↓
 */
public class FuncDepsDG {
    /**
     * Represents an item in the dependency graph.
     */
    static class DGItem {
        Set<Slot> slots;        // The set of slots that this node represents
        int index;              // The index of this node in the graph
        Set<Integer> parents;   // Indices of parent nodes in the graph
        Set<Integer> children;  // Indices of child nodes in the graph

        /**
         * Constructs a new DGItem with specified slots and index.
         */
        public DGItem(Set<Slot> slots, int index) {
            this.index = index;
            this.slots = ImmutableSet.copyOf(slots);
            this.parents = new HashSet<>();
            this.children = new HashSet<>();
        }

        /**
         * Copy constructor for DGItem.
         */
        public DGItem(DGItem dgItem) {
            this.index = dgItem.index;
            this.slots = ImmutableSet.copyOf(dgItem.slots);
            this.parents = new HashSet<>(dgItem.parents);
            this.children = new HashSet<>(dgItem.children);
        }

        public void replace(Map<Slot, Slot> replaceMap) {
            Set<Slot> newSlots = new HashSet<>();
            for (Slot slot : slots) {
                newSlots.add(replaceMap.getOrDefault(slot, slot));
            }
            this.slots = newSlots;
        }
    }

    private final Map<Set<Slot>, Integer> itemMap; // Maps sets of slots to their indices in the dgItems list
    private List<DGItem> dgItems;                  // List of all DGItems in the graph

    /**
     * Constructs a new FuncDepsDG from a map of slot sets to indices and a list of DGItems.
     */
    FuncDepsDG(Map<Set<Slot>, Integer> itemMap, List<DGItem> dgItems) {
        this.itemMap = ImmutableMap.copyOf(itemMap);
        this.dgItems = ImmutableList.copyOf(dgItems);
    }

    /**
     * Checks if the graph is empty.
     */
    public boolean isEmpty() {
        return dgItems.isEmpty();
    }

    /**
     * Finds all functional dependencies that are applicable to a given set of valid slots.
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

    /**
     * Helper method to recursively collect all child nodes of a given root node
     * that are valid according to the specified slots.
     */
    private void collectAllChildren(Set<Slot> validSlot, DGItem root,
            Set<DGItem> visited, Set<DGItem> children) {
        for (int childIdx : root.children) {
            DGItem child = dgItems.get(childIdx);
            if (!visited.contains(child)) {
                if (validSlot.containsAll(child.slots)) {
                    children.add(child);
                }
                visited.add(child);
                collectAllChildren(validSlot, child, visited, children);
            }
        }
    }

    /**
     * Builder class for FuncDepsDG.
     */
    static class Builder {
        private List<DGItem> dgItems;
        private Map<Set<Slot>, Integer> itemMap;

        public Builder() {
            dgItems = new ArrayList<>();
            itemMap = new HashMap<>();
        }

        public Builder(FuncDepsDG funcDepsDG) {
            this();
            for (DGItem dgItem : funcDepsDG.dgItems) {
                dgItems.add(new DGItem(dgItem));
            }
            this.itemMap = new HashMap<>(funcDepsDG.itemMap);
        }

        public FuncDepsDG build() {
            return new FuncDepsDG(ImmutableMap.copyOf(itemMap), ImmutableList.copyOf(dgItems));
        }

        public void removeNotContain(Set<Slot> validSlot) {
            FuncDeps funcDeps = findValidFuncDeps(validSlot);
            dgItems.clear();
            itemMap.clear();
            for (FuncDeps.FuncDepsItem item : funcDeps.getItems()) {
                this.addDeps(item.determinants, item.dependencies);
            }
        }

        /**
         * Finds all functional dependencies that are applicable to a given set of valid slots.
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

        /**
         * Helper method to recursively collect all child nodes of a given root node
         * that are valid according to the specified slots.
         */
        private void collectAllChildren(Set<Slot> validSlot, DGItem root,
                Set<DGItem> visited, Set<DGItem> children) {
            for (int childIdx : root.children) {
                DGItem child = dgItems.get(childIdx);
                if (!visited.contains(child)) {
                    if (validSlot.containsAll(child.slots)) {
                        children.add(child);
                    }
                    visited.add(child);
                    collectAllChildren(validSlot, child, visited, children);
                }
            }
        }

        public void addDeps(Set<Slot> dominant, Set<Slot> dependency) {
            DGItem dominateItem = getOrCreateNode(dominant);
            DGItem dependencyItem = getOrCreateNode(dependency);
            addEdge(dominateItem, dependencyItem);
        }

        public void addDeps(FuncDepsDG funcDepsDG) {
            for (DGItem dgItem : funcDepsDG.dgItems) {
                for (int childIdx : dgItem.children) {
                    addDeps(dgItem.slots, funcDepsDG.dgItems.get(childIdx).slots);
                }
            }
        }

        public void replace(Map<Slot, Slot> replaceSlotMap) {
            for (DGItem item : dgItems) {
                item.replace(replaceSlotMap);
            }
            Map<Set<Slot>, Integer> newItemMap = new HashMap<>();
            for (Entry<Set<Slot>, Integer> e : itemMap.entrySet()) {
                Set<Slot> key = new HashSet<>();
                for (Slot slot : e.getKey()) {
                    key.add(replaceSlotMap.getOrDefault(slot, slot));
                }
                newItemMap.put(key, e.getValue());
            }
            this.itemMap = newItemMap;
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
