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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Function dependence items.
 */
public class FuncDeps {
    class FuncDepsItem {
        final Set<Slot> determinants;
        final Set<Slot> dependencies;

        public FuncDepsItem(Set<Slot> determinants, Set<Slot> dependencies) {
            this.determinants = ImmutableSet.copyOf(determinants);
            this.dependencies = ImmutableSet.copyOf(dependencies);
        }

        @Override
        public String toString() {
            return String.format("%s -> %s", determinants, dependencies);
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof FuncDepsItem) {
                return other.hashCode() == this.hashCode();
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(determinants, dependencies);
        }
    }

    private final Set<FuncDepsItem> items;
    private final Map<Set<Slot>, Set<Set<Slot>>> edges;

    public FuncDeps() {
        items = new HashSet<>();
        edges = new HashMap<>();
    }

    public void addFuncItems(Set<Slot> determinants, Set<Slot> dependencies) {
        items.add(new FuncDepsItem(determinants, dependencies));
        edges.computeIfAbsent(determinants, k -> new HashSet<>());
        edges.get(determinants).add(dependencies);
    }

    public int size() {
        return items.size();
    }

    public boolean isEmpty() {
        return items.isEmpty();
    }

    private void dfs(Set<Slot> parent, Set<Set<Slot>> visited, Set<FuncDepsItem> circleItem) {
        visited.add(parent);
        if (!edges.containsKey(parent)) {
            return;
        }
        for (Set<Slot> child : edges.get(parent)) {
            if (visited.contains(child)) {
                circleItem.add(new FuncDepsItem(parent, child));
                continue;
            }
            dfs(child, visited, circleItem);
        }
    }

    // find item that not in a circle
    private Set<FuncDepsItem> findValidItems() {
        Set<FuncDepsItem> circleItem = new HashSet<>();
        Set<Set<Slot>> visited = new HashSet<>();
        for (Set<Slot> parent : edges.keySet()) {
            if (!visited.contains(parent)) {
                dfs(parent, visited, circleItem);
            }
        }
        return Sets.difference(items, circleItem);
    }

    /**
     * Reduces a given set of slot sets by eliminating dependencies using a breadth-first search (BFS) approach.
     * <p>
     * Let's assume we have the following sets of slots and functional dependencies:
     * Slots: {A, B, C}, {D, E}, {F}
     * Dependencies: {A} -> {B}, {D, E} -> {F}
     * The BFS reduction process would look like this:
     * 1. Initial set: [{A, B, C}, {D, E}, {F}]
     * 2. Apply {A} -> {B}:
     *    - New set: [{A, C}, {D, E}, {F}]
     * 3. Apply {D, E} -> {F}:
     *    - New set: [{A, C}, {D, E}]
     * 4. No more dependencies can be applied, output: [{A, C}, {D, E}]
     * </p>
     *
     * @param slots the initial set of slot sets to be reduced
     * @return the minimal set of slot sets after applying all possible reductions
     */
    public Set<Set<Slot>> eliminateDeps(Set<Set<Slot>> slots) {
        Set<Set<Slot>> minSlotSet = Sets.newHashSet(slots);
        Set<Set<Slot>> eliminatedSlots = new HashSet<>();
        Set<FuncDepsItem> validItems = findValidItems();
        for (FuncDepsItem funcDepsItem : validItems) {
            if (minSlotSet.contains(funcDepsItem.dependencies)
                    && minSlotSet.contains(funcDepsItem.determinants)) {
                eliminatedSlots.add(funcDepsItem.dependencies);
            }
        }
        minSlotSet.removeAll(eliminatedSlots);
        return minSlotSet;
    }

    public boolean isFuncDeps(Set<Slot> dominate, Set<Slot> dependency) {
        return items.contains(new FuncDepsItem(dominate, dependency));
    }

    @Override
    public String toString() {
        return items.toString();
    }
}
