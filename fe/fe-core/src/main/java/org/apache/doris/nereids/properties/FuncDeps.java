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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    FuncDeps() {
        items = new HashSet<>();
    }

    public void addFuncItems(Set<Slot> determinants, Set<Slot> dependencies) {
        items.add(new FuncDepsItem(determinants, dependencies));
    }

    public int size() {
        return items.size();
    }

    public boolean isEmpty() {
        return items.isEmpty();
    }

    /**
     * Eliminate all deps in slots
     */
    public Set<Slot> eliminateDeps(Set<Slot> slots) {
        Set<Slot> minSlotSet = slots;
        List<Set<Slot>> reduceSlotSets = new ArrayList<>();
        reduceSlotSets.add(slots);
        while (!reduceSlotSets.isEmpty()) {
            List<Set<Slot>> newReduceSlotSets = new ArrayList<>();
            for (Set<Slot> slotSet : reduceSlotSets) {
                for (FuncDepsItem funcDepsItem : items) {
                    if (slotSet.containsAll(funcDepsItem.dependencies)
                            && slotSet.containsAll(funcDepsItem.determinants)) {
                        Set<Slot> newSet = Sets.difference(slotSet, funcDepsItem.dependencies);
                        if (minSlotSet.size() > newSet.size()) {
                            minSlotSet = newSet;
                        }
                        newReduceSlotSets.add(newSet);
                    }
                }
            }
            reduceSlotSets = newReduceSlotSets;
        }
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
