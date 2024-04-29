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
import org.apache.doris.nereids.util.ImmutableEqualSet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Record functional dependencies, aka func deps, including
 * 1. unique slot: it means the column that has ndv = row count, can be null
 * 2. uniform slotL it means the column that has ndv = 1, can be null
 */
public class FunctionalDependencies {

    public static final FunctionalDependencies EMPTY_FUNC_DEPS
            = new FunctionalDependencies(new NestedSet().toImmutable(),
                    new NestedSet().toImmutable(), new ImmutableSet.Builder<FdItem>().build(),
                    ImmutableEqualSet.empty(), new FuncDepsDAG.Builder().build());
    private final NestedSet uniqueSet;
    private final NestedSet uniformSet;
    private final ImmutableSet<FdItem> fdItems;
    private final ImmutableEqualSet<Slot> equalSet;
    private final FuncDepsDAG fdDag;

    private FunctionalDependencies(NestedSet uniqueSet, NestedSet uniformSet, ImmutableSet<FdItem> fdItems,
            ImmutableEqualSet<Slot> equalSet, FuncDepsDAG fdDag) {
        this.uniqueSet = uniqueSet;
        this.uniformSet = uniformSet;
        this.fdItems = fdItems;
        this.equalSet = equalSet;
        this.fdDag = fdDag;
    }

    public boolean isEmpty() {
        return uniformSet.isEmpty() && uniqueSet.isEmpty() && equalSet.isEmpty() && fdDag.isEmpty();
    }

    public boolean isDependent(Set<Slot> dominate, Set<Slot> dependency) {
        return fdDag.findValidFuncDeps(Sets.union(dependency, dominate))
                .isFuncDeps(dominate, dependency);
    }

    public boolean isUnique(Slot slot) {
        return uniqueSet.contains(slot);
    }

    public boolean isUnique(Set<Slot> slotSet) {
        return !slotSet.isEmpty() && uniqueSet.containsAnySub(slotSet);
    }

    public boolean isUniform(Slot slot) {
        return uniformSet.contains(slot);
    }

    public boolean isUniform(Set<Slot> slotSet) {
        return !slotSet.isEmpty()
                && (uniformSet.contains(slotSet) || slotSet.stream().allMatch(uniformSet::contains));
    }

    public boolean isUniqueAndNotNull(Slot slot) {
        return !slot.nullable() && isUnique(slot);
    }

    public boolean isUniqueAndNotNull(Set<Slot> slotSet) {
        Set<Slot> notNullSlotSet = slotSet.stream()
                .filter(s -> !s.nullable())
                .collect(ImmutableSet.toImmutableSet());
        return isUnique(notNullSlotSet);
    }

    public boolean isUniformAndNotNull(Slot slot) {
        return !slot.nullable() && isUniform(slot);
    }

    public boolean isUniformAndNotNull(ImmutableSet<Slot> slotSet) {
        return slotSet.stream().noneMatch(Slot::nullable) && isUniform(slotSet);
    }

    public boolean isNullSafeEqual(Slot l, Slot r) {
        return equalSet.isEqual(l, r);
    }

    public FuncDeps getAllValidFuncDeps(Set<Slot> validSlots) {
        return fdDag.findValidFuncDeps(validSlots);
    }

    public boolean isEqualAndNotNotNull(Slot l, Slot r) {
        return equalSet.isEqual(l, r) && !l.nullable() && !r.nullable();
    }

    public List<Set<Slot>> calAllEqualSet() {
        return equalSet.calEqualSetList();
    }

    public ImmutableSet<FdItem> getFdItems() {
        return fdItems;
    }

    @Override
    public String toString() {
        return String.format("FuncDeps[uniform:%s, unique:%s, fdItems:%s, equalSet:%s, funcDeps: %s]",
                uniformSet, uniqueSet, fdItems, equalSet, fdDag);
    }

    /**
     * Builder of Func Deps
     */
    public static class Builder {
        private final NestedSet uniqueSet;
        private final NestedSet uniformSet;
        private ImmutableSet<FdItem> fdItems;
        private final ImmutableEqualSet.Builder<Slot> equalSetBuilder;
        private final FuncDepsDAG.Builder fdDagBuilder;

        public Builder() {
            uniqueSet = new NestedSet();
            uniformSet = new NestedSet();
            fdItems = new ImmutableSet.Builder<FdItem>().build();
            equalSetBuilder = new ImmutableEqualSet.Builder<>();
            fdDagBuilder = new FuncDepsDAG.Builder();
        }

        public Builder(FunctionalDependencies other) {
            this.uniformSet = new NestedSet(other.uniformSet);
            this.uniqueSet = new NestedSet(other.uniqueSet);
            this.fdItems = ImmutableSet.copyOf(other.fdItems);
            equalSetBuilder = new ImmutableEqualSet.Builder<>(other.equalSet);
            fdDagBuilder = new FuncDepsDAG.Builder(other.fdDag);
        }

        public void addUniformSlot(Slot slot) {
            uniformSet.add(slot);
        }

        public void addUniformSlot(FunctionalDependencies functionalDependencies) {
            uniformSet.add(functionalDependencies.uniformSet);
        }

        public void addUniformSlot(ImmutableSet<Slot> slotSet) {
            uniformSet.add(slotSet);
        }

        public void addUniqueSlot(Slot slot) {
            uniqueSet.add(slot);
        }

        public void addUniqueSlot(ImmutableSet<Slot> slotSet) {
            uniqueSet.add(slotSet);
        }

        public void addUniqueSlot(FunctionalDependencies functionalDependencies) {
            uniqueSet.add(functionalDependencies.uniqueSet);
        }

        public void addFdItems(ImmutableSet<FdItem> items) {
            fdItems = ImmutableSet.copyOf(items);
        }

        public void addFunctionalDependencies(FunctionalDependencies fd) {
            uniformSet.add(fd.uniformSet);
            uniqueSet.add(fd.uniqueSet);
            equalSetBuilder.addEqualSet(fd.equalSet);
            fdDagBuilder.addDeps(fd.fdDag);
        }

        public void addFuncDepsDAG(FunctionalDependencies fd) {
            fdDagBuilder.addDeps(fd.fdDag);
        }

        public void addDeps(Set<Slot> dominate, Set<Slot> dependency) {
            fdDagBuilder.addDeps(dominate, dependency);
        }

        /**
         * add equal set in func deps
         */
        public void addDepsByEqualSet(Set<Slot> equalSet) {
            if (equalSet.size() < 2) {
                return;
            }
            Iterator<Slot> iterator = equalSet.iterator();
            Set<Slot> first = ImmutableSet.of(iterator.next());

            while (iterator.hasNext()) {
                Set<Slot> slotSet = ImmutableSet.of(iterator.next());
                fdDagBuilder.addDeps(first, slotSet);
                fdDagBuilder.addDeps(slotSet, first);
            }
        }

        public List<Set<Slot>> calEqualSetList() {
            return equalSetBuilder.calEqualSetList();
        }

        /**
         * get all unique slots
         */
        public List<Set<Slot>> getAllUnique() {
            List<Set<Slot>> res = new ArrayList<>(uniqueSet.slotSets);
            for (Slot s : uniqueSet.slots) {
                res.add(ImmutableSet.of(s));
            }
            return res;
        }

        /**
         * get all uniform slots
         */
        public List<Set<Slot>> getAllUniform() {
            List<Set<Slot>> res = new ArrayList<>(uniformSet.slotSets);
            for (Slot s : uniformSet.slots) {
                res.add(ImmutableSet.of(s));
            }
            return res;
        }

        public void addEqualPair(Slot l, Slot r) {
            equalSetBuilder.addEqualPair(l, r);
        }

        public void addEqualSet(FunctionalDependencies functionalDependencies) {
            equalSetBuilder.addEqualSet(functionalDependencies.equalSet);
        }

        public FunctionalDependencies build() {
            return new FunctionalDependencies(uniqueSet.toImmutable(), uniformSet.toImmutable(),
                    ImmutableSet.copyOf(fdItems), equalSetBuilder.build(), fdDagBuilder.build());
        }

        public void pruneSlots(Set<Slot> outputSlots) {
            uniformSet.removeNotContain(outputSlots);
            uniqueSet.removeNotContain(outputSlots);
        }

        public void replace(Map<Slot, Slot> replaceMap) {
            uniformSet.replace(replaceMap);
            uniqueSet.replace(replaceMap);
            equalSetBuilder.replace(replaceMap);
        }
    }

    static class NestedSet {
        Set<Slot> slots;
        Set<ImmutableSet<Slot>> slotSets;

        NestedSet() {
            slots = new HashSet<>();
            slotSets = new HashSet<>();
        }

        NestedSet(NestedSet o) {
            this.slots = new HashSet<>(o.slots);
            this.slotSets = new HashSet<>(o.slotSets);
        }

        NestedSet(Set<Slot> slots, Set<ImmutableSet<Slot>> slotSets) {
            this.slots = slots;
            this.slotSets = slotSets;
        }

        public boolean contains(Slot slot) {
            return slots.contains(slot);
        }

        public boolean contains(Set<Slot> slotSet) {
            if (slotSet.size() == 1) {
                return slots.contains(slotSet.iterator().next());
            }
            return slotSets.contains(ImmutableSet.copyOf(slotSet));
        }

        public boolean containsAnySub(Set<Slot> slotSet) {
            return slotSet.stream().anyMatch(s -> slots.contains(s))
                    || slotSets.stream().anyMatch(slotSet::containsAll);
        }

        public void removeNotContain(Set<Slot> slotSet) {
            if (!slotSet.isEmpty()) {
                Set<Slot> newSlots = Sets.newLinkedHashSetWithExpectedSize(slots.size());
                for (Slot slot : slots) {
                    if (slotSet.contains(slot)) {
                        newSlots.add(slot);
                    }
                }
                this.slots = newSlots;

                Set<ImmutableSet<Slot>> newSlotSets = Sets.newLinkedHashSetWithExpectedSize(slots.size());
                for (ImmutableSet<Slot> set : slotSets) {
                    if (slotSet.containsAll(set)) {
                        newSlotSets.add(set);
                    }
                }
                this.slotSets = newSlotSets;
            }
        }

        public void add(Slot slot) {
            slots.add(slot);
        }

        public void add(ImmutableSet<Slot> slotSet) {
            if (slotSet.isEmpty()) {
                return;
            }
            if (slotSet.size() == 1) {
                slots.add(slotSet.iterator().next());
                return;
            }
            slotSets.add(slotSet);
        }

        public void add(NestedSet nestedSet) {
            slots.addAll(nestedSet.slots);
            slotSets.addAll(nestedSet.slotSets);
        }

        public boolean isEmpty() {
            return slots.isEmpty() && slotSets.isEmpty();
        }

        @Override
        public String toString() {
            return "{" + slots + slotSets + "}";
        }

        public void replace(Map<Slot, Slot> replaceMap) {
            slots = slots.stream()
                    .map(s -> replaceMap.getOrDefault(s, s))
                    .collect(Collectors.toSet());
            slotSets = slotSets.stream()
                    .map(set -> set.stream().map(s -> replaceMap.getOrDefault(s, s))
                            .collect(ImmutableSet.toImmutableSet()))
                    .collect(Collectors.toSet());
        }

        public NestedSet toImmutable() {
            return new NestedSet(ImmutableSet.copyOf(slots), ImmutableSet.copyOf(slotSets));
        }
    }
}
