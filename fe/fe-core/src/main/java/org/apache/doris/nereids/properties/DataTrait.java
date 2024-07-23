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
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
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
 * Records data trait, which include:
 * 1. Unique Slot:  Represents a column where the number of distinct values (ndv)
 *                  equals the row count. This column may include null values.
 * 2. Uniform Slot: Represents a column where the number of distinct values (ndv) is 1,
 *                  indicating all values are the same. This column may include null values.
 * 3. Equal Set:    Denotes a group of slots that have equal values.
 * 4. fdDg:         Stands for functional dependencies, expressed as 'a -> b',
 *                   which signifies that if values in column 'a' are identical,
 *                   then values in column 'b' must also be identical.
 */
public class DataTrait {

    public static final DataTrait EMPTY_TRAIT
            = new DataTrait(new NestedSet().toImmutable(),
                    new NestedSet().toImmutable(), new ImmutableSet.Builder<FdItem>().build(),
                    ImmutableEqualSet.empty(), new FuncDepsDG.Builder().build());
    private final NestedSet uniqueSet;
    private final NestedSet uniformSet;
    private final ImmutableSet<FdItem> fdItems;
    private final ImmutableEqualSet<Slot> equalSet;
    private final FuncDepsDG fdDg;

    private DataTrait(NestedSet uniqueSet, NestedSet uniformSet, ImmutableSet<FdItem> fdItems,
            ImmutableEqualSet<Slot> equalSet, FuncDepsDG fdDg) {
        this.uniqueSet = uniqueSet;
        this.uniformSet = uniformSet;
        this.fdItems = fdItems;
        this.equalSet = equalSet;
        this.fdDg = fdDg;
    }

    public boolean isEmpty() {
        return uniformSet.isEmpty() && uniqueSet.isEmpty() && equalSet.isEmpty() && fdDg.isEmpty();
    }

    public boolean isDependent(Set<Slot> dominate, Set<Slot> dependency) {
        return fdDg.findValidFuncDeps(Sets.union(dependency, dominate))
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
                && uniformSet.slots.containsAll(slotSet);
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
        return fdDg.findValidFuncDeps(validSlots);
    }

    public boolean isEqualAndNotNotNull(Slot l, Slot r) {
        return equalSet.isEqual(l, r) && !l.nullable() && !r.nullable();
    }

    public List<Set<Slot>> calAllEqualSet() {
        return equalSet.calEqualSetList();
    }

    public Set<Slot> calEqualSet(Slot s) {
        return equalSet.calEqualSet(s);
    }

    public ImmutableSet<FdItem> getFdItems() {
        return fdItems;
    }

    @Override
    public String toString() {
        return String.format("FuncDeps[uniform:%s, unique:%s, fdItems:%s, equalSet:%s, funcDeps: %s]",
                uniformSet, uniqueSet, fdItems, equalSet, fdDg);
    }

    /**
     * Builder of trait
     */
    public static class Builder {
        private final NestedSet uniqueSet;
        private final NestedSet uniformSet;
        private ImmutableSet<FdItem> fdItems;
        private final ImmutableEqualSet.Builder<Slot> equalSetBuilder;
        private final FuncDepsDG.Builder fdDgBuilder;

        public Builder() {
            uniqueSet = new NestedSet();
            uniformSet = new NestedSet();
            fdItems = new ImmutableSet.Builder<FdItem>().build();
            equalSetBuilder = new ImmutableEqualSet.Builder<>();
            fdDgBuilder = new FuncDepsDG.Builder();
        }

        public Builder(DataTrait other) {
            this.uniformSet = new NestedSet(other.uniformSet);
            this.uniqueSet = new NestedSet(other.uniqueSet);
            this.fdItems = ImmutableSet.copyOf(other.fdItems);
            equalSetBuilder = new ImmutableEqualSet.Builder<>(other.equalSet);
            fdDgBuilder = new FuncDepsDG.Builder(other.fdDg);
        }

        public void addUniformSlot(Slot slot) {
            uniformSet.add(slot);
        }

        public void addUniformSlot(DataTrait dataTrait) {
            uniformSet.add(dataTrait.uniformSet);
        }

        public void addUniqueSlot(Slot slot) {
            uniqueSet.add(slot);
        }

        public void addUniqueSlot(ImmutableSet<Slot> slotSet) {
            uniqueSet.add(slotSet);
        }

        public void addUniqueSlot(DataTrait dataTrait) {
            uniqueSet.add(dataTrait.uniqueSet);
        }

        public void addFdItems(ImmutableSet<FdItem> items) {
            fdItems = ImmutableSet.copyOf(items);
        }

        public void addDataTrait(DataTrait fd) {
            uniformSet.add(fd.uniformSet);
            uniqueSet.add(fd.uniqueSet);
            equalSetBuilder.addEqualSet(fd.equalSet);
            fdDgBuilder.addDeps(fd.fdDg);
        }

        public void addFuncDepsDG(DataTrait fd) {
            fdDgBuilder.addDeps(fd.fdDg);
        }

        public void addDeps(Set<Slot> dominate, Set<Slot> dependency) {
            if (dominate.containsAll(dependency)) {
                return;
            }
            fdDgBuilder.addDeps(dominate, dependency);
        }

        /**
         * add equal set in func deps.
         * For equal Set {a1, a2, a3}, we will add (a1, a2) (a1, a3)
         */
        public void addDepsByEqualSet(Set<Slot> equalSet) {
            if (equalSet.size() < 2) {
                return;
            }
            Iterator<Slot> iterator = equalSet.iterator();
            Set<Slot> first = ImmutableSet.of(iterator.next());

            while (iterator.hasNext()) {
                Set<Slot> slotSet = ImmutableSet.of(iterator.next());
                fdDgBuilder.addDeps(first, slotSet);
                fdDgBuilder.addDeps(slotSet, first);
            }
        }

        /**
         * Extends a unique slot using an equivalence set.
         * Within slots, if any slot in the equivalence set is unique,
         * then all slots in the set are considered unique.
         * For slotSets, if there is an intersection with the equivalence set,
         * the slotSet can be substituted with the equivalence set.
         * Example:
         *          Given an equivalence set {a1, a2, a3} and a uniqueSet {a1, b1, c1},
         *          the sets {a2, b1, c1} and {a3, b1, c1} are also treated as unique.
         */
        public void addUniqueByEqualSet(Set<Slot> equalSet) {
            if (uniqueSet.isIntersect(equalSet, uniqueSet.slots)) {
                uniqueSet.slots.addAll(equalSet);
                return;
            }
            for (Set<Slot> slotSet : uniqueSet.slotSets) {
                Set<Slot> intersection = Sets.intersection(equalSet, uniqueSet.slots);
                if (intersection.size() > 2) {
                    uniqueSet.slotSets.remove(slotSet);
                    slotSet.removeAll(intersection);
                    for (Slot slot : equalSet) {
                        ImmutableSet<Slot> uniqueSlotSet = ImmutableSet.<Slot>builder()
                                .addAll(slotSet)
                                .add(slot)
                                .build();
                        uniqueSet.add(uniqueSlotSet);
                    }
                }
            }
        }

        /**
         * Extend uniform slot by an equivalence set.
         * if there is a uniform slot in the equivalence set, then all slots of an equivalence set are uniform
         */
        public void addUniformByEqualSet(Set<Slot> equalSet) {
            if (uniformSet.isIntersect(uniformSet.slots, equalSet)) {
                uniformSet.slots.addAll(equalSet);
            }
        }

        public List<Set<Slot>> calEqualSetList() {
            return equalSetBuilder.calEqualSetList();
        }

        /**
         * get all unique slots
         */
        public List<Set<Slot>> getAllUniqueAndNotNull() {
            List<Set<Slot>> res = new ArrayList<>();
            for (Slot slot : uniqueSet.slots) {
                if (!slot.nullable()) {
                    res.add(ImmutableSet.of(slot));
                }
            }
            for (Set<Slot> slotSet : uniqueSet.slotSets) {
                if (slotSet.stream().noneMatch(ExpressionTrait::nullable)) {
                    res.add(slotSet);
                }
            }
            return res;
        }

        /**
         * get all uniform slots
         */
        public List<Set<Slot>> getAllUniformAndNotNull() {
            List<Set<Slot>> res = new ArrayList<>();
            for (Slot s : uniformSet.slots) {
                if (!s.nullable()) {
                    res.add(ImmutableSet.of(s));
                }
            }
            return res;
        }

        public void addEqualPair(Slot l, Slot r) {
            equalSetBuilder.addEqualPair(l, r);
        }

        public void addEqualSet(DataTrait dataTrait) {
            equalSetBuilder.addEqualSet(dataTrait.equalSet);
        }

        public DataTrait build() {
            return new DataTrait(uniqueSet.toImmutable(), uniformSet.toImmutable(),
                    ImmutableSet.copyOf(fdItems), equalSetBuilder.build(), fdDgBuilder.build());
        }

        public void pruneSlots(Set<Slot> outputSlots) {
            uniformSet.removeNotContain(outputSlots);
            uniqueSet.removeNotContain(outputSlots);
            equalSetBuilder.removeNotContain(outputSlots);
            fdDgBuilder.removeNotContain(outputSlots);
        }

        public void replaceUniformBy(Map<Slot, Slot> replaceMap) {
            uniformSet.replace(replaceMap);
        }

        public void replaceUniqueBy(Map<Slot, Slot> replaceMap) {
            uniqueSet.replace(replaceMap);
        }

        public void replaceEqualSetBy(Map<Slot, Slot> replaceMap) {
            equalSetBuilder.replace(replaceMap);
        }

        public void replaceFuncDepsBy(Map<Slot, Slot> replaceMap) {
            fdDgBuilder.replace(replaceMap);
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

        public boolean isIntersect(Set<Slot> set1, Set<Slot> set2) {
            if (set1.size() > set2.size()) {
                Set<Slot> temp = set1;
                set1 = set2;
                set2 = temp;
            }
            for (Slot slot : set1) {
                if (set2.contains(slot)) {
                    return true;
                }
            }
            return false;
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
