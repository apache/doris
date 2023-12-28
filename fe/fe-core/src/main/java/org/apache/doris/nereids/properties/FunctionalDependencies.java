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

import java.util.HashSet;
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
            = new FunctionalDependencies(new NestedSet().toImmutable(), new NestedSet().toImmutable());
    private final NestedSet uniqueSet;
    private final NestedSet uniformSet;

    private FunctionalDependencies(NestedSet uniqueSet, NestedSet uniformSet) {
        this.uniqueSet = uniqueSet;
        this.uniformSet = uniformSet;
    }

    public boolean isEmpty() {
        return uniformSet.isEmpty() && uniqueSet.isEmpty();
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

    public boolean isUniform(ImmutableSet<Slot> slotSet) {
        return !slotSet.isEmpty()
                && (uniformSet.contains(slotSet) || slotSet.stream().allMatch(uniformSet::contains));
    }

    public boolean isUniqueAndNotNull(Slot slot) {
        return !slot.nullable() && isUnique(slot);
    }

    public boolean isUniqueAndNotNull(Set<Slot> slotSet) {
        return slotSet.stream().noneMatch(Slot::nullable) && isUnique(slotSet);
    }

    public boolean isUniformAndNotNull(Slot slot) {
        return !slot.nullable() && isUniform(slot);
    }

    public boolean isUniformAndNotNull(ImmutableSet<Slot> slotSet) {
        return slotSet.stream().noneMatch(Slot::nullable) && isUniform(slotSet);
    }

    @Override
    public String toString() {
        return String.format("FuncDeps[uniform:%s,  unique:%s]", uniformSet, uniqueSet);
    }

    /**
     * Builder of Func Deps
     */
    public static class Builder {
        private final NestedSet uniqueSet;
        private final NestedSet uniformSet;

        public Builder() {
            uniqueSet = new NestedSet();
            uniformSet = new NestedSet();
        }

        public Builder(FunctionalDependencies other) {
            this.uniformSet = new NestedSet(other.uniformSet);
            this.uniqueSet = new NestedSet(other.uniqueSet);
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

        public void addFunctionalDependencies(FunctionalDependencies fd) {
            uniformSet.add(fd.uniformSet);
            uniqueSet.add(fd.uniqueSet);
        }

        public FunctionalDependencies build() {
            return new FunctionalDependencies(uniqueSet.toImmutable(), uniformSet.toImmutable());
        }

        public void pruneSlots(Set<Slot> outputSlots) {
            uniformSet.removeNotContain(outputSlots);
            uniqueSet.removeNotContain(outputSlots);
        }

        public void replace(Map<Slot, Slot> replaceMap) {
            uniformSet.replace(replaceMap);
            uniqueSet.replace(replaceMap);
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

        public boolean contains(ImmutableSet<Slot> slotSet) {
            if (slotSet.size() == 1) {
                return slots.contains(slotSet.iterator().next());
            }
            return slotSets.contains(slotSet);
        }

        public boolean containsAnySub(Set<Slot> slotSet) {
            return slotSet.stream().anyMatch(s -> slots.contains(s))
                    || slotSets.stream().anyMatch(slotSet::containsAll);
        }

        public void removeNotContain(Set<Slot> slotSet) {
            slots = slots.stream()
                    .filter(slotSet::contains)
                    .collect(Collectors.toSet());
            slotSets = slotSets.stream()
                    .filter(slotSet::containsAll)
                    .collect(Collectors.toSet());
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

