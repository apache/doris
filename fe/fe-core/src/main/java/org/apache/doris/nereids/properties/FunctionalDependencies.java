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
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Record functional dependencies
 */
public class FunctionalDependencies {
    NestedSet uniqueSet = new NestedSet();
    Set<Slot> uniformSet = new HashSet<>();

    public FunctionalDependencies() {}

    public FunctionalDependencies(FunctionalDependencies other) {
        this.uniformSet = new HashSet<>(other.uniformSet);
        this.uniqueSet = new NestedSet(other.uniqueSet);
    }

    public void addUniformSlot(Slot slot) {
        if (!slot.nullable()) {
            uniformSet.add(slot);
        }
    }

    public void addUniformSlot(Set<Slot> slotSet) {
        slotSet.forEach(this::addUniformSlot);
    }

    public Set<Slot> getUniformSet() {
        return uniformSet;
    }

    public void pruneSlots(Set<Slot> outputSlots) {
        uniformSet.removeAll(outputSlots);
        uniqueSet.removeAll(outputSlots);
    }

    public void addUniqueSlot(Slot slot) {
        uniqueSet.add(slot);
    }

    /**
     * add a unique slot
     */
    public void addUniqueSlot(ImmutableSet<Slot> slotSet) {
        if (slotSet.isEmpty()) {
            return;
        }
        if (slotSet.size() == 1) {
            uniqueSet.add(slotSet.iterator().next());
        }
        uniqueSet.add(slotSet);
    }

    public boolean isUnique(Slot slot) {
        return uniqueSet.contains(slot);
    }

    public boolean isUnique(Set<Slot> slotSet) {
        if (slotSet.isEmpty()) {
            return false;
        }
        return uniqueSet.containsAnySub(slotSet);
    }

    public boolean isUniform(Slot slot) {
        return uniformSet.contains(slot);
    }

    public boolean isUniform(Set<Slot> slotSet) {
        if (slotSet.isEmpty()) {
            return false;
        }
        return uniformSet.containsAll(slotSet);
    }

    public void addFunctionalDependencies(FunctionalDependencies fd) {
        uniformSet.addAll(fd.uniformSet);
        uniqueSet.add(fd.uniqueSet);
    }

    class NestedSet {
        Set<Slot> slots = new HashSet<>();
        Set<ImmutableSet<Slot>> slotSets = new HashSet<>();

        NestedSet() {}

        NestedSet(NestedSet o) {
            this.slots = new HashSet<>(o.slots);
            this.slotSets = new HashSet<>(o.slotSets);
        }

        public boolean contains(Slot slot) {
            return slots.contains(slot);
        }

        public boolean contains(Set<Slot> slotSet) {
            return slotSets.contains(slotSet);
        }

        public boolean containsAnySub(Set<Slot> slotSet) {
            return slotSet.stream().anyMatch(s -> slots.contains(s))
                    || slotSets.stream().anyMatch(slotSet::containsAll);
        }

        public void removeAll(Set<Slot> slotSet) {
            slots.removeAll(slotSet);
            slotSets = slotSets.stream()
                    .filter(set -> slotSet.stream().noneMatch(set::contains))
                    .collect(Collectors.toSet());

        }

        public void add(Slot slot) {
            if (!slot.nullable()) {
                slots.add(slot);
            }
        }

        public void add(ImmutableSet<Slot> slotSet) {
            if (slotSet.stream().noneMatch(Slot::nullable)) {
                slotSets.add(slotSet);
            }
        }

        public void add(NestedSet nestedSet) {
            slots.addAll(nestedSet.slots);
            slotSets.addAll(nestedSet.slotSets);
        }
    }
}

