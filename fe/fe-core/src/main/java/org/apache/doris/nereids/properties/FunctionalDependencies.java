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
    NestedSet uniformSet = new NestedSet();

    public FunctionalDependencies() {}

    public FunctionalDependencies(FunctionalDependencies other) {
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

    public boolean isEmpty() {
        return uniformSet.isEmpty() && uniqueSet.isEmpty();
    }

    public void pruneSlots(Set<Slot> outputSlots) {
        uniformSet.removeNotContain(outputSlots);
        uniqueSet.removeNotContain(outputSlots);
    }

    public void addUniqueSlot(Slot slot) {
        uniqueSet.add(slot);
    }

    public void addUniqueSlot(ImmutableSet<Slot> slotSet) {
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

    public boolean isUniform(ImmutableSet<Slot> slotSet) {
        return uniformSet.contains(slotSet) || slotSet.stream().allMatch(uniformSet::contains);
    }

    public void addFunctionalDependencies(FunctionalDependencies fd) {
        uniformSet.add(fd.uniformSet);
        uniqueSet.add(fd.uniqueSet);
    }

    @Override
    public String toString() {
        return "uniform:" + uniformSet.toString()
                + "\t unique:" + uniqueSet.toString();
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
            if (slot.nullable()) {
                return;
            }
            slots.add(slot);
        }

        public void add(ImmutableSet<Slot> slotSet) {
            if (slotSet.isEmpty()) {
                return;
            }
            if (slotSet.stream().anyMatch(Slot::nullable)) {
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
    }
}

