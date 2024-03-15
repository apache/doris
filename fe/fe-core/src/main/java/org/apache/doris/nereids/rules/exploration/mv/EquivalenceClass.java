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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EquivalenceClass, this is used for equality propagation when predicate compensation
 */
public class EquivalenceClass {

    /**
     * eg: column a = b
     * this will be
     * {
     * a: [a, b],
     * b: [a, b]
     * }
     */
    private Map<SlotReference, Set<SlotReference>> equivalenceSlotMap = new LinkedHashMap<>();
    private List<Set<SlotReference>> equivalenceSlotList;

    public EquivalenceClass() {
    }

    public EquivalenceClass(Map<SlotReference, Set<SlotReference>> equivalenceSlotMap) {
        this.equivalenceSlotMap = equivalenceSlotMap;
    }

    /**
     * EquivalenceClass
     */
    public void addEquivalenceClass(SlotReference leftSlot, SlotReference rightSlot) {

        Set<SlotReference> leftSlotSet = equivalenceSlotMap.get(leftSlot);
        Set<SlotReference> rightSlotSet = equivalenceSlotMap.get(rightSlot);
        if (leftSlotSet != null && rightSlotSet != null) {
            // Both present, we need to merge
            if (leftSlotSet.size() < rightSlotSet.size()) {
                // We swap them to merge
                Set<SlotReference> tmp = rightSlotSet;
                rightSlotSet = leftSlotSet;
                leftSlotSet = tmp;
            }
            for (SlotReference newRef : rightSlotSet) {
                leftSlotSet.add(newRef);
                equivalenceSlotMap.put(newRef, leftSlotSet);
            }
        } else if (leftSlotSet != null) {
            // leftSlotSet present, we need to merge into it
            leftSlotSet.add(rightSlot);
            equivalenceSlotMap.put(rightSlot, leftSlotSet);
        } else if (rightSlotSet != null) {
            // rightSlotSet present, we need to merge into it
            rightSlotSet.add(leftSlot);
            equivalenceSlotMap.put(leftSlot, rightSlotSet);
        } else {
            // None are present, add to same equivalence class
            Set<SlotReference> equivalenceClass = new LinkedHashSet<>();
            equivalenceClass.add(leftSlot);
            equivalenceClass.add(rightSlot);
            equivalenceSlotMap.put(leftSlot, equivalenceClass);
            equivalenceSlotMap.put(rightSlot, equivalenceClass);
        }
    }

    public Map<SlotReference, Set<SlotReference>> getEquivalenceSlotMap() {
        return equivalenceSlotMap;
    }

    public boolean isEmpty() {
        return equivalenceSlotMap.isEmpty();
    }

    /**
     * EquivalenceClass permute
     */
    public EquivalenceClass permute(Map<SlotReference, SlotReference> mapping) {

        Map<SlotReference, Set<SlotReference>> permutedEquivalenceSlotMap = new HashMap<>();
        for (Map.Entry<SlotReference, Set<SlotReference>> slotReferenceSetEntry : equivalenceSlotMap.entrySet()) {
            SlotReference mappedSlotReferenceKey = mapping.get(slotReferenceSetEntry.getKey());
            if (mappedSlotReferenceKey == null) {
                // can not permute then need to return null
                return null;
            }
            Set<SlotReference> equivalenceValueSet = slotReferenceSetEntry.getValue();
            final Set<SlotReference> mappedSlotReferenceSet = new HashSet<>();
            for (SlotReference target : equivalenceValueSet) {
                SlotReference mappedSlotReferenceValue = mapping.get(target);
                if (mappedSlotReferenceValue == null) {
                    return null;
                }
                mappedSlotReferenceSet.add(mappedSlotReferenceValue);
            }
            permutedEquivalenceSlotMap.put(mappedSlotReferenceKey, mappedSlotReferenceSet);
        }
        return new EquivalenceClass(permutedEquivalenceSlotMap);
    }

    /**
     * Return the list of equivalence set, remove duplicate
     */
    public List<Set<SlotReference>> getEquivalenceSetList() {

        if (equivalenceSlotList != null) {
            return equivalenceSlotList;
        }
        List<Set<SlotReference>> equivalenceSets = new ArrayList<>();
        Set<Set<SlotReference>> visited = new HashSet<>();
        equivalenceSlotMap.values().forEach(slotSet -> {
            if (!visited.contains(slotSet)) {
                equivalenceSets.add(slotSet);
            }
            visited.add(slotSet);
        });
        this.equivalenceSlotList = equivalenceSets;
        return this.equivalenceSlotList;
    }

    @Override
    public String toString() {
        return "EquivalenceClass{" + "equivalenceSlotMap=" + equivalenceSlotMap + '}';
    }
}
