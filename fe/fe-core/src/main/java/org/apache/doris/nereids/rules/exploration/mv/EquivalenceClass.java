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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EquivalenceClass, this is used for equality propagation when predicate compensation
 */
public class EquivalenceClass {

    private final Map<SlotReference, Set<SlotReference>> equivalenceSlotMap = new LinkedHashMap<>();

    public EquivalenceClass() {
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
     * EquivalenceClass
     */
    public List<Set<SlotReference>> getEquivalenceValues() {
        List<Set<SlotReference>> values = new ArrayList<>();
        equivalenceSlotMap.values().forEach(each -> values.add(each));
        return values;
    }
}
