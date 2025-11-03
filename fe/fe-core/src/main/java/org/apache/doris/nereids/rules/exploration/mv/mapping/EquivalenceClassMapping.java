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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.nereids.rules.exploration.mv.EquivalenceClass;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EquivalenceClassSetMapping
 * This will extract the equivalence class set in EquivalenceClass and mapping set in
 * two different EquivalenceClass.
 */
public class EquivalenceClassMapping extends Mapping {

    private final Map<List<SlotReference>, List<SlotReference>> equivalenceClassSetMap;

    public EquivalenceClassMapping(Map<List<SlotReference>,
            List<SlotReference>> equivalenceClassSetMap) {
        this.equivalenceClassSetMap = equivalenceClassSetMap;
    }

    public static EquivalenceClassMapping of(Map<List<SlotReference>, List<SlotReference>> equivalenceClassSetMap) {
        return new EquivalenceClassMapping(equivalenceClassSetMap);
    }

    /**
     * Generate source equivalence set map to target equivalence set
     */
    public static EquivalenceClassMapping generate(EquivalenceClass source, EquivalenceClass target) {

        Map<List<SlotReference>, List<SlotReference>> equivalenceClassSetMap = new HashMap<>();
        List<List<SlotReference>> sourceSets = source.getEquivalenceSetList();
        List<List<SlotReference>> targetSets = target.getEquivalenceSetList();

        for (List<SlotReference> sourceList : sourceSets) {
            Set<SlotReference> sourceSet = new HashSet<>(sourceList);
            for (List<SlotReference> targetList : targetSets) {
                Set<SlotReference> targetSet = new HashSet<>(targetList);
                if (sourceSet.containsAll(targetSet)) {
                    equivalenceClassSetMap.put(sourceList, targetList);
                }
            }
        }
        return EquivalenceClassMapping.of(equivalenceClassSetMap);
    }

    public Map<List<SlotReference>, List<SlotReference>> getEquivalenceClassSetMap() {
        return equivalenceClassSetMap;
    }
}
