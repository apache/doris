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

import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * SlotMapping, this is open generated from relationMapping
 */
public class SlotMapping extends Mapping {

    private final BiMap<MappedSlot, MappedSlot> relationSlotMap;
    private Map<SlotReference, SlotReference> slotReferenceMap;

    public SlotMapping(BiMap<MappedSlot, MappedSlot> relationSlotMap,
            Map<SlotReference, SlotReference> slotReferenceMap) {
        this.relationSlotMap = relationSlotMap;
        this.slotReferenceMap = slotReferenceMap;
    }

    public BiMap<MappedSlot, MappedSlot> getRelationSlotMap() {
        return relationSlotMap;
    }

    public SlotMapping inverse() {
        return SlotMapping.of(relationSlotMap.inverse(), null);
    }

    public static SlotMapping of(BiMap<MappedSlot, MappedSlot> relationSlotMap,
            Map<SlotReference, SlotReference> slotReferenceMap) {
        return new SlotMapping(relationSlotMap, slotReferenceMap);
    }

    /**
     * SlotMapping, this is open generated from relationMapping
     */
    @Nullable
    public static SlotMapping generate(RelationMapping relationMapping) {
        BiMap<MappedSlot, MappedSlot> relationSlotMap = HashBiMap.create();
        Map<SlotReference, SlotReference> slotReferenceMap = new HashMap<>();
        BiMap<MappedRelation, MappedRelation> mappedRelationMap = relationMapping.getMappedRelationMap();
        for (Map.Entry<MappedRelation, MappedRelation> mappedRelationEntry : mappedRelationMap.entrySet()) {
            MappedRelation sourceRelation = mappedRelationEntry.getKey();
            MappedRelation targetRelation = mappedRelationEntry.getValue();
            Map<RelationSemanticSlotKey, SlotReference> targetSlotBySemanticKey =
                    targetRelation.getSlotBySemanticKey();
            for (Map.Entry<RelationSemanticSlotKey, SlotReference> entry
                    : sourceRelation.getSlotBySemanticKey().entrySet()) {
                SlotReference sourceSlot = entry.getValue();
                SlotReference targetSlot = targetSlotBySemanticKey.get(entry.getKey());
                // Missing target slot is not always an invalid relation mapping. Variant sub-path slots
                // may be derived later from a mapped root slot during expression rewrite, e.g.
                // payload['issue']['number'] from payload. It may also happen when the base table has
                // columns added after the MV was last refreshed.
                if (targetSlot == null) {
                    continue;
                }
                MappedSlot sourceMappedSlot = MappedSlot.of(sourceSlot, sourceRelation.getBelongedRelation());
                MappedSlot targetMappedSlot = MappedSlot.of(targetSlot, targetRelation.getBelongedRelation());
                if (relationSlotMap.containsKey(sourceMappedSlot)
                        || relationSlotMap.containsValue(targetMappedSlot)) {
                    return null;
                }
                relationSlotMap.put(sourceMappedSlot, targetMappedSlot);
                slotReferenceMap.put(sourceSlot, targetSlot);
            }
        }
        return SlotMapping.of(relationSlotMap, slotReferenceMap);
    }

    /**
     * SlotMapping, toSlotReferenceMap
     */
    public Map<SlotReference, SlotReference> toSlotReferenceMap() {
        if (this.slotReferenceMap != null) {
            return this.slotReferenceMap;
        }
        this.slotReferenceMap = new HashMap<>();
        for (Map.Entry<MappedSlot, MappedSlot> entry : this.getRelationSlotMap().entrySet()) {
            this.slotReferenceMap.put((SlotReference) entry.getKey().getSlot(),
                    (SlotReference) entry.getValue().getSlot());
        }
        return this.slotReferenceMap;
    }

    @Override
    public String toString() {
        return "SlotMapping{" + "relationSlotMap=" + relationSlotMap + '}';
    }
}
