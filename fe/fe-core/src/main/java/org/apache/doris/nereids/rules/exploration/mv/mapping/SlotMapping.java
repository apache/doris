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

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * SlotMapping, this is open generated from relationMapping
 */
public class SlotMapping extends Mapping {

    private final BiMap<MappedSlot, MappedSlot> relationSlotMap;
    private Map<SlotReference, SlotReference> slotReferenceMap;

    public SlotMapping(BiMap<MappedSlot, MappedSlot> relationSlotMap) {
        this.relationSlotMap = relationSlotMap;
    }

    public BiMap<MappedSlot, MappedSlot> getRelationSlotMap() {
        return relationSlotMap;
    }

    public SlotMapping inverse() {
        return SlotMapping.of(relationSlotMap.inverse());
    }

    public static SlotMapping of(BiMap<MappedSlot, MappedSlot> relationSlotMap) {
        return new SlotMapping(relationSlotMap);
    }

    /**
     * SlotMapping, this is open generated from relationMapping
     */
    @Nullable
    public static SlotMapping generate(RelationMapping relationMapping) {
        BiMap<MappedSlot, MappedSlot> relationSlotMap = HashBiMap.create();
        BiMap<MappedRelation, MappedRelation> mappedRelationMap = relationMapping.getMappedRelationMap();
        for (Map.Entry<MappedRelation, MappedRelation> mappedRelationEntry : mappedRelationMap.entrySet()) {
            Map<String, Slot> targetNameSlotMap =
                    mappedRelationEntry.getValue().getBelongedRelation().getOutput().stream()
                            .collect(Collectors.toMap(Slot::getName, slot -> slot));
            for (Slot sourceSlot : mappedRelationEntry.getKey().getBelongedRelation().getOutput()) {
                Slot targetSlot = targetNameSlotMap.get(sourceSlot.getName());
                // source slot can not map from target, bail out
                if (targetSlot == null) {
                    return null;
                }
                relationSlotMap.put(MappedSlot.of(sourceSlot, mappedRelationEntry.getKey().getBelongedRelation()),
                        MappedSlot.of(targetSlot, mappedRelationEntry.getValue().getBelongedRelation()));
            }
        }
        return SlotMapping.of(relationSlotMap);
    }

    public Map<MappedSlot, MappedSlot> toMappedSlotMap() {
        return (Map) this.getRelationSlotMap();
    }

    /**
     * SlotMapping, toSlotReferenceMap
     */
    public Map<SlotReference, SlotReference> toSlotReferenceMap() {
        if (this.slotReferenceMap != null) {
            return this.slotReferenceMap;
        }
        Map<SlotReference, SlotReference> slotReferenceSlotReferenceMap = new HashMap<>();
        for (Map.Entry<MappedSlot, MappedSlot> entry : this.getRelationSlotMap().entrySet()) {
            slotReferenceSlotReferenceMap.put((SlotReference) entry.getKey().getSlot(),
                    (SlotReference) entry.getValue().getSlot());
        }
        this.slotReferenceMap = slotReferenceSlotReferenceMap;
        return this.slotReferenceMap;
    }
}
