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
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * SlotMapping, this is open generated from relationMapping
 */
public class SlotMapping extends Mapping {

    public static final Logger LOG = LogManager.getLogger(SlotMapping.class);

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
            Map<List<String>, Slot> sourceSlotNameToSlotMap = sourceRelation.getSlotNameToSlotMap();

            MappedRelation targetRelation = mappedRelationEntry.getValue();
            Map<List<String>, Slot> targetSlotNameSlotMap = targetRelation.getSlotNameToSlotMap();

            for (List<String> sourceSlotName : sourceSlotNameToSlotMap.keySet()) {
                Slot sourceSlot = sourceSlotNameToSlotMap.get(sourceSlotName);
                Slot targetSlot = targetSlotNameSlotMap.get(sourceSlotName);
                // source slot can not map from target, bail out
                if (targetSlot == null && !(((SlotReference) sourceSlot).getDataType() instanceof VariantType)) {
                    LOG.warn(String.format("SlotMapping generate is null, source relation is %s, "
                            + "target relation is %s", sourceRelation, targetRelation));
                    return null;
                }
                if (targetSlot == null) {
                    // if variant, though can not map slot from query to view, but we maybe derive slot from query
                    // variant self, such as query slot to view slot mapping is payload#4 -> payload#10
                    // and query has a variant which is payload['issue']['number']#20, this can not get from view.
                    // in this scene, we can derive
                    // payload['issue']['number']#20 -> element_at(element_at(payload#10, 'issue'), 'number') mapping
                    // in expression rewrite
                    continue;
                }
                relationSlotMap.put(MappedSlot.of(sourceSlot,
                                sourceRelation.getBelongedRelation()),
                        MappedSlot.of(targetSlot, targetRelation.getBelongedRelation()));
                slotReferenceMap.put((SlotReference) sourceSlot, (SlotReference) targetSlot);
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
