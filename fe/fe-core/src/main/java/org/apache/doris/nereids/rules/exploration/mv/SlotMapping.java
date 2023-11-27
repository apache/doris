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

import com.google.common.collect.BiMap;

/**
 * SlotMapping, this is open generated from relationMapping
 */
public class SlotMapping extends Mapping {

    private final BiMap<MappedSlot, MappedSlot> relationSlotMap;

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

    public static SlotMapping generate(RelationMapping relationMapping) {
        // TODO implement
        return SlotMapping.of(null);
    }
}
