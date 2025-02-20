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

#pragma once

#include <vector>

#include "runtime/descriptors.h"
#include "vec/data_types/data_type.h"

namespace doris {

class MockSlotDescriptor : public SlotDescriptor {
public:
    doris::vectorized::DataTypePtr get_data_type_ptr() const override { return type; }
    vectorized::DataTypePtr type;
};

class MockTupleDescriptor : public TupleDescriptor {
public:
    const std::vector<SlotDescriptor*>& slots() const override { return Slots; }

    std::vector<SlotDescriptor*> Slots;
};

class MockRowDescriptor : public RowDescriptor {
public:
    MockRowDescriptor(std::vector<vectorized::DataTypePtr> types, ObjectPool* pool) {
        std::vector<SlotDescriptor*> slots;
        for (auto type : types) {
            auto* slot = pool->add(new MockSlotDescriptor());
            slot->type = type;
            slots.push_back(slot);
        }
        auto* tuple_desc = pool->add(new MockTupleDescriptor());
        tuple_desc->Slots = slots;
        tuple_desc_map.push_back(tuple_desc);
    }
    const std::vector<TupleDescriptor*>& tuple_descriptors() const override {
        return tuple_desc_map;
    }
    std::vector<TupleDescriptor*> tuple_desc_map;
};

} // namespace doris