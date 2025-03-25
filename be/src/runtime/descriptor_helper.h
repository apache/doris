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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>

#include <vector>

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"

namespace doris {

class TDescriptorTableBuilder {
public:
    TSlotId next_slot_id() { return _next_slot_id++; }
    TTupleId next_tuple_id() { return _next_tuple_id++; }

    void add_slots(const std::vector<TSlotDescriptor>& slots) {
        _desc_tbl.__isset.slotDescriptors = true;
        _desc_tbl.slotDescriptors.insert(_desc_tbl.slotDescriptors.end(), slots.begin(),
                                         slots.end());
    }
    void add_tuple(const TTupleDescriptor& tuple) { _desc_tbl.tupleDescriptors.push_back(tuple); }

    TDescriptorTable desc_tbl() { return _desc_tbl; }

private:
    TSlotId _next_slot_id = 0;
    TTupleId _next_tuple_id = 0;
    TDescriptorTable _desc_tbl;
};

class TTupleDescriptorBuilder;
class TSlotDescriptorBuilder {
public:
    TTypeDesc get_common_type(TPrimitiveType::type type) {
        TTypeNode node;
        node.type = TTypeNodeType::SCALAR;
        node.__isset.scalar_type = true;
        node.scalar_type.type = type;

        TTypeDesc type_desc;
        type_desc.types.push_back(node);

        return type_desc;
    }

    TSlotDescriptorBuilder() { _slot_desc.isMaterialized = true; }
    TSlotDescriptorBuilder& type(PrimitiveType type) {
        _slot_desc.slotType = get_common_type(to_thrift(type));
        return *this;
    }
    TSlotDescriptorBuilder& decimal_type(int precision, int scale) {
        _slot_desc.slotType = get_common_type(to_thrift(TYPE_DECIMALV2));
        _slot_desc.slotType.types[0].scalar_type.__set_precision(precision);
        _slot_desc.slotType.types[0].scalar_type.__set_scale(scale);
        return *this;
    }
    TSlotDescriptorBuilder& string_type(int len) {
        _slot_desc.slotType = get_common_type(to_thrift(TYPE_VARCHAR));
        _slot_desc.slotType.types[0].scalar_type.__set_len(len);
        return *this;
    }
    TSlotDescriptorBuilder& nullable(bool nullable) {
        _slot_desc.nullIndicatorByte = (nullable) ? 0 : -1;
        return *this;
    }
    TSlotDescriptorBuilder& is_materialized(bool is_materialized) {
        _slot_desc.isMaterialized = is_materialized;
        return *this;
    }
    TSlotDescriptorBuilder& column_name(const std::string& name) {
        _slot_desc.colName = name;
        return *this;
    }
    TSlotDescriptorBuilder& column_pos(int column_pos) {
        _slot_desc.columnPos = column_pos;
        return *this;
    }
    TSlotDescriptor build() { return _slot_desc; }

private:
    friend TTupleDescriptorBuilder;
    TSlotDescriptor _slot_desc;
};

class TTupleDescriptorBuilder {
public:
    TTupleDescriptorBuilder& add_slot(const TSlotDescriptor& slot_desc) {
        _slot_descs.push_back(slot_desc);
        return *this;
    }
    void build(TDescriptorTableBuilder* tb) {
        // build slot desc
        _tuple_id = tb->next_tuple_id();
        int null_offset = 0;
        for (int i = 0; i < _slot_descs.size(); ++i) {
            auto& slot_desc = _slot_descs[i];
            slot_desc.id = tb->next_slot_id();
            slot_desc.parent = _tuple_id;
            if (slot_desc.nullIndicatorByte >= 0) {
                slot_desc.nullIndicatorBit = null_offset % 8;
                slot_desc.nullIndicatorByte = null_offset / 8;
                null_offset++;
            } else {
                slot_desc.nullIndicatorByte = 0;
                slot_desc.nullIndicatorBit = -1;
            }
            slot_desc.slotIdx = i;
        }

        _tuple_desc.id = _tuple_id;
        // Useless not set it.
        _tuple_desc.byteSize = 0;
        _tuple_desc.numNullBytes = 0;
        _tuple_desc.numNullSlots = _slot_descs.size();

        tb->add_slots(_slot_descs);
        tb->add_tuple(_tuple_desc);
    }

private:
    TTupleId _tuple_id;
    std::vector<TSlotDescriptor> _slot_descs;
    TTupleDescriptor _tuple_desc;
};

} // namespace doris
