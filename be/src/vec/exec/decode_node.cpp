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

#include "vec/exec/decode_node.h"
namespace doris::vectorized {

DecodeNode::DecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _decode_node(tnode.decode_node),
          _input_tuple_ids(tnode.decode_node.input_tuple_ids),
          _slot_to_dict(tnode.decode_node.slot_to_dict) {
    assert(!_slot_to_dict.empty());
    for (const auto id : _input_tuple_ids) {
        const TupleDescriptor* tuple_desc = descs.get_tuple_descriptor(id);
        assert(tuple_desc);
        _tuple_descs.push_back(tuple_desc);
    }
}

Status DecodeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    assert(state);
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    for (const auto& item : _decode_node.slot_to_dict) {
        _dicts.emplace(item.first, state->get_global_dict_by_dict_id(item.second));
    }

    for (const auto& slot : _slot_to_dict) {
        int pos = 0;
        for (const auto& tuple_desc : _tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                if (slot.first == slot_desc->id()) {
                    _slot_to_pos.insert({slot.first, pos});
                }
                ++pos;
            }
        }
    }
    assert(!_slot_to_pos.empty());
    return Status::OK();
}

Status DecodeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _decode_timer = ADD_TIMER(runtime_profile(), "DecodeTimer");
    return Status::OK();
}

Status DecodeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    return child(0)->open(state);
}

Status DecodeNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    Block input_block;
    bool child_eos = false;
    do {
        RETURN_IF_ERROR(child(0)->get_next(state, &input_block, &child_eos));
    } while (input_block.rows() == 0 && !child_eos);

    if (input_block.rows() > 0) {
        for (const auto& slot : _slot_to_pos) {
            SCOPED_TIMER(_decode_timer);
            const auto it = _dicts.find(slot.first);
            assert(it != _dicts.end());
            assert(slot.second < input_block.columns());
            auto pos = slot.second;
            ColumnWithTypeAndName decoded_column;
            if (it->second->decode(input_block.get_by_position(pos), decoded_column)) {
                input_block.erase(pos);
                input_block.insert(pos, decoded_column);
            } else {
                return Status::Aborted("Decode dict encoded column failed");
            }
        }
        std::swap(input_block, *block);
    }
    *eos = child_eos;

    return Status::OK();
}

} // namespace doris::vectorized