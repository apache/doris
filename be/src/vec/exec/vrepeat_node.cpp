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

#include "vec/exec/vrepeat_node.h"

#include "exprs/expr.h"
#include "gutil/strings/join.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {
VRepeatNode::VRepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : RepeatNode(pool, tnode, descs),
          _child_block(nullptr),
          _virtual_tuple_id(tnode.repeat_node.output_tuple_id) {}

Status VRepeatNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::prepare";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(RepeatNode::prepare(state));

    // get current all output slots
    for (const auto& tuple_desc : this->row_desc().tuple_descriptors()) {
        for (const auto& slot_desc : tuple_desc->slots()) {
            _output_slots.push_back(slot_desc);
        }
    }

    // get all input slots
    for (const auto& child_tuple_desc : child(0)->row_desc().tuple_descriptors()) {
        for (const auto& child_slot_desc : child_tuple_desc->slots()) {
            _child_slots.push_back(child_slot_desc);
        }
    }

    _virtual_tuple_desc = state->desc_tbl().get_tuple_descriptor(_virtual_tuple_id);
    if (_virtual_tuple_desc == NULL) {
        return Status::InternalError("Failed to get virtual tuple descriptor.");
    }

    std::stringstream ss;
    ss << "The output slots size " << _output_slots.size()
       << " is not equal to the sum of child_slots_size " << _child_slots.size()
       << ",virtual_slots_size " << _virtual_tuple_desc->slots().size();
    if (_output_slots.size() != (_child_slots.size() + _virtual_tuple_desc->slots().size())) {
        return Status::InternalError(ss.str());
    }

    _child_block.reset(new Block());

    return Status::OK();
}

Status VRepeatNode::open(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(RepeatNode::open(state));
    return Status::OK();
}

Status VRepeatNode::get_repeated_block(Block* child_block, int repeat_id_idx, Block* output_block) {
    VLOG_CRITICAL << "VRepeatNode::get_repeated_block";
    DCHECK(child_block != nullptr);
    DCHECK_EQ(output_block->rows(), 0);

    size_t child_column_size = child_block->columns();
    size_t column_size = _output_slots.size();
    bool mem_reuse = output_block->mem_reuse();
    DCHECK_EQ(child_column_size, _child_slots.size());
    DCHECK_LT(child_column_size, column_size);
    std::vector<vectorized::MutableColumnPtr> columns(column_size);
    for (size_t i = 0; i < column_size; i++) {
        if (mem_reuse) {
            columns[i] = std::move(*output_block->get_by_position(i).column).mutate();
        } else {
            columns[i] = _output_slots[i]->get_empty_mutable_column();
        }
    }

    /* Fill all slots according to child, for example:select tc1,tc2,sum(tc3) from t1 group by grouping sets((tc1),(tc2));
     * insert into t1 values(1,2,1),(1,3,1),(2,1,1),(3,1,1);
     * slot_id_set_list=[[0],[1]],repeat_id_idx=0,
     * child_block 1,2,1 | 1,3,1 | 2,1,1 | 3,1,1
     * output_block 1,null,1,1 | 1,null,1,1 | 2,nul,1,1 | 3,null,1,1
     */
    size_t cur_col = 0;
    for (size_t i = 0; i < child_column_size; i++) {
        const ColumnWithTypeAndName& src_column = child_block->get_by_position(i);

        DCHECK_EQ(_child_slots[i]->type().type, _output_slots[cur_col]->type().type);
        DCHECK_EQ(_child_slots[i]->col_name(), _output_slots[cur_col]->col_name());

        std::set<SlotId>& repeat_ids = _slot_id_set_list[repeat_id_idx];
        bool is_repeat_slot =
                _all_slot_ids.find(_output_slots[cur_col]->id()) != _all_slot_ids.end();
        bool is_set_null_slot = repeat_ids.find(_output_slots[cur_col]->id()) == repeat_ids.end();
        const auto column_size = src_column.column->size();

        if (is_repeat_slot) {
            DCHECK(_output_slots[cur_col]->is_nullable());
            auto* nullable_column = reinterpret_cast<ColumnNullable*>(columns[cur_col].get());
            auto& null_map = nullable_column->get_null_map_data();
            auto* column_ptr = columns[cur_col].get();

            // set slot null not in repeat_ids
            if (is_set_null_slot) {
                nullable_column->resize(column_size);
                memset(nullable_column->get_null_map_data().data(), 1, sizeof(UInt8) * column_size);
            } else {
                if (!src_column.type->is_nullable()) {
                    for (size_t j = 0; j < column_size; ++j) {
                        null_map.push_back(0);
                    }
                    column_ptr = &nullable_column->get_nested_column();
                }
                column_ptr->insert_range_from(*src_column.column, 0, column_size);
            }
        } else {
            columns[cur_col]->insert_range_from(*src_column.column, 0, column_size);
        }
        cur_col++;
    }

    // Fill grouping ID to block
    for (auto slot_idx = 0; slot_idx < _grouping_list.size(); slot_idx++) {
        DCHECK_LT(slot_idx, _virtual_tuple_desc->slots().size());
        const SlotDescriptor* _virtual_slot_desc = _virtual_tuple_desc->slots()[slot_idx];
        DCHECK_EQ(_virtual_slot_desc->type().type, _output_slots[cur_col]->type().type);
        DCHECK_EQ(_virtual_slot_desc->col_name(), _output_slots[cur_col]->col_name());
        int64_t val = _grouping_list[slot_idx][repeat_id_idx];
        auto* column_ptr = columns[cur_col].get();
        DCHECK(!_output_slots[cur_col]->is_nullable());

        auto* col = assert_cast<ColumnVector<Int64>*>(column_ptr);
        for (size_t i = 0; i < child_block->rows(); ++i) {
            col->insert_value(val);
        }
        cur_col++;
    }

    DCHECK_EQ(cur_col, column_size);

    if (!columns.empty() && !columns[0]->empty()) {
        auto n_columns = 0;
        if (!mem_reuse) {
            for (const auto slot_desc : _output_slots) {
                output_block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                           slot_desc->get_data_type_ptr(),
                                                           slot_desc->col_name()));
            }
        } else {
            columns.clear();
        }
    }
    return Status::OK();
}

Status VRepeatNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    VLOG_CRITICAL << "VRepeatNode::get_next";
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (state == nullptr || block == nullptr || eos == nullptr) {
        return Status::InternalError("input is NULL pointer");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    DCHECK(_repeat_id_idx >= 0);
    for (const std::vector<int64_t>& v : _grouping_list) {
        DCHECK(_repeat_id_idx <= (int)v.size());
    }
    DCHECK(block->rows() == 0);

    // current child block has finished its repeat, get child's next block
    if (_child_block->rows() == 0) {
        while (_child_block->rows() == 0 && !_child_eos) {
            RETURN_IF_ERROR(child(0)->get_next(state, _child_block.get(), &_child_eos));
        }

        if (_child_eos and _child_block->rows() == 0) {
            *eos = true;
            return Status::OK();
        }
    }

    RETURN_IF_ERROR(get_repeated_block(_child_block.get(), _repeat_id_idx, block));

    _repeat_id_idx++;

    int size = _repeat_id_list.size();
    if (_repeat_id_idx >= size) {
        release_block_memory(*_child_block.get());
        _repeat_id_idx = 0;
    }

    reached_limit(block, eos);
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    VLOG_ROW << "VRepeatNode output rows: " << block->rows();
    return Status::OK();
}

Status VRepeatNode::close(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::close";
    if (is_closed()) {
        return Status::OK();
    }
    release_block_memory(*_child_block.get());
    RETURN_IF_ERROR(child(0)->close(state));
    return ExecNode::close(state);
}

void VRepeatNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "VRepeatNode(";
    *out << "repeat pattern: [" << JoinElements(_repeat_id_list, ",") << "]\n";
    *out << "add " << _grouping_list.size() << " columns. \n";
    *out << "added column values: ";
    for (const std::vector<int64_t>& v : _grouping_list) {
        *out << "[" << JoinElements(v, ",") << "] ";
    }
    *out << "\n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}
} // namespace doris::vectorized
