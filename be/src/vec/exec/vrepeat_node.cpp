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

#include <gen_cpp/PlanNodes_types.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <string.h>

#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/join.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
VRepeatNode::VRepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _slot_id_set_list(tnode.repeat_node.slot_id_set_list),
          _all_slot_ids(tnode.repeat_node.all_slot_ids),
          _repeat_id_list(tnode.repeat_node.repeat_id_list),
          _grouping_list(tnode.repeat_node.grouping_list),
          _output_tuple_id(tnode.repeat_node.output_tuple_id),
          _child_eos(false),
          _repeat_id_idx(0) {}

Status VRepeatNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.repeat_node.exprs, _expr_ctxs));
    return Status::OK();
}

Status VRepeatNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::prepare";
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_TIMER(_exec_timer);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    if (_output_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    RETURN_IF_ERROR(VExpr::prepare(_expr_ctxs, state, child(0)->row_desc()));

    for (const auto& slot_desc : _output_tuple_desc->slots()) {
        _output_slots.push_back(slot_desc);
    }

    return Status::OK();
}

Status VRepeatNode::open(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

Status VRepeatNode::alloc_resource(RuntimeState* state) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    RETURN_IF_ERROR(VExpr::open(_expr_ctxs, state));
    return Status::OK();
}

Status VRepeatNode::get_repeated_block(Block* child_block, int repeat_id_idx, Block* output_block) {
    VLOG_CRITICAL << "VRepeatNode::get_repeated_block";
    DCHECK(child_block != nullptr);
    DCHECK_EQ(output_block->rows(), 0);

    size_t child_column_size = child_block->columns();
    size_t column_size = _output_slots.size();
    DCHECK_LT(child_column_size, column_size);
    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, _output_slots);
    MutableColumns& columns = m_block.mutable_columns();
    /* Fill all slots according to child, for example:select tc1,tc2,sum(tc3) from t1 group by grouping sets((tc1),(tc2));
     * insert into t1 values(1,2,1),(1,3,1),(2,1,1),(3,1,1);
     * slot_id_set_list=[[0],[1]],repeat_id_idx=0,
     * child_block 1,2,1 | 1,3,1 | 2,1,1 | 3,1,1
     * output_block 1,null,1,1 | 1,null,1,1 | 2,nul,1,1 | 3,null,1,1
     */
    size_t cur_col = 0;
    for (size_t i = 0; i < child_column_size; i++) {
        const ColumnWithTypeAndName& src_column = child_block->get_by_position(i);

        std::set<SlotId>& repeat_ids = _slot_id_set_list[repeat_id_idx];
        bool is_repeat_slot =
                _all_slot_ids.find(_output_slots[cur_col]->id()) != _all_slot_ids.end();
        bool is_set_null_slot = repeat_ids.find(_output_slots[cur_col]->id()) == repeat_ids.end();
        const auto row_size = src_column.column->size();

        if (is_repeat_slot) {
            DCHECK(_output_slots[cur_col]->is_nullable());
            auto* nullable_column = reinterpret_cast<ColumnNullable*>(columns[cur_col].get());
            auto& null_map = nullable_column->get_null_map_data();
            auto* column_ptr = columns[cur_col].get();

            // set slot null not in repeat_ids
            if (is_set_null_slot) {
                nullable_column->resize(row_size);
                memset(nullable_column->get_null_map_data().data(), 1, sizeof(UInt8) * row_size);
            } else {
                if (!src_column.type->is_nullable()) {
                    for (size_t j = 0; j < row_size; ++j) {
                        null_map.push_back(0);
                    }
                    column_ptr = &nullable_column->get_nested_column();
                }
                column_ptr->insert_range_from(*src_column.column, 0, row_size);
            }
        } else {
            columns[cur_col]->insert_range_from(*src_column.column, 0, row_size);
        }
        cur_col++;
    }

    // Fill grouping ID to block
    for (auto slot_idx = 0; slot_idx < _grouping_list.size(); slot_idx++) {
        DCHECK_LT(slot_idx, _output_tuple_desc->slots().size());
        const SlotDescriptor* _virtual_slot_desc = _output_tuple_desc->slots()[cur_col];
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

    return Status::OK();
}

Status VRepeatNode::pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    SCOPED_TIMER(_exec_timer);
    RETURN_IF_CANCELLED(state);
    DCHECK(_repeat_id_idx >= 0);
    for (const std::vector<int64_t>& v : _grouping_list) {
        DCHECK(_repeat_id_idx <= (int)v.size());
    }
    DCHECK(output_block->rows() == 0);

    if (_intermediate_block && _intermediate_block->rows() > 0) {
        RETURN_IF_ERROR(
                get_repeated_block(_intermediate_block.get(), _repeat_id_idx, output_block));

        _repeat_id_idx++;

        int size = _repeat_id_list.size();
        if (_repeat_id_idx >= size) {
            _intermediate_block->clear();
            release_block_memory(_child_block);
            _repeat_id_idx = 0;
        }
    }
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    *eos = _child_eos && _child_block.rows() == 0;
    reached_limit(output_block, eos);
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status VRepeatNode::push(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    SCOPED_TIMER(_exec_timer);
    _child_eos = eos;
    DCHECK(!_intermediate_block || _intermediate_block->rows() == 0);
    DCHECK(!_expr_ctxs.empty());

    if (input_block->rows() > 0) {
        _intermediate_block = Block::create_unique();

        for (auto& expr : _expr_ctxs) {
            int result_column_id = -1;
            RETURN_IF_ERROR(expr->execute(input_block, &result_column_id));
            DCHECK(result_column_id != -1);
            input_block->get_by_position(result_column_id).column =
                    input_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            _intermediate_block->insert(input_block->get_by_position(result_column_id));
        }
        DCHECK_EQ(_expr_ctxs.size(), _intermediate_block->columns());
    }

    return Status::OK();
}

bool VRepeatNode::need_more_input_data() const {
    return !_child_block.rows() && !_child_eos;
}

Status VRepeatNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    if (state == nullptr || block == nullptr || eos == nullptr) {
        return Status::InternalError("input is nullptr");
    }
    VLOG_CRITICAL << "VRepeatNode::get_next";
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_CANCELLED(state);
    DCHECK(_repeat_id_idx >= 0);
    for (const std::vector<int64_t>& v : _grouping_list) {
        DCHECK(_repeat_id_idx <= (int)v.size());
    }
    DCHECK(block->rows() == 0);
    while (need_more_input_data()) {
        RETURN_IF_ERROR(child(0)->get_next_after_projects(
                state, &_child_block, &_child_eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));

        static_cast<void>(push(state, &_child_block, _child_eos));
    }

    return pull(state, block, eos);
}

Status VRepeatNode::close(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::close";
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

void VRepeatNode::release_resource(RuntimeState* state) {
    ExecNode::release_resource(state);
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
