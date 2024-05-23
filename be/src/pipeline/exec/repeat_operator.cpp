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

#include "pipeline/exec/repeat_operator.h"

#include <memory>

#include "common/logging.h"
#include "pipeline/exec/operator.h"
#include "vec/core/block.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

RepeatLocalState::RepeatLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent),
          _child_block(vectorized::Block::create_unique()),
          _repeat_id_idx(0) {}

Status RepeatLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = _parent->cast<Parent>();
    _expr_ctxs.resize(p._expr_ctxs.size());
    for (size_t i = 0; i < _expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._expr_ctxs[i]->clone(state, _expr_ctxs[i]));
    }
    return Status::OK();
}

Status RepeatOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(tnode.repeat_node.exprs, _expr_ctxs));
    return Status::OK();
}

Status RepeatOperatorX::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::prepare";
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    if (_output_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_expr_ctxs, state, _child_x->row_desc()));
    for (const auto& slot_desc : _output_tuple_desc->slots()) {
        _output_slots.push_back(slot_desc);
    }

    return Status::OK();
}

Status RepeatOperatorX::open(RuntimeState* state) {
    VLOG_CRITICAL << "VRepeatNode::open";
    RETURN_IF_ERROR(OperatorXBase::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_expr_ctxs, state));
    return Status::OK();
}

RepeatOperatorX::RepeatOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                 const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _slot_id_set_list(tnode.repeat_node.slot_id_set_list),
          _all_slot_ids(tnode.repeat_node.all_slot_ids),
          _repeat_id_list(tnode.repeat_node.repeat_id_list),
          _grouping_list(tnode.repeat_node.grouping_list),
          _output_tuple_id(tnode.repeat_node.output_tuple_id) {};

bool RepeatOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = state->get_local_state(operator_id())->cast<RepeatLocalState>();
    return !local_state._child_block->rows() && !local_state._child_eos;
}

Status RepeatLocalState::get_repeated_block(vectorized::Block* child_block, int repeat_id_idx,
                                            vectorized::Block* output_block) {
    auto& p = _parent->cast<RepeatOperatorX>();
    DCHECK(child_block != nullptr);
    DCHECK_EQ(output_block->rows(), 0);

    size_t child_column_size = child_block->columns();
    size_t column_size = p._output_slots.size();
    DCHECK_LT(child_column_size, column_size);
    auto m_block = vectorized::VectorizedUtils::build_mutable_mem_reuse_block(output_block,
                                                                              p._output_slots);
    auto& columns = m_block.mutable_columns();
    /* Fill all slots according to child, for example:select tc1,tc2,sum(tc3) from t1 group by grouping sets((tc1),(tc2));
     * insert into t1 values(1,2,1),(1,3,1),(2,1,1),(3,1,1);
     * slot_id_set_list=[[0],[1]],repeat_id_idx=0,
     * child_block 1,2,1 | 1,3,1 | 2,1,1 | 3,1,1
     * output_block 1,null,1,1 | 1,null,1,1 | 2,nul,1,1 | 3,null,1,1
     */
    size_t cur_col = 0;
    for (size_t i = 0; i < child_column_size; i++) {
        const vectorized::ColumnWithTypeAndName& src_column = child_block->get_by_position(i);

        std::set<SlotId>& repeat_ids = p._slot_id_set_list[repeat_id_idx];
        bool is_repeat_slot =
                p._all_slot_ids.find(p._output_slots[cur_col]->id()) != p._all_slot_ids.end();
        bool is_set_null_slot = repeat_ids.find(p._output_slots[cur_col]->id()) == repeat_ids.end();
        const auto row_size = src_column.column->size();

        if (is_repeat_slot) {
            DCHECK(p._output_slots[cur_col]->is_nullable());
            auto* nullable_column =
                    reinterpret_cast<vectorized::ColumnNullable*>(columns[cur_col].get());
            auto& null_map = nullable_column->get_null_map_data();
            auto* column_ptr = columns[cur_col].get();

            // set slot null not in repeat_ids
            if (is_set_null_slot) {
                nullable_column->resize(row_size);
                memset(nullable_column->get_null_map_data().data(), 1,
                       sizeof(vectorized::UInt8) * row_size);
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

    const auto rows = child_block->rows();
    // Fill grouping ID to block
    RETURN_IF_ERROR(add_grouping_id_column(rows, cur_col, columns, repeat_id_idx));

    DCHECK_EQ(cur_col, column_size);

    return Status::OK();
}

Status RepeatLocalState::add_grouping_id_column(std::size_t rows, std::size_t& cur_col,
                                                vectorized::MutableColumns& columns,
                                                int repeat_id_idx) {
    auto& p = _parent->cast<RepeatOperatorX>();
    for (auto slot_idx = 0; slot_idx < p._grouping_list.size(); slot_idx++) {
        DCHECK_LT(slot_idx, p._output_tuple_desc->slots().size());
        const SlotDescriptor* _virtual_slot_desc = p._output_tuple_desc->slots()[cur_col];
        DCHECK_EQ(_virtual_slot_desc->type().type, p._output_slots[cur_col]->type().type);
        DCHECK_EQ(_virtual_slot_desc->col_name(), p._output_slots[cur_col]->col_name());
        int64_t val = p._grouping_list[slot_idx][repeat_id_idx];
        auto* column_ptr = columns[cur_col].get();
        DCHECK(!p._output_slots[cur_col]->is_nullable());
        auto* col = assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(column_ptr);
        col->insert_many_vals(val, rows);
        cur_col++;
    }
    return Status::OK();
}

Status RepeatOperatorX::push(RuntimeState* state, vectorized::Block* input_block, bool eos) const {
    auto& local_state = get_local_state(state);
    local_state._child_eos = eos;
    auto& _intermediate_block = local_state._intermediate_block;
    auto& _expr_ctxs = local_state._expr_ctxs;
    DCHECK(!_intermediate_block || _intermediate_block->rows() == 0);
    if (input_block->rows() > 0) {
        _intermediate_block = vectorized::Block::create_unique();

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

Status RepeatOperatorX::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                             bool* eos) const {
    auto& local_state = get_local_state(state);
    auto& _repeat_id_idx = local_state._repeat_id_idx;
    auto& _child_block = *local_state._child_block;
    auto& _child_eos = local_state._child_eos;
    auto& _intermediate_block = local_state._intermediate_block;
    RETURN_IF_CANCELLED(state);
    DCHECK(_repeat_id_idx >= 0);
    for (const std::vector<int64_t>& v : _grouping_list) {
        DCHECK(_repeat_id_idx <= (int)v.size());
    }
    DCHECK(output_block->rows() == 0);

    if (_intermediate_block && _intermediate_block->rows() > 0) {
        RETURN_IF_ERROR(local_state.get_repeated_block(_intermediate_block.get(), _repeat_id_idx,
                                                       output_block));

        _repeat_id_idx++;

        int size = _repeat_id_list.size();
        if (_repeat_id_idx >= size) {
            _intermediate_block->clear();
            _child_block.clear_column_data(_child_x->row_desc().num_materialized_slots());
            _repeat_id_idx = 0;
        }
    } else if (local_state._expr_ctxs.empty()) {
        auto m_block = vectorized::VectorizedUtils::build_mutable_mem_reuse_block(output_block,
                                                                                  _output_slots);
        auto rows = _child_block.rows();
        auto& columns = m_block.mutable_columns();

        for (int repeat_id_idx = 0; repeat_id_idx < _repeat_id_list.size(); repeat_id_idx++) {
            std::size_t cur_col = 0;
            RETURN_IF_ERROR(
                    local_state.add_grouping_id_column(rows, cur_col, columns, repeat_id_idx));
        }
        _child_block.clear_column_data(_child_x->row_desc().num_materialized_slots());
    }
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, output_block,
                                                           output_block->columns()));
    *eos = _child_eos && _child_block.rows() == 0;
    local_state.reached_limit(output_block, eos);
    COUNTER_SET(local_state._rows_returned_counter, local_state._num_rows_returned);
    return Status::OK();
}

} // namespace doris::pipeline
