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

#include "set_source_operator.h"

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;
} // namespace doris

namespace doris::pipeline {

template <bool is_intersect>
SetSourceOperatorBuilder<is_intersect>::SetSourceOperatorBuilder(int32_t id, ExecNode* set_node)
        : OperatorBuilder<vectorized::VSetOperationNode<is_intersect>>(id, builder_name, set_node) {
}

template <bool is_intersect>
OperatorPtr SetSourceOperatorBuilder<is_intersect>::build_operator() {
    return std::make_shared<SetSourceOperator<is_intersect>>(this, this->_node);
}

template <bool is_intersect>
SetSourceOperator<is_intersect>::SetSourceOperator(
        OperatorBuilderBase* builder, vectorized::VSetOperationNode<is_intersect>* set_node)
        : SourceOperator<SetSourceOperatorBuilder<is_intersect>>(builder, set_node) {}

template class SetSourceOperatorBuilder<true>;
template class SetSourceOperatorBuilder<false>;
template class SetSourceOperator<true>;
template class SetSourceOperator<false>;

template <bool is_intersect>
Status SetSourceOperatorX<is_intersect>::get_block(RuntimeState* state, vectorized::Block* block,
                                                   SourceState& source_state) {
    RETURN_IF_CANCELLED(state);
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state._pull_timer);
    create_mutable_cols(local_state, block);
    auto st = std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return get_data_in_hashtable<HashTableCtxType>(
                            local_state, arg, block, state->batch_size(), source_state);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *local_state._shared_state->_hash_table_variants);
    RETURN_IF_ERROR(st);
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, block, block->columns()));
    local_state.reached_limit(block, source_state);
    return Status::OK();
}

template <bool is_intersect>
void SetSourceOperatorX<is_intersect>::create_mutable_cols(
        SetSourceLocalState<is_intersect>& local_state, vectorized::Block* output_block) {
    _mutable_cols.resize(local_state._shared_state->_left_table_data_types.size());
    bool mem_reuse = output_block->mem_reuse();

    for (int i = 0; i < local_state._shared_state->_left_table_data_types.size(); ++i) {
        if (mem_reuse) {
            _mutable_cols[i] = std::move(*output_block->get_by_position(i).column).mutate();
        } else {
            _mutable_cols[i] =
                    (local_state._shared_state->_left_table_data_types[i]->create_column());
        }
    }
}

template <bool is_intersect>
template <typename HashTableContext>
Status SetSourceOperatorX<is_intersect>::get_data_in_hashtable(
        SetSourceLocalState<is_intersect>& local_state, HashTableContext& hash_table_ctx,
        vectorized::Block* output_block, const int batch_size, SourceState& source_state) {
    hash_table_ctx.init_once();
    int left_col_len = local_state._shared_state->_left_table_data_types.size();
    auto& iter = hash_table_ctx.iter;
    auto block_size = 0;

    if constexpr (std::is_same_v<typename HashTableContext::Mapped,
                                 vectorized::RowRefListWithFlags>) {
        for (; iter != hash_table_ctx.hash_table.end() && block_size < batch_size; ++iter) {
            auto& value = iter->get_second();
            auto it = value.begin();
            if constexpr (is_intersect) {
                if (it->visited) { //intersected: have done probe, so visited values it's the result
                    add_result_columns(local_state, value, block_size);
                }
            } else {
                if (!it->visited) { //except: haven't visited values it's the needed result
                    add_result_columns(local_state, value, block_size);
                }
            }
        }
    } else {
        return Status::InternalError("Invalid RowRefListType!");
    }

    if (iter == hash_table_ctx.hash_table.end()) {
        source_state = SourceState::FINISHED;
    }
    if (!output_block->mem_reuse()) {
        for (int i = 0; i < left_col_len; ++i) {
            output_block->insert(vectorized::ColumnWithTypeAndName(
                    std::move(_mutable_cols[i]),
                    local_state._shared_state->_left_table_data_types[i], ""));
        }
    } else {
        _mutable_cols.clear();
    }

    return Status::OK();
}

template <bool is_intersect>
void SetSourceOperatorX<is_intersect>::add_result_columns(
        SetSourceLocalState<is_intersect>& local_state, vectorized::RowRefListWithFlags& value,
        int& block_size) {
    auto& build_col_idx = local_state._shared_state->_build_col_idx;
    auto& build_blocks = local_state._shared_state->_build_blocks;

    auto it = value.begin();
    for (auto idx = build_col_idx.begin(); idx != build_col_idx.end(); ++idx) {
        auto& column = *build_blocks[it->block_offset].get_by_position(idx->first).column;
        if (_mutable_cols[idx->second]->is_nullable() xor column.is_nullable()) {
            DCHECK(_mutable_cols[idx->second]->is_nullable());
            ((vectorized::ColumnNullable*)(_mutable_cols[idx->second].get()))
                    ->insert_from_not_nullable(column, it->row_num);

        } else {
            _mutable_cols[idx->second]->insert_from(column, it->row_num);
        }
    }
    block_size++;
}

template class SetSourceLocalState<true>;
template class SetSourceLocalState<false>;
template class SetSourceOperatorX<true>;
template class SetSourceOperatorX<false>;

} // namespace doris::pipeline
