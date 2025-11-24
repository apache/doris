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
#include <type_traits>

#include "common/status.h"
#include "pipeline/exec/operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
template <bool is_intersect>
Status SetSourceLocalState<is_intersect>::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _get_data_timer = ADD_TIMER(custom_profile(), "GetDataTime");
    _filter_timer = ADD_TIMER(custom_profile(), "FilterTime");
    _shared_state->probe_finished_children_dependency.resize(
            _parent->cast<SetSourceOperatorX<is_intersect>>()._child_quantity, nullptr);
    return Status::OK();
}

template <bool is_intersect>
Status SetSourceLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& child_exprs_lists = _shared_state->child_exprs_lists;

    auto output_data_types = vectorized::VectorizedUtils::get_data_types(
            _parent->cast<SetSourceOperatorX<is_intersect>>().row_descriptor());
    auto column_nums = child_exprs_lists[0].size();
    DCHECK_EQ(output_data_types.size(), column_nums)
            << output_data_types.size() << " " << column_nums;
    // the nullable is not depend on child, it's should use _row_descriptor from FE plan
    // some case all not nullable column from children, but maybe need output nullable.
    std::vector<bool> nullable_flags(column_nums, false);
    for (int i = 0; i < column_nums; ++i) {
        nullable_flags[i] = output_data_types[i]->is_nullable();
        if (nullable_flags[i] != _shared_state->build_not_ignore_null[i]) {
            return Status::InternalError(
                    "SET operator expects a nullalbe : {} column in column {}, but the computed "
                    "output is a nullable : {} column",
                    nullable_flags[i], i, _shared_state->build_not_ignore_null[i]);
        }
    }

    _left_table_data_types.clear();
    for (int i = 0; i < child_exprs_lists[0].size(); ++i) {
        const auto& ctx = child_exprs_lists[0][i];
        _left_table_data_types.push_back(nullable_flags[i] ? make_nullable(ctx->root()->data_type())
                                                           : ctx->root()->data_type());
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetSourceOperatorX<is_intersect>::get_block(RuntimeState* state, vectorized::Block* block,
                                                   bool* eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);

    _create_mutable_cols(local_state, block);
    {
        SCOPED_TIMER(local_state._get_data_timer);
        RETURN_IF_ERROR(std::visit(
                [&](auto&& arg) -> Status {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        return _get_data_in_hashtable<HashTableCtxType>(local_state, arg, block,
                                                                        state->batch_size(), eos);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                        __builtin_unreachable();
                    }
                },
                local_state._shared_state->hash_table_variants->method_variant));
    }
    {
        SCOPED_TIMER(local_state._filter_timer);
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, block,
                                                               block->columns()));
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}

template <bool is_intersect>
void SetSourceOperatorX<is_intersect>::_create_mutable_cols(
        SetSourceLocalState<is_intersect>& local_state, vectorized::Block* output_block) {
    local_state._mutable_cols.resize(local_state._left_table_data_types.size());
    bool mem_reuse = output_block->mem_reuse();

    for (int i = 0; i < local_state._left_table_data_types.size(); ++i) {
        if (mem_reuse) {
            local_state._mutable_cols[i] =
                    std::move(*output_block->get_by_position(i).column).mutate();
        } else {
            local_state._mutable_cols[i] = (local_state._left_table_data_types[i]->create_column());
        }
    }
}

template <bool is_intersect>
template <typename HashTableContext>
Status SetSourceOperatorX<is_intersect>::_get_data_in_hashtable(
        SetSourceLocalState<is_intersect>& local_state, HashTableContext& hash_table_ctx,
        vectorized::Block* output_block, const int batch_size, bool* eos) {
    size_t left_col_len = local_state._left_table_data_types.size();
    hash_table_ctx.init_iterator();
    local_state._result_indexs.clear();
    local_state._result_indexs.reserve(batch_size);

    auto add_result = [&local_state](auto value) {
        auto* it = &value;
        if constexpr (is_intersect) {
            if (it->visited) { //intersected: have done probe, so visited values it's the result
                local_state._result_indexs.push_back(value.row_num);
            }
        } else {
            if (!it->visited) { //except: haven't visited values it's the needed result
                local_state._result_indexs.push_back(value.row_num);
            }
        }
    };

    auto& iter = hash_table_ctx.begin;
    while (iter != hash_table_ctx.end && local_state._result_indexs.size() < batch_size) {
        add_result(iter.get_second());
        ++iter;
    }

    *eos = iter == hash_table_ctx.end;

    local_state._add_result_columns();

    if (!output_block->mem_reuse()) {
        for (int i = 0; i < left_col_len; ++i) {
            output_block->insert(
                    vectorized::ColumnWithTypeAndName(std::move(local_state._mutable_cols[i]),
                                                      local_state._left_table_data_types[i], ""));
        }
    } else {
        local_state._mutable_cols.clear();
    }

    return Status::OK();
}

template <bool is_intersect>
void SetSourceLocalState<is_intersect>::_add_result_columns() {
    auto& build_col_idx = _shared_state->build_col_idx;
    auto& build_block = _shared_state->build_block;

    for (auto& idx : build_col_idx) {
        const auto& column = *build_block.get_by_position(idx.second).column;
        column.append_data_by_selector(_mutable_cols[idx.first], _result_indexs);
    }
}
template class SetSourceLocalState<true>;
template class SetSourceLocalState<false>;
template class SetSourceOperatorX<true>;
template class SetSourceOperatorX<false>;

} // namespace doris::pipeline
