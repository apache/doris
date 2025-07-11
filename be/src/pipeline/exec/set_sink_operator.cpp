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

#include "set_sink_operator.h"

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/common/hash_table/hash_table_set_build.h"
#include "vec/core/materialize_block.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

template <bool is_intersect>
Status SetSinkLocalState<is_intersect>::terminate(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    if (_terminated) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_runtime_filter_producer_helper->terminate(state));
    return Base::terminate(state);
}

template <bool is_intersect>
Status SetSinkLocalState<is_intersect>::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    if (!_terminated && _runtime_filter_producer_helper && !state->is_cancelled()) {
        try {
            RETURN_IF_ERROR(_runtime_filter_producer_helper->process(
                    state, &_shared_state->build_block, _shared_state->get_hash_table_size()));
        } catch (Exception& e) {
            return Status::InternalError(
                    "rf process meet error: {}, _terminated: {}, _finish_dependency: {}",
                    e.to_string(), _terminated,
                    _finish_dependency ? _finish_dependency->debug_string() : "null");
        }
    }

    if (_runtime_filter_producer_helper) {
        _runtime_filter_producer_helper->collect_realtime_profile(custom_profile());
    }

    return Base::close(state, exec_status);
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::sink(RuntimeState* state, vectorized::Block* in_block,
                                            bool eos) {
    constexpr static auto BUILD_BLOCK_MAX_SIZE = 4 * 1024UL * 1024UL * 1024UL;
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);

    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    auto& build_block = local_state._shared_state->build_block;
    auto& valid_element_in_hash_tbl = local_state._shared_state->valid_element_in_hash_tbl;

    if (in_block->rows() != 0) {
        {
            SCOPED_TIMER(local_state._merge_block_timer);
            RETURN_IF_ERROR(local_state._mutable_block.merge(*in_block));
        }
        if (local_state._mutable_block.rows() > std::numeric_limits<uint32_t>::max()) {
            return Status::NotSupported("set operator do not support build table rows over:" +
                                        std::to_string(std::numeric_limits<uint32_t>::max()));
        }
    }

    if (eos || local_state._mutable_block.allocated_bytes() >= BUILD_BLOCK_MAX_SIZE) {
        SCOPED_TIMER(local_state._build_timer);
        build_block = local_state._mutable_block.to_block();
        RETURN_IF_ERROR(_process_build_block(local_state, build_block, state));
        local_state._mutable_block.clear();

        if (eos) {
            uint64_t hash_table_size = local_state._shared_state->get_hash_table_size();
            valid_element_in_hash_tbl = is_intersect ? 0 : hash_table_size;

            local_state._shared_state->probe_finished_children_dependency[_cur_child_id + 1]
                    ->set_ready();
            DCHECK_GT(_child_quantity, 1);
            RETURN_IF_ERROR(local_state._runtime_filter_producer_helper->send_filter_size(
                    state, hash_table_size, local_state._finish_dependency));
        }
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::_process_build_block(
        SetSinkLocalState<is_intersect>& local_state, vectorized::Block& block,
        RuntimeState* state) {
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }

    vectorized::materialize_block_inplace(block);
    vectorized::ColumnRawPtrs raw_ptrs(_child_exprs.size());
    RETURN_IF_ERROR(_extract_build_column(local_state, block, raw_ptrs, rows));
    auto st = Status::OK();
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    vectorized::HashTableBuild<HashTableCtxType, is_intersect>
                            hash_table_build_process(&local_state, rows, raw_ptrs, state);
                    st = hash_table_build_process(arg, local_state._arena);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                    __builtin_unreachable();
                }
            },
            local_state._shared_state->hash_table_variants->method_variant);

    return st;
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::_extract_build_column(
        SetSinkLocalState<is_intersect>& local_state, vectorized::Block& block,
        vectorized::ColumnRawPtrs& raw_ptrs, size_t& rows) {
    // use local state child exprs
    auto& child_expr = local_state._child_exprs;
    std::vector<int> result_locs(child_expr.size(), -1);
    bool is_all_const = true;

    for (size_t i = 0; i < child_expr.size(); ++i) {
        RETURN_IF_ERROR(child_expr[i]->execute(&block, &result_locs[i]));
        is_all_const &= is_column_const(*block.get_by_position(result_locs[i]).column);
    }
    rows = is_all_const ? 1 : rows;

    for (size_t i = 0; i < child_expr.size(); ++i) {
        size_t result_col_id = result_locs[i];

        if (is_all_const) {
            block.get_by_position(result_col_id).column =
                    assert_cast<const vectorized::ColumnConst&>(
                            *block.get_by_position(result_col_id).column)
                            .get_data_column_ptr();
        } else {
            block.get_by_position(result_col_id).column =
                    block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        }
        // Do make nullable should not change the origin column and type in origin block
        // which may cause coredump problem
        if (local_state._shared_state->build_not_ignore_null[i]) {
            auto column_ptr = make_nullable(block.get_by_position(result_col_id).column, false);
            block.insert(
                    {column_ptr, make_nullable(block.get_by_position(result_col_id).type), ""});
            result_col_id = block.columns() - 1;
        }

        raw_ptrs[i] = block.get_by_position(result_col_id).column.get();
        DCHECK_GE(result_col_id, 0);
        local_state._shared_state->build_col_idx.insert({i, result_col_id});
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkLocalState<is_intersect>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<SetSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _merge_block_timer = ADD_TIMER(custom_profile(), "MergeBlocksTime");
    _build_timer = ADD_TIMER(custom_profile(), "BuildTime");
    auto& parent = _parent->cast<Parent>();
    _shared_state->probe_finished_children_dependency[parent._cur_child_id] = _dependency;
    DCHECK(parent._cur_child_id == 0);
    auto& child_exprs_lists = _shared_state->child_exprs_lists;
    DCHECK(child_exprs_lists.empty() || child_exprs_lists.size() == parent._child_quantity);
    if (child_exprs_lists.empty()) {
        child_exprs_lists.resize(parent._child_quantity);
    }
    _child_exprs.resize(parent._child_exprs.size());
    for (size_t i = 0; i < _child_exprs.size(); i++) {
        RETURN_IF_ERROR(parent._child_exprs[i]->clone(state, _child_exprs[i]));
    }
    child_exprs_lists[parent._cur_child_id] = _child_exprs;
    _shared_state->child_quantity = parent._child_quantity;
    _shared_state->build_not_ignore_null.resize(_child_exprs.size());

    RETURN_IF_ERROR(_shared_state->update_build_not_ignore_null(_child_exprs));
    _runtime_filter_producer_helper = std::make_shared<RuntimeFilterProducerHelperSet>();
    RETURN_IF_ERROR(_runtime_filter_producer_helper->init(state, _child_exprs,
                                                          parent._runtime_filter_descs));
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(PipelineXSinkLocalState<SetSharedState>::open(state));

    auto& parent = _parent->cast<Parent>();
    DCHECK(parent._cur_child_id == 0);
    _shared_state->hash_table_variants = std::make_unique<SetDataVariants>();
    RETURN_IF_ERROR(_shared_state->hash_table_init());
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::init(const TPlanNode& tnode, RuntimeState* state) {
    Base::_name = "SET_SINK_OPERATOR";
    const std::vector<std::vector<TExpr>>* result_texpr_lists;

    // Create result_expr_ctx_lists_ from thrift exprs.
    if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
        result_texpr_lists = &(tnode.intersect_node.result_expr_lists);
    } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
        result_texpr_lists = &(tnode.except_node.result_expr_lists);
    } else {
        return Status::NotSupported("Not Implemented, Check The Operation Node.");
    }

    const auto& texpr = (*result_texpr_lists)[_cur_child_id];
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texpr, _child_exprs));

    return Status::OK();
}

template <bool is_intersect>
size_t SetSinkOperatorX<is_intersect>::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    size_t size_to_reserve = std::visit(
            [&](auto&& arg) -> size_t {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<HashTableCtxType, std::monostate>) {
                    return 0;
                } else {
                    return arg.hash_table->estimate_memory(state->batch_size());
                }
            },
            local_state._shared_state->hash_table_variants->method_variant);

    size_to_reserve += local_state._mutable_block.allocated_bytes();
    for (auto& _child_expr : _child_exprs) {
        size_to_reserve += _child_expr->root()->estimate_memory(state->batch_size());
    }
    return size_to_reserve;
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_child_exprs, state, _child->row_desc()));
    return vectorized::VExpr::open(_child_exprs, state);
}

template class SetSinkLocalState<true>;
template class SetSinkLocalState<false>;
template class SetSinkOperatorX<true>;
template class SetSinkOperatorX<false>;

} // namespace doris::pipeline
