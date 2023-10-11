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
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;
} // namespace doris

namespace doris::pipeline {

template <bool is_intersect>
SetSinkOperatorBuilder<is_intersect>::SetSinkOperatorBuilder(int32_t id, ExecNode* set_node)
        : OperatorBuilder<vectorized::VSetOperationNode<is_intersect>>(id, builder_name, set_node) {
}

template <bool is_intersect>
OperatorPtr SetSinkOperatorBuilder<is_intersect>::build_operator() {
    return std::make_shared<SetSinkOperator<is_intersect>>(this, this->_node);
}

template <bool is_intersect>
SetSinkOperator<is_intersect>::SetSinkOperator(
        OperatorBuilderBase* builder, vectorized::VSetOperationNode<is_intersect>* set_node)
        : StreamingOperator<SetSinkOperatorBuilder<is_intersect>>(builder, set_node) {}

template class SetSinkOperatorBuilder<true>;
template class SetSinkOperatorBuilder<false>;
template class SetSinkOperator<true>;
template class SetSinkOperator<false>;

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::sink(RuntimeState* state, vectorized::Block* in_block,
                                            SourceState source_state) {
    constexpr static auto BUILD_BLOCK_MAX_SIZE = 4 * 1024UL * 1024UL * 1024UL;
    RETURN_IF_CANCELLED(state);
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);

    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    auto& mem_used = local_state._shared_state->mem_used;
    auto& build_blocks = local_state._shared_state->build_blocks;
    auto& build_block_index = local_state._shared_state->build_block_index;
    auto& valid_element_in_hash_tbl = local_state._shared_state->valid_element_in_hash_tbl;

    if (in_block->rows() != 0) {
        mem_used += in_block->allocated_bytes();
        RETURN_IF_ERROR(local_state._mutable_block.merge(*in_block));
    }

    if (source_state == SourceState::FINISHED ||
        local_state._mutable_block.allocated_bytes() >= BUILD_BLOCK_MAX_SIZE) {
        build_blocks.emplace_back(local_state._mutable_block.to_block());
        RETURN_IF_ERROR(_process_build_block(local_state, build_blocks[build_block_index],
                                             build_block_index, state));
        local_state._mutable_block.clear();
        ++build_block_index;

        if (source_state == SourceState::FINISHED) {
            if constexpr (is_intersect) {
                valid_element_in_hash_tbl = 0;
            } else {
                std::visit(
                        [&](auto&& arg) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                valid_element_in_hash_tbl = arg.hash_table.size();
                            }
                        },
                        *local_state._shared_state->hash_table_variants);
            }
            local_state._shared_state->probe_finished_children_index[_cur_child_id] = true;
            if (_child_quantity == 1) {
                local_state._dependency->set_ready_for_read();
            }
        }
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::_process_build_block(
        SetSinkLocalState<is_intersect>& local_state, vectorized::Block& block, uint8_t offset,
        RuntimeState* state) {
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }

    vectorized::materialize_block_inplace(block);
    vectorized::ColumnRawPtrs raw_ptrs(_child_exprs.size());
    RETURN_IF_ERROR(_extract_build_column(local_state, block, raw_ptrs));

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    vectorized::HashTableBuildX<HashTableCtxType, is_intersect>
                            hash_table_build_process(rows, raw_ptrs, offset, state);
                    static_cast<void>(hash_table_build_process(local_state, arg));
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *local_state._shared_state->hash_table_variants);

    return Status::OK();
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::_extract_build_column(
        SetSinkLocalState<is_intersect>& local_state, vectorized::Block& block,
        vectorized::ColumnRawPtrs& raw_ptrs) {
    for (size_t i = 0; i < _child_exprs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_child_exprs[i]->execute(&block, &result_col_id));

        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        auto column = block.get_by_position(result_col_id).column.get();

        if (auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column)) {
            auto& col_nested = nullable->get_nested_column();
            if (local_state._shared_state->build_not_ignore_null[i]) {
                raw_ptrs[i] = nullable;
            } else {
                raw_ptrs[i] = &col_nested;
            }

        } else {
            raw_ptrs[i] = column;
        }
        DCHECK_GE(result_col_id, 0);
        local_state._shared_state->build_col_idx.insert({result_col_id, i});
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkLocalState<is_intersect>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<SetDependency>::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    _build_timer = ADD_TIMER(_profile, "BuildTime");

    Parent& parent = _parent->cast<Parent>();
    _child_exprs.resize(parent._child_exprs.size());
    for (size_t i = 0; i < _child_exprs.size(); i++) {
        RETURN_IF_ERROR(parent._child_exprs[i]->clone(state, _child_exprs[i]));
    }

    _shared_state->child_quantity = parent._child_quantity;
    _shared_state->probe_finished_children_index.assign(parent._child_quantity, false);

    auto& child_exprs_lists = _shared_state->child_exprs_lists;
    DCHECK(child_exprs_lists.size() == 0 || child_exprs_lists.size() == parent._child_quantity);
    if (child_exprs_lists.size() == 0) {
        child_exprs_lists.resize(parent._child_quantity);
    }
    child_exprs_lists[parent._cur_child_id] = _child_exprs;

    _shared_state->hash_table_variants = std::make_unique<vectorized::HashTableVariants>();

    for (int i = 0; i < child_exprs_lists[0].size(); ++i) {
        const auto& ctx = child_exprs_lists[0][i];
        _shared_state->build_not_ignore_null.push_back(ctx->root()->is_nullable());
    }
    _shared_state->hash_table_init();
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::init(const TPlanNode& tnode, RuntimeState* state) {
    const std::vector<std::vector<TExpr>>* result_texpr_lists;

    // Create result_expr_ctx_lists_ from thrift exprs.
    if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
        result_texpr_lists = &(tnode.intersect_node.result_expr_lists);
        _child_quantity = tnode.intersect_node.result_expr_lists.size();
    } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
        result_texpr_lists = &(tnode.except_node.result_expr_lists);
        _child_quantity = tnode.except_node.result_expr_lists.size();
    } else {
        return Status::NotSupported("Not Implemented, Check The Operation Node.");
    }

    const auto& texpr = (*result_texpr_lists)[_cur_child_id];
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(texpr, _child_exprs));

    return Status::OK();
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    return vectorized::VExpr::prepare(_child_exprs, state, _child_x->row_desc());
}

template <bool is_intersect>
Status SetSinkOperatorX<is_intersect>::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    return vectorized::VExpr::open(_child_exprs, state);
}

template class SetSinkLocalState<true>;
template class SetSinkLocalState<false>;
template class SetSinkOperatorX<true>;
template class SetSinkOperatorX<false>;

} // namespace doris::pipeline
