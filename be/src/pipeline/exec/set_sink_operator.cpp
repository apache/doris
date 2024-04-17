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
        : StreamingOperator<vectorized::VSetOperationNode<is_intersect>>(builder, set_node) {}

template class SetSinkOperatorBuilder<true>;
template class SetSinkOperatorBuilder<false>;
template class SetSinkOperator<true>;
template class SetSinkOperator<false>;

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
        RETURN_IF_ERROR(local_state._mutable_block.merge(*in_block));

        if (local_state._mutable_block.rows() > std::numeric_limits<uint32_t>::max()) {
            return Status::NotSupported("set operator do not support build table rows over:" +
                                        std::to_string(std::numeric_limits<uint32_t>::max()));
        }
    }

    if (eos || local_state._mutable_block.allocated_bytes() >= BUILD_BLOCK_MAX_SIZE) {
        build_block = local_state._mutable_block.to_block();
        RETURN_IF_ERROR(_process_build_block(local_state, build_block, state));
        local_state._mutable_block.clear();

        if (eos) {
            if constexpr (is_intersect) {
                valid_element_in_hash_tbl = 0;
            } else {
                std::visit(
                        [&](auto&& arg) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                valid_element_in_hash_tbl = arg.hash_table->size();
                            }
                        },
                        *local_state._shared_state->hash_table_variants);
            }
            local_state._shared_state->probe_finished_children_dependency[_cur_child_id + 1]
                    ->set_ready();
            if (_child_quantity == 1) {
                local_state._dependency->set_ready_to_read();
            }
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
    RETURN_IF_ERROR(_extract_build_column(local_state, block, raw_ptrs));

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    vectorized::HashTableBuild<HashTableCtxType, is_intersect>
                            hash_table_build_process(&local_state, rows, raw_ptrs, state);
                    static_cast<void>(hash_table_build_process(arg, local_state._arena));
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
        const auto* column = block.get_by_position(result_col_id).column.get();

        if (const auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column)) {
            const auto& col_nested = nullable->get_nested_column();
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
    RETURN_IF_ERROR(PipelineXSinkLocalState<SetSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _build_timer = ADD_TIMER(_profile, "BuildTime");
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
    return Status::OK();
}

template <bool is_intersect>
Status SetSinkLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(PipelineXSinkLocalState<SetSharedState>::open(state));

    auto& parent = _parent->cast<Parent>();
    DCHECK(parent._cur_child_id == 0);
    auto& child_exprs_lists = _shared_state->child_exprs_lists;

    _shared_state->hash_table_variants = std::make_unique<vectorized::SetHashTableVariants>();

    for (const auto& ctx : child_exprs_lists[parent._cur_child_id]) {
        _shared_state->build_not_ignore_null.push_back(ctx->root()->is_nullable());
    }
    _shared_state->hash_table_init();
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
