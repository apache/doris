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

#include "set_probe_sink_operator.h"

#include <glog/logging.h>

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/common/hash_table/hash_table_set_probe.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

template <bool is_intersect>
Status SetProbeSinkOperatorX<is_intersect>::init(const TPlanNode& tnode, RuntimeState* state) {
    DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::_name = "SET_PROBE_SINK_OPERATOR";
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
Status SetProbeSinkOperatorX<is_intersect>::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_child_exprs, state, _child->row_desc()));
    return vectorized::VExpr::open(_child_exprs, state);
}

template <bool is_intersect>
Status SetProbeSinkOperatorX<is_intersect>::sink(RuntimeState* state, vectorized::Block* in_block,
                                                 bool eos) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    auto probe_rows = in_block->rows();
    if (probe_rows > 0) {
        RETURN_IF_ERROR(_extract_probe_column(local_state, *in_block, local_state._probe_columns,
                                              _cur_child_id));
        RETURN_IF_ERROR(std::visit(
                [&](auto&& arg) -> Status {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        vectorized::HashTableProbe<HashTableCtxType, is_intersect>
                                process_hashtable_ctx(&local_state, probe_rows);
                        return process_hashtable_ctx.mark_data_in_hashtable(arg);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                        __builtin_unreachable();
                    }
                },
                *local_state._shared_state->hash_table_variants));
    }

    if (eos) {
        _finalize_probe(local_state);
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetProbeSinkLocalState<is_intersect>::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    Parent& parent = _parent->cast<Parent>();
    _shared_state->probe_finished_children_dependency[parent._cur_child_id] = _dependency;
    _dependency->block();

    _child_exprs.resize(parent._child_exprs.size());
    for (size_t i = 0; i < _child_exprs.size(); i++) {
        RETURN_IF_ERROR(parent._child_exprs[i]->clone(state, _child_exprs[i]));
    }
    auto& child_exprs_lists = _shared_state->child_exprs_lists;
    child_exprs_lists[parent._cur_child_id] = _child_exprs;

    RETURN_IF_ERROR(_shared_state->update_build_not_ignore_null(_child_exprs));

    return Status::OK();
}

template <bool is_intersect>
Status SetProbeSinkLocalState<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    // Add the if check only for compatible with old optimiser
    if (_shared_state->child_quantity > 1) {
        _probe_columns.resize(_child_exprs.size());
    }
    return Status::OK();
}

template <bool is_intersect>
Status SetProbeSinkOperatorX<is_intersect>::_extract_probe_column(
        SetProbeSinkLocalState<is_intersect>& local_state, vectorized::Block& block,
        vectorized::ColumnRawPtrs& raw_ptrs, int child_id) {
    auto& build_not_ignore_null = local_state._shared_state->build_not_ignore_null;

    for (size_t i = 0; i < _child_exprs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_child_exprs[i]->execute(&block, &result_col_id));

        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        auto column = block.get_by_position(result_col_id).column.get();

        if (auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column)) {
            auto& col_nested = nullable->get_nested_column();
            if (build_not_ignore_null[i]) { //same as build column
                raw_ptrs[i] = nullable;
            } else {
                raw_ptrs[i] = &col_nested;
            }

        } else {
            if (build_not_ignore_null[i]) {
                auto column_ptr = make_nullable(block.get_by_position(result_col_id).column, false);
                local_state._probe_column_inserted_id.emplace_back(block.columns());
                block.insert(
                        {column_ptr, make_nullable(block.get_by_position(result_col_id).type), ""});
                column = column_ptr.get();
            }

            raw_ptrs[i] = column;
        }
    }
    return Status::OK();
}

template <bool is_intersect>
void SetProbeSinkOperatorX<is_intersect>::_finalize_probe(
        SetProbeSinkLocalState<is_intersect>& local_state) {
    auto& valid_element_in_hash_tbl = local_state._shared_state->valid_element_in_hash_tbl;
    auto& hash_table_variants = local_state._shared_state->hash_table_variants;

    if (_cur_child_id != (local_state._shared_state->child_quantity - 1)) {
        _refresh_hash_table(local_state);
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
                    *hash_table_variants);
        }
        local_state._probe_columns.resize(
                local_state._shared_state->child_exprs_lists[_cur_child_id + 1].size());
        local_state._shared_state->probe_finished_children_dependency[_cur_child_id + 1]
                ->set_ready();
    } else {
        local_state._dependency->set_ready_to_read();
    }
}

template <bool is_intersect>
void SetProbeSinkOperatorX<is_intersect>::_refresh_hash_table(
        SetProbeSinkLocalState<is_intersect>& local_state) {
    auto& valid_element_in_hash_tbl = local_state._shared_state->valid_element_in_hash_tbl;
    auto& hash_table_variants = local_state._shared_state->hash_table_variants;
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    auto tmp_hash_table =
                            std::make_shared<typename HashTableCtxType::HashMapType>();
                    bool is_need_shrink =
                            arg.hash_table->should_be_shrink(valid_element_in_hash_tbl);
                    if (is_intersect || is_need_shrink) {
                        tmp_hash_table->init_buf_size(size_t(
                                valid_element_in_hash_tbl / arg.hash_table->get_factor() + 1));
                    }

                    arg.init_iterator();
                    auto& iter = arg.iterator;
                    auto iter_end = arg.hash_table->end();
                    std::visit(
                            [&](auto is_need_shrink_const) {
                                while (iter != iter_end) {
                                    auto& mapped = iter->get_second();
                                    auto it = mapped.begin();

                                    if constexpr (is_intersect) { //intersected
                                        if (it->visited) {
                                            it->visited = false;
                                            tmp_hash_table->insert(iter->get_value());
                                        }
                                        ++iter;
                                    } else { //except
                                        if constexpr (is_need_shrink_const) {
                                            if (!it->visited) {
                                                tmp_hash_table->insert(iter->get_value());
                                            }
                                        }
                                        ++iter;
                                    }
                                }
                            },
                            vectorized::make_bool_variant(is_need_shrink));

                    arg.reset();
                    if (is_intersect || is_need_shrink) {
                        arg.hash_table = std::move(tmp_hash_table);
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                    __builtin_unreachable();
                }
            },
            *hash_table_variants);
}

template class SetProbeSinkLocalState<true>;
template class SetProbeSinkLocalState<false>;
template class SetProbeSinkOperatorX<true>;
template class SetProbeSinkOperatorX<false>;

} // namespace doris::pipeline
