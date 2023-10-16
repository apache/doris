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

#include "vec/exec/vset_operation_node.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include <array>
#include <atomic>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/telemetry/telemetry.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_table_set_build.h"
#include "vec/common/hash_table/hash_table_set_probe.h"
#include "vec/common/uint128.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/materialize_block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/join/join_op.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;

namespace vectorized {

template <bool is_intersect>
VSetOperationNode<is_intersect>::VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode,
                                                   const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _valid_element_in_hash_tbl(0),
          _mem_used(0),
          _build_finished(false) {
    _hash_table_variants = std::make_unique<HashTableVariants>();
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::release_resource(RuntimeState* state) {
    release_mem();
    ExecNode::release_resource(state);
}
template <bool is_intersect>
Status VSetOperationNode<is_intersect>::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    const std::vector<std::vector<::doris::TExpr>>* result_texpr_lists;

    // Create result_expr_ctx_lists_ from thrift exprs.
    if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
        result_texpr_lists = &(tnode.intersect_node.result_expr_lists);
    } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
        result_texpr_lists = &(tnode.except_node.result_expr_lists);
    } else {
        return Status::NotSupported("Not Implemented, Check The Operation Node.");
    }

    for (auto& texprs : *result_texpr_lists) {
        VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(texprs, ctxs));
        _child_expr_lists.push_back(ctxs);
    }

    _probe_finished_children_index.assign(_child_expr_lists.size(), false);

    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::alloc_resource(RuntimeState* state) {
    SCOPED_TIMER(_exec_timer);
    // open result expr lists.
    for (const VExprContextSPtrs& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }
    // Add the if check only for compatible with old optimiser
    if (_child_expr_lists.size() > 1) {
        _probe_columns.resize(_child_expr_lists[1].size());
    }
    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));

    // TODO: build the hash table in a thread to open other children asynchronously.
    RETURN_IF_ERROR(hash_table_build(state));
    bool eos = false;
    Status st = Status::OK();
    for (int i = 1; i < _children.size(); ++i) {
        RETURN_IF_ERROR(child(i)->open(state));
        eos = false;

        while (!eos) {
            release_block_memory(_probe_block, i);
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(child(i)->get_next_after_projects(
                    state, &_probe_block, &eos,
                    std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                      ExecNode::get_next,
                              _children[i], std::placeholders::_1, std::placeholders::_2,
                              std::placeholders::_3)));
            RETURN_IF_ERROR(sink_probe(state, i, &_probe_block, eos));
        }
    }
    return st;
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::get_next(RuntimeState* state, Block* output_block,
                                                 bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return pull(state, output_block, eos);
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_TIMER(_exec_timer);
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");
    _pull_timer = ADD_TIMER(runtime_profile(), "PullTime");

    // Prepare result expr lists.
    vector<bool> nullable_flags;
    nullable_flags.resize(_child_expr_lists[0].size(), false);
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        for (int j = 0; j < _child_expr_lists[i].size(); ++j) {
            nullable_flags[j] = nullable_flags[j] || _child_expr_lists[i][j]->root()->is_nullable();
        }
        RETURN_IF_ERROR(VExpr::prepare(_child_expr_lists[i], state, child(i)->row_desc()));
    }
    for (int i = 0; i < _child_expr_lists[0].size(); ++i) {
        const auto& ctx = _child_expr_lists[0][i];
        _build_not_ignore_null.push_back(ctx->root()->is_nullable());
        _left_table_data_types.push_back(nullable_flags[i] ? make_nullable(ctx->root()->data_type())
                                                           : ctx->root()->data_type());
    }
    hash_table_init();

    return Status::OK();
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::hash_table_init() {
    if (_child_expr_lists[0].size() == 1 && (!_build_not_ignore_null[0])) {
        // Single column optimization
        switch (_child_expr_lists[0][0]->root()->result_type()) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            _hash_table_variants->emplace<I8HashTableContext<RowRefListWithFlags>>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants->emplace<I16HashTableContext<RowRefListWithFlags>>();
            break;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
        case TYPE_DECIMAL32:
            _hash_table_variants->emplace<I32HashTableContext<RowRefListWithFlags>>();
            break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATETIME:
        case TYPE_DATE:
        case TYPE_DECIMAL64:
        case TYPE_DATETIMEV2:
            _hash_table_variants->emplace<I64HashTableContext<RowRefListWithFlags>>();
            break;
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL128I:
            _hash_table_variants->emplace<I128HashTableContext<RowRefListWithFlags>>();
            break;
        default:
            _hash_table_variants->emplace<SerializedHashTableContext<RowRefListWithFlags>>();
        }
        return;
    }
    if (!try_get_hash_map_context_fixed<JoinFixedHashMap, HashCRC32, RowRefListWithFlags>(
                *_hash_table_variants, _child_expr_lists[0])) {
        _hash_table_variants->emplace<SerializedHashTableContext<RowRefListWithFlags>>();
    }
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::sink(RuntimeState* state, Block* block, bool eos) {
    SCOPED_TIMER(_exec_timer);

    if (block->rows() != 0) {
        _mem_used += block->allocated_bytes();
        RETURN_IF_ERROR(_mutable_block.merge(*block));
    }

    if (block->rows() != 0) {
        if (_build_block.empty()) {
            RETURN_IF_ERROR(_mutable_block.merge(*(block->create_same_struct_block(0, false))));
        }
        RETURN_IF_ERROR(_mutable_block.merge(*block));
        if (_mutable_block.rows() > std::numeric_limits<uint32_t>::max()) {
            return Status::NotSupported(
                    "Hash join do not support build table rows"
                    " over:" +
                    std::to_string(std::numeric_limits<uint32_t>::max()));
        }
    }

    if (eos) {
        if (!_mutable_block.empty()) {
            _build_block = _mutable_block.to_block();
        }
        RETURN_IF_ERROR(process_build_block(_build_block, state));
        _mutable_block.clear();

        if constexpr (is_intersect) {
            _valid_element_in_hash_tbl = 0;
        } else {
            std::visit(
                    [&](auto&& arg) {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            _valid_element_in_hash_tbl = arg.hash_table->size();
                        }
                    },
                    *_hash_table_variants);
        }
        _build_finished = true;
        _can_read = _children.size() == 1;
    }
    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::pull(RuntimeState* state, Block* output_block, bool* eos) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_pull_timer);
    create_mutable_cols(output_block);
    auto st = std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return get_data_in_hashtable<HashTableCtxType>(arg, output_block,
                                                                   state->batch_size(), eos);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *_hash_table_variants);
    RETURN_IF_ERROR(st);
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    reached_limit(output_block, eos);
    return Status::OK();
}

//build a hash table from child(0)
template <bool is_intersect>
Status VSetOperationNode<is_intersect>::hash_table_build(RuntimeState* state) {
    SCOPED_TIMER(_build_timer);
    RETURN_IF_ERROR(child(0)->open(state));
    Block block;
    bool eos = false;
    while (!eos) {
        block.clear_column_data();
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(0)->get_next_after_projects(
                state, &block, &eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));
        if (eos) {
            static_cast<void>(child(0)->close(state));
        }
        static_cast<void>(sink(state, &block, eos));
    }

    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::process_build_block(Block& block, RuntimeState* state) {
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }

    vectorized::materialize_block_inplace(block);
    ColumnRawPtrs raw_ptrs(_child_expr_lists[0].size());
    RETURN_IF_ERROR(extract_build_column(block, raw_ptrs));
    auto st = Status::OK();
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    HashTableBuild<HashTableCtxType, is_intersect> hash_table_build_process(
                            this, rows, raw_ptrs, state);
                    st = hash_table_build_process(arg, _arena);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *_hash_table_variants);

    return Status::OK();
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::add_result_columns(RowRefListWithFlags& value,
                                                         int& block_size) {
    auto it = value.begin();
    for (auto idx = _build_col_idx.begin(); idx != _build_col_idx.end(); ++idx) {
        const auto& column = *_build_block.get_by_position(idx->first).column;
        if (_mutable_cols[idx->second]->is_nullable() ^ column.is_nullable()) {
            DCHECK(_mutable_cols[idx->second]->is_nullable());
            ((ColumnNullable*)(_mutable_cols[idx->second].get()))
                    ->insert_from_not_nullable(column, it->row_num);

        } else {
            _mutable_cols[idx->second]->insert_from(column, it->row_num);
        }
    }
    block_size++;
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::sink_probe(RuntimeState* state, int child_id, Block* block,
                                                   bool eos) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_probe_timer);
    CHECK(_build_finished) << "cannot sink probe data before build finished";
    if (child_id > 1) {
        CHECK(_probe_finished_children_index[child_id - 1])
                << fmt::format("child with id: {} should be probed first", child_id);
    }
    auto probe_rows = block->rows();
    if (probe_rows > 0) {
        RETURN_IF_ERROR(extract_probe_column(*block, _probe_columns, child_id));
        RETURN_IF_ERROR(std::visit(
                [&](auto&& arg) -> Status {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        HashTableProbe<HashTableCtxType, is_intersect> process_hashtable_ctx(
                                this, probe_rows);
                        return process_hashtable_ctx.mark_data_in_hashtable(arg);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                },
                *_hash_table_variants));
    }

    if (eos) {
        _finalize_probe(child_id);
    }
    return Status::OK();
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::_finalize_probe(int child_id) {
    if (child_id != (_children.size() - 1)) {
        refresh_hash_table();
        if constexpr (is_intersect) {
            _valid_element_in_hash_tbl = 0;
        } else {
            std::visit(
                    [&](auto&& arg) {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            _valid_element_in_hash_tbl = arg.hash_table->size();
                        }
                    },
                    *_hash_table_variants);
        }
        _probe_columns.resize(_child_expr_lists[child_id + 1].size());
    } else {
        _can_read = true;
    }
    _probe_finished_children_index[child_id] = true;
}

template <bool is_intersect>
bool VSetOperationNode<is_intersect>::is_child_finished(int child_id) const {
    if (child_id == 0) {
        return _build_finished;
    }

    return _probe_finished_children_index[child_id];
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::extract_build_column(Block& block,
                                                             ColumnRawPtrs& raw_ptrs) {
    for (size_t i = 0; i < _child_expr_lists[0].size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_child_expr_lists[0][i]->execute(&block, &result_col_id));

        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        auto column = block.get_by_position(result_col_id).column.get();

        if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            auto& col_nested = nullable->get_nested_column();
            if (_build_not_ignore_null[i]) {
                raw_ptrs[i] = nullable;
            } else {
                raw_ptrs[i] = &col_nested;
            }

        } else {
            raw_ptrs[i] = column;
        }
        DCHECK_GE(result_col_id, 0);
        _build_col_idx.insert({result_col_id, i});
    }
    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::extract_probe_column(Block& block, ColumnRawPtrs& raw_ptrs,
                                                             int child_id) {
    for (size_t i = 0; i < _child_expr_lists[child_id].size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_child_expr_lists[child_id][i]->execute(&block, &result_col_id));

        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        auto column = block.get_by_position(result_col_id).column.get();

        if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            auto& col_nested = nullable->get_nested_column();
            if (_build_not_ignore_null[i]) { //same as build column
                raw_ptrs[i] = nullable;
            } else {
                raw_ptrs[i] = &col_nested;
            }

        } else {
            if (i == 0) {
                LOG(WARNING) << "=========1 " << _build_not_ignore_null[i];
            }
            if (_build_not_ignore_null[i]) {
                auto column_ptr = make_nullable(block.get_by_position(result_col_id).column, false);
                _probe_column_inserted_id.emplace_back(block.columns());
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
void VSetOperationNode<is_intersect>::create_mutable_cols(Block* output_block) {
    _mutable_cols.resize(_left_table_data_types.size());
    bool mem_reuse = output_block->mem_reuse();

    for (int i = 0; i < _left_table_data_types.size(); ++i) {
        if (mem_reuse) {
            _mutable_cols[i] = (std::move(*output_block->get_by_position(i).column).mutate());
        } else {
            _mutable_cols[i] = (_left_table_data_types[i]->create_column());
        }
    }
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::debug_string(int indentation_level,
                                                   std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << " _child_expr_lists=[";
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        *out << VExpr::debug_string(_child_expr_lists[i]) << ", ";
    }
    *out << "] \n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")" << std::endl;
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::release_mem() {
    _hash_table_variants = nullptr;
    _probe_block.clear();
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::refresh_hash_table() {
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    if constexpr (std::is_same_v<typename HashTableCtxType::Mapped,
                                                 RowRefListWithFlags>) {
                        auto tmp_hash_table =
                                std::make_shared<typename HashTableCtxType::HashMapType>();
                        bool is_need_shrink =
                                arg.hash_table->should_be_shrink(_valid_element_in_hash_tbl);
                        if (is_intersect || is_need_shrink) {
                            tmp_hash_table->init_buf_size(
                                    _valid_element_in_hash_tbl / arg.hash_table->get_factor() + 1);
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
                                make_bool_variant(is_need_shrink));

                        arg.reset();
                        if (is_intersect || is_need_shrink) {
                            arg.hash_table = std::move(tmp_hash_table);
                        }
                    } else {
                        LOG(FATAL) << "FATAL: Invalid RowRefList";
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *_hash_table_variants);
}

template <bool is_intersected>
template <typename HashTableContext>
Status VSetOperationNode<is_intersected>::get_data_in_hashtable(HashTableContext& hash_table_ctx,
                                                                Block* output_block,
                                                                const int batch_size, bool* eos) {
    hash_table_ctx.init_iterator();
    int left_col_len = _left_table_data_types.size();
    auto& iter = hash_table_ctx.iterator;
    auto block_size = 0;

    if constexpr (std::is_same_v<typename HashTableContext::Mapped, RowRefListWithFlags>) {
        for (; iter != hash_table_ctx.hash_table->end() && block_size < batch_size; ++iter) {
            auto& value = iter->get_second();
            auto it = value.begin();
            if constexpr (is_intersected) {
                if (it->visited) { //intersected: have done probe, so visited values it's the result
                    add_result_columns(value, block_size);
                }
            } else {
                if (!it->visited) { //except: haven't visited values it's the needed result
                    add_result_columns(value, block_size);
                }
            }
        }
    } else {
        return Status::InternalError("Invalid RowRefListType!");
    }

    *eos = iter == hash_table_ctx.hash_table->end();
    if (!output_block->mem_reuse()) {
        for (int i = 0; i < left_col_len; ++i) {
            output_block->insert(ColumnWithTypeAndName(std::move(_mutable_cols[i]),
                                                       _left_table_data_types[i], ""));
        }
    } else {
        _mutable_cols.clear();
    }

    return Status::OK();
}

template class VSetOperationNode<true>;
template class VSetOperationNode<false>;

} // namespace vectorized
} // namespace doris
