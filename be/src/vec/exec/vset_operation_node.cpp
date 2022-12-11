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

#include "util/defer_op.h"
#include "vec/exprs/vexpr.h"
namespace doris {
namespace vectorized {

//build hash table for operation node, intersect/except node
template <class HashTableContext, bool is_intersect>
struct HashTableBuild {
    HashTableBuild(int rows, ColumnRawPtrs& build_raw_ptrs,
                   VSetOperationNode<is_intersect>* operation_node, uint8_t offset)
            : _rows(rows),
              _offset(offset),
              _build_raw_ptrs(build_raw_ptrs),
              _operation_node(operation_node) {}

    Status operator()(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        int64_t old_bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();

        Defer defer {[&]() {
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            _operation_node->_mem_used += bucket_bytes - old_bucket_bytes;
        }};

        KeyGetter key_getter(_build_raw_ptrs, _operation_node->_build_key_sz, nullptr);

        if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
            hash_table_ctx.serialize_keys(_build_raw_ptrs, _rows);
            key_getter.set_serialized_keys(hash_table_ctx.keys.data());
        }

        for (size_t k = 0; k < _rows; ++k) {
            auto emplace_result = key_getter.emplace_key(hash_table_ctx.hash_table, k,
                                                         *(_operation_node->_arena));

            if (k + 1 < _rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, k + 1, *(_operation_node->_arena));
            }

            if (emplace_result.is_inserted()) { //only inserted once as the same key, others skip
                new (&emplace_result.get_mapped()) Mapped({k, _offset});
            }
        }
        return Status::OK();
    }

private:
    const int _rows;
    const uint8_t _offset;
    ColumnRawPtrs& _build_raw_ptrs;
    VSetOperationNode<is_intersect>* _operation_node;
};

template <class HashTableContext, bool is_intersected>
struct HashTableProbe {
    HashTableProbe(VSetOperationNode<is_intersected>* operation_node, ColumnRawPtrs& probe_columns,
                   int probe_rows)
            : _operation_node(operation_node),
              _probe_rows(probe_rows),
              _probe_raw_ptrs(probe_columns),
              _arena(new Arena) {}

    Status mark_data_in_hashtable(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        KeyGetter key_getter(_probe_raw_ptrs, _operation_node->_probe_key_sz, nullptr);
        if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
            if (_probe_keys.size() < _probe_rows) {
                _probe_keys.resize(_probe_rows);
            }
            size_t keys_size = _probe_raw_ptrs.size();
            for (size_t i = 0; i < _probe_rows; ++i) {
                _probe_keys[i] =
                        serialize_keys_to_pool_contiguous(i, keys_size, _probe_raw_ptrs, *_arena);
            }
            key_getter.set_serialized_keys(_probe_keys.data());
        }

        if constexpr (std::is_same_v<typename HashTableContext::Mapped, RowRefListWithFlagAtomic>) {
            for (int probe_index = 0; probe_index < _probe_rows; probe_index++) {
                auto find_result =
                        key_getter.find_key(hash_table_ctx.hash_table, probe_index, *_arena);
                if (find_result.is_found()) {
                    auto& value = find_result.get_mapped();
                    value.matched_count.fetch_add(1);
                }
            }
        } else {
            LOG(FATAL) << "Invalid RowRefListType!";
        }
        return Status::OK();
    }

private:
    VSetOperationNode<is_intersected>* _operation_node;
    const size_t _probe_rows;
    ColumnRawPtrs& _probe_raw_ptrs;
    std::unique_ptr<Arena> _arena;
    std::vector<StringRef> _probe_keys;
};

template <bool is_intersect>
VSetOperationNode<is_intersect>::VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode,
                                                   const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _mem_used(0), _build_block_index(0) {
    _hash_table_variants = std::make_unique<SetOperationHashTableVariants>();
    _arena = std::make_unique<Arena>();
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::release_resource(RuntimeState* state) {
    for (auto& exprs : _child_expr_lists) {
        VExpr::close(exprs, state);
    }
    release_mem();
}
template <bool is_intersect>
Status VSetOperationNode<is_intersect>::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VSetOperationNode<is_intersect>::close");
    return ExecNode::close(state);
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    std::vector<std::vector<::doris::TExpr>> result_texpr_lists;

    // Create result_expr_ctx_lists_ from thrift exprs.
    if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
        result_texpr_lists = tnode.intersect_node.result_expr_lists;
    } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
        result_texpr_lists = tnode.except_node.result_expr_lists;
    } else {
        return Status::NotSupported("Not Implemented, Check The Operation Node.");
    }

    for (auto& texprs : result_texpr_lists) {
        std::vector<VExprContext*> ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, texprs, &ctxs));
        _child_expr_lists.push_back(ctxs);
    }
    _finished_children_index.assign(_child_expr_lists.size(), false);

    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::alloc_resource(RuntimeState* state) {
    // open result expr lists.
    for (const std::vector<VExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }
    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VSetOperationNode<is_intersect>::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());

    RETURN_IF_ERROR(hash_table_build(state));
    Status status = Status::OK();

    const auto probe_child_count = _children.size() - 1;
    std::thread probe_threads[probe_child_count];
    std::promise<Status> probe_status[probe_child_count];

    auto probe_worker = [&](int child_id, std::promise<Status>& status) {
        auto st = [&]() -> Status {
            RETURN_IF_ERROR(child(child_id)->open(state));
            bool eos = false;
            Block probe_block;
            while (!eos) {
                RETURN_IF_CANCELLED(state);
                RETURN_IF_ERROR_AND_CHECK_SPAN(
                        child(child_id)->get_next_after_projects(state, &probe_block, &eos),
                        child(child_id)->get_next_span(), eos);

                RETURN_IF_ERROR(sink_probe(state, child_id, &probe_block, eos));
                release_block_memory(probe_block, child_id);
            }
            return finalize_probe(state, child_id);
        }();
        status.set_value(st);
    };
    for (int i = 1; i < _children.size(); ++i) {
        probe_threads[i - 1] = std::thread(probe_worker, i, std::ref(probe_status[i - 1]));
    }

    for (int i = 0; i < probe_child_count; ++i) {
        probe_threads[i].join();
        auto st = probe_status[i].get_future().get();
        DCHECK(st.ok());
        if (!st.ok()) {
            status = st;
        }
    }
    return status;
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::get_next(RuntimeState* state, Block* output_block,
                                                 bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VExceptNode::get_next");
    SCOPED_TIMER(_probe_timer);
    return pull(state, output_block, eos);
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");

    // Prepare result expr lists.
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(VExpr::prepare(_child_expr_lists[i], state, child(i)->row_desc()));
    }

    for (auto ctx : _child_expr_lists[0]) {
        _build_not_ignore_null.push_back(ctx->root()->is_nullable());
        _left_table_data_types.push_back(ctx->root()->data_type());
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
            _hash_table_variants->emplace<I8HashTableContext<RowRefListWithFlagAtomic>>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants->emplace<I16HashTableContext<RowRefListWithFlagAtomic>>();
            break;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
        case TYPE_DECIMAL32:
            _hash_table_variants->emplace<I32HashTableContext<RowRefListWithFlagAtomic>>();
            break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATETIME:
        case TYPE_DATE:
        case TYPE_DECIMAL64:
        case TYPE_DATETIMEV2:
            _hash_table_variants->emplace<I64HashTableContext<RowRefListWithFlagAtomic>>();
            break;
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL128I:
            _hash_table_variants->emplace<I128HashTableContext<RowRefListWithFlagAtomic>>();
            break;
        default:
            _hash_table_variants->emplace<SerializedHashTableContext<RowRefListWithFlagAtomic>>();
        }
        return;
    }

    bool use_fixed_key = true;
    bool has_null = false;
    int key_byte_size = 0;

    _probe_key_sz.resize(_child_expr_lists[1].size());
    _build_key_sz.resize(_child_expr_lists[0].size());
    for (int i = 0; i < _child_expr_lists[0].size(); ++i) {
        const auto vexpr = _child_expr_lists[0][i]->root();
        const auto& data_type = vexpr->data_type();

        if (!data_type->have_maximum_size_of_value()) {
            use_fixed_key = false;
            break;
        }

        auto is_null = data_type->is_nullable();
        has_null |= is_null;
        _build_key_sz[i] = data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
        _probe_key_sz[i] = _build_key_sz[i];
        key_byte_size += _probe_key_sz[i];
    }

    if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }
    if (use_fixed_key) {
        if (has_null) {
            if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants
                        ->emplace<I64FixedKeyHashTableContext<true, RowRefListWithFlagAtomic>>();
            } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                       sizeof(UInt128)) {
                _hash_table_variants
                        ->emplace<I128FixedKeyHashTableContext<true, RowRefListWithFlagAtomic>>();
            } else {
                _hash_table_variants
                        ->emplace<I256FixedKeyHashTableContext<true, RowRefListWithFlagAtomic>>();
            }
        } else {
            if (key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants
                        ->emplace<I64FixedKeyHashTableContext<false, RowRefListWithFlagAtomic>>();
            } else if (key_byte_size <= sizeof(UInt128)) {
                _hash_table_variants
                        ->emplace<I128FixedKeyHashTableContext<false, RowRefListWithFlagAtomic>>();
            } else {
                _hash_table_variants
                        ->emplace<I256FixedKeyHashTableContext<false, RowRefListWithFlagAtomic>>();
            }
        }
    } else {
        _hash_table_variants->emplace<SerializedHashTableContext<RowRefListWithFlagAtomic>>();
    }
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::sink(RuntimeState*, Block* block, bool eos) {
    constexpr static auto BUILD_BLOCK_MAX_SIZE = 4 * 1024UL * 1024UL * 1024UL;

    if (block->rows() != 0) {
        _mem_used += block->allocated_bytes();
        _mutable_block.merge(*block);
    }

    if (eos || _mutable_block.allocated_bytes() >= BUILD_BLOCK_MAX_SIZE) {
        _build_blocks.emplace_back(_mutable_block.to_block());
        RETURN_IF_ERROR(process_build_block(_build_blocks[_build_block_index], _build_block_index));
        _mutable_block.clear();
        ++_build_block_index;

        if (eos) {
            _finished_children_index[0] = true;
        }
    }
    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::pull(RuntimeState* state, Block* output_block, bool* eos) {
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
    RETURN_IF_ERROR(
            VExprContext::filter_block(_vconjunct_ctx_ptr, output_block, output_block->columns()));
    reached_limit(output_block, eos);
    return Status::OK();
}

//build a hash table from child(0)
template <bool is_intersect>
Status VSetOperationNode<is_intersect>::hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(0)->open(state));
    Block block;
    MutableBlock mutable_block(child(0)->row_desc().tuple_descriptors());

    bool eos = false;
    while (!eos) {
        block.clear_column_data();
        SCOPED_TIMER(_build_timer);
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR_AND_CHECK_SPAN(child(0)->get_next_after_projects(state, &block, &eos),
                                       child(0)->get_next_span(), eos);
        if (eos) {
            child(0)->close(state);
        }
        sink(state, &block, eos);
    }

    return Status::OK();
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::process_build_block(Block& block, uint8_t offset) {
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }

    vectorized::materialize_block_inplace(block);
    ColumnRawPtrs raw_ptrs(_child_expr_lists[0].size());
    RETURN_IF_ERROR(extract_build_column(block, raw_ptrs));

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    HashTableBuild<HashTableCtxType, is_intersect> hash_table_build_process(
                            rows, raw_ptrs, this, offset);
                    hash_table_build_process(arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *_hash_table_variants);

    return Status::OK();
}

template <bool is_intersect>
void VSetOperationNode<is_intersect>::add_result_columns(RowRefList& value, int& block_size) {
    auto it = value.begin();
    for (auto idx = _build_col_idx.begin(); idx != _build_col_idx.end(); ++idx) {
        auto& column = *_build_blocks[it->block_offset].get_by_position(idx->first).column;
        _mutable_cols[idx->second]->insert_from(column, it->row_num);
    }
    block_size++;
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::sink_probe(RuntimeState* /*state*/, int child_id,
                                                   Block* block, bool eos) {
    CHECK(_finished_children_index[0]) << "cannot sink probe data before build finished";
    auto probe_rows = block->rows();

    if (probe_rows == 0) {
        return Status::OK();
    }

    ColumnRawPtrs probe_columns(_child_expr_lists[child_id].size());
    RETURN_IF_ERROR(extract_probe_column(*block, probe_columns, child_id));

    return std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    HashTableProbe<HashTableCtxType, is_intersect> process_hashtable_ctx(
                            this, probe_columns, probe_rows);
                    return process_hashtable_ctx.mark_data_in_hashtable(arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *_hash_table_variants);
}

template <bool is_intersect>
Status VSetOperationNode<is_intersect>::finalize_probe(RuntimeState* /*state*/, int child_id) {
    std::lock_guard<std::mutex> lock(_lock);
    _finished_children_index[child_id] = true;
    _can_read = std::find(_finished_children_index.begin(), _finished_children_index.end(),
                          false) == _finished_children_index.cend();
    return Status::OK();
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
            if (_build_not_ignore_null[i])
                raw_ptrs[i] = nullable;
            else
                raw_ptrs[i] = &col_nested;

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
    if (block.rows() == 0) {
        return Status::OK();
    }

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
    _arena = nullptr;

    std::vector<Block> tmp_build_blocks;
    _build_blocks.swap(tmp_build_blocks);
}

template <bool is_intersected>
template <typename HashTableContext>
Status VSetOperationNode<is_intersected>::get_data_in_hashtable(HashTableContext& hash_table_ctx,
                                                                Block* output_block,
                                                                const int batch_size, bool* eos) {
    hash_table_ctx.init_once();
    int left_col_len = _left_table_data_types.size();
    auto& iter = hash_table_ctx.iter;
    auto block_size = 0;

    if constexpr (std::is_same_v<typename HashTableContext::Mapped, RowRefListWithFlagAtomic>) {
        const auto probe_children_count = _children.size() - 1;
        for (; iter != hash_table_ctx.hash_table.end() && block_size < batch_size; ++iter) {
            auto& value = iter->get_second();
            if constexpr (is_intersected) {
                if (value.matched_count == probe_children_count) {
                    add_result_columns(value.row_list, block_size);
                }
            } else {
                if (value.matched_count == 0) {
                    add_result_columns(value.row_list, block_size);
                }
            }
        }
    } else {
        LOG(FATAL) << "Invalid RowRefListType!";
    }

    *eos = iter == hash_table_ctx.hash_table.end();
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
