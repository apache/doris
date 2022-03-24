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

#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "vec/exprs/vexpr.h"
namespace doris {
namespace vectorized {

//build hash table for operation node, intersect/except node
template <class HashTableContext>
struct HashTableBuild {
    HashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                   VSetOperationNode* operation_node, uint8_t offset)
            : _rows(rows),
              _offset(offset),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _operation_node(operation_node) {}

    Status operator()(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        int64_t old_bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
        
        Defer defer {[&]() {
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            _operation_node->_hash_table_mem_tracker->consume(bucket_bytes - old_bucket_bytes);
            _operation_node->_mem_used += bucket_bytes - old_bucket_bytes;
        }};

        KeyGetter key_getter(_build_raw_ptrs, _operation_node->_build_key_sz, nullptr);

        for (size_t k = 0; k < _rows; ++k) {
            auto emplace_result =
                    key_getter.emplace_key(hash_table_ctx.hash_table, k, _operation_node->_arena);

            if (k + 1 < _rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, k + 1, _operation_node->_arena);
            }

            if (emplace_result.is_inserted()) { //only inserted once as the same key, others skip
                new (&emplace_result.get_mapped()) Mapped({k, _offset});
                _operation_node->_valid_element_in_hash_tbl++;
            }
        }
        return Status::OK();
    }

private:
    const int _rows;
    const uint8_t _offset;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    VSetOperationNode* _operation_node;
};

VSetOperationNode::VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _valid_element_in_hash_tbl(0),
          _mem_used(0),
          _probe_index(-1),
          _probe_rows(0) {}

Status VSetOperationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    for (auto& exprs : _child_expr_lists) {
        VExpr::close(exprs, state);
    }
    _hash_table_mem_tracker->release(_mem_used);
    return ExecNode::close(state);
}

Status VSetOperationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
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

    return Status::OK();
}

Status VSetOperationNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    // open result expr lists.
    for (const std::vector<VExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }
    RETURN_IF_ERROR(hash_table_build(state));
    return Status::OK();
}

Status VSetOperationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _hash_table_mem_tracker = MemTracker::create_virtual_tracker(-1, "VSetOperationNode:HashTable");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");

    // Prepare result expr lists.
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(VExpr::prepare(_child_expr_lists[i], state, child(i)->row_desc(),
                                       expr_mem_tracker()));
    }

    for (auto ctx : _child_expr_lists[0]) {
        _build_not_ignore_null.push_back(ctx->root()->is_nullable());
        _left_table_data_types.push_back(ctx->root()->data_type());
    }
    hash_table_init();

    return Status::OK();
}

void VSetOperationNode::hash_table_init() {
    if (_child_expr_lists[0].size() == 1 && (!_build_not_ignore_null[0])) {
        // Single column optimization
        switch (_child_expr_lists[0][0]->root()->result_type()) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            _hash_table_variants.emplace<I8HashTableContext>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants.emplace<I16HashTableContext>();
            break;
        case TYPE_INT:
        case TYPE_FLOAT:
            _hash_table_variants.emplace<I32HashTableContext>();
            break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATETIME:
        case TYPE_DATE:
            _hash_table_variants.emplace<I64HashTableContext>();
            break;
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
            _hash_table_variants.emplace<I128HashTableContext>();
            break;
        default:
            _hash_table_variants.emplace<SerializedHashTableContext>();
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
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<true>>();
            } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                       sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<true>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<true>>();
            }
        } else {
            if (key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<false>>();
            } else if (key_byte_size <= sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<false>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<false>>();
            }
        }
    } else {
        _hash_table_variants.emplace<SerializedHashTableContext>();
    }
}

//build a hash table from child(0)
Status VSetOperationNode::hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(0)->open(state));
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_ERR_CB(
                "Vec Set Operation Node, while constructing the hash table");
    Block block;
    MutableBlock mutable_block(child(0)->row_desc().tuple_descriptors());

    uint8_t index = 0;
    int64_t last_mem_used = 0;
    bool eos = false;
    while (!eos) {
        block.clear_column_data();
        SCOPED_TIMER(_build_timer);
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(0)->get_next(state, &block, &eos));

        size_t allocated_bytes = block.allocated_bytes();
        _hash_table_mem_tracker->consume(allocated_bytes);
        _mem_used += allocated_bytes;

        if (block.rows() != 0) { mutable_block.merge(block); }

        // make one block for each 4 gigabytes
        constexpr static auto BUILD_BLOCK_MAX_SIZE =  4 * 1024UL * 1024UL * 1024UL;
        if (_mem_used - last_mem_used > BUILD_BLOCK_MAX_SIZE) {
             _build_blocks.emplace_back(mutable_block.to_block());
            // TODO:: Rethink may we should do the proess after we recevie all build blocks ?
            // which is better.
            RETURN_IF_ERROR(process_build_block(_build_blocks[index], index));
            mutable_block = MutableBlock();
            ++index;
            last_mem_used = _mem_used;
        }
    }

    _build_blocks.emplace_back(mutable_block.to_block());
    RETURN_IF_ERROR(process_build_block(_build_blocks[index], index));
    return Status::OK();
}

Status VSetOperationNode::process_build_block(Block& block, uint8_t offset) {
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
                    HashTableBuild<HashTableCtxType> hash_table_build_process(rows, block,
                                                                              raw_ptrs, this, offset);
                    hash_table_build_process(arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return Status::OK();
}

Status VSetOperationNode::process_probe_block(RuntimeState* state, int child_id, bool* eos) {
    if (!_probe_column_inserted_id.empty()) {
        for (int j = 0; j < _probe_column_inserted_id.size(); ++j) {
            auto column_to_erase = _probe_column_inserted_id[j];
            _probe_block.erase(column_to_erase - j);
        }
        _probe_column_inserted_id.clear();
    }
    release_block_memory(_probe_block, child_id);
    _probe_index = 0;
    _probe_rows = 0;

    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(child_id)->get_next(state, &_probe_block, eos));
    _probe_rows = _probe_block.rows();
    RETURN_IF_ERROR(extract_probe_column(_probe_block, _probe_columns, child_id));
    return Status::OK();
}

Status VSetOperationNode::extract_build_column(Block& block, ColumnRawPtrs& raw_ptrs) {
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

Status VSetOperationNode::extract_probe_column(Block& block, ColumnRawPtrs& raw_ptrs,
                                               int child_id) {
    if (_probe_rows == 0) {
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

void VSetOperationNode::create_mutable_cols(Block* output_block) {
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

void VSetOperationNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << " _child_expr_lists=[";
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        *out << VExpr::debug_string(_child_expr_lists[i]) << ", ";
    }
    *out << "] \n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")" << std::endl;
}

} // namespace vectorized
} // namespace doris