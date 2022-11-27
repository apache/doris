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

#pragma once

#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "vec/core/materialize_block.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris {

namespace vectorized {

class VSetOperationNode : public ExecNode {
public:
    VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
        return Status::NotSupported("Not Implemented get RowBatch in vecorized execution.");
    }
    virtual Status close(RuntimeState* state);
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

protected:
    //Todo: In build process of hashtable, It's same as join node.
    //It's time to abstract out the same methods and provide them directly to others;
    void hash_table_init();
    Status hash_table_build(RuntimeState* state);
    Status process_build_block(Block& block, uint8_t offset);
    Status extract_build_column(Block& block, ColumnRawPtrs& raw_ptrs);
    Status extract_probe_column(Block& block, ColumnRawPtrs& raw_ptrs, int child_id);
    template <bool keep_matched>
    void refresh_hash_table();
    Status process_probe_block(RuntimeState* state, int child_id, bool* eos);
    void create_mutable_cols(Block* output_block);
    void release_mem();

protected:
    std::unique_ptr<HashTableVariants> _hash_table_variants;

    std::vector<size_t> _probe_key_sz;
    std::vector<size_t> _build_key_sz;
    std::vector<bool> _build_not_ignore_null;

    std::unique_ptr<Arena> _arena;
    //record element size in hashtable
    int64_t _valid_element_in_hash_tbl;

    //The i-th result expr list refers to the i-th child.
    std::vector<std::vector<VExprContext*>> _child_expr_lists;
    //record build column type
    DataTypes _left_table_data_types;
    //first:column_id, could point to origin column or cast column
    //second:idx mapped to column types
    std::unordered_map<int, int> _build_col_idx;
    //record memory during running
    int64_t _mem_used;
    //record insert column id during probe
    std::vector<uint16_t> _probe_column_inserted_id;

    std::vector<Block> _build_blocks;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    std::vector<MutableColumnPtr> _mutable_cols;
    int _probe_index;
    size_t _probe_rows;
    RuntimeProfile::Counter* _build_timer; // time to build hash table
    RuntimeProfile::Counter* _probe_timer; // time to probe

    template <class HashTableContext>
    friend struct HashTableBuild;
    template <class HashTableContext, bool is_intersected>
    friend struct HashTableProbe;
};

template <bool keep_matched>
void VSetOperationNode::refresh_hash_table() {
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    if constexpr (std::is_same_v<typename HashTableCtxType::Mapped,
                                                 RowRefListWithFlags>) {
                        HashTableCtxType tmp_hash_table;
                        bool is_need_shrink =
                                arg.hash_table.should_be_shrink(_valid_element_in_hash_tbl);
                        if (is_need_shrink) {
                            tmp_hash_table.hash_table.init_buf_size(
                                    _valid_element_in_hash_tbl / arg.hash_table.get_factor() + 1);
                        }

                        arg.init_once();
                        auto& iter = arg.iter;
                        auto iter_end = arg.hash_table.end();
                        while (iter != iter_end) {
                            auto& mapped = iter->get_second();
                            auto it = mapped.begin();

                            if constexpr (keep_matched) { //intersected
                                if (it->visited) {
                                    it->visited = false;
                                    if (is_need_shrink) {
                                        tmp_hash_table.hash_table.insert(iter->get_value());
                                    }
                                    ++iter;
                                } else {
                                    if (!is_need_shrink) {
                                        arg.hash_table.delete_zero_key(iter->get_first());
                                        // the ++iter would check if the current key is zero. if it does, the iterator will be moved to the container's head.
                                        // so we do ++iter before set_zero to make the iterator move to next valid key correctly.
                                        auto iter_prev = iter;
                                        ++iter;
                                        iter_prev->set_zero();
                                    } else {
                                        ++iter;
                                    }
                                }
                            } else { //except
                                if (!it->visited && is_need_shrink) {
                                    tmp_hash_table.hash_table.insert(iter->get_value());
                                }
                                ++iter;
                            }
                        }

                        arg.inited = false;
                        if (is_need_shrink) {
                            arg.hash_table = std::move(tmp_hash_table.hash_table);
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

template <class HashTableContext, bool is_intersected>
struct HashTableProbe {
    HashTableProbe(VSetOperationNode* operation_node, int batch_size, int probe_rows)
            : _operation_node(operation_node),
              _left_table_data_types(operation_node->_left_table_data_types),
              _batch_size(batch_size),
              _probe_rows(probe_rows),
              _build_blocks(operation_node->_build_blocks),
              _probe_block(operation_node->_probe_block),
              _probe_index(operation_node->_probe_index),
              _num_rows_returned(operation_node->_num_rows_returned),
              _probe_raw_ptrs(operation_node->_probe_columns),
              _rows_returned_counter(operation_node->_rows_returned_counter),
              _build_col_idx(operation_node->_build_col_idx),
              _mutable_cols(operation_node->_mutable_cols) {}

    Status mark_data_in_hashtable(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        KeyGetter key_getter(_probe_raw_ptrs, _operation_node->_probe_key_sz, nullptr);

        if (_probe_index == 0) {
            _arena.reset(new Arena());
            if constexpr (IsSerializedHashTableContextTraits<KeyGetter>::value) {
                if (_probe_keys.size() < _probe_rows) {
                    _probe_keys.resize(_probe_rows);
                }
                size_t keys_size = _probe_raw_ptrs.size();
                for (size_t i = 0; i < _probe_rows; ++i) {
                    _probe_keys[i] = serialize_keys_to_pool_contiguous(i, keys_size,
                                                                       _probe_raw_ptrs, *_arena);
                }
            }
        }

        if constexpr (IsSerializedHashTableContextTraits<KeyGetter>::value) {
            key_getter.set_serialized_keys(_probe_keys.data());
        }

        if constexpr (std::is_same_v<typename HashTableContext::Mapped, RowRefListWithFlags>) {
            for (; _probe_index < _probe_rows;) {
                auto find_result =
                        key_getter.find_key(hash_table_ctx.hash_table, _probe_index, *_arena);
                if (find_result.is_found()) { //if found, marked visited
                    auto it = find_result.get_mapped().begin();
                    if (!(it->visited)) {
                        it->visited = true;
                        if constexpr (is_intersected) //intersected
                            _operation_node->_valid_element_in_hash_tbl++;
                        else
                            _operation_node->_valid_element_in_hash_tbl--; //except
                    }
                }
                _probe_index++;
            }
        } else {
            LOG(FATAL) << "Invalid RowRefListType!";
        }
        return Status::OK();
    }

    void add_result_columns(RowRefListWithFlags& value, int& block_size) {
        auto it = value.begin();
        for (auto idx = _build_col_idx.begin(); idx != _build_col_idx.end(); ++idx) {
            auto& column = *_build_blocks[it->block_offset].get_by_position(idx->first).column;
            _mutable_cols[idx->second]->insert_from(column, it->row_num);
        }
        block_size++;
    }

    Status get_data_in_hashtable(HashTableContext& hash_table_ctx,
                                 std::vector<MutableColumnPtr>& mutable_cols, Block* output_block,
                                 bool* eos) {
        hash_table_ctx.init_once();
        int left_col_len = _left_table_data_types.size();
        auto& iter = hash_table_ctx.iter;
        auto block_size = 0;

        if constexpr (std::is_same_v<typename HashTableContext::Mapped, RowRefListWithFlags>) {
            for (; iter != hash_table_ctx.hash_table.end() && block_size < _batch_size; ++iter) {
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

private:
    VSetOperationNode* _operation_node;
    const DataTypes& _left_table_data_types;
    const int _batch_size;
    const size_t _probe_rows;
    const std::vector<Block>& _build_blocks;
    const Block& _probe_block;
    int& _probe_index;
    int64_t& _num_rows_returned;
    ColumnRawPtrs& _probe_raw_ptrs;
    std::unique_ptr<Arena> _arena;
    std::vector<StringRef> _probe_keys;
    RuntimeProfile::Counter* _rows_returned_counter;
    std::unordered_map<int, int>& _build_col_idx;
    std::vector<MutableColumnPtr>& _mutable_cols;
};

} // namespace vectorized
} // namespace doris
