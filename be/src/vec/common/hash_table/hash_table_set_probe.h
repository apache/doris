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

#include "pipeline/exec/set_probe_sink_operator.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/exec/vset_operation_node.h"

namespace doris::vectorized {
template <class HashTableContext, bool is_intersected>
struct HashTableProbe {
    HashTableProbe(VSetOperationNode<is_intersected>* operation_node, int probe_rows)
            : _operation_node(operation_node),
              _probe_rows(probe_rows),
              _probe_raw_ptrs(operation_node->_probe_columns),
              _arena(new Arena) {}

    Status mark_data_in_hashtable(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;

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

        if constexpr (std::is_same_v<typename HashTableContext::Mapped, RowRefListWithFlags>) {
            for (int probe_index = 0; probe_index < _probe_rows; probe_index++) {
                auto find_result =
                        key_getter.find_key(hash_table_ctx.hash_table, probe_index, *_arena);
                if (find_result.is_found()) { //if found, marked visited
                    auto it = find_result.get_mapped().begin();
                    if (!(it->visited)) {
                        it->visited = true;
                        if constexpr (is_intersected) { //intersected
                            _operation_node->_valid_element_in_hash_tbl++;
                        } else {
                            _operation_node->_valid_element_in_hash_tbl--; //except
                        }
                    }
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

template <class HashTableContext, bool is_intersected>
struct HashTableProbeX {
    HashTableProbeX(pipeline::SetProbeSinkLocalState<is_intersected>& local_state, int probe_rows)
            : _probe_rows(probe_rows),
              _probe_raw_ptrs(local_state._probe_columns),
              _arena(new Arena) {}

    Status mark_data_in_hashtable(pipeline::SetProbeSinkLocalState<is_intersected>& local_state,
                                  HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;

        KeyGetter key_getter(_probe_raw_ptrs, local_state._shared_state->probe_key_sz, nullptr);
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

        if constexpr (std::is_same_v<typename HashTableContext::Mapped, RowRefListWithFlags>) {
            for (int probe_index = 0; probe_index < _probe_rows; probe_index++) {
                auto find_result =
                        key_getter.find_key(hash_table_ctx.hash_table, probe_index, *_arena);
                if (find_result.is_found()) { //if found, marked visited
                    auto it = find_result.get_mapped().begin();
                    if (!(it->visited)) {
                        it->visited = true;
                        if constexpr (is_intersected) { //intersected
                            local_state._shared_state->valid_element_in_hash_tbl++;
                        } else {
                            local_state._shared_state->valid_element_in_hash_tbl--; //except
                        }
                    }
                }
            }
        } else {
            LOG(FATAL) << "Invalid RowRefListType!";
        }
        return Status::OK();
    }

private:
    const size_t _probe_rows;
    ColumnRawPtrs& _probe_raw_ptrs;
    std::unique_ptr<Arena> _arena;
    std::vector<StringRef> _probe_keys;
};

} // namespace doris::vectorized
