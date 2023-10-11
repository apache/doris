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

#include "pipeline/exec/set_sink_operator.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/exec/vset_operation_node.h"

namespace doris::vectorized {
//build hash table for operation node, intersect/except node
template <class HashTableContext, bool is_intersect>
struct HashTableBuild {
    HashTableBuild(int rows, ColumnRawPtrs& build_raw_ptrs,
                   VSetOperationNode<is_intersect>* operation_node, uint8_t offset,
                   RuntimeState* state)
            : _rows(rows),
              _offset(offset),
              _build_raw_ptrs(build_raw_ptrs),
              _operation_node(operation_node),
              _state(state) {}

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

        _build_side_hash_values.resize(_rows);
        const auto& keys = key_getter.get_keys();
        for (size_t k = 0; k < _rows; ++k) {
            _build_side_hash_values[k] = hash_table_ctx.hash_table.hash(keys[k]);
        }

        for (size_t k = 0; k < _rows; ++k) {
            if (k % CHECK_FRECUENCY == 0) {
                RETURN_IF_CANCELLED(_state);
            }
            auto emplace_result = key_getter.emplace_with_key(hash_table_ctx.hash_table, keys[k],
                                                              _build_side_hash_values[k], k);

            if (LIKELY(k + HASH_MAP_PREFETCH_DIST < _rows)) {
                key_getter.template prefetch_by_hash<false>(
                        hash_table_ctx.hash_table,
                        _build_side_hash_values[k + HASH_MAP_PREFETCH_DIST]);
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
    RuntimeState* _state;
    std::vector<size_t> _build_side_hash_values;
};

template <class HashTableContext, bool is_intersect>
struct HashTableBuildX {
    HashTableBuildX(int rows, ColumnRawPtrs& build_raw_ptrs, uint8_t offset, RuntimeState* state)
            : _rows(rows), _offset(offset), _build_raw_ptrs(build_raw_ptrs), _state(state) {}

    Status operator()(pipeline::SetSinkLocalState<is_intersect>& local_state,
                      HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        int64_t old_bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();

        Defer defer {[&]() {
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            local_state._shared_state->mem_used += bucket_bytes - old_bucket_bytes;
        }};

        KeyGetter key_getter(_build_raw_ptrs, local_state._shared_state->build_key_sz, nullptr);

        if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
            hash_table_ctx.serialize_keys(_build_raw_ptrs, _rows);
            key_getter.set_serialized_keys(hash_table_ctx.keys.data());
        }

        _build_side_hash_values.resize(_rows);
        const auto& keys = key_getter.get_keys();
        for (size_t k = 0; k < _rows; ++k) {
            _build_side_hash_values[k] = hash_table_ctx.hash_table.hash(keys[k]);
        }

        for (size_t k = 0; k < _rows; ++k) {
            if (k % CHECK_FRECUENCY == 0) {
                RETURN_IF_CANCELLED(_state);
            }
            auto emplace_result = key_getter.emplace_with_key(hash_table_ctx.hash_table, keys[k],
                                                              _build_side_hash_values[k], k);

            if (LIKELY(k + HASH_MAP_PREFETCH_DIST < _rows)) {
                key_getter.template prefetch_by_hash<false>(
                        hash_table_ctx.hash_table,
                        _build_side_hash_values[k + HASH_MAP_PREFETCH_DIST]);
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
    RuntimeState* _state;
    std::vector<size_t> _build_side_hash_values;
};

} // namespace doris::vectorized
