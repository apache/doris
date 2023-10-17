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
template <class HashTableContext, bool is_intersect>
struct HashTableBuild {
    template <typename Parent>
    HashTableBuild(Parent* parent, int rows, ColumnRawPtrs& build_raw_ptrs, uint8_t offset,
                   RuntimeState* state)
            : _mem_used(parent->mem_used()),
              _rows(rows),
              _offset(offset),
              _build_raw_ptrs(build_raw_ptrs),
              _state(state) {}

    Status operator()(HashTableContext& hash_table_ctx, Arena& arena) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        int64_t old_bucket_bytes = hash_table_ctx.hash_table->get_buffer_size_in_bytes();

        Defer defer {[&]() {
            int64_t bucket_bytes = hash_table_ctx.hash_table->get_buffer_size_in_bytes();
            *_mem_used += bucket_bytes - old_bucket_bytes;
        }};

        KeyGetter key_getter(_build_raw_ptrs);
        hash_table_ctx.init_serialized_keys(_build_raw_ptrs, _rows);

        size_t k = 0;
        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
            HashTableContext::try_presis_key(key, origin, arena);
            ctor(key, Mapped {k, _offset});
        };
        auto creator_for_null_key = [&](auto& mapped) { mapped = {k, _offset}; };

        for (; k < _rows; ++k) {
            if (k % CHECK_FRECUENCY == 0) {
                RETURN_IF_CANCELLED(_state);
            }
            hash_table_ctx.lazy_emplace(key_getter, k, creator, creator_for_null_key);
        }
        return Status::OK();
    }

private:
    int64_t* _mem_used;
    const int _rows;
    const uint8_t _offset;
    ColumnRawPtrs& _build_raw_ptrs;
    RuntimeState* _state;
};

} // namespace doris::vectorized
