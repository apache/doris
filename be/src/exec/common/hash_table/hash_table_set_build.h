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

#include <type_traits>

#include "core/column/column.h"
#include "exec/operator/set_sink_operator.h"
#include "runtime/runtime_state.h"

namespace doris {
template <class HashTableContext, bool is_intersect>
struct HashTableBuild {
    template <typename Parent>
    HashTableBuild(Parent* parent, uint32_t rows, ColumnRawPtrs& build_raw_ptrs,
                   RuntimeState* state)
            : _rows(rows), _build_raw_ptrs(build_raw_ptrs), _state(state) {}

    Status operator()(HashTableContext& hash_table_ctx, Arena& arena) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        KeyGetter key_getter(_build_raw_ptrs);
        hash_table_ctx.init_serialized_keys(_build_raw_ptrs, _rows);

        size_t k = 0;
        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
            HashTableContext::try_presis_key(key, origin, arena);
            ctor(key, Mapped {k});
        };
        auto creator_for_null_key = [&](auto& mapped) { mapped = {k}; };

        RETURN_IF_CANCELLED(_state);
        // The batch path regresses StringRef keys, including serialized and nullable strings.
        if constexpr (std::is_same_v<typename HashTableContext::Key, StringRef>) {
            for (; k < _rows; ++k) {
                hash_table_ctx.lazy_emplace(key_getter, k, creator, creator_for_null_key);
            }
        } else {
            lazy_emplace_batch_void(hash_table_ctx, key_getter, _rows, creator,
                                    creator_for_null_key, [&](uint32_t row) { k = row; });
        }
        return Status::OK();
    }

private:
    const uint32_t _rows;
    ColumnRawPtrs& _build_raw_ptrs;
    RuntimeState* _state = nullptr;
};
} // namespace doris
