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

#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "exprs/runtime_filter_slots.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_map_context_creator.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/exec/join/join_op.h" // IWYU pragma: keep
#include "vec/exprs/vexpr_fwd.h"

template <typename T>
struct HashCRC32;

namespace doris {
class ObjectPool;
class DescriptorTbl;
class RuntimeState;

namespace pipeline {
class HashJoinProbeLocalState;
class HashJoinBuildSinkLocalState;
} // namespace pipeline

namespace vectorized {

constexpr size_t CHECK_FRECUENCY = 65536;

struct UInt128;
struct UInt256;
template <int JoinOpType>
struct ProcessHashTableProbe;

template <typename Parent>
Status process_runtime_filter_build(RuntimeState* state, Block* block, Parent* parent,
                                    bool is_global = false) {
    if (parent->runtime_filters().empty()) {
        return Status::OK();
    }
    uint64_t rows = block->rows();
    {
        SCOPED_TIMER(parent->_runtime_filter_init_timer);
        RETURN_IF_ERROR(parent->_runtime_filter_slots->init_filters(state, rows));
        RETURN_IF_ERROR(parent->_runtime_filter_slots->ignore_filters(state));
    }

    if (!parent->_runtime_filter_slots->empty() && rows > 1) {
        SCOPED_TIMER(parent->_runtime_filter_compute_timer);
        parent->_runtime_filter_slots->insert(block);
    }
    {
        SCOPED_TIMER(parent->_publish_runtime_filter_timer);
        RETURN_IF_ERROR(parent->_runtime_filter_slots->publish());
    }

    return Status::OK();
}

using ProfileCounter = RuntimeProfile::Counter;

template <class HashTableContext, typename Parent>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, ColumnRawPtrs& build_raw_ptrs, Parent* parent, int batch_size,
                          RuntimeState* state)
            : _rows(rows),
              _build_raw_ptrs(build_raw_ptrs),
              _parent(parent),
              _batch_size(batch_size),
              _state(state) {}

    template <int JoinOpType, bool ignore_null, bool short_circuit_for_null,
              bool with_other_conjuncts>
    Status run(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map, bool* has_null_key) {
        if (short_circuit_for_null || ignore_null) {
            // first row is mocked and is null
            for (uint32_t i = 1; i < _rows; i++) {
                if ((*null_map)[i]) {
                    *has_null_key = true;
                }
            }
            if (short_circuit_for_null && *has_null_key) {
                return Status::OK();
            }
        }

        SCOPED_TIMER(_parent->_build_table_insert_timer);
        hash_table_ctx.hash_table->template prepare_build<JoinOpType>(_rows, _batch_size,
                                                                      *has_null_key);

        hash_table_ctx.init_serialized_keys(_build_raw_ptrs, _rows,
                                            null_map ? null_map->data() : nullptr, true, true,
                                            hash_table_ctx.hash_table->get_bucket_size());
        hash_table_ctx.hash_table->template build<JoinOpType, with_other_conjuncts>(
                hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(), _rows);
        hash_table_ctx.bucket_nums.resize(_batch_size);
        hash_table_ctx.bucket_nums.shrink_to_fit();

        COUNTER_SET(_parent->_hash_table_memory_usage,
                    (int64_t)hash_table_ctx.hash_table->get_byte_size());
        COUNTER_SET(_parent->_build_arena_memory_usage,
                    (int64_t)hash_table_ctx.serialized_keys_size(true));
        return Status::OK();
    }

private:
    const uint32_t _rows;
    ColumnRawPtrs& _build_raw_ptrs;
    Parent* _parent = nullptr;
    int _batch_size;
    RuntimeState* _state = nullptr;
};

using I8HashTableContext = PrimaryTypeHashTableContext<UInt8>;
using I16HashTableContext = PrimaryTypeHashTableContext<UInt16>;
using I32HashTableContext = PrimaryTypeHashTableContext<UInt32>;
using I64HashTableContext = PrimaryTypeHashTableContext<UInt64>;
using I128HashTableContext = PrimaryTypeHashTableContext<UInt128>;
using I256HashTableContext = PrimaryTypeHashTableContext<UInt256>;

template <bool has_null>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContext = FixedKeyHashTableContext<UInt256, has_null>;

template <bool has_null>
using I136FixedKeyHashTableContext = FixedKeyHashTableContext<UInt136, has_null>;

using HashTableVariants =
        std::variant<std::monostate, SerializedHashTableContext, I8HashTableContext,
                     I16HashTableContext, I32HashTableContext, I64HashTableContext,
                     I128HashTableContext, I256HashTableContext, I64FixedKeyHashTableContext<true>,
                     I64FixedKeyHashTableContext<false>, I128FixedKeyHashTableContext<true>,
                     I128FixedKeyHashTableContext<false>, I256FixedKeyHashTableContext<true>,
                     I256FixedKeyHashTableContext<false>, I136FixedKeyHashTableContext<true>,
                     I136FixedKeyHashTableContext<false>>;

} // namespace vectorized
} // namespace doris
