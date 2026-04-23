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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "exec/common/hash_table/join_hash_table_types.h"
#include "runtime/runtime_profile.h"

namespace doris {

class RuntimeState;
class Block;
class MutableBlock;
class HashJoinBuildSinkLocalState;
class HashJoinProbeLocalState;

struct JoinDataVariants;

/// Pure virtual interface for all join hash table contexts.
/// Replaces the former 20-alternative std::variant with virtual dispatch.
class IJoinHashTableContext {
public:
    virtual ~IJoinHashTableContext() = default;

    // ── Key serialization ──
    [[nodiscard]] virtual size_t estimated_size(const ColumnRawPtrs& key_columns,
                                                uint32_t num_rows, bool is_join, bool is_build,
                                                uint32_t bucket_size) = 0;
    virtual size_t serialized_keys_size(bool is_build) const = 0;

    // ── Hash table metadata ──
    virtual void get_table_metadata(uint32_t& bucket_size, const uint32_t*& first_array,
                                    const uint32_t*& next_array, size_t& build_rows) const = 0;
    virtual size_t hash_table_byte_size() const = 0;

    // ── Build phase ──
    virtual Status process_build(uint32_t rows, ColumnRawPtrs& raw_ptrs,
                                 HashJoinBuildSinkLocalState* local_state, int batch_size,
                                 RuntimeState* state, const JoinOpVariants& join_op_variants,
                                 const ColumnUInt8::Container* null_map,
                                 bool* has_null_in_build_side,
                                 bool short_circuit_for_null_in_build_side,
                                 bool have_other_join_conjunct) = 0;

    virtual void build_asof_index_groups_impl(
            AsofIndexVariant& asof_index_groups,
            std::vector<uint32_t>& asof_build_row_to_bucket, uint32_t bucket_size,
            const uint32_t* first_array, const uint32_t* next_array, size_t build_rows,
            const ColumnNullable* nullable_col, const IColumn* build_col_nested,
            RuntimeProfile::Counter* asof_index_group_timer,
            RuntimeProfile::Counter* asof_index_sort_timer) = 0;

    virtual bool try_convert_to_direct(
            const ColumnRawPtrs& key_columns,
            std::vector<std::shared_ptr<JoinDataVariants>>& all_variants) {
        return false;
    }

    virtual void share_hash_table_from(const IJoinHashTableContext& src) = 0;

    // ── Probe phase ──
    virtual void init_probe_ctx(HashJoinProbeLocalState* probe_state, int batch_size,
                                const JoinOpVariants& join_op_variants) = 0;

    virtual Status process_probe(const uint8_t* null_map, MutableBlock& out, Block* block,
                                 uint32_t rows, bool is_mark_join) = 0;

    virtual void process_direct_return(MutableBlock& out, Block* block, uint32_t rows) = 0;

    virtual Status finish_probing(MutableBlock& out, Block* block, bool* eos,
                                  bool is_mark_join) = 0;

    virtual void close_probe_ctx() = 0;
};

/// Pure virtual interface for probe context (type-erases JoinOp template parameter).
class IProbeCtxBase {
public:
    virtual ~IProbeCtxBase() = default;

    virtual Status process(IJoinHashTableContext& ht_ctx, const uint8_t* null_map, MutableBlock& out,
                           Block* block, uint32_t rows, bool is_mark_join) = 0;

    virtual void process_direct_return(IJoinHashTableContext& ht_ctx, MutableBlock& out,
                                       Block* block, uint32_t rows) = 0;

    virtual Status finish_probing(IJoinHashTableContext& ht_ctx, MutableBlock& out, Block* block,
                                  bool* eos, bool is_mark_join) = 0;

    virtual void close() = 0;
};
} // namespace doris
