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

#include <vector>

#include "vec/columns/column.h"
#include "vec/common/arena.h"
#include "vec/common/custom_allocator.h"

namespace doris {
namespace vectorized {
class Block;
class MutableBlock;
struct HashJoinProbeContext;
} // namespace vectorized
namespace pipeline {

class HashJoinProbeLocalState;
class HashJoinProbeOperatorX;

using MutableColumnPtr = vectorized::IColumn::MutablePtr;
using MutableColumns = std::vector<vectorized::MutableColumnPtr>;

using NullMap = vectorized::ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap*;

template <int JoinOpType>
struct ProcessHashTableProbe {
    ProcessHashTableProbe(HashJoinProbeLocalState* parent, int batch_size);
    ~ProcessHashTableProbe() = default;

    // output build side result column
    void build_side_output_column(vectorized::MutableColumns& mcol, bool is_mark_join);

    void probe_side_output_column(vectorized::MutableColumns& mcol);

    // Only process the join with no other join conjunct, because of no other join conjunt
    // the output block struct is same with mutable block. we can do more opt on it and simplify
    // the logic of probe
    template <typename HashTableType>
    Status process(HashTableType& hash_table_ctx, const uint8_t* null_map,
                   vectorized::MutableBlock& mutable_block, vectorized::Block* output_block,
                   uint32_t probe_rows, bool is_mark_join);

    // In the presence of other join conjunct, the process of join become more complicated.
    // each matching join column need to be processed by other join conjunct. so the struct of mutable block
    // and output block may be different
    // The output result is determined by the other join conjunct result and same_to_prev struct
    Status do_other_join_conjuncts(vectorized::Block* output_block, DorisVector<uint8_t>& visited);

    Status do_mark_join_conjuncts(vectorized::Block* output_block, const uint8_t* null_map);

    Status finalize_block_with_filter(vectorized::Block* output_block, size_t filter_column_id,
                                      size_t column_to_keep);

    template <typename HashTableType>
    typename HashTableType::State _init_probe_side(HashTableType& hash_table_ctx,
                                                   uint32_t probe_rows, const uint8_t* null_map);

    // Process full outer join/ right join / right semi/anti join to output the join result
    // in hash table
    template <typename HashTableType>
    Status finish_probing(HashTableType& hash_table_ctx, vectorized::MutableBlock& mutable_block,
                          vectorized::Block* output_block, bool* eos, bool is_mark_join);

    /// For null aware join with other conjuncts, if the probe key of one row on left side is null,
    /// we should make this row match with all rows in build side.
    uint32_t _process_probe_null_key(uint32_t probe_idx);

    pipeline::HashJoinProbeLocalState* _parent = nullptr;
    pipeline::HashJoinProbeOperatorX* _parent_operator = nullptr;

    const int _batch_size;
    const std::shared_ptr<vectorized::Block>& _build_block;
    std::unique_ptr<vectorized::Arena> _arena;

    vectorized::ColumnOffset32 _probe_indexs;
    vectorized::ColumnOffset32 _output_row_indexs;
    bool _probe_visited = false;
    bool _picking_null_keys = false;
    vectorized::ColumnOffset32 _build_indexs;
    std::vector<uint8_t> _null_flags;

    /// If the probe key of one row on left side is null,
    /// we will make all rows in build side match with this row,
    /// `_build_index_for_null_probe_key` is used to record the progress if the build block is too big.
    uint32_t _build_index_for_null_probe_key {0};

    std::vector<int> _build_blocks_locs;

    std::vector<char> _probe_side_find_result;

    const bool _have_other_join_conjunct;
    const std::vector<bool>& _left_output_slot_flags;
    const std::vector<bool>& _right_output_slot_flags;
    // nullable column but not has null except first row
    std::vector<bool> _build_column_has_null;
    bool _need_calculate_build_index_has_zero = true;

    RuntimeProfile::Counter* _search_hashtable_timer = nullptr;
    RuntimeProfile::Counter* _init_probe_side_timer = nullptr;
    RuntimeProfile::Counter* _build_side_output_timer = nullptr;
    RuntimeProfile::Counter* _probe_side_output_timer = nullptr;
    RuntimeProfile::Counter* _finish_probe_phase_timer = nullptr;

    // See `HashJoinProbeOperatorX::_right_col_idx`
    const size_t _right_col_idx;

    size_t _right_col_len;

    // For right semi with mark join conjunct, we need to store the mark join flags
    // in the hash table.
    // -1 means null, 0 means false, 1 means true
    DorisVector<int8_t> mark_join_flags;
};

} // namespace pipeline
} // namespace doris
