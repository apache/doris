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

#include "join_op.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"

namespace doris {
namespace vectorized {
class Block;
class MutableBlock;
struct HashJoinProbeContext;
} // namespace vectorized
namespace pipeline {

class HashJoinProbeLocalState;

using MutableColumnPtr = vectorized::IColumn::MutablePtr;
using MutableColumns = std::vector<vectorized::MutableColumnPtr>;

using NullMap = vectorized::ColumnUInt8::Container;
using ConstNullMapPtr = const vectorized::NullMap*;

template <int JoinOpType>
struct ProcessHashTableProbe {
    ProcessHashTableProbe(HashJoinProbeLocalState* parent, int batch_size);
    ~ProcessHashTableProbe() = default;

    // output build side result column
    void build_side_output_column(vectorized::MutableColumns& mcol,
                                  const std::vector<bool>& output_slot_flags, int size,
                                  bool have_other_join_conjunct, bool is_mark_join);

    void probe_side_output_column(vectorized::MutableColumns& mcol,
                                  const std::vector<bool>& output_slot_flags, int size,
                                  int last_probe_index, bool all_match_one,
                                  bool have_other_join_conjunct);

    template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType>
    Status process(HashTableType& hash_table_ctx, ConstNullMapPtr null_map,
                   vectorized::MutableBlock& mutable_block, vectorized::Block* output_block,
                   size_t probe_rows, bool is_mark_join, bool have_other_join_conjunct);

    // Only process the join with no other join conjunct, because of no other join conjunt
    // the output block struct is same with mutable block. we can do more opt on it and simplify
    // the logic of probe
    // TODO: opt the visited here to reduce the size of hash table
    template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType,
              bool with_other_conjuncts, bool is_mark_join>
    Status do_process(HashTableType& hash_table_ctx, ConstNullMapPtr null_map,
                      vectorized::MutableBlock& mutable_block, vectorized::Block* output_block,
                      size_t probe_rows);
    // In the presence of other join conjunct, the process of join become more complicated.
    // each matching join column need to be processed by other join conjunct. so the struct of mutable block
    // and output block may be different
    // The output result is determined by the other join conjunct result and same_to_prev struct
    Status do_other_join_conjuncts(vectorized::Block* output_block, std::vector<uint8_t>& visited,
                                   bool has_null_in_build_side);

    template <bool with_other_conjuncts>
    Status do_mark_join_conjuncts(vectorized::Block* output_block, size_t hash_table_bucket_size);

    template <typename HashTableType>
    typename HashTableType::State _init_probe_side(HashTableType& hash_table_ctx, size_t probe_rows,
                                                   bool with_other_join_conjuncts,
                                                   const uint8_t* null_map, bool need_judge_null);

    // Process full outer join/ right join / right semi/anti join to output the join result
    // in hash table
    template <typename HashTableType>
    Status process_data_in_hashtable(HashTableType& hash_table_ctx,
                                     vectorized::MutableBlock& mutable_block,
                                     vectorized::Block* output_block, bool* eos, bool is_mark_join);

    /// For null aware join with other conjuncts, if the probe key of one row on left side is null,
    /// we should make this row match with all rows in build side.
    size_t _process_probe_null_key(uint32_t probe_idx);

    pipeline::HashJoinProbeLocalState* _parent = nullptr;
    const int _batch_size;
    const std::shared_ptr<vectorized::Block>& _build_block;
    std::unique_ptr<vectorized::Arena> _arena;
    std::vector<StringRef> _probe_keys;

    std::vector<uint32_t> _probe_indexs;
    bool _probe_visited = false;
    bool _picking_null_keys = false;
    std::vector<uint32_t> _build_indexs;
    std::vector<uint8_t> _null_flags;

    /// If the probe key of one row on left side is null,
    /// we will make all rows in build side match with this row,
    /// `_build_index_for_null_probe_key` is used to record the progress if the build block is too big.
    uint32_t _build_index_for_null_probe_key {0};

    std::vector<int> _build_blocks_locs;
    // only need set the tuple is null in RIGHT_OUTER_JOIN and FULL_OUTER_JOIN
    vectorized::ColumnUInt8::Container* _tuple_is_null_left_flags = nullptr;
    // only need set the tuple is null in LEFT_OUTER_JOIN and FULL_OUTER_JOIN
    vectorized::ColumnUInt8::Container* _tuple_is_null_right_flags = nullptr;

    size_t _serialized_key_buffer_size {0};
    uint8_t* _serialized_key_buffer = nullptr;
    std::unique_ptr<vectorized::Arena> _serialize_key_arena;
    std::vector<char> _probe_side_find_result;

    bool _have_other_join_conjunct;
    bool _is_right_semi_anti;
    std::vector<bool>* _left_output_slot_flags = nullptr;
    std::vector<bool>* _right_output_slot_flags = nullptr;
    // nullable column but not has null except first row
    std::vector<bool> _build_column_has_null;
    bool _need_calculate_build_index_has_zero = true;
    bool* _has_null_in_build_side;

    RuntimeProfile::Counter* _rows_returned_counter = nullptr;
    RuntimeProfile::Counter* _search_hashtable_timer = nullptr;
    RuntimeProfile::Counter* _init_probe_side_timer = nullptr;
    RuntimeProfile::Counter* _build_side_output_timer = nullptr;
    RuntimeProfile::Counter* _probe_side_output_timer = nullptr;
    RuntimeProfile::Counter* _probe_process_hashtable_timer = nullptr;

    int _right_col_idx;
    int _right_col_len;
};

} // namespace pipeline
} // namespace doris
