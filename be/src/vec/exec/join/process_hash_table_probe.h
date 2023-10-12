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

using MutableColumnPtr = IColumn::MutablePtr;
using MutableColumns = std::vector<MutableColumnPtr>;

using NullMap = ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap*;

template <int JoinOpType, typename Parent>
struct ProcessHashTableProbe {
    ProcessHashTableProbe(Parent* parent, int batch_size);
    ~ProcessHashTableProbe() = default;

    // output build side result column
    void build_side_output_column(MutableColumns& mcol, const std::vector<bool>& output_slot_flags,
                                  int size, bool have_other_join_conjunct);

    void probe_side_output_column(MutableColumns& mcol, const std::vector<bool>& output_slot_flags,
                                  int size, int last_probe_index, size_t probe_size,
                                  bool all_match_one, bool have_other_join_conjunct);

    template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType>
    Status process(HashTableType& hash_table_ctx, ConstNullMapPtr null_map,
                   MutableBlock& mutable_block, Block* output_block, size_t probe_rows,
                   bool is_mark_join, bool have_other_join_conjunct);

    // Only process the join with no other join conjunct, because of no other join conjunt
    // the output block struct is same with mutable block. we can do more opt on it and simplify
    // the logic of probe
    // TODO: opt the visited here to reduce the size of hash table
    template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType,
              bool with_other_conjuncts, bool is_mark_join>
    Status do_process(HashTableType& hash_table_ctx, ConstNullMapPtr null_map,
                      MutableBlock& mutable_block, Block* output_block, size_t probe_rows);
    // In the presence of other join conjunct, the process of join become more complicated.
    // each matching join column need to be processed by other join conjunct. so the struct of mutable block
    // and output block may be different
    // The output result is determined by the other join conjunct result and same_to_prev struct
    Status do_other_join_conjuncts(Block* output_block, bool is_mark_join,
                                   int multi_matched_output_row_count, bool is_the_last_sub_block);

    void _process_splited_equal_matched_tuples(int start_row_idx, int row_count,
                                               const UInt8* __restrict other_hit_column,
                                               UInt8* __restrict null_map_data,
                                               UInt8* __restrict filter_map, Block* output_block);

    void _emplace_element(int8_t block_offset, int32_t block_row, int& current_offset);

    template <typename HashTableType>
    HashTableType::State _init_probe_side(HashTableType& hash_table_ctx, size_t probe_rows,
                                          bool with_other_join_conjuncts, const uint8_t* null_map);

    template <typename Mapped, bool with_other_join_conjuncts>
    ForwardIterator<Mapped>& _probe_row_match(int& current_offset, int& probe_index,
                                              size_t& probe_size, bool& all_match_one);

    // Process full outer join/ right join / right semi/anti join to output the join result
    // in hash table
    template <typename HashTableType>
    Status process_data_in_hashtable(HashTableType& hash_table_ctx, MutableBlock& mutable_block,
                                     Block* output_block, bool* eos);

    Parent* _parent;
    const int _batch_size;
    std::shared_ptr<std::vector<Block>> _build_blocks;
    std::unique_ptr<Arena> _arena;
    std::vector<StringRef> _probe_keys;

    std::vector<uint32_t> _probe_indexs;
    PaddedPODArray<int8_t> _build_block_offsets;
    PaddedPODArray<int32_t> _build_block_rows;
    std::vector<std::pair<int8_t, int>> _build_blocks_locs;
    // only need set the tuple is null in RIGHT_OUTER_JOIN and FULL_OUTER_JOIN
    ColumnUInt8::Container* _tuple_is_null_left_flags;
    // only need set the tuple is null in LEFT_OUTER_JOIN and FULL_OUTER_JOIN
    ColumnUInt8::Container* _tuple_is_null_right_flags;

    size_t _serialized_key_buffer_size {0};
    uint8_t* _serialized_key_buffer;
    std::unique_ptr<Arena> _serialize_key_arena;
    std::vector<char> _probe_side_find_result;

    std::vector<bool*> _visited_map;
    std::vector<bool> _same_to_prev;

    int _right_col_idx;
    int _right_col_len;
    int _row_count_from_last_probe;

    bool _have_other_join_conjunct;
    bool _is_right_semi_anti;
    Sizes _probe_key_sz;
    std::vector<bool>* _left_output_slot_flags;
    std::vector<bool>* _right_output_slot_flags;
    bool* _has_null_in_build_side;

    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;
    RuntimeProfile::Counter* _probe_process_hashtable_timer;
    static constexpr int PROBE_SIDE_EXPLODE_RATE = 1;
};

} // namespace vectorized
} // namespace doris
