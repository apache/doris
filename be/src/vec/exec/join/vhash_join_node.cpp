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

#include "vec/exec/join/vhash_join_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/defer_op.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// TODO: Best prefetch step is decided by machine. We should also provide a
//  SQL hint to allow users to tune by hand.
static constexpr int PREFETCH_STEP = 64;

using ProfileCounter = RuntimeProfile::Counter;
template <class HashTableContext>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                          HashJoinNode* join_node, int batch_size, uint8_t offset)
            : _rows(rows),
              _skip_rows(0),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _join_node(join_node),
              _batch_size(batch_size),
              _offset(offset),
              _build_side_compute_hash_timer(join_node->_build_side_compute_hash_timer) {}

    template <bool ignore_null, bool build_unique, bool has_runtime_filter>
    void run(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        int64_t old_bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();

        Defer defer {[&]() {
            int64_t bucket_size = hash_table_ctx.hash_table.get_buffer_size_in_cells();
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            _join_node->_mem_used += bucket_bytes - old_bucket_bytes;
            COUNTER_SET(_join_node->_build_buckets_counter, bucket_size);
        }};

        KeyGetter key_getter(_build_raw_ptrs, _join_node->_build_key_sz, nullptr);

        SCOPED_TIMER(_join_node->_build_table_insert_timer);
        // only not build_unique, we need expanse hash table before insert data
        if constexpr (!build_unique) {
            // _rows contains null row, which will cause hash table resize to be large.
            hash_table_ctx.hash_table.expanse_for_add_elem(_rows);
        }
        hash_table_ctx.hash_table.reset_resize_timer();

        vector<int>& inserted_rows = _join_node->_inserted_rows[&_acquired_block];
        if constexpr (has_runtime_filter) {
            inserted_rows.reserve(_batch_size);
        }

        _build_side_hash_values.resize(_rows);
        auto& arena = _join_node->_arena;
        {
            SCOPED_TIMER(_build_side_compute_hash_timer);
            for (size_t k = 0; k < _rows; ++k) {
                if constexpr (ignore_null) {
                    if ((*null_map)[k]) {
                        continue;
                    }
                }
                if constexpr (IsSerializedHashTableContextTraits<KeyGetter>::value) {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena).key);
                } else {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena));
                }
            }
        }

        for (size_t k = 0; k < _rows; ++k) {
            if constexpr (ignore_null) {
                if ((*null_map)[k]) {
                    continue;
                }
            }

            auto emplace_result = key_getter.emplace_key(hash_table_ctx.hash_table,
                                                         _build_side_hash_values[k], k, arena);
            if (k + PREFETCH_STEP < _rows) {
                key_getter.template prefetch_by_hash<false>(
                        hash_table_ctx.hash_table, _build_side_hash_values[k + PREFETCH_STEP]);
            }

            if (emplace_result.is_inserted()) {
                new (&emplace_result.get_mapped()) Mapped({k, _offset});
                if constexpr (has_runtime_filter) {
                    inserted_rows.push_back(k);
                }
            } else {
                if constexpr (!build_unique) {
                    /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                    emplace_result.get_mapped().insert({k, _offset}, _join_node->_arena);
                    if constexpr (has_runtime_filter) {
                        inserted_rows.push_back(k);
                    }
                } else {
                    _skip_rows++;
                }
            }
        }

        COUNTER_UPDATE(_join_node->_build_table_expanse_timer,
                       hash_table_ctx.hash_table.get_resize_timer_value());
    }

    template <bool ignore_null, bool build_unique, bool has_runtime_filter>
    struct Reducer {
        template <typename... TArgs>
        static void run(ProcessHashTableBuild<HashTableContext>& build, TArgs&&... args) {
            build.template run<ignore_null, build_unique, has_runtime_filter>(
                    std::forward<TArgs>(args)...);
        }
    };

private:
    const int _rows;
    int _skip_rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    HashJoinNode* _join_node;
    int _batch_size;
    uint8_t _offset;

    ProfileCounter* _build_side_compute_hash_timer;
    std::vector<size_t> _build_side_hash_values;
};

template <class HashTableContext>
struct ProcessRuntimeFilterBuild {
    ProcessRuntimeFilterBuild(HashJoinNode* join_node) : _join_node(join_node) {}

    Status operator()(RuntimeState* state, HashTableContext& hash_table_ctx) {
        if (_join_node->_runtime_filter_descs.empty()) {
            return Status::OK();
        }
        VRuntimeFilterSlots runtime_filter_slots(_join_node->_probe_expr_ctxs,
                                                 _join_node->_build_expr_ctxs,
                                                 _join_node->_runtime_filter_descs);

        RETURN_IF_ERROR(runtime_filter_slots.init(state, hash_table_ctx.hash_table.get_size()));

        if (!runtime_filter_slots.empty() && !_join_node->_inserted_rows.empty()) {
            {
                SCOPED_TIMER(_join_node->_push_compute_timer);
                runtime_filter_slots.insert(_join_node->_inserted_rows);
            }
        }
        {
            SCOPED_TIMER(_join_node->_push_down_timer);
            runtime_filter_slots.publish();
        }

        return Status::OK();
    }

private:
    HashJoinNode* _join_node;
};

template <class JoinOpType, bool ignore_null>
ProcessHashTableProbe<JoinOpType, ignore_null>::ProcessHashTableProbe(HashJoinNode* join_node,
                                                                      int batch_size)
        : _join_node(join_node),
          _batch_size(batch_size),
          _build_blocks(join_node->_build_blocks),
          _tuple_is_null_left_flags(
                  reinterpret_cast<ColumnUInt8&>(*join_node->_tuple_is_null_left_flag_column)
                          .get_data()),
          _tuple_is_null_right_flags(
                  reinterpret_cast<ColumnUInt8&>(*join_node->_tuple_is_null_right_flag_column)
                          .get_data()),
          _rows_returned_counter(join_node->_rows_returned_counter),
          _search_hashtable_timer(join_node->_search_hashtable_timer),
          _build_side_output_timer(join_node->_build_side_output_timer),
          _probe_side_output_timer(join_node->_probe_side_output_timer) {}

template <class JoinOpType, bool ignore_null>
template <bool have_other_join_conjunct>
void ProcessHashTableProbe<JoinOpType, ignore_null>::build_side_output_column(
        MutableColumns& mcol, int column_offset, int column_length,
        const std::vector<bool>& output_slot_flags, int size) {
    constexpr auto is_semi_anti_join = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                       JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                       JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN ||
                                       JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN;

    constexpr auto probe_all = JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                               JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

    if constexpr (!is_semi_anti_join || have_other_join_conjunct) {
        if (_build_blocks.size() == 1) {
            for (int i = 0; i < column_length; i++) {
                auto& column = *_build_blocks[0].get_by_position(i).column;
                if (output_slot_flags[i]) {
                    mcol[i + column_offset]->insert_indices_from(column, _build_block_rows.data(),
                                                                 _build_block_rows.data() + size);
                } else {
                    mcol[i + column_offset]->resize(size);
                }
            }
        } else {
            for (int i = 0; i < column_length; i++) {
                if (output_slot_flags[i]) {
                    for (int j = 0; j < size; j++) {
                        if constexpr (probe_all) {
                            if (_build_block_offsets[j] == -1) {
                                DCHECK(mcol[i + column_offset]->is_nullable());
                                assert_cast<ColumnNullable*>(mcol[i + column_offset].get())
                                        ->insert_default();
                            } else {
                                auto& column = *_build_blocks[_build_block_offsets[j]]
                                                        .get_by_position(i)
                                                        .column;
                                mcol[i + column_offset]->insert_from(column, _build_block_rows[j]);
                            }
                        } else {
                            if (_build_block_offsets[j] == -1) {
                                // the only case to reach here:
                                // 1. left anti join with other conjuncts, and
                                // 2. equal conjuncts does not match
                                // since nullptr is emplaced back to visited_map,
                                // the output value of the build side does not matter,
                                // just insert default value
                                mcol[i + column_offset]->insert_default();
                            } else {
                                auto& column = *_build_blocks[_build_block_offsets[j]]
                                                        .get_by_position(i)
                                                        .column;
                                mcol[i + column_offset]->insert_from(column, _build_block_rows[j]);
                            }
                        }
                    }
                } else {
                    mcol[i + column_offset]->resize(size);
                }
            }
        }
    }

    // Dispose right tuple is null flags columns
    if constexpr (probe_all && !have_other_join_conjunct) {
        _tuple_is_null_right_flags.resize(size);
        auto* __restrict null_data = _tuple_is_null_right_flags.data();
        for (int i = 0; i < size; ++i) {
            null_data[i] = _build_block_rows[i] == -1;
        }
    }
}

template <class JoinOpType, bool ignore_null>
template <bool have_other_join_conjunct>
void ProcessHashTableProbe<JoinOpType, ignore_null>::probe_side_output_column(
        MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        int last_probe_index, size_t probe_size, bool all_match_one) {
    auto& probe_block = _join_node->_probe_block;
    for (int i = 0; i < output_slot_flags.size(); ++i) {
        if (output_slot_flags[i]) {
            auto& column = probe_block.get_by_position(i).column;
            if (all_match_one) {
                DCHECK_EQ(probe_size, column->size() - last_probe_index);
                mcol[i]->insert_range_from(*column, last_probe_index, probe_size);
            } else {
                DCHECK_GE(_items_counts.size(), last_probe_index + probe_size);
                column->replicate(&_items_counts[0], size, *mcol[i], last_probe_index, probe_size);
            }
        } else {
            mcol[i]->resize(size);
        }
    }

    if constexpr (JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN && !have_other_join_conjunct) {
        _tuple_is_null_left_flags.resize_fill(size, 0);
    }
}

template <class JoinOpType, bool ignore_null>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, ignore_null>::do_process(HashTableType& hash_table_ctx,
                                                                  ConstNullMapPtr null_map,
                                                                  MutableBlock& mutable_block,
                                                                  Block* output_block,
                                                                  size_t probe_rows) {
    auto& probe_index = _join_node->_probe_index;
    auto& probe_raw_ptrs = _join_node->_probe_columns;
    if (probe_index == 0 && _items_counts.size() < probe_rows) {
        _items_counts.resize(probe_rows);
    }

    if (_build_block_rows.size() < probe_rows * PROBE_SIDE_EXPLODE_RATE) {
        _build_block_rows.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
        _build_block_offsets.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
    }
    using KeyGetter = typename HashTableType::State;
    using Mapped = typename HashTableType::Mapped;

    int right_col_idx =
            _join_node->_is_right_semi_anti ? 0 : _join_node->_left_table_data_types.size();
    int right_col_len = _join_node->_right_table_data_types.size();

    KeyGetter key_getter(probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);
    auto& mcol = mutable_block.mutable_columns();
    int current_offset = 0;

    constexpr auto need_to_set_visited = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                         JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                         JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN ||
                                         JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

    constexpr auto is_right_semi_anti_join = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                             JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN;

    constexpr auto probe_all = JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                               JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

    bool all_match_one = true;
    int last_probe_index = probe_index;
    {
        SCOPED_TIMER(_search_hashtable_timer);
        while (probe_index < probe_rows) {
            if constexpr (ignore_null) {
                if ((*null_map)[probe_index]) {
                    _items_counts[probe_index++] = (uint32_t)0;
                    all_match_one = false;
                    continue;
                }
            }
            int last_offset = current_offset;
            auto find_result =
                    (*null_map)[probe_index]
                            ? decltype(key_getter.find_key(hash_table_ctx.hash_table, probe_index,
                                                           _arena)) {nullptr, false}
                            : key_getter.find_key(hash_table_ctx.hash_table, probe_index, _arena);
            if (probe_index + PREFETCH_STEP < probe_rows)
                key_getter.template prefetch<true>(hash_table_ctx.hash_table,
                                                   probe_index + PREFETCH_STEP, _arena);

            if constexpr (JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN) {
                if (!find_result.is_found()) {
                    ++current_offset;
                }
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN) {
                if (find_result.is_found()) {
                    ++current_offset;
                }
            } else {
                if (find_result.is_found()) {
                    auto& mapped = find_result.get_mapped();
                    // TODO: Iterators are currently considered to be a heavy operation and have a certain impact on performance.
                    // We should rethink whether to use this iterator mode in the future. Now just opt the one row case
                    if (mapped.get_row_count() == 1) {
                        if constexpr (need_to_set_visited) mapped.visited = true;

                        if constexpr (!is_right_semi_anti_join) {
                            if (LIKELY(current_offset < _build_block_rows.size())) {
                                _build_block_offsets[current_offset] = mapped.block_offset;
                                _build_block_rows[current_offset] = mapped.row_num;
                            } else {
                                _build_block_offsets.emplace_back(mapped.block_offset);
                                _build_block_rows.emplace_back(mapped.row_num);
                            }
                            ++current_offset;
                        }
                    } else {
                        for (auto it = mapped.begin(); it.ok(); ++it) {
                            if constexpr (!is_right_semi_anti_join) {
                                if (LIKELY(current_offset < _build_block_rows.size())) {
                                    _build_block_offsets[current_offset] = it->block_offset;
                                    _build_block_rows[current_offset] = it->row_num;
                                } else {
                                    _build_block_offsets.emplace_back(it->block_offset);
                                    _build_block_rows.emplace_back(it->row_num);
                                }
                                ++current_offset;
                            }
                            if constexpr (need_to_set_visited) it->visited = true;
                        }
                    }
                } else {
                    if constexpr (probe_all) {
                        // only full outer / left outer need insert the data of right table
                        if (LIKELY(current_offset < _build_block_rows.size())) {
                            _build_block_offsets[current_offset] = -1;
                            _build_block_rows[current_offset] = -1;
                        } else {
                            _build_block_offsets.emplace_back(-1);
                            _build_block_rows.emplace_back(-1);
                        }
                        ++current_offset;
                    }
                }
            }

            uint32_t count = (uint32_t)(current_offset - last_offset);
            _items_counts[probe_index++] = count;
            all_match_one &= (count == 1);
            if (current_offset >= _batch_size && !all_match_one) {
                break;
            }
        }
    }

    {
        SCOPED_TIMER(_build_side_output_timer);
        build_side_output_column(mcol, right_col_idx, right_col_len,
                                 _join_node->_right_output_slot_flags, current_offset);
    }

    if constexpr (JoinOpType::value != TJoinOp::RIGHT_SEMI_JOIN &&
                  JoinOpType::value != TJoinOp::RIGHT_ANTI_JOIN) {
        SCOPED_TIMER(_probe_side_output_timer);
        probe_side_output_column(mcol, _join_node->_left_output_slot_flags, current_offset,
                                 last_probe_index, probe_index - last_probe_index, all_match_one);
    }

    output_block->swap(mutable_block.to_block());

    return Status::OK();
}

template <class JoinOpType, bool ignore_null>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, ignore_null>::do_process_with_other_join_conjunts(
        HashTableType& hash_table_ctx, ConstNullMapPtr null_map, MutableBlock& mutable_block,
        Block* output_block, size_t probe_rows) {
    auto& probe_index = _join_node->_probe_index;
    auto& probe_raw_ptrs = _join_node->_probe_columns;
    if (probe_index == 0 && _items_counts.size() < probe_rows) {
        _items_counts.resize(probe_rows);
    }
    if (_build_block_rows.size() < probe_rows * PROBE_SIDE_EXPLODE_RATE) {
        _build_block_rows.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
        _build_block_offsets.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
    }

    using KeyGetter = typename HashTableType::State;
    using Mapped = typename HashTableType::Mapped;
    KeyGetter key_getter(probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);

    int right_col_idx = _join_node->_left_table_data_types.size();
    int right_col_len = _join_node->_right_table_data_types.size();

    auto& mcol = mutable_block.mutable_columns();
    // use in right join to change visited state after
    // exec the vother join conjunt
    std::vector<bool*> visited_map;
    visited_map.reserve(1.2 * _batch_size);

    std::vector<bool> same_to_prev;
    same_to_prev.reserve(1.2 * _batch_size);

    int current_offset = 0;

    bool all_match_one = true;
    int last_probe_index = probe_index;
    while (probe_index < probe_rows) {
        // ignore null rows
        if constexpr (ignore_null) {
            if ((*null_map)[probe_index]) {
                _items_counts[probe_index++] = (uint32_t)0;
                continue;
            }
        }

        auto last_offset = current_offset;
        auto find_result =
                (*null_map)[probe_index]
                        ? decltype(key_getter.find_key(hash_table_ctx.hash_table, probe_index,
                                                       _arena)) {nullptr, false}
                        : key_getter.find_key(hash_table_ctx.hash_table, probe_index, _arena);
        if (probe_index + PREFETCH_STEP < probe_rows)
            key_getter.template prefetch<true>(hash_table_ctx.hash_table,
                                               probe_index + PREFETCH_STEP, _arena);
        if (find_result.is_found()) {
            auto& mapped = find_result.get_mapped();
            auto origin_offset = current_offset;
            // TODO: Iterators are currently considered to be a heavy operation and have a certain impact on performance.
            // We should rethink whether to use this iterator mode in the future. Now just opt the one row case
            if (mapped.get_row_count() == 1) {
                if (LIKELY(current_offset < _build_block_rows.size())) {
                    _build_block_offsets[current_offset] = mapped.block_offset;
                    _build_block_rows[current_offset] = mapped.row_num;
                } else {
                    _build_block_offsets.emplace_back(mapped.block_offset);
                    _build_block_rows.emplace_back(mapped.row_num);
                }
                ++current_offset;
                visited_map.emplace_back(&mapped.visited);
            } else {
                for (auto it = mapped.begin(); it.ok(); ++it) {
                    if (LIKELY(current_offset < _build_block_rows.size())) {
                        _build_block_offsets[current_offset] = it->block_offset;
                        _build_block_rows[current_offset] = it->row_num;
                    } else {
                        _build_block_offsets.emplace_back(it->block_offset);
                        _build_block_rows.emplace_back(it->row_num);
                    }
                    ++current_offset;
                    visited_map.emplace_back(&it->visited);
                }
            }
            same_to_prev.emplace_back(false);
            for (int i = 0; i < current_offset - origin_offset - 1; ++i) {
                same_to_prev.emplace_back(true);
            }
        } else if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                             JoinOpType::value == TJoinOp::FULL_OUTER_JOIN ||
                             JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN) {
            same_to_prev.emplace_back(false);
            visited_map.emplace_back(nullptr);
            // only full outer / left outer need insert the data of right table
            // left anti use -1 use a default value
            if (LIKELY(current_offset < _build_block_rows.size())) {
                _build_block_offsets[current_offset] = -1;
                _build_block_rows[current_offset] = -1;
            } else {
                _build_block_offsets.emplace_back(-1);
                _build_block_rows.emplace_back(-1);
            }
            ++current_offset;
        } else {
            // other join, no nothing
        }

        uint32_t count = (uint32_t)(current_offset - last_offset);
        _items_counts[probe_index++] = count;
        all_match_one &= (count == 1);
        if (current_offset >= _batch_size && !all_match_one) {
            break;
        }
    }

    {
        SCOPED_TIMER(_build_side_output_timer);
        build_side_output_column<true>(mcol, right_col_idx, right_col_len,
                                       _join_node->_right_output_slot_flags, current_offset);
    }
    {
        SCOPED_TIMER(_probe_side_output_timer);
        probe_side_output_column<true>(mcol, _join_node->_left_output_slot_flags, current_offset,
                                       last_probe_index, probe_index - last_probe_index,
                                       all_match_one);
    }
    output_block->swap(mutable_block.to_block());

    // dispose the other join conjunt exec
    if (output_block->rows()) {
        int result_column_id = -1;
        int orig_columns = output_block->columns();
        (*_join_node->_vother_join_conjunct_ptr)->execute(output_block, &result_column_id);

        auto column = output_block->get_by_position(result_column_id).column;
        if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                      JoinOpType::value == TJoinOp::FULL_OUTER_JOIN) {
            auto new_filter_column = ColumnVector<UInt8>::create();
            auto& filter_map = new_filter_column->get_data();

            auto null_map_column = ColumnVector<UInt8>::create(column->size(), 0);
            auto* __restrict null_map_data = null_map_column->get_data().data();

            for (int i = 0; i < column->size(); ++i) {
                auto join_hit = visited_map[i] != nullptr;
                auto other_hit = column->get_bool(i);

                if (!other_hit) {
                    for (size_t j = 0; j < right_col_len; ++j) {
                        typeid_cast<ColumnNullable*>(
                                std::move(*output_block->get_by_position(j + right_col_idx).column)
                                        .assume_mutable()
                                        .get())
                                ->get_null_map_data()[i] = true;
                    }
                }
                null_map_data[i] = !join_hit || !other_hit;

                if (join_hit) {
                    *visited_map[i] |= other_hit;
                    filter_map.push_back(other_hit || !same_to_prev[i] ||
                                         (!column->get_bool(i - 1) && filter_map.back()));
                    // Here to keep only hit join conjunt and other join conjunt is true need to be output.
                    // if not, only some key must keep one row will output will null right table column
                    if (same_to_prev[i] && filter_map.back() && !column->get_bool(i - 1))
                        filter_map[i - 1] = false;
                } else {
                    filter_map.push_back(true);
                }
            }

            for (int i = 0; i < column->size(); ++i) {
                if (filter_map[i]) {
                    _tuple_is_null_right_flags.emplace_back(null_map_data[i]);
                }
            }
            output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
        } else if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN) {
            auto new_filter_column = ColumnVector<UInt8>::create();
            auto& filter_map = new_filter_column->get_data();

            if (!column->empty()) filter_map.emplace_back(column->get_bool(0));
            for (int i = 1; i < column->size(); ++i) {
                if (column->get_bool(i) || (same_to_prev[i] && filter_map[i - 1])) {
                    // Only last same element is true, output last one
                    filter_map.push_back(true);
                    filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                } else {
                    filter_map.push_back(false);
                }
            }

            output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
        } else if constexpr (JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN) {
            auto new_filter_column = ColumnVector<UInt8>::create();
            auto& filter_map = new_filter_column->get_data();

            if (!column->empty()) filter_map.emplace_back(column->get_bool(0) && visited_map[0]);
            for (int i = 1; i < column->size(); ++i) {
                if ((visited_map[i] && column->get_bool(i)) ||
                    (same_to_prev[i] && filter_map[i - 1])) {
                    filter_map.push_back(true);
                    filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                } else {
                    filter_map.push_back(false);
                }
            }

            // Same to the semi join, but change the last value to opposite value
            for (int i = 1; i < same_to_prev.size(); ++i) {
                if (!same_to_prev[i]) filter_map[i - 1] = !filter_map[i - 1];
            }
            filter_map[same_to_prev.size() - 1] = !filter_map[same_to_prev.size() - 1];

            output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
        } else if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                             JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN) {
            for (int i = 0; i < column->size(); ++i) {
                DCHECK(visited_map[i]);
                *visited_map[i] |= column->get_bool(i);
            }
        } else if constexpr (JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN) {
            auto filter_size = 0;
            for (int i = 0; i < column->size(); ++i) {
                DCHECK(visited_map[i]);
                auto result = column->get_bool(i);
                *visited_map[i] |= result;
                filter_size += result;
            }
            _tuple_is_null_left_flags.resize_fill(filter_size, 0);
        } else {
            // inner join do nothing
        }

        if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                      JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN) {
            output_block->clear();
        } else {
            if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN ||
                          JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN)
                orig_columns = right_col_idx;
            Block::filter_block(output_block, result_column_id, orig_columns);
        }
    }

    return Status::OK();
}

template <class JoinOpType, bool ignore_null>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, ignore_null>::process_data_in_hashtable(
        HashTableType& hash_table_ctx, MutableBlock& mutable_block, Block* output_block,
        bool* eos) {
    hash_table_ctx.init_once();
    auto& mcol = mutable_block.mutable_columns();

    bool right_semi_anti_without_other =
            _join_node->_is_right_semi_anti && !_join_node->_have_other_join_conjunct;
    int right_col_idx =
            right_semi_anti_without_other ? 0 : _join_node->_left_table_data_types.size();
    int right_col_len = _join_node->_right_table_data_types.size();

    auto& iter = hash_table_ctx.iter;
    auto block_size = 0;

    auto insert_from_hash_table = [&](uint8_t offset, uint32_t row_num) {
        block_size++;
        for (size_t j = 0; j < right_col_len; ++j) {
            auto& column = *_build_blocks[offset].get_by_position(j).column;
            mcol[j + right_col_idx]->insert_from(column, row_num);
        }
    };

    for (; iter != hash_table_ctx.hash_table.end() && block_size < _batch_size; ++iter) {
        auto& mapped = iter->get_second();
        for (auto it = mapped.begin(); it.ok(); ++it) {
            if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN) {
                if (it->visited) insert_from_hash_table(it->block_offset, it->row_num);
            } else {
                if (!it->visited) insert_from_hash_table(it->block_offset, it->row_num);
            }
        }
    }

    // just resize the left table column in case with other conjunct to make block size is not zero
    if (_join_node->_is_right_semi_anti && _join_node->_have_other_join_conjunct) {
        auto target_size = mcol[right_col_idx]->size();
        for (int i = 0; i < right_col_idx; ++i) {
            mcol[i]->resize(target_size);
        }
    }

    // right outer join / full join need insert data of left table
    if constexpr (JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN ||
                  JoinOpType::value == TJoinOp::FULL_OUTER_JOIN) {
        for (int i = 0; i < right_col_idx; ++i) {
            assert_cast<ColumnNullable*>(mcol[i].get())->insert_many_defaults(block_size);
        }
        _tuple_is_null_left_flags.resize_fill(block_size, 1);
    }
    *eos = iter == hash_table_ctx.hash_table.end();

    output_block->swap(mutable_block.to_block(right_semi_anti_without_other ? right_col_idx : 0));
    return Status::OK();
}

HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.hash_join_node.join_op),
          _hash_table_rows(0),
          _mem_used(0),
          _match_all_probe(_join_op == TJoinOp::LEFT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _match_one_build(_join_op == TJoinOp::LEFT_SEMI_JOIN),
          _match_all_build(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _build_unique(_join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN),
          _is_right_semi_anti(_join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                              _join_op == TJoinOp::RIGHT_SEMI_JOIN),
          _is_outer_join(_match_all_build || _match_all_probe),
          _hash_output_slot_ids(tnode.hash_join_node.__isset.hash_output_slot_ids
                                        ? tnode.hash_join_node.hash_output_slot_ids
                                        : std::vector<SlotId> {}),
          _intermediate_row_desc(
                  descs, tnode.hash_join_node.vintermediate_tuple_id_list,
                  std::vector<bool>(tnode.hash_join_node.vintermediate_tuple_id_list.size())),
          _output_row_desc(descs, {tnode.hash_join_node.voutput_tuple_id}, {false}) {
    _runtime_filter_descs = tnode.runtime_filters;
    init_join_op();

    // avoid vector expand change block address.
    // one block can store 4g data, _build_blocks can store 128*4g data.
    // if probe data bigger than 512g, runtime filter maybe will core dump when insert data.
    _build_blocks.reserve(_MAX_BUILD_BLOCK_COUNT);
}

HashJoinNode::~HashJoinNode() = default;

void HashJoinNode::init_join_op() {
    switch (_join_op) {
#define M(NAME)                                                                            \
    case TJoinOp::NAME:                                                                    \
        _join_op_variants.emplace<std::integral_constant<TJoinOp::type, TJoinOp::NAME>>(); \
        break;
        APPLY_FOR_JOINOP_VARIANTS(M);
#undef M
    default:
        //do nothing
        break;
    }
}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    if (tnode.hash_join_node.join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        return Status::InternalError("Do not support null aware left anti join");
    }

    const bool build_stores_null = _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                                   _join_op == TJoinOp::FULL_OUTER_JOIN ||
                                   _join_op == TJoinOp::RIGHT_ANTI_JOIN;
    const bool probe_dispose_null =
            _match_all_probe || _build_unique || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjunct.right, &ctx));
        _build_expr_ctxs.push_back(ctx);

        bool null_aware = eq_join_conjunct.__isset.opcode &&
                          eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL;
        _is_null_safe_eq_join.push_back(null_aware);

        // if is null aware, build join column and probe join column both need dispose null value
        _build_not_ignore_null.emplace_back(
                null_aware ||
                (_build_expr_ctxs.back()->root()->is_nullable() && build_stores_null));
        _probe_not_ignore_null.emplace_back(
                null_aware ||
                (_probe_expr_ctxs.back()->root()->is_nullable() && probe_dispose_null));
    }
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        _probe_ignore_null |= !_probe_not_ignore_null[i];
    }

    _probe_column_disguise_null.reserve(eq_join_conjuncts.size());

    if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _vother_join_conjunct_ptr.reset(new doris::vectorized::VExprContext*);
        RETURN_IF_ERROR(doris::vectorized::VExpr::create_expr_tree(
                _pool, tnode.hash_join_node.vother_join_conjunct, _vother_join_conjunct_ptr.get()));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        _build_unique = false;
        _have_other_join_conjunct = true;
    }

    const auto& output_exprs = tnode.hash_join_node.srcExprList;
    for (const auto& expr : output_exprs) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, expr, &ctx));
        _output_expr_ctxs.push_back(ctx);
    }

    for (const auto& filter_desc : _runtime_filter_descs) {
        RETURN_IF_ERROR(state->runtime_filter_mgr()->regist_filter(
                RuntimeFilterRole::PRODUCER, filter_desc, state->query_options()));
    }

    // init left/right output slots flags, only column of slot_id in _hash_output_slot_ids need
    // insert to output block of hash join.
    // _left_output_slots_flags : column of left table need to output set flag = true
    // _rgiht_output_slots_flags : column of right table need to output set flag = true
    // if _hash_output_slot_ids is empty, means all column of left/right table need to output.
    auto init_output_slots_flags = [this](auto& tuple_descs, auto& output_slot_flags) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        _hash_output_slot_ids.empty() ||
                        std::find(_hash_output_slot_ids.begin(), _hash_output_slot_ids.end(),
                                  slot_desc->id()) != _hash_output_slot_ids.end());
            }
        }
    };
    init_output_slots_flags(child(0)->row_desc().tuple_descriptors(), _left_output_slot_flags);
    init_output_slots_flags(child(1)->row_desc().tuple_descriptors(), _right_output_slot_flags);

    // only use in outer join as the bool column to mark for function of `tuple_is_null`
    if (_is_outer_join) {
        _tuple_is_null_left_flag_column = ColumnUInt8::create();
        _tuple_is_null_right_flag_column = ColumnUInt8::create();
    }
    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    DCHECK(_runtime_profile.get() != nullptr);
    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
    _rows_returned_rate = runtime_profile()->add_derived_counter(
            ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _rows_returned_counter,
                               runtime_profile()->total_time_counter()),
            "");
    _mem_tracker = std::make_unique<MemTracker>("ExecNode:" + _runtime_profile->name(),
                                                _runtime_profile.get());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    if (_vconjunct_ctx_ptr) {
        RETURN_IF_ERROR((*_vconjunct_ctx_ptr)->prepare(state, _intermediate_row_desc));
    }

    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state));
    }

    // Build phase
    auto build_phase_profile = runtime_profile()->create_child("BuildPhase", true, true);
    runtime_profile()->add_child(build_phase_profile, false, nullptr);
    _build_timer = ADD_TIMER(build_phase_profile, "BuildTime");
    _build_table_timer = ADD_TIMER(build_phase_profile, "BuildTableTime");
    _build_side_merge_block_timer = ADD_TIMER(build_phase_profile, "BuildSideMergeBlockTime");
    _build_table_insert_timer = ADD_TIMER(build_phase_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(build_phase_profile, "BuildExprCallTime");
    _build_table_expanse_timer = ADD_TIMER(build_phase_profile, "BuildTableExpanseTime");
    _build_rows_counter = ADD_COUNTER(build_phase_profile, "BuildRows", TUnit::UNIT);
    _build_side_compute_hash_timer = ADD_TIMER(build_phase_profile, "BuildSideHashComputingTime");

    // Probe phase
    auto probe_phase_profile = runtime_profile()->create_child("ProbePhase", true, true);
    _probe_timer = ADD_TIMER(probe_phase_profile, "ProbeTime");
    _probe_next_timer = ADD_TIMER(probe_phase_profile, "ProbeFindNextTime");
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");
    _probe_rows_counter = ADD_COUNTER(probe_phase_profile, "ProbeRows", TUnit::UNIT);
    _search_hashtable_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenSearchHashTableTime");
    _build_side_output_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenBuildSideOutputTime");
    _probe_side_output_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenProbeSideOutputTime");

    _join_filter_timer = ADD_TIMER(runtime_profile(), "JoinFilterTimer");

    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);

    RETURN_IF_ERROR(VExpr::prepare(_build_expr_ctxs, state, child(1)->row_desc()));
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc()));

    // _vother_join_conjuncts are evaluated in the context of the rows produced by this node
    if (_vother_join_conjunct_ptr) {
        RETURN_IF_ERROR((*_vother_join_conjunct_ptr)->prepare(state, _intermediate_row_desc));
    }
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, _intermediate_row_desc));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_projections, state, _intermediate_row_desc));

    // right table data types
    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());

    // Hash Table Init
    _hash_table_init();
    _process_hashtable_ctx_variants_init(state);
    _construct_mutable_join_block();

    return Status::OK();
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    START_AND_SCOPE_SPAN(state->get_tracer(), span, "ashJoinNode::close");
    VExpr::close(_build_expr_ctxs, state);
    VExpr::close(_probe_expr_ctxs, state);

    if (_vother_join_conjunct_ptr) (*_vother_join_conjunct_ptr)->close(state);
    VExpr::close(_output_expr_ctxs, state);

    return ExecNode::close(state);
}

Status HashJoinNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented HashJoin Node::get_next scalar");
}

Status HashJoinNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "HashJoinNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);

    size_t probe_rows = _probe_block.rows();
    if ((probe_rows == 0 || _probe_index == probe_rows) && !_probe_eos) {
        _probe_index = 0;
        _prepare_probe_block();

        do {
            SCOPED_TIMER(_probe_next_timer);
            RETURN_IF_ERROR_AND_CHECK_SPAN(
                    child(0)->get_next_after_projects(state, &_probe_block, &_probe_eos),
                    child(0)->get_next_span(), _probe_eos);
        } while (_probe_block.rows() == 0 && !_probe_eos);

        probe_rows = _probe_block.rows();
        if (probe_rows != 0) {
            COUNTER_UPDATE(_probe_rows_counter, probe_rows);
            if (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
                _probe_column_convert_to_null = _convert_block_to_null(_probe_block);
            }

            int probe_expr_ctxs_sz = _probe_expr_ctxs.size();
            _probe_columns.resize(probe_expr_ctxs_sz);
            if (_null_map_column == nullptr) {
                _null_map_column = ColumnUInt8::create();
            }
            _null_map_column->get_data().assign(probe_rows, (uint8_t)0);

            Status st = std::visit(
                    [&](auto&& arg) -> Status {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            auto& null_map_val = _null_map_column->get_data();
                            return _extract_probe_join_column(_probe_block, null_map_val,
                                                              _probe_columns,
                                                              *_probe_expr_call_timer);
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table";
                        }
                        __builtin_unreachable();
                    },
                    _hash_table_variants);

            RETURN_IF_ERROR(st);
        }
    }

    Status st;
    _join_block.clear_column_data();
    MutableBlock mutable_join_block(&_join_block);
    Block temp_block;

    if (_probe_index < _probe_block.rows()) {
        std::visit(
                [&](auto&& arg, auto&& process_hashtable_ctx, auto have_other_join_conjunct) {
                    using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                    if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (have_other_join_conjunct) {
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                st = process_hashtable_ctx.do_process_with_other_join_conjunts(
                                        arg, &_null_map_column->get_data(), mutable_join_block,
                                        &temp_block, probe_rows);
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        } else {
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                st = process_hashtable_ctx.do_process(
                                        arg, &_null_map_column->get_data(), mutable_join_block,
                                        &temp_block, probe_rows);
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        }
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash probe";
                    }
                },
                _hash_table_variants, _process_hashtable_ctx_variants,
                make_bool_variant(_have_other_join_conjunct));
    } else if (_probe_eos) {
        if (_is_right_semi_anti || (_is_outer_join && _join_op != TJoinOp::LEFT_OUTER_JOIN)) {
            std::visit(
                    [&](auto&& arg, auto&& process_hashtable_ctx) {
                        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                st = process_hashtable_ctx.process_data_in_hashtable(
                                        arg, mutable_join_block, &temp_block, eos);
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table";
                        }
                    },
                    _hash_table_variants, _process_hashtable_ctx_variants);
        } else {
            *eos = true;
            return Status::OK();
        }
    } else {
        return Status::OK();
    }

    _add_tuple_is_null_column(&temp_block);
    {
        SCOPED_TIMER(_join_filter_timer);
        RETURN_IF_ERROR(
                VExprContext::filter_block(_vconjunct_ctx_ptr, &temp_block, temp_block.columns()));
    }
    RETURN_IF_ERROR(_build_output_block(&temp_block, output_block));
    _reset_tuple_is_null_column();
    reached_limit(output_block, eos);

    return st;
}

void HashJoinNode::_prepare_probe_block() {
    // clear_column_data of _probe_block
    if (!_probe_column_disguise_null.empty()) {
        for (int i = 0; i < _probe_column_disguise_null.size(); ++i) {
            auto column_to_erase = _probe_column_disguise_null[i];
            _probe_block.erase(column_to_erase - i);
        }
        _probe_column_disguise_null.clear();
    }

    // remove add nullmap of probe columns
    for (auto index : _probe_column_convert_to_null) {
        auto& column_type = _probe_block.safe_get_by_position(index);
        DCHECK(column_type.column->is_nullable());
        DCHECK(column_type.type->is_nullable());

        column_type.column = remove_nullable(column_type.column);
        column_type.type = remove_nullable(column_type.type);
    }
    release_block_memory(_probe_block);
}

void HashJoinNode::_construct_mutable_join_block() {
    const auto& mutable_block_desc = _intermediate_row_desc;
    for (const auto tuple_desc : mutable_block_desc.tuple_descriptors()) {
        for (const auto slot_desc : tuple_desc->slots()) {
            auto type_ptr = slot_desc->get_data_type_ptr();
            _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
        }
    }
}

Status HashJoinNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "HashJoinNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    if (_vother_join_conjunct_ptr) {
        RETURN_IF_ERROR((*_vother_join_conjunct_ptr)->open(state));
    }
    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));

    std::promise<Status> thread_status;
    std::thread([this, state, thread_status_p = &thread_status,
                 parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
        OpentelemetryScope scope {parent_span};
        this->_hash_table_build_thread(state, thread_status_p);
    }).detach();

    // Open the probe-side child so that it may perform any initialisation in parallel.
    // Don't exit even if we see an error, we still need to wait for the build thread
    // to finish.
    // ISSUE-1247, check open_status after buildThread execute.
    // If this return first, build thread will use 'thread_status'
    // which is already destructor and then coredump.
    Status open_status = child(0)->open(state);
    RETURN_IF_ERROR(thread_status.get_future().get());
    return open_status;
}

void HashJoinNode::_hash_table_build_thread(RuntimeState* state, std::promise<Status>* status) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "HashJoinNode::_hash_table_build_thread");
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    status->set_value(_hash_table_build(state));
}

Status HashJoinNode::_hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(1)->open(state));
    SCOPED_TIMER(_build_timer);
    MutableBlock mutable_block(child(1)->row_desc().tuple_descriptors());

    uint8_t index = 0;
    int64_t last_mem_used = 0;
    bool eos = false;

    // make one block for each 4 gigabytes
    constexpr static auto BUILD_BLOCK_MAX_SIZE = 4 * 1024UL * 1024UL * 1024UL;

    Block block;
    while (!eos) {
        block.clear_column_data();
        RETURN_IF_CANCELLED(state);

        RETURN_IF_ERROR_AND_CHECK_SPAN(child(1)->get_next_after_projects(state, &block, &eos),
                                       child(1)->get_next_span(), eos);
        _mem_used += block.allocated_bytes();

        if (block.rows() != 0) {
            SCOPED_TIMER(_build_side_merge_block_timer);
            mutable_block.merge(block);
        }

        if (UNLIKELY(_mem_used - last_mem_used > BUILD_BLOCK_MAX_SIZE)) {
            if (_build_blocks.size() == _MAX_BUILD_BLOCK_COUNT) {
                return Status::NotSupported(
                        strings::Substitute("data size of right table in hash join > $0",
                                            BUILD_BLOCK_MAX_SIZE * _MAX_BUILD_BLOCK_COUNT));
            }
            _build_blocks.emplace_back(mutable_block.to_block());
            // TODO:: Rethink may we should do the proess after we recevie all build blocks ?
            // which is better.
            RETURN_IF_ERROR(_process_build_block(state, _build_blocks[index], index));

            mutable_block = MutableBlock();
            ++index;
            last_mem_used = _mem_used;
        }
    }

    if (!mutable_block.empty()) {
        if (_build_blocks.size() == _MAX_BUILD_BLOCK_COUNT) {
            return Status::NotSupported(
                    strings::Substitute("data size of right table in hash join > $0",
                                        BUILD_BLOCK_MAX_SIZE * _MAX_BUILD_BLOCK_COUNT));
        }
        _build_blocks.emplace_back(mutable_block.to_block());
        RETURN_IF_ERROR(_process_build_block(state, _build_blocks[index], index));
    }

    return std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    ProcessRuntimeFilterBuild<HashTableCtxType> runtime_filter_build_process(this);
                    return runtime_filter_build_process(state, arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);
}

// TODO:: unify the code of extract probe join column
Status HashJoinNode::_extract_build_join_column(Block& block, NullMap& null_map,
                                                ColumnRawPtrs& raw_ptrs, bool& ignore_null,
                                                RuntimeProfile::Counter& expr_call_timer) {
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(_build_expr_ctxs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();

        if (_is_null_safe_eq_join[i]) {
            raw_ptrs[i] = block.get_by_position(result_col_id).column.get();
        } else {
            auto column = block.get_by_position(result_col_id).column.get();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& col_nested = nullable->get_nested_column();
                auto& col_nullmap = nullable->get_null_map_data();

                ignore_null |= !_build_not_ignore_null[i];
                if (_build_not_ignore_null[i]) {
                    raw_ptrs[i] = nullable;
                } else {
                    VectorizedUtils::update_null_map(null_map, col_nullmap);
                    raw_ptrs[i] = &col_nested;
                }
            } else {
                raw_ptrs[i] = column;
            }
        }
    }
    return Status::OK();
}

Status HashJoinNode::_extract_probe_join_column(Block& block, NullMap& null_map,
                                                ColumnRawPtrs& raw_ptrs,
                                                RuntimeProfile::Counter& expr_call_timer) {
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();

        if (_is_null_safe_eq_join[i]) {
            raw_ptrs[i] = block.get_by_position(result_col_id).column.get();
        } else {
            auto column = block.get_by_position(result_col_id).column.get();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& col_nested = nullable->get_nested_column();
                auto& col_nullmap = nullable->get_null_map_data();

                VectorizedUtils::update_null_map(null_map, col_nullmap);
                if (_build_not_ignore_null[i]) {
                    raw_ptrs[i] = nullable;
                } else {
                    raw_ptrs[i] = &col_nested;
                }
            } else {
                if (_build_not_ignore_null[i]) {
                    auto column_ptr =
                            make_nullable(block.get_by_position(result_col_id).column, false);
                    _probe_column_disguise_null.emplace_back(block.columns());
                    block.insert({column_ptr,
                                  make_nullable(block.get_by_position(result_col_id).type), ""});
                    column = column_ptr.get();
                }
                raw_ptrs[i] = column;
            }
        }
    }
    return Status::OK();
}

Status HashJoinNode::_process_build_block(RuntimeState* state, Block& block, uint8_t offset) {
    SCOPED_TIMER(_build_table_timer);
    if (_join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
        _convert_block_to_null(block);
    }
    size_t rows = block.rows();
    if (UNLIKELY(rows == 0)) {
        return Status::OK();
    }
    COUNTER_UPDATE(_build_rows_counter, rows);

    ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

    NullMap null_map_val(rows);
    null_map_val.assign(rows, (uint8_t)0);
    bool has_null = false;

    // Get the key column that needs to be built
    Status st = std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return _extract_build_join_column(block, null_map_val, raw_ptrs, has_null,
                                                      *_build_expr_call_timer);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
                __builtin_unreachable();
            },
            _hash_table_variants);

    bool has_runtime_filter = !_runtime_filter_descs.empty();

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    ProcessHashTableBuild<HashTableCtxType> hash_table_build_process(
                            rows, block, raw_ptrs, this, state->batch_size(), offset);

                    constexpr_3_bool_match<ProcessHashTableBuild<
                            HashTableCtxType>::template Reducer>::run(has_null, _build_unique,
                                                                      has_runtime_filter,
                                                                      hash_table_build_process, arg,
                                                                      &null_map_val);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return st;
}

void HashJoinNode::_hash_table_init() {
    if (_build_expr_ctxs.size() == 1 && !_build_not_ignore_null[0]) {
        // Single column optimization
        switch (_build_expr_ctxs[0]->root()->result_type()) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            _hash_table_variants.emplace<I8HashTableContext>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants.emplace<I16HashTableContext>();
            break;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
            _hash_table_variants.emplace<I32HashTableContext>();
            break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATETIME:
        case TYPE_DATE:
        case TYPE_DATETIMEV2:
            _hash_table_variants.emplace<I64HashTableContext>();
            break;
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128: {
            DataTypePtr& type_ptr = _build_expr_ctxs[0]->root()->data_type();
            TypeIndex idx = _build_expr_ctxs[0]->root()->is_nullable()
                                    ? assert_cast<const DataTypeNullable&>(*type_ptr)
                                              .get_nested_type()
                                              ->get_type_id()
                                    : type_ptr->get_type_id();
            WhichDataType which(idx);
            if (which.is_decimal32()) {
                _hash_table_variants.emplace<I32HashTableContext>();
            } else if (which.is_decimal64()) {
                _hash_table_variants.emplace<I64HashTableContext>();
            } else {
                _hash_table_variants.emplace<I128HashTableContext>();
            }
            break;
        }
        default:
            _hash_table_variants.emplace<SerializedHashTableContext>();
        }
        return;
    }

    bool use_fixed_key = true;
    bool has_null = false;
    int key_byte_size = 0;

    _probe_key_sz.resize(_probe_expr_ctxs.size());
    _build_key_sz.resize(_build_expr_ctxs.size());

    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        const auto vexpr = _build_expr_ctxs[i]->root();
        const auto& data_type = vexpr->data_type();

        if (!data_type->have_maximum_size_of_value()) {
            use_fixed_key = false;
            break;
        }

        auto is_null = data_type->is_nullable();
        has_null |= is_null;
        _build_key_sz[i] = data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
        _probe_key_sz[i] = _build_key_sz[i];
        key_byte_size += _probe_key_sz[i];
    }

    if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        // TODO: may we should support uint256 in the future
        if (has_null) {
            if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<true>>();
            } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                       sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<true>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<true>>();
            }
        } else {
            if (key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<false>>();
            } else if (key_byte_size <= sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<false>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<false>>();
            }
        }
    } else {
        _hash_table_variants.emplace<SerializedHashTableContext>();
    }
}

void HashJoinNode::_process_hashtable_ctx_variants_init(RuntimeState* state) {
    std::visit(
            [&](auto&& join_op_variants, auto probe_ignore_null) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                _process_hashtable_ctx_variants
                        .emplace<ProcessHashTableProbe<JoinOpType, probe_ignore_null>>(
                                this, state->batch_size());
            },
            _join_op_variants, make_bool_variant(_probe_ignore_null));
}

std::vector<uint16_t> HashJoinNode::_convert_block_to_null(Block& block) {
    std::vector<uint16_t> results;
    for (int i = 0; i < block.columns(); ++i) {
        if (auto& column_type = block.safe_get_by_position(i); !column_type.type->is_nullable()) {
            DCHECK(!column_type.column->is_nullable());
            column_type.column = make_nullable(column_type.column);
            column_type.type = make_nullable(column_type.type);
            results.emplace_back(i);
        }
    }
    return results;
}

Status HashJoinNode::_build_output_block(Block* origin_block, Block* output_block) {
    auto is_mem_reuse = output_block->mem_reuse();
    MutableBlock mutable_block =
            is_mem_reuse ? MutableBlock(output_block)
                         : MutableBlock(VectorizedUtils::create_empty_columnswithtypename(
                                   _output_row_desc));
    auto rows = origin_block->rows();
    // TODO: After FE plan support same nullable of output expr and origin block and mutable column
    // we should replace `insert_column_datas` by `insert_range_from`

    auto insert_column_datas = [](auto& to, const auto& from, size_t rows) {
        if (to->is_nullable() && !from.is_nullable()) {
            auto& null_column = reinterpret_cast<ColumnNullable&>(*to);
            null_column.get_nested_column().insert_range_from(from, 0, rows);
            null_column.get_null_map_column().get_data().resize_fill(rows, 0);
        } else {
            to->insert_range_from(from, 0, rows);
        }
    };
    if (rows != 0) {
        auto& mutable_columns = mutable_block.mutable_columns();
        if (_output_expr_ctxs.empty()) {
            DCHECK(mutable_columns.size() == _output_row_desc.num_materialized_slots());
            for (int i = 0; i < mutable_columns.size(); ++i) {
                insert_column_datas(mutable_columns[i], *origin_block->get_by_position(i).column,
                                    rows);
            }
        } else {
            DCHECK(mutable_columns.size() == _output_row_desc.num_materialized_slots());
            for (int i = 0; i < mutable_columns.size(); ++i) {
                auto result_column_id = -1;
                RETURN_IF_ERROR(_output_expr_ctxs[i]->execute(origin_block, &result_column_id));
                auto column_ptr = origin_block->get_by_position(result_column_id)
                                          .column->convert_to_full_column_if_const();
                insert_column_datas(mutable_columns[i], *column_ptr, rows);
            }
        }

        if (!is_mem_reuse) {
            output_block->swap(mutable_block.to_block());
        }
        DCHECK(output_block->rows() == rows);
    }

    return Status::OK();
}

void HashJoinNode::_add_tuple_is_null_column(doris::vectorized::Block* block) {
    if (_is_outer_join) {
        auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
        auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
        auto& left_null_map = reinterpret_cast<ColumnUInt8&>(*p0);
        auto& right_null_map = reinterpret_cast<ColumnUInt8&>(*p1);
        auto left_size = left_null_map.size();
        auto right_size = right_null_map.size();

        if (left_size == 0) {
            DCHECK_EQ(right_size, block->rows());
            left_null_map.get_data().resize_fill(right_size, 0);
        }
        if (right_size == 0) {
            DCHECK_EQ(left_size, block->rows());
            right_null_map.get_data().resize_fill(left_size, 0);
        }

        block->insert({std::move(p0), std::make_shared<vectorized::DataTypeUInt8>(),
                       "left_tuples_is_null"});
        block->insert({std::move(p1), std::make_shared<vectorized::DataTypeUInt8>(),
                       "right_tuples_is_null"});
    }
}

void HashJoinNode::_reset_tuple_is_null_column() {
    if (_is_outer_join) {
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_left_flag_column).clear();
        reinterpret_cast<ColumnUInt8&>(*_tuple_is_null_right_flag_column).clear();
    }
}

} // namespace doris::vectorized
