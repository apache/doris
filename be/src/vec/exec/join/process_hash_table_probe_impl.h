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

#include "common/status.h"
#include "process_hash_table_probe.h"
#include "vhash_join_node.h"

namespace doris::vectorized {

static constexpr int PREFETCH_STEP = HashJoinNode::PREFETCH_STEP;

template <int JoinOpType>
ProcessHashTableProbe<JoinOpType>::ProcessHashTableProbe(HashJoinNode* join_node, int batch_size)
        : _join_node(join_node),
          _batch_size(batch_size),
          _build_blocks(*join_node->_build_blocks),
          _tuple_is_null_left_flags(join_node->_is_outer_join
                                            ? &(reinterpret_cast<ColumnUInt8&>(
                                                        *join_node->_tuple_is_null_left_flag_column)
                                                        .get_data())
                                            : nullptr),
          _tuple_is_null_right_flags(
                  join_node->_is_outer_join
                          ? &(reinterpret_cast<ColumnUInt8&>(
                                      *join_node->_tuple_is_null_right_flag_column)
                                      .get_data())
                          : nullptr),
          _rows_returned_counter(join_node->_rows_returned_counter),
          _search_hashtable_timer(join_node->_search_hashtable_timer),
          _build_side_output_timer(join_node->_build_side_output_timer),
          _probe_side_output_timer(join_node->_probe_side_output_timer) {}

template <int JoinOpType>
template <bool have_other_join_conjunct>
void ProcessHashTableProbe<JoinOpType>::build_side_output_column(
        MutableColumns& mcol, int column_offset, int column_length,
        const std::vector<bool>& output_slot_flags, int size) {
    constexpr auto is_semi_anti_join = JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                                       JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::LEFT_SEMI_JOIN;

    constexpr auto probe_all =
            JoinOpType == TJoinOp::LEFT_OUTER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN;

    if constexpr (!is_semi_anti_join || have_other_join_conjunct) {
        if (_build_blocks.size() == 1) {
            for (int i = 0; i < column_length; i++) {
                auto& column = *_build_blocks[0].get_by_position(i).column;
                if (output_slot_flags[i]) {
                    mcol[i + column_offset]->insert_indices_from(column, _build_block_rows.data(),
                                                                 _build_block_rows.data() + size);
                } else {
                    mcol[i + column_offset]->insert_many_defaults(size);
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
                    mcol[i + column_offset]->insert_many_defaults(size);
                }
            }
        }
    }

    // Dispose right tuple is null flags columns
    if constexpr (probe_all && !have_other_join_conjunct) {
        _tuple_is_null_right_flags->resize(size);
        auto* __restrict null_data = _tuple_is_null_right_flags->data();
        for (int i = 0; i < size; ++i) {
            null_data[i] = _build_block_rows[i] == -1;
        }
    }
}

template <int JoinOpType>
void ProcessHashTableProbe<JoinOpType>::probe_side_output_column(
        MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        int last_probe_index, size_t probe_size, bool all_match_one,
        bool have_other_join_conjunct) {
    auto& probe_block = _join_node->_probe_block;
    for (int i = 0; i < output_slot_flags.size(); ++i) {
        if (output_slot_flags[i]) {
            auto& column = probe_block.get_by_position(i).column;
            if (all_match_one) {
                mcol[i]->insert_range_from(*column, last_probe_index, probe_size);
            } else {
                DCHECK_GE(_items_counts.size(), last_probe_index + probe_size);
                column->replicate(&_items_counts[0], size, *mcol[i], last_probe_index, probe_size);
            }
        } else {
            mcol[i]->insert_many_defaults(size);
        }
    }

    if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
        if (!have_other_join_conjunct) {
            _tuple_is_null_left_flags->resize_fill(size, 0);
        }
    }
}

template <int JoinOpType>
template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType>
Status ProcessHashTableProbe<JoinOpType>::do_process(HashTableType& hash_table_ctx,
                                                     ConstNullMapPtr null_map,
                                                     MutableBlock& mutable_block,
                                                     Block* output_block, size_t probe_rows,
                                                     bool is_mark_join) {
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

    if (probe_index == 0) {
        size_t old_probe_keys_memory_usage = 0;
        if (_arena) {
            old_probe_keys_memory_usage = _arena->size();
        }
        _arena.reset(new Arena());
        if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
            if (_probe_keys.size() < probe_rows) {
                _probe_keys.resize(probe_rows);
            }
            size_t keys_size = probe_raw_ptrs.size();
            for (size_t i = 0; i < probe_rows; ++i) {
                _probe_keys[i] =
                        serialize_keys_to_pool_contiguous(i, keys_size, probe_raw_ptrs, *_arena);
            }
            _join_node->_probe_arena_memory_usage->add(_arena->size() -
                                                       old_probe_keys_memory_usage);
        }
    }

    if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
        key_getter.set_serialized_keys(_probe_keys.data());
    }

    auto& mcol = mutable_block.mutable_columns();
    int current_offset = 0;

    constexpr auto is_right_semi_anti_join =
            JoinOpType == TJoinOp::RIGHT_ANTI_JOIN || JoinOpType == TJoinOp::RIGHT_SEMI_JOIN;

    constexpr auto probe_all =
            JoinOpType == TJoinOp::LEFT_OUTER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN;

    bool all_match_one = true;
    int last_probe_index = probe_index;
    size_t probe_size = 0;
    auto& probe_row_match_iter =
            std::get<ForwardIterator<Mapped>>(_join_node->_probe_row_match_iter);
    {
        SCOPED_TIMER(_search_hashtable_timer);
        if constexpr (!is_right_semi_anti_join) {
            // handle ramaining matched rows from last probe row
            if (probe_row_match_iter.ok()) {
                for (; probe_row_match_iter.ok() && current_offset < _batch_size;
                     ++probe_row_match_iter) {
                    if (LIKELY(current_offset < _build_block_rows.size())) {
                        _build_block_offsets[current_offset] = probe_row_match_iter->block_offset;
                        _build_block_rows[current_offset] = probe_row_match_iter->row_num;
                    } else {
                        _build_block_offsets.emplace_back(probe_row_match_iter->block_offset);
                        _build_block_rows.emplace_back(probe_row_match_iter->row_num);
                    }
                    ++current_offset;
                }
                _items_counts[probe_index] = current_offset;
                all_match_one &= (current_offset == 1);
                if (!probe_row_match_iter.ok()) {
                    ++probe_index;
                }
                probe_size = 1;
            }
        }

        if (current_offset < _batch_size) {
            while (probe_index < probe_rows) {
                if constexpr (ignore_null && need_null_map_for_probe) {
                    if ((*null_map)[probe_index]) {
                        if constexpr (probe_all) {
                            _items_counts[probe_index++] = (uint32_t)1;
                            // only full outer / left outer need insert the data of right table
                            if (LIKELY(current_offset < _build_block_rows.size())) {
                                _build_block_offsets[current_offset] = -1;
                                _build_block_rows[current_offset] = -1;
                            } else {
                                _build_block_offsets.emplace_back(-1);
                                _build_block_rows.emplace_back(-1);
                            }
                            ++current_offset;
                        } else {
                            _items_counts[probe_index++] = (uint32_t)0;
                        }
                        all_match_one = false;
                        if constexpr (probe_all) {
                            if (current_offset >= _batch_size) {
                                break;
                            }
                        }
                        continue;
                    }
                }
                int last_offset = current_offset;
                auto find_result = !need_null_map_for_probe
                                           ? key_getter.find_key(hash_table_ctx.hash_table,
                                                                 probe_index, *_arena)
                                   : (*null_map)[probe_index]
                                           ? decltype(key_getter.find_key(hash_table_ctx.hash_table,
                                                                          probe_index,
                                                                          *_arena)) {nullptr, false}
                                           : key_getter.find_key(hash_table_ctx.hash_table,
                                                                 probe_index, *_arena);
                if (probe_index + PREFETCH_STEP < probe_rows) {
                    key_getter.template prefetch<true>(hash_table_ctx.hash_table,
                                                       probe_index + PREFETCH_STEP, *_arena);
                }

                auto current_probe_index = probe_index;
                if constexpr (JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                              JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                    if (is_mark_join) {
                        ++current_offset;
                        assert_cast<doris::vectorized::ColumnVector<UInt8>&>(*mcol[mcol.size() - 1])
                                .get_data()
                                .template push_back(!find_result.is_found());
                    } else {
                        if (!find_result.is_found()) {
                            ++current_offset;
                        }
                    }
                    ++probe_index;
                } else if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
                    if (is_mark_join) {
                        ++current_offset;
                        assert_cast<doris::vectorized::ColumnVector<UInt8>&>(*mcol[mcol.size() - 1])
                                .get_data()
                                .template push_back(find_result.is_found());
                    } else {
                        if (find_result.is_found()) {
                            ++current_offset;
                        }
                    }
                    ++probe_index;
                } else {
                    DCHECK(!is_mark_join);
                    if (find_result.is_found()) {
                        auto& mapped = find_result.get_mapped();
                        // TODO: Iterators are currently considered to be a heavy operation and have a certain impact on performance.
                        // We should rethink whether to use this iterator mode in the future. Now just opt the one row case
                        if (mapped.get_row_count() == 1) {
                            if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                                mapped.visited = true;
                            }

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
                            ++probe_index;
                        } else {
                            if constexpr (!is_right_semi_anti_join) {
                                auto it = mapped.begin();
                                for (; it.ok() && current_offset < _batch_size; ++it) {
                                    if (LIKELY(current_offset < _build_block_rows.size())) {
                                        _build_block_offsets[current_offset] = it->block_offset;
                                        _build_block_rows[current_offset] = it->row_num;
                                    } else {
                                        _build_block_offsets.emplace_back(it->block_offset);
                                        _build_block_rows.emplace_back(it->row_num);
                                    }
                                    ++current_offset;
                                }
                                probe_row_match_iter = it;
                                if (!it.ok()) {
                                    // If all matched rows for the current probe row are handled,
                                    // advance to next probe row.
                                    // If not(which means it excceed batch size), probe_index is not increased and
                                    // remaining matched rows for the current probe row will be
                                    // handled in the next call of this function
                                    ++probe_index;
                                }
                            } else {
                                ++probe_index;
                            }
                            if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                                mapped.visited = true;
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
                        ++probe_index;
                    }
                }

                uint32_t count = (uint32_t)(current_offset - last_offset);
                _items_counts[current_probe_index] = count;
                all_match_one &= (count == 1);
                if (current_offset >= _batch_size) {
                    break;
                }
            }
            probe_size = probe_index - last_probe_index + (probe_row_match_iter.ok() ? 1 : 0);
        }
    }

    {
        SCOPED_TIMER(_build_side_output_timer);
        build_side_output_column(mcol, right_col_idx, right_col_len,
                                 _join_node->_right_output_slot_flags, current_offset);
    }

    if constexpr (JoinOpType != TJoinOp::RIGHT_SEMI_JOIN &&
                  JoinOpType != TJoinOp::RIGHT_ANTI_JOIN) {
        SCOPED_TIMER(_probe_side_output_timer);
        probe_side_output_column(mcol, _join_node->_left_output_slot_flags, current_offset,
                                 last_probe_index, probe_size, all_match_one, false);
    }

    output_block->swap(mutable_block.to_block());

    return Status::OK();
}

template <int JoinOpType>
template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType>
Status ProcessHashTableProbe<JoinOpType>::do_process_with_other_join_conjuncts(
        HashTableType& hash_table_ctx, ConstNullMapPtr null_map, MutableBlock& mutable_block,
        Block* output_block, size_t probe_rows, bool is_mark_join) {
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
    if constexpr (std::is_same_v<Mapped, RowRefListWithFlags>) {
        constexpr auto probe_all =
                JoinOpType == TJoinOp::LEFT_OUTER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN;
        KeyGetter key_getter(probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);

        if (probe_index == 0) {
            size_t old_probe_keys_memory_usage = 0;
            if (_arena) {
                old_probe_keys_memory_usage = _arena->size();
            }
            _arena.reset(new Arena());
            if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
                if (_probe_keys.size() < probe_rows) {
                    _probe_keys.resize(probe_rows);
                }
                size_t keys_size = probe_raw_ptrs.size();
                for (size_t i = 0; i < probe_rows; ++i) {
                    _probe_keys[i] = serialize_keys_to_pool_contiguous(i, keys_size, probe_raw_ptrs,
                                                                       *_arena);
                }
            }
            _join_node->_probe_arena_memory_usage->add(_arena->size() -
                                                       old_probe_keys_memory_usage);
        }

        if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
            key_getter.set_serialized_keys(_probe_keys.data());
        }

        int right_col_idx = _join_node->_left_table_data_types.size();
        int right_col_len = _join_node->_right_table_data_types.size();

        auto& mcol = mutable_block.mutable_columns();
        // use in right join to change visited state after
        // exec the vother join conjunct
        std::vector<bool*> visited_map;
        visited_map.reserve(1.2 * _batch_size);

        std::vector<bool> same_to_prev;
        same_to_prev.reserve(1.2 * _batch_size);

        int current_offset = 0;

        bool all_match_one = true;
        int last_probe_index = probe_index;
        while (probe_index < probe_rows) {
            // ignore null rows
            if constexpr (ignore_null && need_null_map_for_probe) {
                if ((*null_map)[probe_index]) {
                    if constexpr (probe_all) {
                        _items_counts[probe_index++] = (uint32_t)1;
                        same_to_prev.emplace_back(false);
                        visited_map.emplace_back(nullptr);
                        // only full outer / left outer need insert the data of right table
                        if (LIKELY(current_offset < _build_block_rows.size())) {
                            _build_block_offsets[current_offset] = -1;
                            _build_block_rows[current_offset] = -1;
                        } else {
                            _build_block_offsets.emplace_back(-1);
                            _build_block_rows.emplace_back(-1);
                        }
                        ++current_offset;
                    } else {
                        _items_counts[probe_index++] = (uint32_t)0;
                    }
                    all_match_one = false;
                    continue;
                }
            }

            auto last_offset = current_offset;
            auto find_result =
                    !need_null_map_for_probe
                            ? key_getter.find_key(hash_table_ctx.hash_table, probe_index, *_arena)
                    : (*null_map)[probe_index]
                            ? decltype(key_getter.find_key(hash_table_ctx.hash_table, probe_index,
                                                           *_arena)) {nullptr, false}
                            : key_getter.find_key(hash_table_ctx.hash_table, probe_index, *_arena);
            if (probe_index + PREFETCH_STEP < probe_rows) {
                key_getter.template prefetch<true>(hash_table_ctx.hash_table,
                                                   probe_index + PREFETCH_STEP, *_arena);
            }
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
            } else if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                                 JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                                 JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                                 JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
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
            } else if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
                if (is_mark_join) {
                    same_to_prev.emplace_back(false);
                    visited_map.emplace_back(nullptr);
                    if (LIKELY(current_offset < _build_block_rows.size())) {
                        _build_block_offsets[current_offset] = -1;
                        _build_block_rows[current_offset] = -1;
                    } else {
                        _build_block_offsets.emplace_back(-1);
                        _build_block_rows.emplace_back(-1);
                    }
                    ++current_offset;
                }
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
            probe_side_output_column(mcol, _join_node->_left_output_slot_flags, current_offset,
                                     last_probe_index, probe_index - last_probe_index,
                                     all_match_one, true);
        }
        auto num_cols = mutable_block.columns();
        output_block->swap(mutable_block.to_block());

        // dispose the other join conjunct exec
        if (output_block->rows()) {
            int result_column_id = -1;
            int orig_columns = output_block->columns();
            RETURN_IF_ERROR((*_join_node->_vother_join_conjunct_ptr)
                                    ->execute(output_block, &result_column_id));

            auto column = output_block->get_by_position(result_column_id).column;
            if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
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
                                    std::move(*output_block->get_by_position(j + right_col_idx)
                                                       .column)
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
                        // Here to keep only hit join conjunct and other join conjunt is true need to be output.
                        // if not, only some key must keep one row will output will null right table column
                        if (same_to_prev[i] && filter_map.back() && !column->get_bool(i - 1)) {
                            filter_map[i - 1] = false;
                        }
                    } else {
                        filter_map.push_back(true);
                    }
                }

                for (int i = 0; i < column->size(); ++i) {
                    if (filter_map[i]) {
                        _tuple_is_null_right_flags->emplace_back(null_map_data[i]);
                    }
                }
                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                if (!column->empty()) {
                    filter_map.emplace_back(column->get_bool(0));
                }
                for (int i = 1; i < column->size(); ++i) {
                    if (column->get_bool(i) || (same_to_prev[i] && filter_map[i - 1])) {
                        // Only last same element is true, output last one
                        filter_map.push_back(true);
                        filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                    } else {
                        filter_map.push_back(false);
                    }
                }
                if (is_mark_join) {
                    auto& matched_map = assert_cast<doris::vectorized::ColumnVector<UInt8>&>(
                                                *(output_block->get_by_position(num_cols - 1)
                                                          .column->assume_mutable()))
                                                .get_data();

                    // For mark join, we only filter rows which have duplicate join keys.
                    // And then, we set matched_map to the join result to do the mark join's filtering.
                    for (size_t i = 1; i < column->size(); ++i) {
                        if (!same_to_prev[i]) {
                            matched_map.push_back(filter_map[i - 1]);
                            filter_map[i - 1] = true;
                        }
                    }
                    matched_map.push_back(filter_map[filter_map.size() - 1]);
                    filter_map[filter_map.size() - 1] = true;
                }

                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                                 JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                if (!column->empty()) {
                    // Both equal conjuncts and other conjuncts are true
                    filter_map.emplace_back(column->get_bool(0) && visited_map[0]);
                }
                for (int i = 1; i < column->size(); ++i) {
                    if ((visited_map[i] && column->get_bool(i)) ||
                        (same_to_prev[i] && filter_map[i - 1])) {
                        // When either of two conditions is meet:
                        // 1. Both equal conjuncts and other conjuncts are true or same_to_prev
                        // 2. This row is joined from the same build side row as the previous row
                        // Set filter_map[i] to true and filter_map[i - 1] to false if same_to_prev[i]
                        // is true.
                        filter_map.push_back(true);
                        filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                    } else {
                        filter_map.push_back(false);
                    }
                }

                if (is_mark_join) {
                    auto& matched_map = assert_cast<doris::vectorized::ColumnVector<UInt8>&>(
                                                *(output_block->get_by_position(num_cols - 1)
                                                          .column->assume_mutable()))
                                                .get_data();
                    for (int i = 1; i < same_to_prev.size(); ++i) {
                        if (!same_to_prev[i]) {
                            matched_map.push_back(!filter_map[i - 1]);
                            filter_map[i - 1] = true;
                        }
                    }
                    matched_map.push_back(!filter_map[filter_map.size() - 1]);
                    filter_map[filter_map.size() - 1] = true;
                } else {
                    // Same to the semi join, but change the last value to opposite value
                    for (int i = 1; i < same_to_prev.size(); ++i) {
                        if (!same_to_prev[i]) {
                            filter_map[i - 1] = !filter_map[i - 1];
                        }
                    }
                    filter_map[same_to_prev.size() - 1] = !filter_map[same_to_prev.size() - 1];
                }

                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                                 JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
                for (int i = 0; i < column->size(); ++i) {
                    DCHECK(visited_map[i]);
                    *visited_map[i] |= column->get_bool(i);
                }
            } else if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
                auto filter_size = 0;
                for (int i = 0; i < column->size(); ++i) {
                    DCHECK(visited_map[i]);
                    auto result = column->get_bool(i);
                    *visited_map[i] |= result;
                    filter_size += result;
                }
                _tuple_is_null_left_flags->resize_fill(filter_size, 0);
            } else {
                // inner join do nothing
            }

            if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                          JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
                output_block->clear();
            } else {
                if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
                              JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                              JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                    orig_columns = right_col_idx;
                }
                if (is_mark_join) {
                    Block::filter_block(output_block, result_column_id, output_block->columns());
                } else {
                    Block::filter_block(output_block, result_column_id, orig_columns);
                }
            }
        }

        return Status::OK();
    } else {
        LOG(FATAL) << "Invalid RowRefList";
        return Status::InvalidArgument("Invalid RowRefList");
    }
}

template <int JoinOpType>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType>::process_data_in_hashtable(HashTableType& hash_table_ctx,
                                                                    MutableBlock& mutable_block,
                                                                    Block* output_block,
                                                                    bool* eos) {
    using Mapped = typename HashTableType::Mapped;
    if constexpr (std::is_same_v<Mapped, RowRefListWithFlag> ||
                  std::is_same_v<Mapped, RowRefListWithFlags>) {
        hash_table_ctx.init_once();
        auto& mcol = mutable_block.mutable_columns();

        bool right_semi_anti_without_other =
                _join_node->_is_right_semi_anti && !_join_node->_have_other_join_conjunct;
        int right_col_idx =
                right_semi_anti_without_other ? 0 : _join_node->_left_table_data_types.size();
        int right_col_len = _join_node->_right_table_data_types.size();

        auto& iter = hash_table_ctx.iter;
        auto block_size = 0;
        auto& visited_iter = _join_node->_outer_join_pull_visited_iter;

        auto insert_from_hash_table = [&](uint8_t offset, uint32_t row_num) {
            block_size++;
            for (size_t j = 0; j < right_col_len; ++j) {
                auto& column = *_build_blocks[offset].get_by_position(j).column;
                mcol[j + right_col_idx]->insert_from(column, row_num);
            }
        };

        if (visited_iter.ok()) {
            DCHECK((std::is_same_v<Mapped, RowRefListWithFlag>));
            for (; visited_iter.ok() && block_size < _batch_size; ++visited_iter) {
                insert_from_hash_table(visited_iter->block_offset, visited_iter->row_num);
            }
            if (!visited_iter.ok()) {
                ++iter;
            }
        }

        for (; iter != hash_table_ctx.hash_table.end() && block_size < _batch_size; ++iter) {
            auto& mapped = iter->get_second();
            if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                if (mapped.visited) {
                    for (auto it = mapped.begin(); it.ok(); ++it) {
                        if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                            insert_from_hash_table(it->block_offset, it->row_num);
                        }
                    }
                } else {
                    if constexpr (JoinOpType != TJoinOp::RIGHT_SEMI_JOIN) {
                        visited_iter = mapped.begin();
                        for (; visited_iter.ok() && block_size < _batch_size; ++visited_iter) {
                            insert_from_hash_table(visited_iter->block_offset,
                                                   visited_iter->row_num);
                        }
                        if (visited_iter.ok()) {
                            // block_size >= _batch_size, quit for loop
                            break;
                        }
                    }
                }
            } else {
                for (auto it = mapped.begin(); it.ok(); ++it) {
                    if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                        if (it->visited) {
                            insert_from_hash_table(it->block_offset, it->row_num);
                        }
                    } else {
                        if (!it->visited) {
                            insert_from_hash_table(it->block_offset, it->row_num);
                        }
                    }
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
        if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
            for (int i = 0; i < right_col_idx; ++i) {
                assert_cast<ColumnNullable*>(mcol[i].get())->insert_many_defaults(block_size);
            }
            _tuple_is_null_left_flags->resize_fill(block_size, 1);
        }
        *eos = iter == hash_table_ctx.hash_table.end();
        output_block->swap(
                mutable_block.to_block(right_semi_anti_without_other ? right_col_idx : 0));
        return Status::OK();
    } else {
        LOG(FATAL) << "Invalid RowRefList";
        return Status::InvalidArgument("Invalid RowRefList");
    }
}

template <typename T>
struct ExtractType;

template <typename T, typename U>
struct ExtractType<T(U)> {
    using Type = U;
};

#define INSTANTIATION(JoinOpType, T)                                                          \
    template Status                                                                           \
    ProcessHashTableProbe<JoinOpType>::do_process<false, false, ExtractType<void(T)>::Type>(  \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
    template Status                                                                           \
    ProcessHashTableProbe<JoinOpType>::do_process<false, true, ExtractType<void(T)>::Type>(   \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
    template Status                                                                           \
    ProcessHashTableProbe<JoinOpType>::do_process<true, false, ExtractType<void(T)>::Type>(   \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
    template Status                                                                           \
    ProcessHashTableProbe<JoinOpType>::do_process<true, true, ExtractType<void(T)>::Type>(    \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
                                                                                              \
    template Status ProcessHashTableProbe<JoinOpType>::do_process_with_other_join_conjuncts<  \
            false, false, ExtractType<void(T)>::Type>(                                        \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
    template Status ProcessHashTableProbe<JoinOpType>::do_process_with_other_join_conjuncts<  \
            false, true, ExtractType<void(T)>::Type>(                                         \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
    template Status ProcessHashTableProbe<JoinOpType>::do_process_with_other_join_conjuncts<  \
            true, false, ExtractType<void(T)>::Type>(                                         \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
    template Status ProcessHashTableProbe<JoinOpType>::do_process_with_other_join_conjuncts<  \
            true, true, ExtractType<void(T)>::Type>(                                          \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,            \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,            \
            bool is_mark_join);                                                               \
                                                                                              \
    template Status                                                                           \
    ProcessHashTableProbe<JoinOpType>::process_data_in_hashtable<ExtractType<void(T)>::Type>( \
            ExtractType<void(T)>::Type & hash_table_ctx, MutableBlock & mutable_block,        \
            Block * output_block, bool* eos)

#define INSTANTIATION_FOR(JoinOpType)                                                      \
    template struct ProcessHashTableProbe<JoinOpType>;                                     \
                                                                                           \
    template void ProcessHashTableProbe<JoinOpType>::build_side_output_column<false>(      \
            MutableColumns & mcol, int column_offset, int column_length,                   \
            const std::vector<bool>& output_slot_flags, int size);                         \
    template void ProcessHashTableProbe<JoinOpType>::build_side_output_column<true>(       \
            MutableColumns & mcol, int column_offset, int column_length,                   \
            const std::vector<bool>& output_slot_flags, int size);                         \
                                                                                           \
    INSTANTIATION(JoinOpType, (SerializedHashTableContext<RowRefList>));                   \
    INSTANTIATION(JoinOpType, (I8HashTableContext<RowRefList>));                           \
    INSTANTIATION(JoinOpType, (I16HashTableContext<RowRefList>));                          \
    INSTANTIATION(JoinOpType, (I32HashTableContext<RowRefList>));                          \
    INSTANTIATION(JoinOpType, (I64HashTableContext<RowRefList>));                          \
    INSTANTIATION(JoinOpType, (I128HashTableContext<RowRefList>));                         \
    INSTANTIATION(JoinOpType, (I256HashTableContext<RowRefList>));                         \
    INSTANTIATION(JoinOpType, (I64FixedKeyHashTableContext<true, RowRefList>));            \
    INSTANTIATION(JoinOpType, (I64FixedKeyHashTableContext<false, RowRefList>));           \
    INSTANTIATION(JoinOpType, (I128FixedKeyHashTableContext<true, RowRefList>));           \
    INSTANTIATION(JoinOpType, (I128FixedKeyHashTableContext<false, RowRefList>));          \
    INSTANTIATION(JoinOpType, (I256FixedKeyHashTableContext<true, RowRefList>));           \
    INSTANTIATION(JoinOpType, (I256FixedKeyHashTableContext<false, RowRefList>));          \
    INSTANTIATION(JoinOpType, (SerializedHashTableContext<RowRefListWithFlag>));           \
    INSTANTIATION(JoinOpType, (I8HashTableContext<RowRefListWithFlag>));                   \
    INSTANTIATION(JoinOpType, (I16HashTableContext<RowRefListWithFlag>));                  \
    INSTANTIATION(JoinOpType, (I32HashTableContext<RowRefListWithFlag>));                  \
    INSTANTIATION(JoinOpType, (I64HashTableContext<RowRefListWithFlag>));                  \
    INSTANTIATION(JoinOpType, (I128HashTableContext<RowRefListWithFlag>));                 \
    INSTANTIATION(JoinOpType, (I256HashTableContext<RowRefListWithFlag>));                 \
    INSTANTIATION(JoinOpType, (I64FixedKeyHashTableContext<true, RowRefListWithFlag>));    \
    INSTANTIATION(JoinOpType, (I64FixedKeyHashTableContext<false, RowRefListWithFlag>));   \
    INSTANTIATION(JoinOpType, (I128FixedKeyHashTableContext<true, RowRefListWithFlag>));   \
    INSTANTIATION(JoinOpType, (I128FixedKeyHashTableContext<false, RowRefListWithFlag>));  \
    INSTANTIATION(JoinOpType, (I256FixedKeyHashTableContext<true, RowRefListWithFlag>));   \
    INSTANTIATION(JoinOpType, (I256FixedKeyHashTableContext<false, RowRefListWithFlag>));  \
    INSTANTIATION(JoinOpType, (SerializedHashTableContext<RowRefListWithFlags>));          \
    INSTANTIATION(JoinOpType, (I8HashTableContext<RowRefListWithFlags>));                  \
    INSTANTIATION(JoinOpType, (I16HashTableContext<RowRefListWithFlags>));                 \
    INSTANTIATION(JoinOpType, (I32HashTableContext<RowRefListWithFlags>));                 \
    INSTANTIATION(JoinOpType, (I64HashTableContext<RowRefListWithFlags>));                 \
    INSTANTIATION(JoinOpType, (I128HashTableContext<RowRefListWithFlags>));                \
    INSTANTIATION(JoinOpType, (I256HashTableContext<RowRefListWithFlags>));                \
    INSTANTIATION(JoinOpType, (I64FixedKeyHashTableContext<true, RowRefListWithFlags>));   \
    INSTANTIATION(JoinOpType, (I64FixedKeyHashTableContext<false, RowRefListWithFlags>));  \
    INSTANTIATION(JoinOpType, (I128FixedKeyHashTableContext<true, RowRefListWithFlags>));  \
    INSTANTIATION(JoinOpType, (I128FixedKeyHashTableContext<false, RowRefListWithFlags>)); \
    INSTANTIATION(JoinOpType, (I256FixedKeyHashTableContext<true, RowRefListWithFlags>));  \
    INSTANTIATION(JoinOpType, (I256FixedKeyHashTableContext<false, RowRefListWithFlags>))

} // namespace doris::vectorized
