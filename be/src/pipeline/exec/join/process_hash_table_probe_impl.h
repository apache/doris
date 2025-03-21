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

#include "common/cast_set.h"
#include "common/status.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "process_hash_table_probe.h"
#include "runtime/thread_context.h" // IWYU pragma: keep
#include "util/simd/bits.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/hash_table/join_hash_table.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
template <int JoinOpType>
ProcessHashTableProbe<JoinOpType>::ProcessHashTableProbe(HashJoinProbeLocalState* parent,
                                                         int batch_size)
        : _parent(parent),
          _batch_size(batch_size),
          _build_block(parent->build_block()),
          _have_other_join_conjunct(parent->have_other_join_conjunct()),
          _is_right_semi_anti(parent->is_right_semi_anti()),
          _left_output_slot_flags(parent->left_output_slot_flags()),
          _right_output_slot_flags(parent->right_output_slot_flags()),
          _has_null_in_build_side(parent->has_null_in_build_side()),
          _search_hashtable_timer(parent->_search_hashtable_timer),
          _init_probe_side_timer(parent->_init_probe_side_timer),
          _build_side_output_timer(parent->_build_side_output_timer),
          _probe_side_output_timer(parent->_probe_side_output_timer),
          _finish_probe_phase_timer(parent->_finish_probe_phase_timer),
          _right_col_idx((_is_right_semi_anti && !_have_other_join_conjunct)
                                 ? 0
                                 : _parent->left_table_data_types().size()),
          _right_col_len(_parent->right_table_data_types().size()) {}

template <int JoinOpType>
void ProcessHashTableProbe<JoinOpType>::build_side_output_column(
        vectorized::MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        bool have_other_join_conjunct, bool is_mark_join) {
    SCOPED_TIMER(_build_side_output_timer);

    // indicates whether build_indexs contain 0
    bool build_index_has_zero =
            (JoinOpType != TJoinOp::INNER_JOIN && JoinOpType != TJoinOp::RIGHT_OUTER_JOIN) ||
            have_other_join_conjunct || is_mark_join;
    if (!size) {
        return;
    }

    if (!build_index_has_zero && _build_column_has_null.empty()) {
        _need_calculate_build_index_has_zero = false;
        _build_column_has_null.resize(output_slot_flags.size());
        for (int i = 0; i < _right_col_len; i++) {
            const auto& column = *_build_block->safe_get_by_position(i).column;
            _build_column_has_null[i] = false;
            if (output_slot_flags[i] && column.is_nullable()) {
                const auto& nullable = assert_cast<const vectorized::ColumnNullable&>(column);
                _build_column_has_null[i] = !simd::contain_byte(
                        nullable.get_null_map_data().data() + 1, nullable.size() - 1, 1);
                _need_calculate_build_index_has_zero |= _build_column_has_null[i];
            }
        }
    }

    for (size_t i = 0; i < _right_col_len && i + _right_col_idx < mcol.size(); i++) {
        const auto& column = *_build_block->safe_get_by_position(i).column;
        if (output_slot_flags[i]) {
            if (!build_index_has_zero && _build_column_has_null[i]) {
                assert_cast<vectorized::ColumnNullable*>(mcol[i + _right_col_idx].get())
                        ->insert_indices_from_not_has_null(column, _build_indexs.data(),
                                                           _build_indexs.data() + size);
            } else {
                mcol[i + _right_col_idx]->insert_indices_from(column, _build_indexs.data(),
                                                              _build_indexs.data() + size);
            }
        } else if (i + _right_col_idx != _parent->_mark_column_id) {
            mcol[i + _right_col_idx]->insert_default();
            mcol[i + _right_col_idx] =
                    vectorized::ColumnConst::create(std::move(mcol[i + _right_col_idx]), size);
        }
    }
    if (_parent->_mark_column_id != -1) {
        // resize mark column and fill with true
        auto& mark_column =
                assert_cast<vectorized::ColumnNullable&>(*mcol[_parent->_mark_column_id]);
        mark_column.resize(size);
        auto* null_map = mark_column.get_null_map_column().get_data().data();
        auto* data = assert_cast<vectorized::ColumnUInt8&>(mark_column.get_nested_column())
                             .get_data()
                             .data();
        std::fill(null_map, null_map + size, 0);
        std::fill(data, data + size, 1);
    }
}

template <int JoinOpType>
void ProcessHashTableProbe<JoinOpType>::probe_side_output_column(
        vectorized::MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        bool all_match_one, bool have_other_join_conjunct) {
    SCOPED_TIMER(_probe_side_output_timer);
    auto& probe_block = _parent->_probe_block;
    for (int i = 0; i < output_slot_flags.size(); ++i) {
        if (output_slot_flags[i]) {
            auto& column = probe_block.get_by_position(i).column;
            if (all_match_one) {
                mcol[i]->insert_range_from(*column, _probe_indexs[0], size);
            } else {
                mcol[i]->insert_indices_from(*column, _probe_indexs.data(),
                                             _probe_indexs.data() + size);
            }
        } else {
            mcol[i]->insert_default();
            mcol[i] = vectorized::ColumnConst::create(std::move(mcol[i]), size);
        }
    }
}

template <int JoinOpType>
template <typename HashTableType>
typename HashTableType::State ProcessHashTableProbe<JoinOpType>::_init_probe_side(
        HashTableType& hash_table_ctx, size_t probe_rows, bool with_other_join_conjuncts,
        const uint8_t* null_map) {
    // may over batch size 1 for some outer join case
    _probe_indexs.resize(_batch_size + 1);
    _build_indexs.resize(_batch_size + 1);
    if ((JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
         JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) &&
        with_other_join_conjuncts) {
        _null_flags.resize(_batch_size + 1);
    }

    if (!_parent->_ready_probe) {
        _parent->_ready_probe = true;
        hash_table_ctx.reset();
        // In order to make the null keys equal when using single null eq, all null keys need to be set to default value.
        if (_parent->_probe_columns.size() == 1 && null_map) {
            _parent->_probe_columns[0]->assume_mutable()->replace_column_null_data(null_map);
        }

        hash_table_ctx.init_serialized_keys(_parent->_probe_columns, probe_rows, null_map, true,
                                            false, hash_table_ctx.hash_table->get_bucket_size());
        hash_table_ctx.hash_table->pre_build_idxs(hash_table_ctx.bucket_nums);
        int64_t arena_memory_usage = hash_table_ctx.serialized_keys_size(false);
        COUNTER_SET(_parent->_probe_arena_memory_usage, arena_memory_usage);
        COUNTER_UPDATE(_parent->_memory_used_counter, arena_memory_usage);
    }

    return typename HashTableType::State(_parent->_probe_columns);
}

template <int JoinOpType>
template <typename HashTableType, bool with_other_conjuncts, bool is_mark_join>
Status ProcessHashTableProbe<JoinOpType>::do_process(HashTableType& hash_table_ctx,
                                                     const uint8_t* null_map,
                                                     vectorized::MutableBlock& mutable_block,
                                                     vectorized::Block* output_block,
                                                     uint32_t probe_rows) {
    if (_right_col_len && !_build_block) {
        return Status::InternalError("build block is nullptr");
    }

    auto& probe_index = _parent->_probe_index;
    auto& build_index = _parent->_build_index;
    {
        SCOPED_TIMER(_init_probe_side_timer);
        _init_probe_side<HashTableType>(hash_table_ctx, probe_rows, with_other_conjuncts, null_map);
    }

    auto& mcol = mutable_block.mutable_columns();

    uint32_t current_offset = 0;
    if ((JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
         JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) &&
        with_other_conjuncts) {
        SCOPED_TIMER(_search_hashtable_timer);

        /// If `_build_index_for_null_probe_key` is not zero, it means we are in progress of handling probe null key.
        if (_build_index_for_null_probe_key) {
            if (!null_map || !null_map[probe_index]) {
                return Status::InternalError(
                        "null_map is nullptr or null_map[probe_index] is false");
            }
            current_offset = _process_probe_null_key(probe_index);
            if (!_build_index_for_null_probe_key) {
                probe_index++;
                build_index = 0;
            }
        } else {
            auto [new_probe_idx, new_build_idx, new_current_offset, picking_null_keys] =
                    hash_table_ctx.hash_table->find_null_aware_with_other_conjuncts(
                            hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(), probe_index,
                            build_index, probe_rows, _probe_indexs.data(), _build_indexs.data(),
                            _null_flags.data(), _picking_null_keys, null_map);
            probe_index = new_probe_idx;
            build_index = new_build_idx;
            current_offset = new_current_offset;
            _picking_null_keys = picking_null_keys;

            if (probe_index < probe_rows && null_map && null_map[probe_index]) {
                _build_index_for_null_probe_key = 1;
                if (current_offset == 0) {
                    current_offset = _process_probe_null_key(probe_index);
                    if (!_build_index_for_null_probe_key) {
                        probe_index++;
                        build_index = 0;
                    }
                }
            }
        }
    } else {
        SCOPED_TIMER(_search_hashtable_timer);
        auto [new_probe_idx, new_build_idx, new_current_offset] =
                hash_table_ctx.hash_table->template find_batch<JoinOpType>(
                        hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(), probe_index,
                        build_index, cast_set<int32_t>(probe_rows), _probe_indexs.data(),
                        _probe_visited, _build_indexs.data(), null_map, with_other_conjuncts,
                        is_mark_join, !_parent->_mark_join_conjuncts.empty());
        probe_index = new_probe_idx;
        build_index = new_build_idx;
        current_offset = new_current_offset;
    }

    build_side_output_column(mcol, *_right_output_slot_flags, current_offset, with_other_conjuncts,
                             is_mark_join);

    if constexpr (with_other_conjuncts || (JoinOpType != TJoinOp::RIGHT_SEMI_JOIN &&
                                           JoinOpType != TJoinOp::RIGHT_ANTI_JOIN)) {
        auto check_all_match_one = [](const std::vector<uint32_t>& vecs, int size) {
            if (!size || vecs[size - 1] != vecs[0] + size - 1) {
                return false;
            }
            for (int i = 1; i < size; i++) {
                if (vecs[i] == vecs[i - 1]) {
                    return false;
                }
            }
            return true;
        };

        probe_side_output_column(mcol, *_left_output_slot_flags, current_offset,
                                 check_all_match_one(_probe_indexs, current_offset),
                                 with_other_conjuncts);
    }

    output_block->swap(mutable_block.to_block());

    if constexpr (is_mark_join && JoinOpType != TJoinOp::RIGHT_SEMI_JOIN) {
        bool ignore_null_map =
                (JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                 JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) &&
                hash_table_ctx.hash_table
                        ->empty_build_side(); // empty build side will return false to instead null
        return do_mark_join_conjuncts<with_other_conjuncts>(output_block,
                                                            ignore_null_map ? nullptr : null_map);
    } else if constexpr (with_other_conjuncts) {
        return do_other_join_conjuncts(output_block, hash_table_ctx.hash_table->get_visited(),
                                       hash_table_ctx.hash_table->has_null_key());
    }

    return Status::OK();
}

template <int JoinOpType>
uint32_t ProcessHashTableProbe<JoinOpType>::_process_probe_null_key(uint32_t probe_index) {
    const auto rows = _build_block->rows();

    DCHECK_LT(_build_index_for_null_probe_key, rows);
    DCHECK_LT(0, _build_index_for_null_probe_key);
    uint32_t matched_cnt = 0;
    for (; _build_index_for_null_probe_key < rows && matched_cnt < _batch_size; ++matched_cnt) {
        _probe_indexs[matched_cnt] = probe_index;
        _build_indexs[matched_cnt] = _build_index_for_null_probe_key++;
        _null_flags[matched_cnt] = 1;
    }

    if (_build_index_for_null_probe_key == rows) {
        _build_index_for_null_probe_key = 0;
        _probe_indexs[matched_cnt] = probe_index;
        _build_indexs[matched_cnt] = 0;
        _null_flags[matched_cnt] = 0;
        matched_cnt++;
    }

    return matched_cnt;
}

/**
     * Mark join: there is a column named mark column which stores the result of mark join conjunct.
     * For example:
     * ```sql
     *  select * from t1 where t1.k1 not in (select t2.k1 from t2 where t2.k2 = t1.k2 and t2.k3 > t1.k3) or t1.k4 < 10;
     * ```
     * equal join conjuncts: t2.k2 = t1.k2
     * mark join conjunct: t1.k1 = t2.k1
     * other join conjuncts: t2.k3 > t1.k3
     * other predicates: $c$1 or t1.k4 < 10   # `$c$1` means the result of mark join conjunct(mark column)
     *
     * Executing flow:
     *
     * Equal join conjuncts (probe hash table)
     *                  ↓↓
     * Mark join conjuncts (result is nullable, stored in mark column)
     *                  ↓↓
     * Other join conjuncts (update the mark column)
     *                  ↓↓
     * Other predicates (filter rows)
     *
     * ```sql
     *   select * from t1 where t1.k1 not in (select t2.k1 from t2 where t2.k3 > t1.k3) or t1.k4 < 10;
     * ```
     * This sql has no equal join conjuncts:
     * equal join conjuncts: NAN
     * mark join conjunct: t1.k1 = t2.k1
     * other join conjuncts: t2.k3 > t1.k3
     * other predicates: $c$1 or t1.k4 < 10   # `$c$1` means the result of mark join conjunct(mark column)
     *
     * To avoid using nested loop join, we use the mark join conjunct(`t1.k1 = t2.k1`) as the equal join conjunct.
     * So this query will be a "null aware left anti join", which means the equal conjunct's result should be nullable.
     */
template <int JoinOpType>
template <bool with_other_conjuncts>
Status ProcessHashTableProbe<JoinOpType>::do_mark_join_conjuncts(vectorized::Block* output_block,
                                                                 const uint8_t* null_map) {
    DCHECK(JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
           JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
           JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
           JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN);

    constexpr bool is_anti_join = JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                                  JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    constexpr bool is_null_aware_join = JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN ||
                                        JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    const auto row_count = output_block->rows();
    if (!row_count) {
        return Status::OK();
    }

    auto mark_column_mutable =
            output_block->get_by_position(_parent->_mark_column_id).column->assume_mutable();
    auto& mark_column = assert_cast<vectorized::ColumnNullable&>(*mark_column_mutable);
    vectorized::IColumn::Filter& filter =
            assert_cast<vectorized::ColumnUInt8&>(mark_column.get_nested_column()).get_data();
    RETURN_IF_ERROR(
            vectorized::VExprContext::execute_conjuncts(_parent->_mark_join_conjuncts, output_block,
                                                        mark_column.get_null_map_column(), filter));
    uint8_t* mark_filter_data = filter.data();
    uint8_t* mark_null_map = mark_column.get_null_map_data().data();

    if (is_null_aware_join) {
        // For null aware anti/semi join, if the equal conjuncts was not matched and the build side has null value,
        // the result should be null. Like:
        // select 4 not in (2, 3, null) => null, select 4 not in (2, 3) => true
        // select 4 in (2, 3, null) => null, select 4 in (2, 3) => false
        for (size_t i = 0; i != row_count; ++i) {
            mark_filter_data[i] = _build_indexs[i] != 0;
        }

        if constexpr (with_other_conjuncts) {
            // _null_flags is true means build or probe side of the row is null
            memcpy(mark_null_map, _null_flags.data(), row_count);
        } else {
            if (null_map) {
                // probe side of the row is null, so the mark sign should also be null.
                for (size_t i = 0; i != row_count; ++i) {
                    mark_null_map[i] |= null_map[_probe_indexs[i]];
                }
            }
            if (!with_other_conjuncts && *_has_null_in_build_side) {
                // _has_null_in_build_side will change false to null when row not matched
                for (size_t i = 0; i != row_count; ++i) {
                    mark_null_map[i] |= _build_indexs[i] == 0;
                }
            }
        }
    } else {
        // for non null aware join, build_indexs is 0 which means there is no match
        // sometimes null will be returned in conjunct, but it should not actually be null.
        for (size_t i = 0; i != row_count; ++i) {
            mark_null_map[i] &= _build_indexs[i] != 0;
        }
    }

    if constexpr (with_other_conjuncts) {
        vectorized::IColumn::Filter other_conjunct_filter(row_count, 1);
        {
            bool can_be_filter_all = false;
            RETURN_IF_ERROR(vectorized::VExprContext::execute_conjuncts(
                    _parent->_other_join_conjuncts, nullptr, output_block, &other_conjunct_filter,
                    &can_be_filter_all));
        }
        DCHECK_EQ(filter.size(), other_conjunct_filter.size());
        const auto* other_filter_data = other_conjunct_filter.data();
        for (size_t i = 0; i != filter.size(); ++i) {
            // null & any(true or false) => null => false
            mark_filter_data[i] &= (!mark_null_map[i]) & other_filter_data[i];

            // null & true => null
            // null & false => false
            mark_null_map[i] &= other_filter_data[i];
        }
    }

    auto filter_column = vectorized::ColumnUInt8::create(row_count, 0);
    auto* __restrict filter_map = filter_column->get_data().data();
    for (size_t i = 0; i != row_count; ++i) {
        if (_parent->_last_probe_match == _probe_indexs[i]) {
            continue;
        }
        if (_build_indexs[i] == 0) {
            bool has_null_mark_value = _parent->_last_probe_null_mark == _probe_indexs[i];
            filter_map[i] = true;
            mark_filter_data[i] = false;
            mark_null_map[i] |= has_null_mark_value;
        } else if (mark_null_map[i]) {
            _parent->_last_probe_null_mark = _probe_indexs[i];
        } else if (mark_filter_data[i]) {
            filter_map[i] = true;
            _parent->_last_probe_match = _probe_indexs[i];
        }
    }

    if constexpr (is_anti_join) {
        // flip the mark column
        for (size_t i = 0; i != row_count; ++i) {
            mark_filter_data[i] ^= 1; // not null/ null
        }
    }

    auto result_column_id = output_block->columns();
    output_block->insert(
            {std::move(filter_column), std::make_shared<vectorized::DataTypeUInt8>(), ""});
    return vectorized::Block::filter_block(output_block, result_column_id, result_column_id);
}

template <int JoinOpType>
Status ProcessHashTableProbe<JoinOpType>::do_other_join_conjuncts(vectorized::Block* output_block,
                                                                  DorisVector<uint8_t>& visited,
                                                                  bool has_null_in_build_side) {
    // dispose the other join conjunct exec
    auto row_count = output_block->rows();
    if (!row_count) {
        return Status::OK();
    }

    SCOPED_TIMER(_parent->_non_equal_join_conjuncts_timer);
    size_t orig_columns = output_block->columns();
    vectorized::IColumn::Filter other_conjunct_filter(row_count, 1);
    {
        bool can_be_filter_all = false;
        RETURN_IF_ERROR(vectorized::VExprContext::execute_conjuncts(
                _parent->_other_join_conjuncts, nullptr, output_block, &other_conjunct_filter,
                &can_be_filter_all));
    }

    auto filter_column = vectorized::ColumnUInt8::create();
    filter_column->get_data() = std::move(other_conjunct_filter);
    auto result_column_id = output_block->columns();
    output_block->insert(
            {std::move(filter_column), std::make_shared<vectorized::DataTypeUInt8>(), ""});
    uint8_t* __restrict filter_column_ptr =
            assert_cast<vectorized::ColumnUInt8&>(
                    output_block->get_by_position(result_column_id).column->assume_mutable_ref())
                    .get_data()
                    .data();

    if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                  JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
        auto new_filter_column = vectorized::ColumnUInt8::create(row_count);
        auto* __restrict filter_map = new_filter_column->get_data().data();

        // process equal-conjuncts-matched tuples that are newly generated
        // in this run if there are any.
        for (int i = 0; i < row_count; ++i) {
            bool join_hit = _build_indexs[i];
            bool other_hit = filter_column_ptr[i];

            if (!join_hit) {
                filter_map[i] = _parent->_last_probe_match != _probe_indexs[i];
            } else {
                filter_map[i] = other_hit;
            }
            if (filter_map[i]) {
                _parent->_last_probe_match = _probe_indexs[i];
            }
        }

        for (size_t i = 0; i < row_count; ++i) {
            if (filter_map[i]) {
                if constexpr (JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                    visited[_build_indexs[i]] = 1;
                }
            }
        }
        output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
    } else if constexpr (JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                         JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                         JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
        auto new_filter_column = vectorized::ColumnUInt8::create(row_count);
        auto* __restrict filter_map = new_filter_column->get_data().data();

        for (size_t i = 0; i < row_count; ++i) {
            bool not_matched_before = _parent->_last_probe_match != _probe_indexs[i];

            if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
                if (_build_indexs[i] == 0) {
                    filter_map[i] = false;
                } else if (filter_column_ptr[i]) {
                    filter_map[i] = not_matched_before;
                    _parent->_last_probe_match = _probe_indexs[i];
                } else {
                    filter_map[i] = false;
                }
            } else {
                if (_build_indexs[i] == 0) {
                    filter_map[i] = not_matched_before;
                } else {
                    filter_map[i] = false;
                    if (filter_column_ptr[i]) {
                        _parent->_last_probe_match = _probe_indexs[i];
                    }
                }
            }
        }

        output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
    } else if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                         JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
        for (int i = 0; i < row_count; ++i) {
            visited[_build_indexs[i]] |= filter_column_ptr[i];
        }
    } else if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
        for (int i = 0; i < row_count; ++i) {
            visited[_build_indexs[i]] |= filter_column_ptr[i];
        }
    }

    if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                  JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
        output_block->clear();
    } else {
        if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
                      JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                      JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            orig_columns = _right_col_idx;
        }
        RETURN_IF_ERROR(
                vectorized::Block::filter_block(output_block, result_column_id, orig_columns));
    }

    return Status::OK();
}

template <int JoinOpType>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType>::finish_probing(HashTableType& hash_table_ctx,
                                                         vectorized::MutableBlock& mutable_block,
                                                         vectorized::Block* output_block, bool* eos,
                                                         bool is_mark_join) {
    SCOPED_TIMER(_finish_probe_phase_timer);
    auto& mcol = mutable_block.mutable_columns();
    if (is_mark_join) {
        std::unique_ptr<vectorized::ColumnFilterHelper> mark_column =
                std::make_unique<vectorized::ColumnFilterHelper>(*mcol[mcol.size() - 1]);
        *eos = hash_table_ctx.hash_table->template iterate_map<JoinOpType, true>(_build_indexs,
                                                                                 mark_column.get());
    } else {
        *eos = hash_table_ctx.hash_table->template iterate_map<JoinOpType, false>(_build_indexs,
                                                                                  nullptr);
    }

    auto block_size = _build_indexs.size();

    if (block_size) {
        if (mcol.size() < _right_col_len + _right_col_idx) {
            return Status::InternalError(
                    "output block invalid, mcol.size()={}, _right_col_len={}, _right_col_idx={}",
                    mcol.size(), _right_col_len, _right_col_idx);
        }
        for (size_t j = 0; j < _right_col_len; ++j) {
            if (_right_output_slot_flags->at(j)) {
                const auto& column = *_build_block->safe_get_by_position(j).column;
                mcol[j + _right_col_idx]->insert_indices_from(column, _build_indexs.data(),
                                                              _build_indexs.data() + block_size);
            } else {
                mcol[j + _right_col_idx]->resize(block_size);
            }
        }

        // just resize the left table column in case with other conjunct to make block size is not zero
        if (_is_right_semi_anti && _have_other_join_conjunct) {
            for (int i = 0; i < _right_col_idx; ++i) {
                mcol[i]->resize(block_size);
            }
        }

        // right outer join / full join need insert data of left table
        if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
            for (int i = 0; i < _right_col_idx; ++i) {
                assert_cast<vectorized::ColumnNullable*>(mcol[i].get())
                        ->insert_many_defaults(block_size);
            }
        }
        output_block->swap(mutable_block.to_block(0));
        DCHECK(block_size <= _batch_size);
    }
    return Status::OK();
}

template <int JoinOpType>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType>::process(HashTableType& hash_table_ctx,
                                                  vectorized::ConstNullMapPtr null_map,
                                                  vectorized::MutableBlock& mutable_block,
                                                  vectorized::Block* output_block,
                                                  uint32_t probe_rows, bool is_mark_join,
                                                  bool have_other_join_conjunct) {
    Status res;
    std::visit(
            [&](auto is_mark_join, auto have_other_join_conjunct) {
                res = do_process<HashTableType, have_other_join_conjunct, is_mark_join>(
                        hash_table_ctx, null_map ? null_map->data() : nullptr, mutable_block,
                        output_block, probe_rows);
            },
            vectorized::make_bool_variant(is_mark_join),
            vectorized::make_bool_variant(have_other_join_conjunct));
    return res;
}

template <typename T>
struct ExtractType;

template <typename T, typename U>
struct ExtractType<T(U)> {
    using Type = U;
};

#define INSTANTIATION(JoinOpType, T)                                                               \
    template Status ProcessHashTableProbe<JoinOpType>::process<ExtractType<void(T)>::Type>(        \
            ExtractType<void(T)>::Type & hash_table_ctx, vectorized::ConstNullMapPtr null_map,     \
            vectorized::MutableBlock & mutable_block, vectorized::Block * output_block,            \
            uint32_t probe_rows, bool is_mark_join, bool have_other_join_conjunct);                \
    template Status ProcessHashTableProbe<JoinOpType>::finish_probing<ExtractType<void(T)>::Type>( \
            ExtractType<void(T)>::Type & hash_table_ctx, vectorized::MutableBlock & mutable_block, \
            vectorized::Block * output_block, bool* eos, bool is_mark_join);

#define INSTANTIATION_FOR(JoinOpType)                                              \
    template struct ProcessHashTableProbe<JoinOpType>;                             \
                                                                                   \
    INSTANTIATION(JoinOpType, (SerializedHashTableContext));                       \
    INSTANTIATION(JoinOpType, (PrimaryTypeHashTableContext<vectorized::UInt8>));   \
    INSTANTIATION(JoinOpType, (PrimaryTypeHashTableContext<vectorized::UInt16>));  \
    INSTANTIATION(JoinOpType, (PrimaryTypeHashTableContext<vectorized::UInt32>));  \
    INSTANTIATION(JoinOpType, (PrimaryTypeHashTableContext<vectorized::UInt64>));  \
    INSTANTIATION(JoinOpType, (PrimaryTypeHashTableContext<vectorized::UInt128>)); \
    INSTANTIATION(JoinOpType, (PrimaryTypeHashTableContext<vectorized::UInt256>)); \
    INSTANTIATION(JoinOpType, (FixedKeyHashTableContext<vectorized::UInt64>));     \
    INSTANTIATION(JoinOpType, (FixedKeyHashTableContext<vectorized::UInt128>));    \
    INSTANTIATION(JoinOpType, (FixedKeyHashTableContext<vectorized::UInt136>));    \
    INSTANTIATION(JoinOpType, (FixedKeyHashTableContext<vectorized::UInt256>));    \
    INSTANTIATION(JoinOpType, (MethodOneString));
#include "common/compile_check_end.h"
} // namespace doris::pipeline
