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
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "process_hash_table_probe.h"
#include "runtime/thread_context.h" // IWYU pragma: keep
#include "util/simd/bits.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/exprs/vexpr_context.h"
#include "vhash_join_node.h"

namespace doris::vectorized {

template <int JoinOpType, typename Parent>
ProcessHashTableProbe<JoinOpType, Parent>::ProcessHashTableProbe(Parent* parent, int batch_size)
        : _parent(parent),
          _batch_size(batch_size),
          _build_block(parent->build_block()),
          _tuple_is_null_left_flags(parent->is_outer_join()
                                            ? &(reinterpret_cast<ColumnUInt8&>(
                                                        *parent->_tuple_is_null_left_flag_column)
                                                        .get_data())
                                            : nullptr),
          _tuple_is_null_right_flags(parent->is_outer_join()
                                             ? &(reinterpret_cast<ColumnUInt8&>(
                                                         *parent->_tuple_is_null_right_flag_column)
                                                         .get_data())
                                             : nullptr),
          _have_other_join_conjunct(parent->have_other_join_conjunct()),
          _is_right_semi_anti(parent->is_right_semi_anti()),
          _left_output_slot_flags(parent->left_output_slot_flags()),
          _right_output_slot_flags(parent->right_output_slot_flags()),
          _has_null_in_build_side(parent->has_null_in_build_side()),
          _rows_returned_counter(parent->_rows_returned_counter),
          _search_hashtable_timer(parent->_search_hashtable_timer),
          _build_side_output_timer(parent->_build_side_output_timer),
          _probe_side_output_timer(parent->_probe_side_output_timer),
          _probe_process_hashtable_timer(parent->_probe_process_hashtable_timer) {}

template <int JoinOpType, typename Parent>
void ProcessHashTableProbe<JoinOpType, Parent>::build_side_output_column(
        MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        bool have_other_join_conjunct) {
    SCOPED_TIMER(_build_side_output_timer);
    constexpr auto is_semi_anti_join = JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                                       JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::LEFT_SEMI_JOIN;

    constexpr auto probe_all =
            JoinOpType == TJoinOp::LEFT_OUTER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN;

    if (!is_semi_anti_join || have_other_join_conjunct) {
        for (int i = 0; i < _right_col_len; i++) {
            const auto& column = *_build_block->get_by_position(i).column;
            if (output_slot_flags[i]) {
                mcol[i + _right_col_idx]->insert_indices_from_join(column, _build_indexs.data(),
                                                                   _build_indexs.data() + size);
            } else {
                mcol[i + _right_col_idx]->insert_many_defaults(size);
            }
        }
    }

    // Dispose right tuple is null flags columns
    if (probe_all && !have_other_join_conjunct) {
        _tuple_is_null_right_flags->resize(size);
        auto* __restrict null_data = _tuple_is_null_right_flags->data();
        for (int i = 0; i < size; ++i) {
            null_data[i] = _build_indexs[i] == 0;
        }
    }
}

template <int JoinOpType, typename Parent>
void ProcessHashTableProbe<JoinOpType, Parent>::probe_side_output_column(
        MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        int last_probe_index, size_t probe_size, bool all_match_one,
        bool have_other_join_conjunct) {
    SCOPED_TIMER(_probe_side_output_timer);
    auto& probe_block = _parent->_probe_block;
    for (int i = 0; i < output_slot_flags.size(); ++i) {
        if (output_slot_flags[i]) {
            auto& column = probe_block.get_by_position(i).column;
            if (all_match_one) {
                mcol[i]->insert_range_from(*column, last_probe_index, probe_size);
            } else {
                column->replicate(_probe_indexs.data(), size, *mcol[i]);
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

template <int JoinOpType, typename Parent>
template <typename HashTableType>
typename HashTableType::State ProcessHashTableProbe<JoinOpType, Parent>::_init_probe_side(
        HashTableType& hash_table_ctx, size_t probe_rows, bool with_other_join_conjuncts,
        const uint8_t* null_map) {
    _right_col_idx = _is_right_semi_anti && !with_other_join_conjuncts
                             ? 0
                             : _parent->left_table_data_types().size();
    _right_col_len = _parent->right_table_data_types().size();
    _row_count_from_last_probe = 0;

    _build_indexs.clear();
    _probe_indexs.clear();
    if (with_other_join_conjuncts) {
        // use in right join to change visited state after exec the vother join conjunct
        _visited_map.clear();
        _same_to_prev.clear();
        _visited_map.reserve(_batch_size * PROBE_SIDE_EXPLODE_RATE);
        _same_to_prev.reserve(_batch_size * PROBE_SIDE_EXPLODE_RATE);
    }
    _probe_indexs.reserve(_batch_size * PROBE_SIDE_EXPLODE_RATE);
    _build_indexs.reserve(_batch_size * PROBE_SIDE_EXPLODE_RATE);

    if (!_parent->_ready_probe) {
        _parent->_ready_probe = true;
        hash_table_ctx.reset();
        hash_table_ctx.init_serialized_keys_join(_parent->_probe_columns, probe_rows, null_map,
                                                 hash_table_ctx.hash_table->get_bucket_size());
    }
    return typename HashTableType::State(_parent->_probe_columns);
}

template <int JoinOpType, typename Parent>
template <typename Mapped, bool with_other_join_conjuncts>
ForwardIterator<Mapped>& ProcessHashTableProbe<JoinOpType, Parent>::_probe_row_match(
        int& current_offset, int& probe_index, size_t& probe_size, bool& all_match_one) {
    auto& probe_row_match_iter = std::get<ForwardIterator<Mapped>>(_parent->_probe_row_match_iter);
    if (!probe_row_match_iter.ok()) {
        return probe_row_match_iter;
    }

    SCOPED_TIMER(_search_hashtable_timer);
    for (; probe_row_match_iter.ok() && current_offset < _batch_size; ++probe_row_match_iter) {
        _emplace_element(probe_row_match_iter->row_num, current_offset);
        _probe_indexs.emplace_back(probe_index);
        if constexpr (with_other_join_conjuncts) {
            _visited_map.emplace_back(&probe_row_match_iter->visited);
        }
    }

    _row_count_from_last_probe = current_offset;
    all_match_one &= (current_offset == 1);
    if (!probe_row_match_iter.ok()) {
        ++probe_index;
    }
    probe_size = 1;

    return probe_row_match_iter;
}

template <int JoinOpType, typename Parent>
void ProcessHashTableProbe<JoinOpType, Parent>::_emplace_element(int32_t block_row,
                                                                 int& current_offset) {
    _build_indexs.emplace_back(block_row);
    current_offset++;
}

template <int JoinOpType, typename Parent>
template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType,
          bool with_other_conjuncts, bool is_mark_join>
Status ProcessHashTableProbe<JoinOpType, Parent>::do_process(HashTableType& hash_table_ctx,
                                                             ConstNullMapPtr null_map,
                                                             MutableBlock& mutable_block,
                                                             Block* output_block,
                                                             size_t probe_rows) {
    auto& probe_index = _parent->_probe_index;

    using Mapped = typename HashTableType::Mapped;

    _init_probe_side<HashTableType>(hash_table_ctx, probe_rows, with_other_conjuncts,
                                    need_null_map_for_probe ? null_map->data() : nullptr);

    auto& mcol = mutable_block.mutable_columns();

    int last_probe_index = probe_index;

    int current_offset = 0;
    bool all_match_one = true;
    size_t probe_size = 0;

    auto& probe_row_match_iter = _probe_row_match<Mapped, with_other_conjuncts>(
            current_offset, probe_index, probe_size, all_match_one);

    // If not(which means it excceed batch size), probe_index is not increased and
    // remaining matched rows for the current probe row will be
    // handled in the next call of this function
    int multi_matched_output_row_count = 0;

    // Is the last sub block of splitted block
    bool is_the_last_sub_block = false;

    if (with_other_conjuncts && probe_size != 0) {
        is_the_last_sub_block = !probe_row_match_iter.ok();
        _same_to_prev.emplace_back(false);
        for (int i = 0; i < current_offset - 1; ++i) {
            _same_to_prev.emplace_back(true);
        }
    }

    std::unique_ptr<ColumnFilterHelper> mark_column;
    if (is_mark_join) {
        mark_column = std::make_unique<ColumnFilterHelper>(*mcol[mcol.size() - 1]);
    }

    {
        SCOPED_TIMER(_search_hashtable_timer);
        auto [new_probe_idx, new_current_offset] = hash_table_ctx.hash_table->find_batch(
                hash_table_ctx.keys, hash_table_ctx.join_hash_values.data(), probe_index,
                probe_rows, _probe_indexs.data(), _build_indexs.data());
        probe_index = new_probe_idx;
        current_offset = new_current_offset;
        probe_size = probe_index - last_probe_index;
    }

    build_side_output_column(mcol, *_right_output_slot_flags, current_offset, with_other_conjuncts);

    if constexpr (with_other_conjuncts || (JoinOpType != TJoinOp::RIGHT_SEMI_JOIN &&
                                           JoinOpType != TJoinOp::RIGHT_ANTI_JOIN)) {
        RETURN_IF_CATCH_EXCEPTION(probe_side_output_column(
                mcol, *_left_output_slot_flags, current_offset, last_probe_index, probe_size,
                all_match_one, with_other_conjuncts));
    }

    output_block->swap(mutable_block.to_block());

    if constexpr (with_other_conjuncts) {
        return do_other_join_conjuncts(output_block, is_mark_join, multi_matched_output_row_count,
                                       is_the_last_sub_block);
    }

    return Status::OK();
}

template <int JoinOpType, typename Parent>
Status ProcessHashTableProbe<JoinOpType, Parent>::do_other_join_conjuncts(
        Block* output_block, bool is_mark_join, int multi_matched_output_row_count,
        bool is_the_last_sub_block) {
    // dispose the other join conjunct exec
    auto row_count = output_block->rows();
    if (!row_count) {
        return Status::OK();
    }

    SCOPED_TIMER(_parent->_process_other_join_conjunct_timer);
    int orig_columns = output_block->columns();
    IColumn::Filter other_conjunct_filter(row_count, 1);
    {
        bool can_be_filter_all = false;
        RETURN_IF_ERROR(VExprContext::execute_conjuncts(_parent->_other_join_conjuncts, nullptr,
                                                        output_block, &other_conjunct_filter,
                                                        &can_be_filter_all));
    }

    auto filter_column = ColumnUInt8::create();
    filter_column->get_data() = std::move(other_conjunct_filter);
    auto result_column_id = output_block->columns();
    output_block->insert({std::move(filter_column), std::make_shared<DataTypeUInt8>(), ""});
    const uint8_t* __restrict filter_column_ptr =
            assert_cast<const ColumnUInt8*>(
                    output_block->get_by_position(result_column_id).column.get())
                    ->get_data()
                    .data();

    if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                  JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
        auto new_filter_column = ColumnVector<UInt8>::create(row_count);
        auto* __restrict filter_map = new_filter_column->get_data().data();

        auto null_map_column = ColumnVector<UInt8>::create(row_count, 0);
        auto* __restrict null_map_data = null_map_column->get_data().data();

        // It contains non-first sub block of splited equal-conjuncts-matched tuples from last probe row
        if (_row_count_from_last_probe > 0) {
            _process_splited_equal_matched_tuples(0, _row_count_from_last_probe, filter_column_ptr,
                                                  null_map_data, filter_map, output_block);
            // This is the last sub block of splitted block, and no equal-conjuncts-matched tuple
            // is output in all sub blocks, need to output a tuple for this probe row
            if (is_the_last_sub_block && !_parent->_is_any_probe_match_row_output) {
                filter_map[0] = true;
                null_map_data[0] = true;
            }
        }
        int end_idx = row_count - multi_matched_output_row_count;
        // process equal-conjuncts-matched tuples that are newly generated
        // in this run if there are any.
        for (int i = _row_count_from_last_probe; i < end_idx; ++i) {
            auto join_hit = _visited_map[i] != nullptr;
            auto other_hit = filter_column_ptr[i];

            if (!other_hit) {
                for (size_t j = 0; j < _right_col_len; ++j) {
                    typeid_cast<ColumnNullable*>(
                            std::move(*output_block->get_by_position(j + _right_col_idx).column)
                                    .assume_mutable()
                                    .get())
                            ->get_null_map_data()[i] = true;
                }
            }
            null_map_data[i] = !join_hit || !other_hit;

            // For cases where one probe row matches multiple build rows for equal conjuncts,
            // all the other-conjuncts-matched tuples should be output.
            //
            // Other-conjuncts-NOT-matched tuples fall into two categories:
            //    1. The beginning consecutive one(s).
            //       For these tuples, only the last one is marked to output;
            //       If there are any following other-conjuncts-matched tuples,
            //       the last tuple is also marked NOT to output.
            //    2. All the remaining other-conjuncts-NOT-matched tuples.
            //       All these tuples are marked not to output.
            if (join_hit) {
                *_visited_map[i] |= other_hit;
                filter_map[i] = other_hit || !_same_to_prev[i] ||
                                (!filter_column_ptr[i] && filter_map[i - 1]);
                // Here to keep only hit join conjunct and other join conjunt is true need to be output.
                // if not, only some key must keep one row will output will null right table column
                if (_same_to_prev[i] && filter_map[i] && !filter_column_ptr[i - 1]) {
                    filter_map[i - 1] = false;
                }
            } else {
                filter_map[i] = true;
            }
        }

        // It contains the first sub block of splited equal-conjuncts-matched tuples of the current probe row
        if (multi_matched_output_row_count > 0) {
            _parent->_is_any_probe_match_row_output = false;
            _process_splited_equal_matched_tuples(row_count - multi_matched_output_row_count,
                                                  multi_matched_output_row_count, filter_column_ptr,
                                                  null_map_data, filter_map, output_block);
        }

        for (size_t i = 0; i < row_count; ++i) {
            if (filter_map[i]) {
                _tuple_is_null_right_flags->emplace_back(null_map_data[i]);
            }
        }
        output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
    } else if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
        // TODO: resize in advance
        auto new_filter_column = ColumnVector<UInt8>::create();
        auto& filter_map = new_filter_column->get_data();

        size_t start_row_idx = 1;
        // We are handling euqual-conjuncts matched tuples that are splitted into multiple blocks
        if (_row_count_from_last_probe > 0) {
            if (_parent->_is_any_probe_match_row_output) {
                // if any matched tuple for this probe row is output,
                // ignore all the following tuples for this probe row.
                for (int row_idx = 0; row_idx < _row_count_from_last_probe; ++row_idx) {
                    filter_map.emplace_back(false);
                }
                start_row_idx += _row_count_from_last_probe;
                if (_row_count_from_last_probe < row_count) {
                    filter_map.emplace_back(filter_column_ptr[_row_count_from_last_probe]);
                }
            } else {
                filter_map.emplace_back(filter_column_ptr[0]);
            }
        } else {
            filter_map.emplace_back(filter_column_ptr[0]);
        }
        for (size_t i = start_row_idx; i < row_count; ++i) {
            if (filter_column_ptr[i] || (_same_to_prev[i] && filter_map[i - 1])) {
                // Only last same element is true, output last one
                filter_map.push_back(true);
                filter_map[i - 1] = !_same_to_prev[i] && filter_map[i - 1];
            } else {
                filter_map.push_back(false);
            }
        }
        // It contains the first sub block of splited equal-conjuncts-matched tuples of the current probe row
        if (multi_matched_output_row_count > 0) {
            // If a matched row is output, all the equal-matched tuples in
            // the following sub blocks should be ignored
            _parent->_is_any_probe_match_row_output = filter_map[row_count - 1];
        } else if (_row_count_from_last_probe > 0 && !_parent->_is_any_probe_match_row_output) {
            // We are handling euqual-conjuncts matched tuples that are splitted into multiple blocks,
            // and no matched tuple has been output in all previous run.
            // If a tuple is output in this run, all the following mathced tuples should be ignored
            if (filter_map[_row_count_from_last_probe - 1]) {
                _parent->_is_any_probe_match_row_output = true;
            }
        }

        /// FIXME: incorrect result of semi mark join with other conjuncts(null value missed).
        if (is_mark_join) {
            auto mark_column =
                    output_block->get_by_position(orig_columns - 1).column->assume_mutable();
            ColumnFilterHelper helper(*mark_column);

            // For mark join, we only filter rows which have duplicate join keys.
            // And then, we set matched_map to the join result to do the mark join's filtering.
            for (size_t i = 1; i < row_count; ++i) {
                if (!_same_to_prev[i]) {
                    helper.insert_value(filter_map[i - 1]);
                    filter_map[i - 1] = true;
                }
            }
            helper.insert_value(filter_map[filter_map.size() - 1]);
            filter_map[filter_map.size() - 1] = true;
        }

        output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
    } else if constexpr (JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                         JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        auto new_filter_column = ColumnVector<UInt8>::create(row_count);
        auto* __restrict filter_map = new_filter_column->get_data().data();

        // for left anti join, the probe side is output only when
        // there are no matched tuples for the probe row.

        // If multiple equal-conjuncts-matched tuples is splitted into several
        // sub blocks, just filter out all the other-conjuncts-NOT-matched tuples at first,
        // and when processing the last sub block, check whether there are any
        // equal-conjuncts-matched tuple is output in all sub blocks,
        // if there are none, just pick a tuple and output.

        size_t start_row_idx = 1;
        // We are handling euqual-conjuncts matched tuples that are splitted into multiple blocks
        if (_row_count_from_last_probe > 0 && _parent->_is_any_probe_match_row_output) {
            // if any matched tuple for this probe row is output,
            // ignore all the following tuples for this probe row.
            for (int row_idx = 0; row_idx < _row_count_from_last_probe; ++row_idx) {
                filter_map[row_idx] = false;
            }
            start_row_idx += _row_count_from_last_probe;
            if (_row_count_from_last_probe < row_count) {
                filter_map[_row_count_from_last_probe] =
                        filter_column_ptr[_row_count_from_last_probe] &&
                        _visited_map[_row_count_from_last_probe];
            }
        } else {
            // Both equal conjuncts and other conjuncts are true
            filter_map[0] = filter_column_ptr[0] && _visited_map[0];
        }

        for (size_t i = start_row_idx; i < row_count; ++i) {
            if ((_visited_map[i] && filter_column_ptr[i]) ||
                (_same_to_prev[i] && filter_map[i - 1])) {
                // When either of two conditions is meet:
                // 1. Both equal conjuncts and other conjuncts are true or same_to_prev
                // 2. This row is joined from the same build side row as the previous row
                // Set filter_map[i] to true and filter_map[i - 1] to false if same_to_prev[i]
                // is true.
                filter_map[i] = true;
                filter_map[i - 1] = !_same_to_prev[i] && filter_map[i - 1];
            } else {
                filter_map[i] = false;
            }
        }

        if (is_mark_join) {
            auto& matched_map = assert_cast<ColumnVector<UInt8>&>(
                                        *(output_block->get_by_position(orig_columns - 1)
                                                  .column->assume_mutable()))
                                        .get_data();
            for (int i = 1; i < row_count; ++i) {
                if (!_same_to_prev[i]) {
                    matched_map.push_back(!filter_map[i - 1]);
                    filter_map[i - 1] = true;
                }
            }
            matched_map.push_back(!filter_map[row_count - 1]);
            filter_map[row_count - 1] = true;
        } else {
            int end_row_idx = 0;
            if (_row_count_from_last_probe > 0) {
                end_row_idx = row_count - multi_matched_output_row_count;
                if (!_parent->_is_any_probe_match_row_output) {
                    // We are handling euqual-conjuncts matched tuples that are splitted into multiple blocks,
                    // and no matched tuple has been output in all previous run.
                    // If a tuple is output in this run, all the following mathced tuples should be ignored
                    if (filter_map[_row_count_from_last_probe - 1]) {
                        _parent->_is_any_probe_match_row_output = true;
                        filter_map[_row_count_from_last_probe - 1] = false;
                    }
                    if (is_the_last_sub_block && !_parent->_is_any_probe_match_row_output) {
                        // This is the last sub block of splitted block, and no equal-conjuncts-matched tuple
                        // is output in all sub blocks, output a tuple for this probe row
                        filter_map[0] = true;
                    }
                }
                if (multi_matched_output_row_count > 0) {
                    // It contains the first sub block of splited equal-conjuncts-matched tuples of the current probe row
                    // If a matched row is output, all the equal-matched tuples in
                    // the following sub blocks should be ignored
                    _parent->_is_any_probe_match_row_output = filter_map[row_count - 1];
                    filter_map[row_count - 1] = false;
                }
            } else if (multi_matched_output_row_count > 0) {
                end_row_idx = row_count - multi_matched_output_row_count;
                // It contains the first sub block of splited equal-conjuncts-matched tuples of the current probe row
                // If a matched row is output, all the equal-matched tuples in
                // the following sub blocks should be ignored
                _parent->_is_any_probe_match_row_output = filter_map[row_count - 1];
                filter_map[row_count - 1] = false;
            } else {
                end_row_idx = row_count;
            }

            // Same to the semi join, but change the last value to opposite value
            for (int i = 1 + _row_count_from_last_probe; i < end_row_idx; ++i) {
                if (!_same_to_prev[i]) {
                    filter_map[i - 1] = !filter_map[i - 1];
                }
            }
            auto non_sub_blocks_matched_row_count =
                    row_count - _row_count_from_last_probe - multi_matched_output_row_count;
            if (non_sub_blocks_matched_row_count > 0) {
                filter_map[end_row_idx - 1] = !filter_map[end_row_idx - 1];
            }
        }

        output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
    } else if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                         JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
        for (int i = 0; i < row_count; ++i) {
            DCHECK(_visited_map[i]);
            *_visited_map[i] |= filter_column_ptr[i];
        }
    } else if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
        auto filter_size = 0;
        for (int i = 0; i < row_count; ++i) {
            DCHECK(_visited_map[i]);
            auto result = filter_column_ptr[i];
            *_visited_map[i] |= result;
            filter_size += result;
        }
        _tuple_is_null_left_flags->resize_fill(filter_size, 0);
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
        static_cast<void>(
                Block::filter_block(output_block, result_column_id,
                                    is_mark_join ? output_block->columns() : orig_columns));
    }

    return Status::OK();
}

// For left or full outer join with other conjuncts.
// If multiple equal-conjuncts-matched tuples is splitted into several
// sub blocks, just filter out all the other-conjuncts-NOT-matched tuples at first,
// and when processing the last sub block, check whether there are any
// equal-conjuncts-matched tuple is output in all sub blocks,
// if not, just pick a tuple and output.
template <int JoinOpType, typename Parent>
void ProcessHashTableProbe<JoinOpType, Parent>::_process_splited_equal_matched_tuples(
        int start_row_idx, int row_count, const UInt8* __restrict other_hit_column,
        UInt8* __restrict null_map_data, UInt8* __restrict filter_map, Block* output_block) {
    int end_row_idx = start_row_idx + row_count;
    for (int i = start_row_idx; i < end_row_idx; ++i) {
        auto join_hit = _visited_map[i] != nullptr;
        auto other_hit = other_hit_column[i];

        if (!other_hit) {
            for (size_t j = 0; j < _right_col_len; ++j) {
                typeid_cast<ColumnNullable*>(
                        std::move(*output_block->get_by_position(j + _right_col_idx).column)
                                .assume_mutable()
                                .get())
                        ->get_null_map_data()[i] = true;
            }
        }

        null_map_data[i] = !join_hit || !other_hit;
        filter_map[i] = other_hit;

        if (join_hit) {
            *_visited_map[i] |= other_hit;
        }
    }
    _parent->_is_any_probe_match_row_output |=
            simd::contain_byte(filter_map + start_row_idx, row_count, 1);
}

template <int JoinOpType, typename Parent>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, Parent>::process_data_in_hashtable(
        HashTableType& hash_table_ctx, MutableBlock& mutable_block, Block* output_block,
        bool* eos) {
    using Mapped = typename HashTableType::Mapped;
    SCOPED_TIMER(_probe_process_hashtable_timer);
    if constexpr (std::is_same_v<Mapped, RowRefListWithFlag> ||
                  std::is_same_v<Mapped, RowRefListWithFlags>) {
        hash_table_ctx.init_iterator();
        auto& mcol = mutable_block.mutable_columns();

        bool right_semi_anti_without_other = _is_right_semi_anti && !_have_other_join_conjunct;
        int right_col_idx =
                right_semi_anti_without_other ? 0 : _parent->left_table_data_types().size();
        int right_col_len = _parent->right_table_data_types().size();

        auto& iter = hash_table_ctx.iterator;
        auto block_size = 0;
        auto& visited_iter =
                std::get<ForwardIterator<Mapped>>(_parent->_outer_join_pull_visited_iter);
        _build_blocks_locs.resize(_batch_size);
        if (visited_iter.ok()) {
            if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                for (; visited_iter.ok() && block_size < _batch_size; ++visited_iter) {
                    _build_blocks_locs[block_size++] = visited_iter->row_num;
                }
            } else {
                for (; visited_iter.ok() && block_size < _batch_size; ++visited_iter) {
                    if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                        if (visited_iter->visited) {
                            _build_blocks_locs[block_size++] = visited_iter->row_num;
                        }
                    } else {
                        if (!visited_iter->visited) {
                            _build_blocks_locs[block_size++] = visited_iter->row_num;
                        }
                    }
                }
            }
            if (!visited_iter.ok()) {
                ++iter;
            }
        }

        for (; iter != hash_table_ctx.hash_table->end() && block_size < _batch_size; ++iter) {
            auto& mapped = iter->get_second();
            if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                if (mapped.visited) {
                    if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                        visited_iter = mapped.begin();
                        for (; visited_iter.ok() && block_size < _batch_size; ++visited_iter) {
                            _build_blocks_locs[block_size++] = visited_iter->row_num;
                        }
                        if (visited_iter.ok()) {
                            // block_size >= _batch_size, quit for loop
                            break;
                        }
                    }
                } else {
                    if constexpr (JoinOpType != TJoinOp::RIGHT_SEMI_JOIN) {
                        visited_iter = mapped.begin();
                        for (; visited_iter.ok() && block_size < _batch_size; ++visited_iter) {
                            _build_blocks_locs[block_size++] = visited_iter->row_num;
                        }
                        if (visited_iter.ok()) {
                            // block_size >= _batch_size, quit for loop
                            break;
                        }
                    }
                }
            } else {
                visited_iter = mapped.begin();
                for (; visited_iter.ok() && block_size < _batch_size; ++visited_iter) {
                    if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                        if (visited_iter->visited) {
                            _build_blocks_locs[block_size++] = visited_iter->row_num;
                        }
                    } else {
                        if (!visited_iter->visited) {
                            _build_blocks_locs[block_size++] = visited_iter->row_num;
                        }
                    }
                }
                if (visited_iter.ok()) {
                    // block_size >= _batch_size, quit for loop
                    break;
                }
            }
        }
        _build_blocks_locs.resize(block_size);

        const auto size = _build_blocks_locs.size();
        _build_indexs.resize(_build_blocks_locs.size());
        for (int i = 0; i < size; i++) {
            _build_indexs[i] = _build_blocks_locs[i];
        }

        for (size_t j = 0; j < right_col_len; ++j) {
            const auto& column = *_build_block->get_by_position(j).column;
            mcol[j + right_col_idx]->insert_indices_from_join(
                    column, _build_indexs.data(), _build_indexs.data() + _build_indexs.size());
        }

        // just resize the left table column in case with other conjunct to make block size is not zero
        if (_is_right_semi_anti && _have_other_join_conjunct) {
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
        *eos = iter == hash_table_ctx.hash_table->end();
        output_block->swap(
                mutable_block.to_block(right_semi_anti_without_other ? right_col_idx : 0));
        DCHECK(block_size <= _batch_size);
        return Status::OK();
    } else {
        LOG(FATAL) << "Invalid RowRefList";
        return Status::InvalidArgument("Invalid RowRefList");
    }
}

template <int JoinOpType, typename Parent>
template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, Parent>::process(
        HashTableType& hash_table_ctx, ConstNullMapPtr null_map, MutableBlock& mutable_block,
        Block* output_block, size_t probe_rows, bool is_mark_join, bool have_other_join_conjunct) {
    Status res;
    if constexpr (!std::is_same_v<typename HashTableType::Mapped, RowRefListWithFlags>) {
        if (have_other_join_conjunct) {
            res = Status::InvalidArgument("Invalid HashTableType::Mapped");
        } else {
            std::visit(
                    [&](auto is_mark_join) {
                        res = do_process<need_null_map_for_probe, ignore_null, HashTableType, false,
                                         is_mark_join>(hash_table_ctx, null_map, mutable_block,
                                                       output_block, probe_rows);
                    },
                    make_bool_variant(is_mark_join));
        }
    } else {
        std::visit(
                [&](auto is_mark_join, auto have_other_join_conjunct) {
                    res = do_process<need_null_map_for_probe, ignore_null, HashTableType,
                                     have_other_join_conjunct, is_mark_join>(
                            hash_table_ctx, null_map, mutable_block, output_block, probe_rows);
                },
                make_bool_variant(is_mark_join), make_bool_variant(have_other_join_conjunct));
    }
    return res;
}

template <typename T>
struct ExtractType;

template <typename T, typename U>
struct ExtractType<T(U)> {
    using Type = U;
};

#define INSTANTIATION(JoinOpType, Parent, T)                                                      \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<false, false, ExtractType<void(T)>::Type>( \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<false, true, ExtractType<void(T)>::Type>(  \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<true, false, ExtractType<void(T)>::Type>(  \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<true, true, ExtractType<void(T)>::Type>(   \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
                                                                                                  \
    template Status ProcessHashTableProbe<JoinOpType, Parent>::process_data_in_hashtable<         \
            ExtractType<void(T)>::Type>(ExtractType<void(T)>::Type & hash_table_ctx,              \
                                        MutableBlock & mutable_block, Block * output_block,       \
                                        bool* eos)

#define INSTANTIATION_FOR1(JoinOpType, Parent)                                                     \
    template struct ProcessHashTableProbe<JoinOpType, Parent>;                                     \
                                                                                                   \
    INSTANTIATION(JoinOpType, Parent, (SerializedHashTableContext<RowRefList>));                   \
    INSTANTIATION(JoinOpType, Parent, (I8HashTableContext<RowRefList>));                           \
    INSTANTIATION(JoinOpType, Parent, (I16HashTableContext<RowRefList>));                          \
    INSTANTIATION(JoinOpType, Parent, (I32HashTableContext<RowRefList>));                          \
    INSTANTIATION(JoinOpType, Parent, (I64HashTableContext<RowRefList>));                          \
    INSTANTIATION(JoinOpType, Parent, (I128HashTableContext<RowRefList>));                         \
    INSTANTIATION(JoinOpType, Parent, (I256HashTableContext<RowRefList>));                         \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<true, RowRefList>));            \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<false, RowRefList>));           \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<true, RowRefList>));           \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<false, RowRefList>));          \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<true, RowRefList>));           \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<false, RowRefList>));          \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<true, RowRefList>));           \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<false, RowRefList>));          \
    INSTANTIATION(JoinOpType, Parent, (SerializedHashTableContext<RowRefListWithFlag>));           \
    INSTANTIATION(JoinOpType, Parent, (I8HashTableContext<RowRefListWithFlag>));                   \
    INSTANTIATION(JoinOpType, Parent, (I16HashTableContext<RowRefListWithFlag>));                  \
    INSTANTIATION(JoinOpType, Parent, (I32HashTableContext<RowRefListWithFlag>));                  \
    INSTANTIATION(JoinOpType, Parent, (I64HashTableContext<RowRefListWithFlag>));                  \
    INSTANTIATION(JoinOpType, Parent, (I128HashTableContext<RowRefListWithFlag>));                 \
    INSTANTIATION(JoinOpType, Parent, (I256HashTableContext<RowRefListWithFlag>));                 \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<true, RowRefListWithFlag>));    \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<false, RowRefListWithFlag>));   \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<true, RowRefListWithFlag>));   \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<false, RowRefListWithFlag>));  \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<true, RowRefListWithFlag>));   \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<false, RowRefListWithFlag>));  \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<true, RowRefListWithFlag>));   \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<false, RowRefListWithFlag>));  \
    INSTANTIATION(JoinOpType, Parent, (SerializedHashTableContext<RowRefListWithFlags>));          \
    INSTANTIATION(JoinOpType, Parent, (I8HashTableContext<RowRefListWithFlags>));                  \
    INSTANTIATION(JoinOpType, Parent, (I16HashTableContext<RowRefListWithFlags>));                 \
    INSTANTIATION(JoinOpType, Parent, (I32HashTableContext<RowRefListWithFlags>));                 \
    INSTANTIATION(JoinOpType, Parent, (I64HashTableContext<RowRefListWithFlags>));                 \
    INSTANTIATION(JoinOpType, Parent, (I128HashTableContext<RowRefListWithFlags>));                \
    INSTANTIATION(JoinOpType, Parent, (I256HashTableContext<RowRefListWithFlags>));                \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<true, RowRefListWithFlags>));   \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<false, RowRefListWithFlags>));  \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<true, RowRefListWithFlags>));  \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<false, RowRefListWithFlags>)); \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<true, RowRefListWithFlags>));  \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<false, RowRefListWithFlags>)); \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<true, RowRefListWithFlags>));  \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<false, RowRefListWithFlags>))

#define INSTANTIATION_FOR(JoinOpType)             \
    INSTANTIATION_FOR1(JoinOpType, HashJoinNode); \
    INSTANTIATION_FOR1(JoinOpType, pipeline::HashJoinProbeLocalState)

} // namespace doris::vectorized
