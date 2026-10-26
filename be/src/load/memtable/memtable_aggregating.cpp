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

#include "load/memtable/memtable_aggregating.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_complex.h"
#include "core/column/column_vector.h"
#include "core/value/bitmap_value.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "load/memtable/memtable_sort.h"
#include "runtime/descriptors.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/stopwatch.hpp"

namespace doris {

AggregatingMemTable::AggregatingMemTable(int64_t tablet_id,
                                         std::shared_ptr<TabletSchema> tablet_schema,
                                         const std::vector<SlotDescriptor*>* slot_descs,
                                         TupleDescriptor* tuple_desc,
                                         PartialUpdateInfo* partial_update_info,
                                         const std::shared_ptr<ResourceContext>& resource_ctx,
                                         bool need_lsn)
        : MemTable(tablet_id, std::move(tablet_schema), slot_descs, tuple_desc, partial_update_info,
                   resource_ctx, need_lsn),
          _vec_row_comparator(_tablet_schema) {
    // These members are owned by std::allocator / std::make_unique, which Doris does not route
    // through the memtable tracker, so there is nothing to account for here.
    _row_in_blocks = std::make_unique<DorisVector<std::shared_ptr<RowInBlock>>>();
    _agg_functions.resize(_tablet_schema->num_columns());
    _offsets_of_aggregate_states.resize(_tablet_schema->num_columns());
}

AggregatingMemTable::~AggregatingMemTable() {
    // Derived members are destroyed after this body, so release the tracked row metadata and the
    // aggregation arena here, while the memtable tracker still accounts for them.
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    for (auto it = _row_in_blocks->begin(); it != _row_in_blocks->end(); it++) {
        if (!(*it)->has_init_agg()) {
            continue;
        }
        // We should release agg_places here, because they are not released when a
        // load is canceled.
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            DCHECK(function != nullptr);
            function->destroy((*it)->agg_places(i));
        }
    }

    _arena.clear(true);
    _row_in_blocks.reset();
    _agg_functions.clear();
}

Status AggregatingMemTable::_on_first_insertion() {
    _init_aggregation_columns();
    RETURN_IF_CATCH_EXCEPTION(_init_agg_functions());
    return Status::OK();
}

void AggregatingMemTable::_init_aggregation_columns() {
    if (_tablet_schema->has_sequence_col()) {
        _seq_col_idx_in_block = _tablet_schema->sequence_col_idx();
    }
}

AggregateFunctionPtr AggregatingMemTable::_make_agg_function(int cid) {
    auto function = _tablet_schema->column(cid).get_aggregate_function(
            AGG_LOAD_SUFFIX, _tablet_schema->column(cid).get_be_exec_version(),
            _input_mutable_block.get_datatype_by_position(cid));
    if (function == nullptr) {
        LOG(WARNING) << "column get aggregate function failed, column="
                     << _tablet_schema->column(cid).name();
    }
    return function;
}

void AggregatingMemTable::_init_agg_functions() {
    // _agg_functions and _offsets_of_aggregate_states are sized by the tablet schema and indexed by
    // the input column count below, so the input can never address more columns than the schema.
    // The invariant holds because _num_columns is _column_offset.size(): one entry per slot matched
    // in the tuple, plus at most one appended auto-increment column, and that column is appended
    // only when the input omits it. A partial update's slots are a subset of the schema columns.
    DORIS_CHECK(_num_columns <= _tablet_schema->num_columns());
    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        AggregateFunctionPtr function = _make_agg_function(static_cast<int>(cid));

        DCHECK(function != nullptr);
        const auto* input_column = _input_mutable_block.get_column_by_position(cid).get();
        const IColumn* columns[] = {input_column};
        function->check_input_columns_type(columns);
        function->check_result_column_type(*_output_mutable_block.get_column_by_position(cid));
        _agg_functions[cid] = function;
    }

    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        _offsets_of_aggregate_states[cid] = _total_size_of_aggregate_states;
        _total_size_of_aggregate_states += _agg_functions[cid]->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (cid + 1 < _num_columns) {
            size_t alignment_of_next_state = _agg_functions[cid + 1]->align_of_data();

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }
}

void AggregatingMemTable::_reserve_rows(size_t num_rows, size_t cursor) {
    DORIS_CHECK(_row_in_blocks->size() == cursor);
    reserve_for_append(*_row_in_blocks, num_rows, MAX_MEMTABLE_ROWS);
}

void AggregatingMemTable::_append_row_metadata(size_t num_rows, size_t cursor,
                                               const DorisVector<int64_t>& allocated_lsns) {
    DBUG_EXECUTE_IF("MemTable.insert.row_batch_allocation",
                    DBUG_RUN_CALLBACK(static_cast<MemTable*>(this)));
    auto row_batch = make_row_batch(num_rows, [&](size_t i) {
        return RowInBlock(cursor + i, _need_lsn ? allocated_lsns[i] : 0);
    });
    for (size_t i = 0; i < num_rows; ++i) {
        _row_in_blocks->emplace_back(row_batch, &row_batch[i]);
    }
}

size_t AggregatingMemTable::_sort() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    return memtable_detail::sort_rows(
            _input_mutable_block, *_tablet_schema, _last_sorted_pos, *_row_in_blocks,
            [](const std::shared_ptr<RowInBlock>& row) { return row->_row_pos; }, false);
}

Status AggregatingMemTable::_put_into_output(Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    if (_need_lsn) {
        _output_allocated_lsns->reserve(_output_allocated_lsns->size() + in_block.rows());
    }
    DorisVector<uint32_t> row_pos_vec;
    row_pos_vec.reserve(in_block.rows());
    for (const auto& row : *_row_in_blocks) {
        row_pos_vec.emplace_back(row->_row_pos);
        _append_output_allocated_lsn(row.get());
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + row_pos_vec.size());
}

Status AggregatingMemTable::_to_block(std::unique_ptr<Block>* res) {
    _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
    size_t same_keys_num = _sort();
    if (same_keys_num == 0) {
        Block in_block = _input_mutable_block.to_block();
        RETURN_IF_ERROR(_put_into_output(in_block));
    } else {
        (_skip_bitmap_col_idx == -1) ? _aggregate<true, false>() : _aggregate<true, true>();
    }
    RETURN_IF_ERROR(_after_aggregate());
    if (_need_lsn) {
        DCHECK_EQ(_output_allocated_lsns->size(), _output_mutable_block.rows());
    }
    _input_mutable_block.clear();
    *res = Block::create_unique(_output_mutable_block.to_block());
    return Status::OK();
}

void AggregatingMemTable::shrink_memtable_by_agg() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    size_t same_keys_num = _sort();
    if (same_keys_num != 0) {
        (_skip_bitmap_col_idx == -1) ? _aggregate<false, false>() : _aggregate<false, true>();
    }
    _last_agg_pos = memory_usage();
}

void AggregatingMemTable::_merge_allocated_lsn(RowInBlock* src_row, RowInBlock* dst_row) {
    if (_need_lsn) {
        dst_row->_allocated_lsn = std::max(dst_row->_allocated_lsn, src_row->_allocated_lsn);
    }
}

void AggregatingMemTable::_append_output_allocated_lsn(RowInBlock* row) {
    if (_need_lsn) {
        _output_allocated_lsns->emplace_back(row->_allocated_lsn);
    }
}

void AggregatingMemTable::_aggregate_two_row_with_sequence_map(MutableBlock& mutable_block,
                                                               RowInBlock* src_row,
                                                               RowInBlock* dst_row) {
    _merge_allocated_lsn(src_row, dst_row);
    // for each mapping replace value columns according to the sequence column compare result
    // for example: a b c d s1 s2  (key:a , s1=>[b,c], s2=>[d])
    // src row:     1 4 5 6  8 9
    // dst row:     1 2 3 4  7 10
    // after aggregate
    // dst row:     1 4 5 4  8 10  (b,c,s1 will be replaced, d,s2)
    const auto& seq_map = _tablet_schema->seq_col_idx_to_value_cols_idx();
    for (const auto& it : seq_map) {
        auto sequence = it.first;
        auto* sequence_col_ptr = mutable_block.mutable_columns()[sequence].get();
        auto res = sequence_col_ptr->compare_at(dst_row->_row_pos, src_row->_row_pos,
                                                *sequence_col_ptr, -1);
        if (res > 0) {
            continue;
        }
        for (auto cid : it.second) {
            if (cid < _num_columns) {
                auto* col_ptr = mutable_block.mutable_columns()[cid].get();
                _agg_functions[cid]->add(dst_row->agg_places(cid),
                                         const_cast<const doris::IColumn**>(&col_ptr),
                                         src_row->_row_pos, _arena);
            }
        }
        if (sequence < _num_columns) {
            _agg_functions[sequence]->add(dst_row->agg_places(sequence),
                                          const_cast<const doris::IColumn**>(&sequence_col_ptr),
                                          src_row->_row_pos, _arena);
            // must use replace column instead of update row_pos
            // because one row may have multi sequence column
            // and agg function add method won't change the real column value
            sequence_col_ptr->replace_column_data(*sequence_col_ptr, src_row->_row_pos,
                                                  dst_row->_row_pos);
        }
    }
}

template <bool has_skip_bitmap_col>
void AggregatingMemTable::_aggregate_two_row_in_block(MutableBlock& mutable_block,
                                                      RowInBlock* src_row, RowInBlock* dst_row) {
    _merge_allocated_lsn(src_row, dst_row);
    // for flexible partial update, the caller must guarantees that either src_row and dst_row
    // both specify the sequence column, or src_row and dst_row both don't specify the
    // sequence column
    if (_tablet_schema->has_sequence_col() && _seq_col_idx_in_block >= 0) {
        DCHECK_LT(_seq_col_idx_in_block, mutable_block.columns());
        auto col_ptr = mutable_block.mutable_columns()[_seq_col_idx_in_block].get();
        auto res = col_ptr->compare_at(dst_row->_row_pos, src_row->_row_pos, *col_ptr, -1);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
        // need to update the row pos in dst row to the src row pos when has
        // sequence column
        dst_row->_row_pos = src_row->_row_pos;
    }
    // dst is non-sequence row, or dst sequence is smaller
    if constexpr (!has_skip_bitmap_col) {
        DCHECK(_skip_bitmap_col_idx == -1);
        for (size_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
            auto* col_ptr = mutable_block.mutable_columns()[cid].get();
            _agg_functions[cid]->add(dst_row->agg_places(cid),
                                     const_cast<const doris::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    } else {
        DCHECK(_skip_bitmap_col_idx != -1);
        DCHECK_LT(_skip_bitmap_col_idx, mutable_block.columns());
        const BitmapValue& skip_bitmap =
                assert_cast<ColumnBitmap*, TypeCheckOnRelease::DISABLE>(
                        mutable_block.mutable_columns()[_skip_bitmap_col_idx].get())
                        ->get_data()[src_row->_row_pos];
        for (size_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
            const auto& col = _tablet_schema->column(cid);
            if (cid != _skip_bitmap_col_idx && skip_bitmap.contains(col.unique_id())) {
                continue;
            }
            auto* col_ptr = mutable_block.mutable_columns()[cid].get();
            _agg_functions[cid]->add(dst_row->agg_places(cid),
                                     const_cast<const doris::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    }
}

template void AggregatingMemTable::_aggregate_two_row_in_block<true>(MutableBlock& mutable_block,
                                                                     RowInBlock* src_row,
                                                                     RowInBlock* dst_row);

template <bool is_final>
void AggregatingMemTable::_finalize_one_row(RowInBlock* row, MutableBlock& mutable_block,
                                            int row_pos) {
    // move key columns
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); ++i) {
        _output_mutable_block.get_column_by_position(i)->insert_from(
                *mutable_block.get_column_by_position(i), row->_row_pos);
    }
    if (row->has_init_agg()) {
        // get value columns from agg_places
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
            function->insert_result_into(agg_place, *col_ptr);

            if constexpr (is_final) {
                function->destroy(agg_place);
            } else {
                function->reset(agg_place);
            }
        }

        if constexpr (is_final) {
            row->remove_init_agg();
        } else {
            for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
                auto function = _agg_functions[i];
                auto* agg_place = row->agg_places(i);
                auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
                function->add(agg_place, const_cast<const doris::IColumn**>(&col_ptr), row_pos,
                              _arena);
            }
        }
    } else {
        // move columns for rows do not need agg
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            _output_mutable_block.get_column_by_position(i)->insert_from(
                    *mutable_block.get_column_by_position(i), row->_row_pos);
        }
    }
    _append_output_allocated_lsn(row);
    if constexpr (!is_final) {
        row->_row_pos = row_pos;
    }
}

void AggregatingMemTable::_init_row_for_agg(RowInBlock* row, MutableBlock& mutable_block) {
    auto* agg_mem = _arena.aligned_alloc(_total_size_of_aggregate_states, 16);
    auto first_value_column = _tablet_schema->num_key_columns();
    auto cid = first_value_column;
    try {
        for (; cid < _num_columns; ++cid) {
            _agg_functions[cid]->create(agg_mem + _offsets_of_aggregate_states[cid]);
        }
    } catch (...) {
        // A failed constructor leaves only the completed prefix available for destruction.
        while (cid > first_value_column) {
            --cid;
            _agg_functions[cid]->destroy(agg_mem + _offsets_of_aggregate_states[cid]);
        }
        throw;
    }
    // add() can allocate. Publish the row only after all states can be safely destroyed.
    row->init_agg_places(agg_mem, _offsets_of_aggregate_states.data());
    for (cid = first_value_column; cid < _num_columns; ++cid) {
        auto* col_ptr = mutable_block.mutable_columns()[cid].get();
        auto* data = row->agg_places(cid);
        _agg_functions[cid]->add(data, const_cast<const doris::IColumn**>(&col_ptr), row->_row_pos,
                                 _arena);
    }
}

void AggregatingMemTable::_clear_row_agg(RowInBlock* row) {
    if (row->has_init_agg()) {
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            function->destroy(agg_place);
        }
        row->remove_init_agg();
    }
}

// only in `to_block` the `is_final` flag will be true, in other cases, it will be false
template <bool is_final, bool has_skip_bitmap_col>
void AggregatingMemTable::_aggregate() {
    SCOPED_RAW_TIMER(&_stat.agg_ns);
    _stat.agg_times++;
    Block in_block = _input_mutable_block.to_block();
    std::unique_ptr<Block> empty_input_block = in_block.create_same_struct_block(0);
    MutableBlock mutable_block = MutableBlock::build_mutable_block(std::move(in_block));
    DorisVector<std::shared_ptr<RowInBlock>> temp_row_in_blocks;
    temp_row_in_blocks.reserve(_last_sorted_pos);
    //only init agg if needed

    if constexpr (!has_skip_bitmap_col) {
        RowInBlock* prev_row = nullptr;
        int row_pos = -1;
        for (const auto& cur_row_ptr : *_row_in_blocks) {
            RowInBlock* cur_row = cur_row_ptr.get();
            if (!temp_row_in_blocks.empty() &&
                _vec_row_comparator(mutable_block, prev_row, cur_row) == 0) {
                if (!prev_row->has_init_agg()) {
                    _init_row_for_agg(prev_row, mutable_block);
                }
                _stat.merged_rows++;
                if (_tablet_schema->has_seq_map()) {
                    _aggregate_two_row_with_sequence_map(mutable_block, cur_row, prev_row);
                } else {
                    _aggregate_two_row_in_block<has_skip_bitmap_col>(mutable_block, cur_row,
                                                                     prev_row);
                }

                // Clean up aggregation state of the merged row to avoid memory leak
                if (cur_row) {
                    _clear_row_agg(cur_row);
                }
            } else {
                prev_row = cur_row;
                if (!temp_row_in_blocks.empty()) {
                    // The rows from the previous batch of _row_in_blocks have been merged into temp_row_in_blocks,
                    // now call finalize to write the aggregation results into _output_mutable_block.
                    _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), mutable_block,
                                                row_pos);
                }
                temp_row_in_blocks.push_back(cur_row_ptr);
                row_pos++;
            }
        }
        if (!temp_row_in_blocks.empty()) {
            // finalize the last low
            _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), mutable_block, row_pos);
        }
    } else {
        _aggregate_flexible_partial_update(is_final, mutable_block, temp_row_in_blocks);
    }
    if constexpr (!is_final) {
        // if is not final, we collect the agg results to input_block and then continue to insert
        _input_mutable_block.swap(_output_mutable_block);
        //TODO(weixang):opt here.
        _output_mutable_block = MutableBlock::build_mutable_block(std::move(*empty_input_block));
        _output_mutable_block.clear_column_data();
        _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
        // Repack surviving rows so they do not retain batches of merged-away rows.
        // Aggregation states belong to the memtable; preserve their pointers and flags.
        DBUG_EXECUTE_IF("MemTable.aggregate.row_batch_allocation",
                        DBUG_RUN_CALLBACK(static_cast<MemTable*>(this)));
        auto rows = make_row_batch(temp_row_in_blocks.size(),
                                   [&](size_t i) { return *temp_row_in_blocks[i]; });
        for (size_t i = 0; i < temp_row_in_blocks.size(); ++i) {
            temp_row_in_blocks[i] = {rows, &rows[i]};
        }
        *_row_in_blocks = temp_row_in_blocks;
        _last_sorted_pos = _row_in_blocks->size();
    }
}

} // namespace doris
