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

#include "olap/memtable.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <pdqsort.h>

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "bvar/bvar.h"
#include "common/config.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "tablet_meta.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"

namespace doris {

bvar::Adder<int64_t> g_memtable_cnt("memtable_cnt");

using namespace ErrorCode;

MemTable::MemTable(int64_t tablet_id, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
                   const std::shared_ptr<MemTracker>& insert_mem_tracker,
                   const std::shared_ptr<MemTracker>& flush_mem_tracker)
        : _tablet_id(tablet_id),
          _enable_unique_key_mow(enable_unique_key_mow),
          _keys_type(tablet_schema->keys_type()),
          _tablet_schema(tablet_schema),
          _insert_mem_tracker(insert_mem_tracker),
          _flush_mem_tracker(flush_mem_tracker),
          _is_first_insertion(true),
          _agg_functions(tablet_schema->num_columns()),
          _offsets_of_aggregate_states(tablet_schema->num_columns()),
          _total_size_of_aggregate_states(0),
          _mem_usage(0) {
    g_memtable_cnt << 1;
#ifndef BE_TEST
    _insert_mem_tracker_use_hook = std::make_unique<MemTracker>(
            fmt::format("MemTableHookInsert:TabletId={}", std::to_string(tablet_id)),
            ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker());
#else
    _insert_mem_tracker_use_hook = std::make_unique<MemTracker>(
            fmt::format("MemTableHookInsert:TabletId={}", std::to_string(tablet_id)));
#endif
    _arena = std::make_unique<vectorized::Arena>();
    _vec_row_comparator = std::make_shared<RowInBlockComparator>(_tablet_schema);
    // TODO: Support ZOrderComparator in the future
    _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
    _num_columns = _tablet_schema->num_columns();
    if (partial_update_info != nullptr) {
        _is_partial_update = partial_update_info->is_partial_update;
        if (_is_partial_update) {
            _num_columns = partial_update_info->partial_update_input_columns.size();
        }
    }
}
void MemTable::_init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                                  const TupleDescriptor* tuple_desc) {
    for (auto slot_desc : *slot_descs) {
        const auto& slots = tuple_desc->slots();
        for (int j = 0; j < slots.size(); ++j) {
            if (slot_desc->id() == slots[j]->id()) {
                _column_offset.emplace_back(j);
                break;
            }
        }
    }
}

void MemTable::_init_agg_functions(const vectorized::Block* block) {
    for (uint32_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        vectorized::AggregateFunctionPtr function;
        if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow) {
            // In such table, non-key column's aggregation type is NONE, so we need to construct
            // the aggregate function manually.
            function = vectorized::AggregateFunctionSimpleFactory::instance().get(
                    "replace_load", {block->get_data_type(cid)},
                    block->get_data_type(cid)->is_nullable());
        } else {
            function =
                    _tablet_schema->column(cid).get_aggregate_function(vectorized::AGG_LOAD_SUFFIX);
            if (function == nullptr) {
                LOG(WARNING) << "column get aggregate function failed, column="
                             << _tablet_schema->column(cid).name();
            }
        }

        DCHECK(function != nullptr);
        _agg_functions[cid] = function;
    }

    for (uint32_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
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

MemTable::~MemTable() {
    g_memtable_cnt << -1;
    if (_keys_type != KeysType::DUP_KEYS) {
        for (auto it = _row_in_blocks.begin(); it != _row_in_blocks.end(); it++) {
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
    }
    std::for_each(_row_in_blocks.begin(), _row_in_blocks.end(), std::default_delete<RowInBlock>());
    _insert_mem_tracker->release(_mem_usage);
    _flush_mem_tracker->set_consumption(0);
    DCHECK_EQ(_insert_mem_tracker->consumption(), 0)
            << std::endl
            << MemTracker::log_usage(_insert_mem_tracker->make_snapshot());
    DCHECK_EQ(_flush_mem_tracker->consumption(), 0);
}

int RowInBlockComparator::operator()(const RowInBlock* left, const RowInBlock* right) const {
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _tablet_schema->num_key_columns(),
                               *_pblock, -1);
}

void MemTable::insert(const vectorized::Block* input_block, const std::vector<int>& row_idxs,
                      bool is_append) {
    SCOPED_CONSUME_MEM_TRACKER(_insert_mem_tracker_use_hook.get());
    vectorized::Block target_block = *input_block;
    target_block = input_block->copy_block(_column_offset);
    if (_is_first_insertion) {
        _is_first_insertion = false;
        auto cloneBlock = target_block.clone_without_columns();
        _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        _vec_row_comparator->set_block(&_input_mutable_block);
        _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        if (_keys_type != KeysType::DUP_KEYS) {
            _init_agg_functions(&target_block);
        }
        if (_tablet_schema->has_sequence_col()) {
            if (_is_partial_update) {
                // for unique key partial update, sequence column index in block
                // may be different with the index in `_tablet_schema`
                for (size_t i = 0; i < cloneBlock.columns(); i++) {
                    if (cloneBlock.get_by_position(i).name == SEQUENCE_COL) {
                        _seq_col_idx_in_block = i;
                        break;
                    }
                }
            } else {
                _seq_col_idx_in_block = _tablet_schema->sequence_col_idx();
            }
        }
    }

    auto num_rows = row_idxs.size();
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    if (is_append) {
        // Append the block, call insert range from
        _input_mutable_block.add_rows(&target_block, 0, target_block.rows());
        num_rows = target_block.rows();
    } else {
        _input_mutable_block.add_rows(&target_block, row_idxs.data(), row_idxs.data() + num_rows);
    }
    size_t input_size = target_block.allocated_bytes() * num_rows / target_block.rows();
    _mem_usage += input_size;
    _insert_mem_tracker->consume(input_size);
    for (int i = 0; i < num_rows; i++) {
        _row_in_blocks.emplace_back(new RowInBlock {cursor_in_mutableblock + i});
    }

    _stat.raw_rows += num_rows;
}

void MemTable::_aggregate_two_row_in_block(vectorized::MutableBlock& mutable_block,
                                           RowInBlock* src_row, RowInBlock* dst_row) {
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
    for (uint32_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        auto col_ptr = mutable_block.mutable_columns()[cid].get();
        _agg_functions[cid]->add(dst_row->agg_places(cid),
                                 const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                 src_row->_row_pos, _arena.get());
    }
}
void MemTable::_put_into_output(vectorized::Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    std::vector<int> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < _row_in_blocks.size(); i++) {
        row_pos_vec.emplace_back(_row_in_blocks[i]->_row_pos);
    }
    _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                   row_pos_vec.data() + in_block.rows());
}

size_t MemTable::_sort() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    size_t same_keys_num = 0;
    // sort new rows
    Tie tie = Tie(_last_sorted_pos, _row_in_blocks.size());
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        auto cmp = [&](const RowInBlock* lhs, const RowInBlock* rhs) -> int {
            return _input_mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, i, -1);
        };
        _sort_one_column(_row_in_blocks, tie, cmp);
    }
    bool is_dup = (_keys_type == KeysType::DUP_KEYS);
    // sort extra round by _row_pos to make the sort stable
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(_row_in_blocks.begin(), iter.left()),
                std::next(_row_in_blocks.begin(), iter.right()),
                [&is_dup](const RowInBlock* lhs, const RowInBlock* rhs) -> bool {
                    return is_dup ? lhs->_row_pos > rhs->_row_pos : lhs->_row_pos < rhs->_row_pos;
                });
        same_keys_num += iter.right() - iter.left();
    }
    // merge new rows and old rows
    _vec_row_comparator->set_block(&_input_mutable_block);
    auto cmp_func = [this, is_dup, &same_keys_num](const RowInBlock* l,
                                                   const RowInBlock* r) -> bool {
        auto value = (*(this->_vec_row_comparator))(l, r);
        if (value == 0) {
            same_keys_num++;
            return is_dup ? l->_row_pos > r->_row_pos : l->_row_pos < r->_row_pos;
        } else {
            return value < 0;
        }
    };
    auto new_row_it = std::next(_row_in_blocks.begin(), _last_sorted_pos);
    std::inplace_merge(_row_in_blocks.begin(), new_row_it, _row_in_blocks.end(), cmp_func);
    _last_sorted_pos = _row_in_blocks.size();
    return same_keys_num;
}

void MemTable::_sort_one_column(std::vector<RowInBlock*>& row_in_blocks, Tie& tie,
                                std::function<int(const RowInBlock*, const RowInBlock*)> cmp) {
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), iter.left()),
                std::next(row_in_blocks.begin(), iter.right()),
                [&cmp](auto lhs, auto rhs) -> bool { return cmp(lhs, rhs) < 0; });
        tie[iter.left()] = 0;
        for (int i = iter.left() + 1; i < iter.right(); i++) {
            tie[i] = (cmp(row_in_blocks[i - 1], row_in_blocks[i]) == 0);
        }
    }
}

template <bool is_final>
void MemTable::_finalize_one_row(RowInBlock* row,
                                 const vectorized::ColumnsWithTypeAndName& block_data,
                                 int row_pos) {
    // move key columns
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); ++i) {
        _output_mutable_block.get_column_by_position(i)->insert_from(*block_data[i].column.get(),
                                                                     row->_row_pos);
    }
    if (row->has_init_agg()) {
        // get value columns from agg_places
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto agg_place = row->agg_places(i);
            auto col_ptr = _output_mutable_block.get_column_by_position(i).get();
            function->insert_result_into(agg_place, *col_ptr);
            if constexpr (is_final) {
                function->destroy(agg_place);
            } else {
                function->reset(agg_place);
                function->add(agg_place, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                              row_pos, _arena.get());
            }
        }
        if constexpr (is_final) {
            row->remove_init_agg();
        }
    } else {
        // move columns for rows do not need agg
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            _output_mutable_block.get_column_by_position(i)->insert_from(
                    *block_data[i].column.get(), row->_row_pos);
        }
    }
    if constexpr (!is_final) {
        row->_row_pos = row_pos;
    }
}

template <bool is_final>
void MemTable::_aggregate() {
    SCOPED_RAW_TIMER(&_stat.agg_ns);
    _stat.agg_times++;
    vectorized::Block in_block = _input_mutable_block.to_block();
    vectorized::MutableBlock mutable_block =
            vectorized::MutableBlock::build_mutable_block(&in_block);
    _vec_row_comparator->set_block(&mutable_block);
    auto& block_data = in_block.get_columns_with_type_and_name();
    std::vector<RowInBlock*> temp_row_in_blocks;
    temp_row_in_blocks.reserve(_last_sorted_pos);
    RowInBlock* prev_row = nullptr;
    int row_pos = -1;
    //only init agg if needed
    for (int i = 0; i < _row_in_blocks.size(); i++) {
        if (!temp_row_in_blocks.empty() &&
            (*_vec_row_comparator)(prev_row, _row_in_blocks[i]) == 0) {
            if (!prev_row->has_init_agg()) {
                prev_row->init_agg_places(
                        _arena->aligned_alloc(_total_size_of_aggregate_states, 16),
                        _offsets_of_aggregate_states.data());
                for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; cid++) {
                    auto col_ptr = mutable_block.mutable_columns()[cid].get();
                    auto data = prev_row->agg_places(cid);
                    _agg_functions[cid]->create(data);
                    _agg_functions[cid]->add(
                            data, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                            prev_row->_row_pos, _arena.get());
                }
            }
            _stat.merged_rows++;
            _aggregate_two_row_in_block(mutable_block, _row_in_blocks[i], prev_row);
        } else {
            prev_row = _row_in_blocks[i];
            if (!temp_row_in_blocks.empty()) {
                // no more rows to merge for prev row, finalize it
                _finalize_one_row<is_final>(temp_row_in_blocks.back(), block_data, row_pos);
            }
            temp_row_in_blocks.push_back(prev_row);
            row_pos++;
        }
    }
    if (!temp_row_in_blocks.empty()) {
        // finalize the last low
        _finalize_one_row<is_final>(temp_row_in_blocks.back(), block_data, row_pos);
    }
    if constexpr (!is_final) {
        // if is not final, we collect the agg results to input_block and then continue to insert
        size_t shrunked_after_agg = _output_mutable_block.allocated_bytes();
        // flush will not run here, so will not duplicate `_flush_mem_tracker`
        _insert_mem_tracker->consume(shrunked_after_agg - _mem_usage);
        _mem_usage = shrunked_after_agg;
        _input_mutable_block.swap(_output_mutable_block);
        //TODO(weixang):opt here.
        std::unique_ptr<vectorized::Block> empty_input_block = in_block.create_same_struct_block(0);
        _output_mutable_block =
                vectorized::MutableBlock::build_mutable_block(empty_input_block.get());
        _output_mutable_block.clear_column_data();
    }
}

void MemTable::shrink_memtable_by_agg() {
    SCOPED_CONSUME_MEM_TRACKER(_insert_mem_tracker_use_hook.get());
    if (_keys_type == KeysType::DUP_KEYS) {
        return;
    }
    size_t same_keys_num = _sort();
    if (same_keys_num == 0) {
        vectorized::Block in_block = _input_mutable_block.to_block();
        _put_into_output(in_block);
    } else {
        _aggregate<false>();
    }
}

bool MemTable::need_flush() const {
    auto max_size = config::write_buffer_size;
    if (_is_partial_update) {
        auto update_columns_size = _num_columns;
        max_size = max_size * update_columns_size / _tablet_schema->num_columns();
        max_size = max_size > 1048576 ? max_size : 1048576;
    }
    return memory_usage() >= max_size;
}

bool MemTable::need_agg() const {
    if (_keys_type == KeysType::AGG_KEYS) {
        auto max_size = config::write_buffer_size_for_agg;
        return memory_usage() >= max_size;
    }
    return false;
}

std::unique_ptr<vectorized::Block> MemTable::to_block() {
    size_t same_keys_num = _sort();
    if (_keys_type == KeysType::DUP_KEYS || same_keys_num == 0) {
        if (_keys_type == KeysType::DUP_KEYS && _tablet_schema->num_key_columns() == 0) {
            _output_mutable_block.swap(_input_mutable_block);
        } else {
            vectorized::Block in_block = _input_mutable_block.to_block();
            _put_into_output(in_block);
        }
    } else {
        _aggregate<true>();
    }
    return vectorized::Block::create_unique(_output_mutable_block.to_block());
}

} // namespace doris
