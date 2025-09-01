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
#include "util/debug_points.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"

namespace doris {
#include "common/compile_check_begin.h"

bvar::Adder<int64_t> g_memtable_cnt("memtable_cnt");

using namespace ErrorCode;

MemTable::MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
                   const std::shared_ptr<ResourceContext>& resource_ctx)
        : _mem_type(MemType::ACTIVE),
          _tablet_id(tablet_id),
          _enable_unique_key_mow(enable_unique_key_mow),
          _keys_type(tablet_schema->keys_type()),
          _tablet_schema(tablet_schema),
          _resource_ctx(resource_ctx),
          _is_first_insertion(true),
          _agg_functions(tablet_schema->num_columns()),
          _offsets_of_aggregate_states(tablet_schema->num_columns()),
          _total_size_of_aggregate_states(0) {
    g_memtable_cnt << 1;
    _mem_tracker = std::make_shared<MemTracker>();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    _vec_row_comparator = std::make_shared<RowInBlockComparator>(_tablet_schema);
    _num_columns = _tablet_schema->num_columns();
    if (partial_update_info != nullptr) {
        _partial_update_mode = partial_update_info->update_mode();
        if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
            _num_columns = partial_update_info->partial_update_input_columns.size();
            if (partial_update_info->is_schema_contains_auto_inc_column &&
                !partial_update_info->is_input_columns_contains_auto_inc_column) {
                _is_partial_update_and_auto_inc = true;
                _num_columns += 1;
            }
        }
    }
    // TODO: Support ZOrderComparator in the future
    _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
    _row_in_blocks = std::make_unique<DorisVector<std::shared_ptr<RowInBlock>>>();
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
    if (_is_partial_update_and_auto_inc) {
        _column_offset.emplace_back(_column_offset.size());
    }
}

void MemTable::_init_agg_functions(const vectorized::Block* block) {
    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        vectorized::AggregateFunctionPtr function;
        if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow) {
            // In such table, non-key column's aggregation type is NONE, so we need to construct
            // the aggregate function manually.
            if (_skip_bitmap_col_idx != cid) {
                function = vectorized::AggregateFunctionSimpleFactory::instance().get(
                        "replace_load", {block->get_data_type(cid)},
                        block->get_data_type(cid)->is_nullable(),
                        BeExecVersionManager::get_newest_version());
            } else {
                function = vectorized::AggregateFunctionSimpleFactory::instance().get(
                        "bitmap_intersect", {block->get_data_type(cid)}, false,
                        BeExecVersionManager::get_newest_version());
            }
        } else {
            function = _tablet_schema->column(cid).get_aggregate_function(
                    vectorized::AGG_LOAD_SUFFIX, _tablet_schema->column(cid).get_be_exec_version());
            if (function == nullptr) {
                LOG(WARNING) << "column get aggregate function failed, column="
                             << _tablet_schema->column(cid).name();
            }
        }

        DCHECK(function != nullptr);
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

MemTable::~MemTable() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    {
        SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
        g_memtable_cnt << -1;
        if (_keys_type != KeysType::DUP_KEYS) {
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
        }

        _arena.clear(true);
        _vec_row_comparator.reset();
        _row_in_blocks.reset();
        _agg_functions.clear();
        _input_mutable_block.clear();
        _output_mutable_block.clear();
    }
    if (_is_flush_success) {
        // If the memtable is flush success, then its memtracker's consumption should be 0
        if (_mem_tracker->consumption() != 0 && config::crash_in_memory_tracker_inaccurate) {
            LOG(FATAL) << "memtable flush success but cosumption is not 0, it is "
                       << _mem_tracker->consumption();
        }
    }
}

int RowInBlockComparator::operator()(const RowInBlock* left, const RowInBlock* right) const {
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _tablet_schema->num_key_columns(),
                               *_pblock, -1);
}

Status MemTable::insert(const vectorized::Block* input_block,
                        const DorisVector<uint32_t>& row_idxs) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);

    if (_is_first_insertion) {
        _is_first_insertion = false;
        auto clone_block = input_block->clone_without_columns(&_column_offset);
        _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);
        _vec_row_comparator->set_block(&_input_mutable_block);
        _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);
        if (_tablet_schema->has_sequence_col()) {
            if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
                // for unique key fixed partial update, sequence column index in block
                // may be different with the index in `_tablet_schema`
                for (int32_t i = 0; i < clone_block.columns(); i++) {
                    if (clone_block.get_by_position(i).name == SEQUENCE_COL) {
                        _seq_col_idx_in_block = i;
                        break;
                    }
                }
            } else {
                _seq_col_idx_in_block = _tablet_schema->sequence_col_idx();
            }
        }
        if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS &&
            _tablet_schema->has_skip_bitmap_col()) {
            // init of _skip_bitmap_col_idx and _delete_sign_col_idx must be before _init_agg_functions()
            _skip_bitmap_col_idx = _tablet_schema->skip_bitmap_col_idx();
            _delete_sign_col_idx = _tablet_schema->delete_sign_idx();
            _delete_sign_col_unique_id = _tablet_schema->column(_delete_sign_col_idx).unique_id();
            if (_seq_col_idx_in_block != -1) {
                _seq_col_unique_id = _tablet_schema->column(_seq_col_idx_in_block).unique_id();
            }
        }
        if (_keys_type != KeysType::DUP_KEYS) {
            // there may be additional intermediate columns in input_block
            // we only need columns indicated by column offset in the output
            RETURN_IF_CATCH_EXCEPTION(_init_agg_functions(&clone_block));
        }
    }

    auto num_rows = row_idxs.size();
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    RETURN_IF_ERROR(_input_mutable_block.add_rows(input_block, row_idxs.data(),
                                                  row_idxs.data() + num_rows, &_column_offset));
    for (int i = 0; i < num_rows; i++) {
        _row_in_blocks->emplace_back(std::make_shared<RowInBlock>(cursor_in_mutableblock + i));
    }

    _stat.raw_rows += num_rows;
    return Status::OK();
}

template <bool has_skip_bitmap_col>
void MemTable::_aggregate_two_row_in_block(vectorized::MutableBlock& mutable_block,
                                           RowInBlock* src_row, RowInBlock* dst_row) {
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
                                     const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    } else {
        DCHECK(_skip_bitmap_col_idx != -1);
        DCHECK_LT(_skip_bitmap_col_idx, mutable_block.columns());
        const BitmapValue& skip_bitmap =
                assert_cast<vectorized::ColumnBitmap*, TypeCheckOnRelease::DISABLE>(
                        mutable_block.mutable_columns()[_skip_bitmap_col_idx].get())
                        ->get_data()[src_row->_row_pos];
        for (size_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
            const auto& col = _tablet_schema->column(cid);
            if (cid != _skip_bitmap_col_idx && skip_bitmap.contains(col.unique_id())) {
                continue;
            }
            auto* col_ptr = mutable_block.mutable_columns()[cid].get();
            _agg_functions[cid]->add(dst_row->agg_places(cid),
                                     const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    }
}
Status MemTable::_put_into_output(vectorized::Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DorisVector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < _row_in_blocks->size(); i++) {
        row_pos_vec.emplace_back((*_row_in_blocks)[i]->_row_pos);
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows());
}

size_t MemTable::_sort() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    size_t same_keys_num = 0;
    // sort new rows
    Tie tie = Tie(_last_sorted_pos, _row_in_blocks->size());
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        auto cmp = [&](RowInBlock* lhs, RowInBlock* rhs) -> int {
            return _input_mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, i, -1);
        };
        _sort_one_column(*_row_in_blocks, tie, cmp);
    }
    bool is_dup = (_keys_type == KeysType::DUP_KEYS);
    // sort extra round by _row_pos to make the sort stable
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(_row_in_blocks->begin(), iter.left()),
                std::next(_row_in_blocks->begin(), iter.right()),
                [&is_dup](const std::shared_ptr<RowInBlock>& lhs,
                          const std::shared_ptr<RowInBlock>& rhs) -> bool {
                    return is_dup ? lhs->_row_pos > rhs->_row_pos : lhs->_row_pos < rhs->_row_pos;
                });
        same_keys_num += iter.right() - iter.left();
    }
    // merge new rows and old rows
    _vec_row_comparator->set_block(&_input_mutable_block);
    auto cmp_func = [this, is_dup, &same_keys_num](const std::shared_ptr<RowInBlock>& l,
                                                   const std::shared_ptr<RowInBlock>& r) -> bool {
        auto value = (*(this->_vec_row_comparator))(l.get(), r.get());
        if (value == 0) {
            same_keys_num++;
            return is_dup ? l->_row_pos > r->_row_pos : l->_row_pos < r->_row_pos;
        } else {
            return value < 0;
        }
    };
    auto new_row_it = std::next(_row_in_blocks->begin(), _last_sorted_pos);
    std::inplace_merge(_row_in_blocks->begin(), new_row_it, _row_in_blocks->end(), cmp_func);
    _last_sorted_pos = _row_in_blocks->size();
    return same_keys_num;
}

Status MemTable::_sort_by_cluster_keys() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    // sort all rows
    vectorized::Block in_block = _output_mutable_block.to_block();
    vectorized::MutableBlock mutable_block =
            vectorized::MutableBlock::build_mutable_block(&in_block);
    auto clone_block = in_block.clone_without_columns();
    _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);

    DorisVector<std::shared_ptr<RowInBlock>> row_in_blocks;
    row_in_blocks.reserve(mutable_block.rows());
    for (size_t i = 0; i < mutable_block.rows(); i++) {
        row_in_blocks.emplace_back(std::make_shared<RowInBlock>(i));
    }
    Tie tie = Tie(0, mutable_block.rows());

    for (auto cid : _tablet_schema->cluster_key_uids()) {
        auto index = _tablet_schema->field_index(cid);
        if (index == -1) {
            return Status::InternalError("could not find cluster key column with unique_id=" +
                                         std::to_string(cid) + " in tablet schema");
        }
        auto cmp = [&](const RowInBlock* lhs, const RowInBlock* rhs) -> int {
            return mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, index, -1);
        };
        _sort_one_column(row_in_blocks, tie, cmp);
    }

    // sort extra round by _row_pos to make the sort stable
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), iter.left()),
                std::next(row_in_blocks.begin(), iter.right()),
                [](const std::shared_ptr<RowInBlock>& lhs, const std::shared_ptr<RowInBlock>& rhs)
                        -> bool { return lhs->_row_pos < rhs->_row_pos; });
    }

    in_block = mutable_block.to_block();
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DorisVector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < row_in_blocks.size(); i++) {
        row_pos_vec.emplace_back(row_in_blocks[i]->_row_pos);
    }
    std::vector<int> column_offset;
    for (int i = 0; i < _column_offset.size(); ++i) {
        column_offset.emplace_back(i);
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows(), &column_offset);
}

void MemTable::_sort_one_column(DorisVector<std::shared_ptr<RowInBlock>>& row_in_blocks, Tie& tie,
                                std::function<int(RowInBlock*, RowInBlock*)> cmp) {
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), static_cast<int>(iter.left())),
                std::next(row_in_blocks.begin(), static_cast<int>(iter.right())),
                [&cmp](auto lhs, auto rhs) -> bool { return cmp(lhs.get(), rhs.get()) < 0; });
        tie[iter.left()] = 0;
        for (auto i = iter.left() + 1; i < iter.right(); i++) {
            tie[i] = (cmp(row_in_blocks[i - 1].get(), row_in_blocks[i].get()) == 0);
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
                function->add(agg_place, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                              row_pos, _arena);
            }
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

void MemTable::_init_row_for_agg(RowInBlock* row, vectorized::MutableBlock& mutable_block) {
    row->init_agg_places(_arena.aligned_alloc(_total_size_of_aggregate_states, 16),
                         _offsets_of_aggregate_states.data());
    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; cid++) {
        auto* col_ptr = mutable_block.mutable_columns()[cid].get();
        auto* data = row->agg_places(cid);
        _agg_functions[cid]->create(data);
        _agg_functions[cid]->add(data, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                 row->_row_pos, _arena);
    }
}
void MemTable::_clear_row_agg(RowInBlock* row) {
    if (row->has_init_agg()) {
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            function->destroy(agg_place);
        }
        row->remove_init_agg();
    }
}

template <bool is_final, bool has_skip_bitmap_col>
void MemTable::_aggregate() {
    SCOPED_RAW_TIMER(&_stat.agg_ns);
    _stat.agg_times++;
    vectorized::Block in_block = _input_mutable_block.to_block();
    vectorized::MutableBlock mutable_block =
            vectorized::MutableBlock::build_mutable_block(&in_block);
    _vec_row_comparator->set_block(&mutable_block);
    auto& block_data = in_block.get_columns_with_type_and_name();
    DorisVector<std::shared_ptr<RowInBlock>> temp_row_in_blocks;
    temp_row_in_blocks.reserve(_last_sorted_pos);
    //only init agg if needed

    if constexpr (!has_skip_bitmap_col) {
        RowInBlock* prev_row = nullptr;
        int row_pos = -1;
        for (const auto& cur_row_ptr : *_row_in_blocks) {
            RowInBlock* cur_row = cur_row_ptr.get();
            if (!temp_row_in_blocks.empty() && (*_vec_row_comparator)(prev_row, cur_row) == 0) {
                if (!prev_row->has_init_agg()) {
                    _init_row_for_agg(prev_row, mutable_block);
                }
                _stat.merged_rows++;
                _aggregate_two_row_in_block<has_skip_bitmap_col>(mutable_block, cur_row, prev_row);
            } else {
                prev_row = cur_row;
                if (!temp_row_in_blocks.empty()) {
                    // no more rows to merge for prev row, finalize it
                    _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), block_data,
                                                row_pos);
                }
                temp_row_in_blocks.push_back(cur_row_ptr);
                row_pos++;
            }
        }
        if (!temp_row_in_blocks.empty()) {
            // finalize the last low
            _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), block_data, row_pos);
        }
    } else {
        DCHECK(_delete_sign_col_idx != -1);
        if (_seq_col_idx_in_block == -1) {
            _aggregate_for_flexible_partial_update_without_seq_col<is_final>(
                    block_data, mutable_block, temp_row_in_blocks);
        } else {
            _aggregate_for_flexible_partial_update_with_seq_col<is_final>(block_data, mutable_block,
                                                                          temp_row_in_blocks);
        }
    }
    if constexpr (!is_final) {
        // if is not final, we collect the agg results to input_block and then continue to insert
        _input_mutable_block.swap(_output_mutable_block);
        //TODO(weixang):opt here.
        std::unique_ptr<vectorized::Block> empty_input_block = in_block.create_same_struct_block(0);
        _output_mutable_block =
                vectorized::MutableBlock::build_mutable_block(empty_input_block.get());
        _output_mutable_block.clear_column_data();
        *_row_in_blocks = temp_row_in_blocks;
        _last_sorted_pos = _row_in_blocks->size();
    }
}

template <bool is_final>
void MemTable::_aggregate_for_flexible_partial_update_without_seq_col(
        const vectorized::ColumnsWithTypeAndName& block_data,
        vectorized::MutableBlock& mutable_block,
        DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) {
    std::shared_ptr<RowInBlock> prev_row {nullptr};
    int row_pos = -1;
    auto& skip_bitmaps = assert_cast<vectorized::ColumnBitmap*>(
                                 mutable_block.mutable_columns()[_skip_bitmap_col_idx].get())
                                 ->get_data();
    auto& delete_signs = assert_cast<vectorized::ColumnInt8*>(
                                 mutable_block.mutable_columns()[_delete_sign_col_idx].get())
                                 ->get_data();
    std::shared_ptr<RowInBlock> row_with_delete_sign {nullptr};
    std::shared_ptr<RowInBlock> row_without_delete_sign {nullptr};

    auto finalize_rows = [&]() {
        if (row_with_delete_sign != nullptr) {
            temp_row_in_blocks.push_back(row_with_delete_sign);
            _finalize_one_row<is_final>(row_with_delete_sign.get(), block_data, ++row_pos);
            row_with_delete_sign = nullptr;
        }
        if (row_without_delete_sign != nullptr) {
            temp_row_in_blocks.push_back(row_without_delete_sign);
            _finalize_one_row<is_final>(row_without_delete_sign.get(), block_data, ++row_pos);
            row_without_delete_sign = nullptr;
        }
        // _arena.clear();
    };

    auto add_row = [&](std::shared_ptr<RowInBlock> row, bool with_delete_sign) {
        if (with_delete_sign) {
            row_with_delete_sign = std::move(row);
        } else {
            row_without_delete_sign = std::move(row);
        }
    };
    for (const auto& cur_row_ptr : *_row_in_blocks) {
        RowInBlock* cur_row = cur_row_ptr.get();
        const BitmapValue& skip_bitmap = skip_bitmaps[cur_row->_row_pos];
        bool cur_row_has_delete_sign = (!skip_bitmap.contains(_delete_sign_col_unique_id) &&
                                        delete_signs[cur_row->_row_pos] != 0);
        prev_row =
                (row_with_delete_sign == nullptr) ? row_without_delete_sign : row_with_delete_sign;
        // compare keys, the keys of row_with_delete_sign and row_without_delete_sign is the same,
        // choose any of them if it's valid
        if (prev_row != nullptr && (*_vec_row_comparator)(prev_row.get(), cur_row) == 0) {
            if (cur_row_has_delete_sign) {
                if (row_without_delete_sign != nullptr) {
                    // if there exits row without delete sign, remove it first
                    _clear_row_agg(row_without_delete_sign.get());
                    _stat.merged_rows++;
                    row_without_delete_sign = nullptr;
                }
                // and then unconditionally replace the previous row
                prev_row = row_with_delete_sign;
            } else {
                prev_row = row_without_delete_sign;
            }

            if (prev_row == nullptr) {
                add_row(cur_row_ptr, cur_row_has_delete_sign);
            } else {
                if (!prev_row->has_init_agg()) {
                    _init_row_for_agg(prev_row.get(), mutable_block);
                }
                _stat.merged_rows++;
                _aggregate_two_row_in_block<true>(mutable_block, cur_row, prev_row.get());
            }
        } else {
            finalize_rows();
            add_row(cur_row_ptr, cur_row_has_delete_sign);
        }
    }
    // finalize the last lows
    finalize_rows();
}

template <bool is_final>
void MemTable::_aggregate_for_flexible_partial_update_with_seq_col(
        const vectorized::ColumnsWithTypeAndName& block_data,
        vectorized::MutableBlock& mutable_block,
        DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) {
    // For flexible partial update, when table has sequence column, we don't do any aggregation
    // in memtable. These duplicate rows will be aggregated in VerticalSegmentWriter
    int row_pos = -1;
    for (const auto& row_ptr : *_row_in_blocks) {
        RowInBlock* row = row_ptr.get();
        temp_row_in_blocks.push_back(row_ptr);
        _finalize_one_row<is_final>(row, block_data, ++row_pos);
    }
}

void MemTable::shrink_memtable_by_agg() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    if (_keys_type == KeysType::DUP_KEYS) {
        return;
    }
    size_t same_keys_num = _sort();
    if (same_keys_num != 0) {
        (_skip_bitmap_col_idx == -1) ? _aggregate<false, false>() : _aggregate<false, true>();
    }
    _last_agg_pos = memory_usage();
}

bool MemTable::need_flush() const {
    DBUG_EXECUTE_IF("MemTable.need_flush", { return true; });
    auto max_size = config::write_buffer_size;
    if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
        auto update_columns_size = _num_columns;
        auto min_buffer_size = config::min_write_buffer_size_for_partial_update;
        max_size = max_size * update_columns_size / _tablet_schema->num_columns();
        max_size = max_size > min_buffer_size ? max_size : min_buffer_size;
    }
    return memory_usage() >= max_size;
}

bool MemTable::need_agg() const {
    if (_keys_type == KeysType::AGG_KEYS) {
        auto max_size = _last_agg_pos + config::write_buffer_size_for_agg;
        return memory_usage() >= max_size;
    }
    return false;
}

size_t MemTable::get_flush_reserve_memory_size() const {
    if (_keys_type == KeysType::DUP_KEYS && _tablet_schema->num_key_columns() == 0) {
        return 0; // no need to reserve
    }
    return static_cast<size_t>(static_cast<double>(_input_mutable_block.allocated_bytes()) * 1.2);
}

Status MemTable::_to_block(std::unique_ptr<vectorized::Block>* res) {
    size_t same_keys_num = _sort();
    if (_keys_type == KeysType::DUP_KEYS || same_keys_num == 0) {
        if (_keys_type == KeysType::DUP_KEYS && _tablet_schema->num_key_columns() == 0) {
            _output_mutable_block.swap(_input_mutable_block);
        } else {
            vectorized::Block in_block = _input_mutable_block.to_block();
            RETURN_IF_ERROR(_put_into_output(in_block));
        }
    } else {
        (_skip_bitmap_col_idx == -1) ? _aggregate<true, false>() : _aggregate<true, true>();
    }
    if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow &&
        !_tablet_schema->cluster_key_uids().empty()) {
        if (_partial_update_mode != UniqueKeyUpdateModePB::UPSERT) {
            return Status::InternalError(
                    "Partial update for mow with cluster keys is not supported");
        }
        RETURN_IF_ERROR(_sort_by_cluster_keys());
    }
    _input_mutable_block.clear();
    *res = vectorized::Block::create_unique(_output_mutable_block.to_block());
    return Status::OK();
}

Status MemTable::to_block(std::unique_ptr<vectorized::Block>* res) {
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_to_block(res));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
