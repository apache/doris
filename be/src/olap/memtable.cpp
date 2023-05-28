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

#include <algorithm>
#include <cstddef>
#include <limits>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/json/path_in_data.h"
#include "vec/jsonb/serialize.h"

namespace doris {
using namespace ErrorCode;

MemTable::MemTable(TabletSharedPtr tablet, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   RowsetWriter* rowset_writer, std::shared_ptr<MowContext> mow_context,
                   const std::shared_ptr<MemTracker>& insert_mem_tracker,
                   const std::shared_ptr<MemTracker>& flush_mem_tracker)
        : _tablet(std::move(tablet)),
          _keys_type(_tablet->keys_type()),
          _schema(schema),
          _tablet_schema(tablet_schema),
          _insert_mem_tracker(insert_mem_tracker),
          _flush_mem_tracker(flush_mem_tracker),
          _schema_size(_schema->schema_size()),
          _rowset_writer(rowset_writer),
          _is_first_insertion(true),
          _agg_functions(schema->num_columns()),
          _offsets_of_aggregate_states(schema->num_columns()),
          _total_size_of_aggregate_states(0),
          _mem_usage(0),
          _mow_context(mow_context) {
#ifndef BE_TEST
    _insert_mem_tracker_use_hook = std::make_unique<MemTracker>(
            fmt::format("MemTableHookInsert:TabletId={}", std::to_string(tablet_id())),
            ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker());
#else
    _insert_mem_tracker_use_hook = std::make_unique<MemTracker>(
            fmt::format("MemTableHookInsert:TabletId={}", std::to_string(tablet_id())));
#endif
    _arena = std::make_unique<vectorized::Arena>();
    _vec_row_comparator = std::make_shared<RowInBlockComparator>(_schema);
    // TODO: Support ZOrderComparator in the future
    _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
    _num_columns = _tablet_schema->num_columns();
    if (_tablet_schema->is_partial_update()) {
        _num_columns = _tablet_schema->partial_input_column_size();
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
    for (uint32_t cid = _schema->num_key_columns(); cid < _num_columns; ++cid) {
        vectorized::AggregateFunctionPtr function;
        if (_keys_type == KeysType::UNIQUE_KEYS && _tablet->enable_unique_key_merge_on_write()) {
            // In such table, non-key column's aggregation type is NONE, so we need to construct
            // the aggregate function manually.
            function = vectorized::AggregateFunctionSimpleFactory::instance().get(
                    "replace_load", {block->get_data_type(cid)},
                    block->get_data_type(cid)->is_nullable());
        } else {
            function =
                    _tablet_schema->column(cid).get_aggregate_function(vectorized::AGG_LOAD_SUFFIX);
        }

        DCHECK(function != nullptr);
        _agg_functions[cid] = function;
    }

    for (uint32_t cid = _schema->num_key_columns(); cid < _num_columns; ++cid) {
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
    if (_keys_type != KeysType::DUP_KEYS) {
        for (auto it = _row_in_blocks.begin(); it != _row_in_blocks.end(); it++) {
            if (!(*it)->has_init_agg()) {
                continue;
            }
            // We should release agg_places here, because they are not released when a
            // load is canceled.
            for (size_t i = _schema->num_key_columns(); i < _num_columns; ++i) {
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
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _schema->num_key_columns(),
                               *_pblock, -1);
}

void MemTable::insert(const vectorized::Block* input_block, const std::vector<int>& row_idxs,
                      bool is_append) {
    SCOPED_CONSUME_MEM_TRACKER(_insert_mem_tracker_use_hook.get());
    vectorized::Block target_block = *input_block;
    if (!_tablet_schema->is_dynamic_schema()) {
        // This insert may belong to a rollup tablet, rollup columns is a subset of base table
        // but for dynamic table, it's need full columns, so input_block should ignore _column_offset
        // of each column and avoid copy_block
        target_block = input_block->copy_block(_column_offset);
    }
    if (_is_first_insertion) {
        _is_first_insertion = false;
        auto cloneBlock = target_block.clone_without_columns();
        _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        _vec_row_comparator->set_block(&_input_mutable_block);
        _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        if (_keys_type != KeysType::DUP_KEYS) {
            _init_agg_functions(&target_block);
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

    _stat.raw_rows++;
}

void MemTable::_aggregate_two_row_in_block(vectorized::MutableBlock& mutable_block,
                                           RowInBlock* new_row, RowInBlock* row_in_skiplist) {
    if (_tablet_schema->has_sequence_col()) {
        auto sequence_idx = _tablet_schema->sequence_col_idx();
        DCHECK_LT(sequence_idx, mutable_block.columns());
        auto col_ptr = mutable_block.mutable_columns()[sequence_idx].get();
        auto res = col_ptr->compare_at(row_in_skiplist->_row_pos, new_row->_row_pos, *col_ptr, -1);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
        // need to update the row pos in skiplist to the new row pos when has
        // sequence column
        row_in_skiplist->_row_pos = new_row->_row_pos;
    }
    // dst is non-sequence row, or dst sequence is smaller
    for (uint32_t cid = _schema->num_key_columns(); cid < _num_columns; ++cid) {
        auto col_ptr = mutable_block.mutable_columns()[cid].get();
        _agg_functions[cid]->add(row_in_skiplist->agg_places(cid),
                                 const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                 new_row->_row_pos, nullptr);
    }
}
void MemTable::_put_into_output(vectorized::Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    if (_keys_type == KeysType::DUP_KEYS && _schema->num_key_columns() == 0) {
        // skip sort if the table is dup table without keys
        _output_mutable_block.swap(_input_mutable_block);
        return;
    }
    std::vector<int> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < _row_in_blocks.size(); i++) {
        row_pos_vec.emplace_back(_row_in_blocks[i]->_row_pos);
    }
    _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                   row_pos_vec.data() + in_block.rows());
}
int MemTable::_sort() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    _vec_row_comparator->set_block(&_input_mutable_block);
    auto new_row_it = std::next(_row_in_blocks.begin(), _last_sorted_pos);
    size_t same_keys_num = 0;
    bool is_dup = (_keys_type == KeysType::DUP_KEYS);
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
    // sort new rows
    std::sort(new_row_it, _row_in_blocks.end(), cmp_func);
    // merge new rows and old rows
    std::inplace_merge(_row_in_blocks.begin(), new_row_it, _row_in_blocks.end(), cmp_func);
    _last_sorted_pos = _row_in_blocks.size();
    return same_keys_num;
}

template <bool is_final>
void MemTable::_finalize_one_row(RowInBlock* row,
                                 const vectorized::ColumnsWithTypeAndName& block_data,
                                 int row_pos) {
    // move key columns
    for (size_t i = 0; i < _schema->num_key_columns(); ++i) {
        _output_mutable_block.get_column_by_position(i)->insert_from(*block_data[i].column.get(),
                                                                     row->_row_pos);
    }
    if (row->has_init_agg()) {
        // get value columns from agg_places
        for (size_t i = _schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto agg_place = row->agg_places(i);
            auto col_ptr = _output_mutable_block.get_column_by_position(i).get();
            function->insert_result_into(agg_place, *col_ptr);
            if constexpr (is_final) {
                function->destroy(agg_place);
            } else {
                function->reset(agg_place);
                function->add(agg_place, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                              row_pos, nullptr);
            }
        }
        if constexpr (is_final) {
            row->remove_init_agg();
        }
    } else {
        // move columns for rows do not need agg
        for (size_t i = _schema->num_key_columns(); i < _num_columns; ++i) {
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
    RowInBlock* prev_row;
    int row_pos = -1;
    //only init agg if needed
    for (int i = 0; i < _row_in_blocks.size(); i++) {
        if (!temp_row_in_blocks.empty() &&
            (*_vec_row_comparator)(prev_row, _row_in_blocks[i]) == 0) {
            if (!prev_row->has_init_agg()) {
                prev_row->init_agg_places(
                        _arena->aligned_alloc(_total_size_of_aggregate_states, 16),
                        _offsets_of_aggregate_states.data());
                for (auto cid = _schema->num_key_columns(); cid < _schema->num_columns(); cid++) {
                    auto col_ptr = mutable_block.mutable_columns()[cid].get();
                    auto data = prev_row->agg_places(cid);
                    _agg_functions[cid]->create(data);
                    _agg_functions[cid]->add(
                            data, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                            prev_row->_row_pos, nullptr);
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
    int same_keys_num = _sort();
    if (same_keys_num == 0) {
        vectorized::Block in_block = _input_mutable_block.to_block();
        _put_into_output(in_block);
    } else {
        _aggregate<false>();
    }
}

bool MemTable::need_flush() const {
    auto max_size = config::write_buffer_size;
    if (_tablet_schema->is_partial_update()) {
        auto update_columns_size = _tablet_schema->partial_input_column_size();
        max_size = max_size * update_columns_size / _tablet_schema->num_columns();
        max_size = max_size > 1048576 ? max_size : 1048576;
    }
    return memory_usage() >= max_size;
}

bool MemTable::need_agg() const {
    if (_keys_type == KeysType::AGG_KEYS) {
        auto max_size = config::write_buffer_size_for_agg;
        if (_tablet_schema->is_partial_update()) {
            auto update_columns_size = _tablet_schema->partial_input_column_size();
            max_size = max_size * update_columns_size / _tablet_schema->num_columns();
            max_size = max_size > 1048576 ? max_size : 1048576;
        }
        return memory_usage() >= max_size;
    }
    return false;
}

Status MemTable::_generate_delete_bitmap(int64_t atomic_num_segments_before_flush,
                                         int64_t atomic_num_segments_after_flush) {
    SCOPED_RAW_TIMER(&_stat.delete_bitmap_ns);
    // generate delete bitmap, build a tmp rowset and load recent segment
    if (!_tablet->enable_unique_key_merge_on_write()) {
        return Status::OK();
    }
    auto rowset = _rowset_writer->build_tmp();
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    if (atomic_num_segments_before_flush >= atomic_num_segments_after_flush) {
        return Status::OK();
    }
    RETURN_IF_ERROR(beta_rowset->load_segments(atomic_num_segments_before_flush,
                                               atomic_num_segments_after_flush, &segments));
    std::shared_lock meta_rlock(_tablet->get_header_lock());
    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (_tablet->tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(_tablet->tablet_id())) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_tablet->calc_delete_bitmap(rowset, segments, &_mow_context->rowset_ids,
                                                _mow_context->delete_bitmap,
                                                _mow_context->max_version));
    return Status::OK();
}

Status MemTable::flush() {
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << tablet_id()
                  << ", memsize: " << memory_usage() << ", rows: " << _stat.raw_rows;
    // For merge_on_write table, it must get all segments in this flush.
    // The id of new segment is set by the _num_segment of beta_rowset_writer,
    // and new segment ids is between [atomic_num_segments_before_flush, atomic_num_segments_after_flush),
    // and use the ids to load segment data file for calc delete bitmap.
    int64_t atomic_num_segments_before_flush = _rowset_writer->get_atomic_num_segment();
    int64_t duration_ns;
    SCOPED_RAW_TIMER(&duration_ns);
    SKIP_MEMORY_CHECK(RETURN_IF_ERROR(_do_flush()));
    int64_t atomic_num_segments_after_flush = _rowset_writer->get_atomic_num_segment();
    if (!_tablet_schema->is_partial_update()) {
        RETURN_IF_ERROR(_generate_delete_bitmap(atomic_num_segments_before_flush,
                                                atomic_num_segments_after_flush));
    }
    _delta_writer_callback(_stat);
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << tablet_id()
                  << ", flushsize: " << _flush_size;

    return Status::OK();
}

Status MemTable::_do_flush() {
    SCOPED_CONSUME_MEM_TRACKER(_flush_mem_tracker);
    int same_keys_num = _sort();
    if (_keys_type == KeysType::DUP_KEYS || same_keys_num == 0) {
        vectorized::Block in_block = _input_mutable_block.to_block();
        _put_into_output(in_block);
    } else {
        _aggregate<true>();
    }
    vectorized::Block block = _output_mutable_block.to_block();
    if (_tablet_schema->is_dynamic_schema()) {
        // Unfold variant column
        unfold_variant_column(block);
    }
    SCOPED_RAW_TIMER(&_stat.segment_writer_ns);
    RETURN_IF_ERROR(_rowset_writer->flush_single_memtable(&block, &_flush_size));
    return Status::OK();
}

Status MemTable::close() {
    return flush();
}

void MemTable::unfold_variant_column(vectorized::Block& block) {
    if (block.rows() == 0) {
        return;
    }
    vectorized::ColumnWithTypeAndName* variant_column =
            block.try_get_by_name(BeConsts::DYNAMIC_COLUMN_NAME);
    if (!variant_column) {
        return;
    }
    // remove it
    vectorized::ColumnObject& object_column =
            assert_cast<vectorized::ColumnObject&>(variant_column->column->assume_mutable_ref());
    // extend
    for (auto& entry : object_column.get_subcolumns()) {
        if (entry->path.get_path() == vectorized::ColumnObject::COLUMN_NAME_DUMMY) {
            continue;
        }
        block.insert({entry->data.get_finalized_column().get_ptr(),
                      entry->data.get_least_common_type(), entry->path.get_path()});
    }
    block.erase(BeConsts::DYNAMIC_COLUMN_NAME);
}

void MemTable::serialize_block_to_row_column(vectorized::Block& block) {
    if (block.rows() == 0) {
        return;
    }
    MonotonicStopWatch watch;
    watch.start();
    // find row column id
    int row_column_id = 0;
    for (int i = 0; i < _num_columns; ++i) {
        if (_tablet_schema->column(i).is_row_store_column()) {
            row_column_id = i;
            break;
        }
    }
    if (row_column_id == 0) {
        return;
    }
    vectorized::ColumnString* row_store_column =
            static_cast<vectorized::ColumnString*>(block.get_by_position(row_column_id)
                                                           .column->assume_mutable_ref()
                                                           .assume_mutable()
                                                           .get());
    row_store_column->clear();
    vectorized::DataTypeSerDeSPtrs serdes =
            vectorized::create_data_type_serdes(block.get_data_types());
    vectorized::JsonbSerializeUtil::block_to_jsonb(*_tablet_schema, block, *row_store_column,
                                                   _tablet_schema->num_columns(), serdes);
    VLOG_DEBUG << "serialize , num_rows:" << block.rows() << ", row_column_id:" << row_column_id
               << ", total_byte_size:" << block.allocated_bytes() << ", serialize_cost(us)"
               << watch.elapsed_time() / 1000;
}

} // namespace doris
