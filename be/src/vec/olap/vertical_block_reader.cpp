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

#include "vec/olap/vertical_block_reader.h"

#include <assert.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <ostream>

#include "cloud/config.h"
#include "olap/compaction.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/tablet_schema.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_number.h"
#include "vec/olap/vertical_merge_iterator.h"

namespace doris::vectorized {
using namespace ErrorCode;

uint64_t VerticalBlockReader::nextId = 1;

VerticalBlockReader::~VerticalBlockReader() {
    for (int i = 0; i < _agg_functions.size(); ++i) {
        _agg_functions[i]->destroy(_agg_places[i]);
        delete[] _agg_places[i];
    }
}

Status VerticalBlockReader::next_block_with_aggregation(Block* block, bool* eof) {
    auto res = (this->*_next_block_func)(block, eof);
    if (!config::is_cloud_mode()) {
        if (!res.ok()) [[unlikely]] {
            static_cast<Tablet*>(_tablet.get())->report_error(res);
        }
    }
    return res;
}

Status VerticalBlockReader::_get_segment_iterators(const ReaderParams& read_params,
                                                   std::vector<RowwiseIteratorUPtr>* segment_iters,
                                                   std::vector<bool>* iterator_init_flag,
                                                   std::vector<RowsetId>* rowset_ids) {
    auto res = _capture_rs_readers(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return res;
    }
    _reader_context.is_vertical_compaction = true;
    for (const auto& rs_split : read_params.rs_splits) {
        // segment iterator will be inited here
        // In vertical compaction, every group will load segment so we should cache
        // segment to avoid tot many s3 head request
        bool use_cache = !rs_split.rs_reader->rowset()->is_local();
        RETURN_IF_ERROR(rs_split.rs_reader->get_segment_iterators(&_reader_context, segment_iters,
                                                                  use_cache));
        // if segments overlapping, all segment iterator should be inited in
        // heap merge iterator. If segments are none overlapping, only first segment of this
        // rowset will be inited and push to heap, other segment will be inited later when current
        // segment reached it's end.
        // Use this iterator_init_flag so we can load few segments in HeapMergeIterator to save memory
        if (rs_split.rs_reader->rowset()->is_segments_overlapping()) {
            for (int i = 0; i < rs_split.rs_reader->rowset()->num_segments(); ++i) {
                iterator_init_flag->push_back(true);
            }
        } else {
            for (int i = 0; i < rs_split.rs_reader->rowset()->num_segments(); ++i) {
                if (i == 0) {
                    iterator_init_flag->push_back(true);
                    continue;
                }
                iterator_init_flag->push_back(false);
            }
        }
        for (int i = 0; i < rs_split.rs_reader->rowset()->num_segments(); ++i) {
            rowset_ids->push_back(rs_split.rs_reader->rowset()->rowset_id());
        }
        rs_split.rs_reader->reset_read_options();
    }
    return Status::OK();
}

Status VerticalBlockReader::_init_collect_iter(const ReaderParams& read_params,
                                               CompactionSampleInfo* sample_info) {
    std::vector<bool> iterator_init_flag;
    std::vector<RowsetId> rowset_ids;
    std::vector<RowwiseIteratorUPtr>* segment_iters_ptr = read_params.segment_iters_ptr;
    std::vector<RowwiseIteratorUPtr> iter_ptr_vector;

    if (!segment_iters_ptr) {
        RETURN_IF_ERROR(_get_segment_iterators(read_params, &iter_ptr_vector, &iterator_init_flag,
                                               &rowset_ids));
        CHECK(iter_ptr_vector.size() == iterator_init_flag.size());
        segment_iters_ptr = &iter_ptr_vector;
    } else {
        for (int i = 0; i < segment_iters_ptr->size(); ++i) {
            iterator_init_flag.push_back(true);
            RowsetId rowsetid;
            rowset_ids.push_back(rowsetid); // TODO: _record_rowids need it
        }
        // TODO(zhangzhengyu): is it enough for a context?
        _reader_context.reader_type = read_params.reader_type;
        _reader_context.need_ordered_result = true; // TODO: should it be?
        _reader_context.is_unique = tablet()->keys_type() == UNIQUE_KEYS;
        _reader_context.is_key_column_group = read_params.is_key_column_group;
        _reader_context.record_rowids = read_params.record_rowids;
    }

    // build heap if key column iterator or build vertical merge iterator if value column
    auto ori_return_col_size = _return_columns.size();
    if (read_params.is_key_column_group) {
        uint32_t seq_col_idx = -1;
        if (read_params.tablet->tablet_schema()->has_sequence_col()) {
            seq_col_idx = read_params.tablet->tablet_schema()->sequence_col_idx();
        }
        if (read_params.tablet->tablet_schema()->num_key_columns() == 0) {
            _vcollect_iter = new_vertical_fifo_merge_iterator(
                    std::move(*segment_iters_ptr), iterator_init_flag, rowset_ids,
                    ori_return_col_size, read_params.tablet->keys_type(), seq_col_idx,
                    _row_sources_buffer);
        } else {
            _vcollect_iter = new_vertical_heap_merge_iterator(
                    std::move(*segment_iters_ptr), iterator_init_flag, rowset_ids,
                    ori_return_col_size, read_params.tablet->keys_type(), seq_col_idx,
                    _row_sources_buffer, read_params.key_group_cluster_key_idxes);
        }
    } else {
        _vcollect_iter = new_vertical_mask_merge_iterator(std::move(*segment_iters_ptr),
                                                          ori_return_col_size, _row_sources_buffer);
    }
    // init collect iterator
    StorageReadOptions opts;
    opts.record_rowids = read_params.record_rowids;
    if (read_params.batch_size > 0) {
        opts.block_row_max = read_params.batch_size;
    }
    RETURN_IF_ERROR(_vcollect_iter->init(opts, sample_info));

    // In agg keys value columns compact, get first row for _init_agg_state
    if (!read_params.is_key_column_group && read_params.tablet->keys_type() == KeysType::AGG_KEYS) {
        auto st = _vcollect_iter->next_row(&_next_row);
        if (!st.ok() && !st.is<END_OF_FILE>()) {
            LOG(WARNING) << "failed to init first row for agg key";
            return st;
        }
        _eof = st.is<END_OF_FILE>();
    }

    return Status::OK();
}

void VerticalBlockReader::_init_agg_state(const ReaderParams& read_params) {
    if (_eof) {
        return;
    }
    DCHECK(_return_columns.size() == _next_row.block->columns());
    _stored_data_columns =
            _next_row.block->create_same_struct_block(_reader_context.batch_size)->mutate_columns();

    _stored_has_null_tag.resize(_stored_data_columns.size());
    _stored_has_variable_length_tag.resize(_stored_data_columns.size());

    auto& tablet_schema = *_tablet_schema;
    for (size_t idx = 0; idx < _return_columns.size(); ++idx) {
        AggregateFunctionPtr function =
                tablet_schema.column(_return_columns.at(idx))
                        .get_aggregate_function(vectorized::AGG_READER_SUFFIX,
                                                read_params.get_be_exec_version());
        DCHECK(function != nullptr);
        _agg_functions.push_back(function);
        // create aggregate data
        auto* place = new char[function->size_of_data()];
        SAFE_CREATE(function->create(place), {
            _agg_functions.pop_back();
            delete[] place;
        });
        _agg_places.push_back(place);

        // calculate `_has_variable_length_tag` tag. like string, array, map
        _stored_has_variable_length_tag[idx] = _stored_data_columns[idx]->is_variable_length();
    }
}

Status VerticalBlockReader::init(const ReaderParams& read_params) {
    return init(read_params, nullptr);
}

Status VerticalBlockReader::init(const ReaderParams& read_params,
                                 CompactionSampleInfo* sample_info) {
    StorageReadOptions opts;
    if (read_params.batch_size > 0) {
        _reader_context.batch_size = read_params.batch_size;
    } else {
        _reader_context.batch_size = opts.block_row_max;
    }
    RETURN_IF_ERROR(TabletReader::init(read_params));

    auto status = _init_collect_iter(read_params, sample_info);
    if (!status.ok()) [[unlikely]] {
        if (!config::is_cloud_mode()) {
            static_cast<Tablet*>(_tablet.get())->report_error(status);
        }
        return status;
    }

    switch (tablet()->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &VerticalBlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        if (tablet()->tablet_meta()->tablet_schema()->cluster_key_idxes().empty()) {
            _next_block_func = &VerticalBlockReader::_unique_key_next_block;
            if (_filter_delete) {
                _delete_filter_column = ColumnUInt8::create();
            }
        } else {
            _next_block_func = &VerticalBlockReader::_direct_next_block;
        }
        break;
    case KeysType::AGG_KEYS:
        _next_block_func = &VerticalBlockReader::_agg_key_next_block;
        if (!read_params.is_key_column_group) {
            _init_agg_state(read_params);
        }
        break;
    default:
        DCHECK(false) << "No next row function for type:" << tablet()->keys_type();
        break;
    }
    return Status::OK();
}

Status VerticalBlockReader::_direct_next_block(Block* block, bool* eof) {
    auto res = _vcollect_iter->next_batch(block);
    if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
        return res;
    }
    *eof = (res.is<END_OF_FILE>());
    _eof = *eof;
    if (_reader_context.is_key_column_group && UNLIKELY(_reader_context.record_rowids)) {
        res = _vcollect_iter->current_block_row_locations(&_block_row_locations);
        if (UNLIKELY(!res.ok() && res != Status::Error<END_OF_FILE>(""))) {
            return res;
        }
        DCHECK_EQ(_block_row_locations.size(), block->rows());
    }
    return Status::OK();
}

void VerticalBlockReader::_append_agg_data(MutableColumns& columns) {
    _stored_row_ref.push_back(_next_row);
    _last_agg_data_counter++;

    // execute aggregate when have `batch_size` column or some ref invalid soon
    bool is_last = (_next_row.block->rows() == _next_row.row_pos + 1);
    if (is_last || _stored_row_ref.size() == _reader_context.batch_size) {
        _update_agg_data(columns);
    }
}

void VerticalBlockReader::_update_agg_data(MutableColumns& columns) {
    // copy data to stored block
    size_t copy_size = _copy_agg_data();

    // calculate has_null_tag
    for (size_t idx = 0; idx < _return_columns.size(); ++idx) {
        _stored_has_null_tag[idx] = _stored_data_columns[idx]->has_null(copy_size);
    }

    // calculate aggregate and insert
    int counter_sum = 0;
    for (int counter : _agg_data_counters) {
        _update_agg_value(columns, counter_sum, counter_sum + counter - 1);
        counter_sum += counter;
    }

    // some key still has value at next block, so do not insert
    if (_last_agg_data_counter) {
        _update_agg_value(columns, counter_sum, counter_sum + _last_agg_data_counter - 1, false);
        _last_agg_data_counter = 0;
    }

    _agg_data_counters.clear();
}

void VerticalBlockReader::_update_agg_value(MutableColumns& columns, int begin, int end,
                                            bool is_close) {
    for (size_t idx = 0; idx < _return_columns.size(); ++idx) {
        AggregateFunctionPtr function = _agg_functions[idx];
        AggregateDataPtr place = _agg_places[idx];
        auto* column_ptr = _stored_data_columns[idx].get();

        if (begin <= end) {
            function->add_batch_range(begin, end, place, const_cast<const IColumn**>(&column_ptr),
                                      &_arena, _stored_has_null_tag[idx]);
        }

        if (is_close) {
            function->insert_result_into(place, *columns[idx]);
            // reset aggregate data
            function->reset(place);
        }
    }
    if (is_close) {
        _arena.clear();
    }
}

size_t VerticalBlockReader::_copy_agg_data() {
    size_t copy_size = _stored_row_ref.size();

    for (size_t i = 0; i < copy_size; i++) {
        auto& ref = _stored_row_ref[i];
        _temp_ref_map[ref.block.get()].emplace_back(ref.row_pos, i);
    }
    for (size_t idx = 0; idx < _return_columns.size(); ++idx) {
        auto& dst_column = _stored_data_columns[idx];
        if (_stored_has_variable_length_tag[idx]) {
            //variable length type should replace ordered
            dst_column->clear();
            for (size_t i = 0; i < copy_size; i++) {
                auto& ref = _stored_row_ref[i];
                dst_column->insert_from(*ref.block->get_by_position(idx).column, ref.row_pos);
            }
        } else {
            for (auto& it : _temp_ref_map) {
                if (!it.second.empty()) {
                    const auto& src_column = *it.first->get_by_position(idx).column;
                    for (auto& pos : it.second) {
                        dst_column->replace_column_data(src_column, pos.first, pos.second);
                    }
                }
            }
        }
    }

    for (auto& it : _temp_ref_map) {
        it.second.clear();
    }
    _stored_row_ref.clear();

    return copy_size;
}

Status VerticalBlockReader::_agg_key_next_block(Block* block, bool* eof) {
    if (_reader_context.is_key_column_group) {
        // collect_iter will filter agg keys
        auto res = _vcollect_iter->next_batch(block);
        if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
            return res;
        }
        *eof = (res.is<END_OF_FILE>());
        _eof = *eof;
        return Status::OK();
    }
    // handle value agg
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }
    int target_block_row = 0;
    auto target_columns = block->mutate_columns();

    // copy first row get from collect_iter in init
    _append_agg_data(target_columns);
    target_block_row++;

    do {
        Status res = _vcollect_iter->next_row(&_next_row);
        if (UNLIKELY(!res.ok())) {
            if (UNLIKELY(res.is<END_OF_FILE>())) {
                *eof = true;
                _eof = true;
                break;
            }
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        DCHECK(_next_row.block->columns() == block->columns());
        if (!_next_row.is_same) {
            if (target_block_row == _reader_context.batch_size) {
                break;
            }
            _agg_data_counters.push_back(_last_agg_data_counter);
            _last_agg_data_counter = 0;
            target_block_row++;
        }
        _append_agg_data(target_columns);
    } while (true);

    _agg_data_counters.push_back(_last_agg_data_counter);
    _last_agg_data_counter = 0;
    _update_agg_data(target_columns);
    block->set_columns(std::move(target_columns));

    return Status::OK();
}

Status VerticalBlockReader::_unique_key_next_block(Block* block, bool* eof) {
    if (_reader_context.is_key_column_group) {
        // Record row_source_buffer current size for key column agg flag
        // _vcollect_iter->next_batch(block) will fill row_source_buffer but delete sign is ignored
        // we calc delete sign column if it's base compaction and update row_sourece_buffer's agg flag
        // after we get current block
        VLOG_NOTICE << "reader id: " << _id
                    << ", buffer size: " << _row_sources_buffer->buffered_size();
        uint64_t row_source_idx = _row_sources_buffer->buffered_size();
        uint64_t row_buffer_size_start = row_source_idx;
        uint64_t merged_rows_start = _vcollect_iter->merged_rows();
        uint64_t filtered_rows_start = _stats.rows_del_filtered;

        auto res = _vcollect_iter->next_batch(block);
        if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
            return res;
        }
        if (UNLIKELY(_reader_context.record_rowids)) {
            auto ret = _vcollect_iter->current_block_row_locations(&_block_row_locations);
            if (UNLIKELY(!ret.ok() && !ret.is<END_OF_FILE>())) {
                return res;
            }
            DCHECK_EQ(_block_row_locations.size(), block->rows());
        }

        if (_row_sources_buffer->buffered_size() < row_buffer_size_start) {
            row_buffer_size_start = 0;
            row_source_idx = 0;
        }

        size_t merged_rows_in_rs_buffer = 0;
        for (uint64_t i = row_buffer_size_start; i < _row_sources_buffer->buffered_size(); i++) {
            if (_row_sources_buffer->get_agg_flag(i)) {
                merged_rows_in_rs_buffer++;
            }
        }

        size_t block_rows = block->rows();
        if (_delete_sign_available && block_rows > 0) {
            int ori_delete_sign_idx = _reader_context.tablet_schema->field_index(DELETE_SIGN);
            if (ori_delete_sign_idx < 0) {
                *eof = (res.is<END_OF_FILE>());
                _eof = *eof;
                return Status::OK();
            }
            // delete sign column must store in last column of the block
            int delete_sign_idx = block->columns() - 1;
            DCHECK(delete_sign_idx > 0);
            auto target_columns = block->mutate_columns();
            MutableColumnPtr delete_filter_column = (*std::move(_delete_filter_column)).mutate();
            reinterpret_cast<ColumnUInt8*>(delete_filter_column.get())->resize(block_rows);

            auto* __restrict filter_data =
                    reinterpret_cast<ColumnUInt8*>(delete_filter_column.get())->get_data().data();
            auto* __restrict delete_data =
                    reinterpret_cast<ColumnInt8*>(target_columns[delete_sign_idx].get())
                            ->get_data()
                            .data();

            int cur_row = 0;
            int delete_count = 0;
            while (cur_row < block_rows) {
                if (_row_sources_buffer->get_agg_flag(row_source_idx)) {
                    row_source_idx++;
                    continue;
                }
                bool sign = (delete_data[cur_row] == 0);
                filter_data[cur_row] = sign;
                if (UNLIKELY(!sign)) {
                    _row_sources_buffer->set_agg_flag(row_source_idx, true);
                    if (UNLIKELY(_reader_context.record_rowids)) {
                        _block_row_locations[cur_row].row_id = -1;
                        delete_count++;
                    }
                }
                cur_row++;
                row_source_idx++;
            }
            while (row_source_idx < _row_sources_buffer->buffered_size()) {
                row_source_idx++;
            }

            ColumnWithTypeAndName column_with_type_and_name {_delete_filter_column,
                                                             std::make_shared<DataTypeUInt8>(),
                                                             "__DORIS_COMPACTION_FILTER__"};
            block->insert(column_with_type_and_name);
            RETURN_IF_ERROR(
                    Block::filter_block(block, target_columns.size(), target_columns.size()));
            _stats.rows_del_filtered += block_rows - block->rows();
            DCHECK(block->try_get_by_name("__DORIS_COMPACTION_FILTER__") == nullptr);
            if (UNLIKELY(_reader_context.record_rowids)) {
                DCHECK_EQ(_block_row_locations.size(), block->rows() + delete_count);
            }
        }

        size_t filtered_rows_in_rs_buffer = 0;
        for (auto i = row_buffer_size_start; i < _row_sources_buffer->buffered_size(); i++) {
            if (_row_sources_buffer->get_agg_flag(i)) {
                filtered_rows_in_rs_buffer++;
            }
        }
        filtered_rows_in_rs_buffer -= merged_rows_in_rs_buffer;

        auto merged_rows_cur_batch = _vcollect_iter->merged_rows() - merged_rows_start;
        auto filtered_rows_cur_batch = _stats.rows_del_filtered - filtered_rows_start;

        DCHECK_EQ(merged_rows_in_rs_buffer, merged_rows_cur_batch);
        DCHECK_EQ(filtered_rows_in_rs_buffer, filtered_rows_cur_batch);
        *eof = (res.is<END_OF_FILE>());
        _eof = *eof;
        return Status::OK();
    }
    int target_block_row = 0;
    auto target_columns = block->mutate_columns();
    size_t column_count = block->columns();
    do {
        Status res = _vcollect_iter->unique_key_next_row(&_next_row);
        if (UNLIKELY(!res.ok())) {
            if (UNLIKELY(res.is<END_OF_FILE>())) {
                *eof = true;
                _eof = true;
                break;
            }
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        const auto& src_block = _next_row.block;
        assert(src_block->columns() == column_count);
        RETURN_IF_CATCH_EXCEPTION({
            for (size_t i = 0; i < column_count; ++i) {
                target_columns[i]->insert_from(*(src_block->get_by_position(i).column),
                                               _next_row.row_pos);
            }
        });
        ++target_block_row;
    } while (target_block_row < _reader_context.batch_size);
    block->set_columns(std::move(target_columns));
    return Status::OK();
}

} // namespace doris::vectorized
