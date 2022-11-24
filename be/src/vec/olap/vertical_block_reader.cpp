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

#include "common/status.h"
#include "olap/like_column_predicate.h"
#include "olap/olap_common.h"
#include "runtime/mem_pool.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/olap/block_reader.h"
#include "vec/olap/vcollect_iterator.h"
#include "vec/olap/vertical_merge_iterator.h"

namespace doris::vectorized {

VerticalBlockReader::~VerticalBlockReader() {
    for (int i = 0; i < _agg_functions.size(); ++i) {
        _agg_functions[i]->destroy(_agg_places[i]);
        delete[] _agg_places[i];
    }
}

Status VerticalBlockReader::_get_segment_iterators(const ReaderParams& read_params,
                                                   std::vector<RowwiseIterator*>* segment_iters) {
    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto res = _capture_rs_readers(read_params, &rs_readers);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }
    _reader_context.batch_size = _batch_size;
    _reader_context.is_vec = true;
    _reader_context.is_vertical_compaction = true;
    for (auto& rs_reader : rs_readers) {
        // segment iterator will be inited here
        RETURN_NOT_OK(rs_reader->get_segment_iterators(&_reader_context, segment_iters));
        rs_reader->reset_read_options();
    }
    return Status::OK();
}

Status VerticalBlockReader::_init_collect_iter(const ReaderParams& read_params) {
    // get segment iterators
    std::vector<RowwiseIterator*> segment_iters;
    RETURN_IF_ERROR(_get_segment_iterators(read_params, &segment_iters));

    // build heap if key column iterator or build vertical merge iterator if value column
    auto ori_return_col_size = _return_columns.size();
    if (read_params.is_key_column_group) {
        uint32_t seq_col_idx = -1;
        if (read_params.tablet->tablet_schema()->has_sequence_col()) {
            seq_col_idx = read_params.tablet->tablet_schema()->sequence_col_idx();
        }
        _vcollect_iter = new_vertical_heap_merge_iterator(segment_iters, ori_return_col_size,
                                                          read_params.tablet->keys_type(),
                                                          seq_col_idx, _row_sources_buffer);
    } else {
        _vcollect_iter = new_vertical_mask_merge_iterator(segment_iters, ori_return_col_size,
                                                          _row_sources_buffer);
    }
    // init collect iterator
    StorageReadOptions opts;
    RETURN_IF_ERROR(_vcollect_iter->init(opts));

    // In dup keys value columns compact, get first row for _init_agg_state
    if (!read_params.is_key_column_group && read_params.tablet->keys_type() == KeysType::AGG_KEYS) {
        auto st = _vcollect_iter->next_row(&_next_row);
        _eof = st.is_end_of_file();
    }

    return Status::OK();
}

void VerticalBlockReader::_init_agg_state(const ReaderParams& read_params) {
    if (_eof) {
        return;
    }
    DCHECK(_return_columns.size() == _next_row.block->columns());
    _stored_data_columns = _next_row.block->create_same_struct_block(_batch_size)->mutate_columns();

    _stored_has_null_tag.resize(_stored_data_columns.size());
    _stored_has_string_tag.resize(_stored_data_columns.size());

    auto& tablet_schema = *_tablet_schema;
    for (size_t idx = 0; idx < _return_columns.size(); ++idx) {
        AggregateFunctionPtr function =
                tablet_schema.column(_return_columns.at(idx))
                        .get_aggregate_function({_next_row.block->get_data_type(idx)},
                                                vectorized::AGG_READER_SUFFIX);
        DCHECK(function != nullptr);
        _agg_functions.push_back(function);
        // create aggregate data
        AggregateDataPtr place = new char[function->size_of_data()];
        function->create(place);
        _agg_places.push_back(place);

        // calculate `has_string` tag.
        _stored_has_string_tag[idx] =
                _stored_data_columns[idx]->is_column_string() ||
                (_stored_data_columns[idx]->is_nullable() &&
                 reinterpret_cast<ColumnNullable*>(_stored_data_columns[idx].get())
                         ->get_nested_column_ptr()
                         ->is_column_string());
    }
}

Status VerticalBlockReader::init(const ReaderParams& read_params) {
    StorageReadOptions opts;
    _batch_size = opts.block_row_max;
    RETURN_NOT_OK(TabletReader::init(read_params));

    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto status = _init_collect_iter(read_params);
    if (!status.ok()) {
        return status;
    }

    switch (tablet()->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &VerticalBlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        _next_block_func = &VerticalBlockReader::_unique_key_next_block;
        if (_filter_delete) {
            _delete_filter_column = ColumnUInt8::create();
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

Status VerticalBlockReader::_direct_next_block(Block* block, MemPool* mem_pool,
                                               ObjectPool* agg_pool, bool* eof) {
    auto res = _vcollect_iter->next_batch(block);
    if (UNLIKELY(!res.ok() && !res.is_end_of_file())) {
        return res;
    }
    *eof = (res.is_end_of_file());
    _eof = *eof;
    return Status::OK();
}

void VerticalBlockReader::_append_agg_data(MutableColumns& columns) {
    _stored_row_ref.push_back(_next_row);
    _last_agg_data_counter++;

    // execute aggregate when have `batch_size` column or some ref invalid soon
    bool is_last = (_next_row.block->rows() == _next_row.row_pos + 1);
    if (is_last || _stored_row_ref.size() == _batch_size) {
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
        auto column_ptr = _stored_data_columns[idx].get();

        if (begin <= end) {
            function->add_batch_range(begin, end, place, const_cast<const IColumn**>(&column_ptr),
                                      nullptr, _stored_has_null_tag[idx]);
        }

        if (is_close) {
            function->insert_result_into(place, *columns[idx]);
            // reset aggregate data
            function->destroy(place);
            function->create(place);
        }
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
        if (_stored_has_string_tag[idx]) {
            //string type should replace ordered
            for (size_t i = 0; i < copy_size; i++) {
                auto& ref = _stored_row_ref[i];
                dst_column->replace_column_data(*ref.block->get_by_position(idx).column,
                                                ref.row_pos, i);
            }
        } else {
            for (auto& it : _temp_ref_map) {
                if (!it.second.empty()) {
                    auto& src_column = *it.first->get_by_position(idx).column;
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

Status VerticalBlockReader::_agg_key_next_block(Block* block, MemPool* mem_pool,
                                                ObjectPool* agg_pool, bool* eof) {
    if (_reader_context.is_key_column_group) {
        // collect_iter will filter agg keys
        auto res = _vcollect_iter->next_batch(block);
        if (UNLIKELY(!res.ok() && !res.is_end_of_file())) {
            return res;
        }
        *eof = (res.is_end_of_file());
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
            if (UNLIKELY(res.is_end_of_file())) {
                *eof = true;
                _eof = true;
                break;
            }
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        DCHECK(_next_row.block->columns() == block->columns());
        if (!_next_row.is_same) {
            if (target_block_row == _batch_size) {
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

    return Status::OK();
}

Status VerticalBlockReader::_unique_key_next_block(Block* block, MemPool* mem_pool,
                                                   ObjectPool* agg_pool, bool* eof) {
    if (_reader_context.is_key_column_group) {
        // Record row_source_buffer current size for key column agg flag
        // _vcollect_iter->next_batch(block) will fill row_source_buffer but delete sign is ignored
        // we calc delete sign column if it's base compaction and update row_sourece_buffer's agg flag
        // after we get current block
        auto row_source_idx = _row_sources_buffer->buffered_size();

        auto res = _vcollect_iter->next_batch(block);
        if (UNLIKELY(!res.ok() && !res.is_end_of_file())) {
            return res;
        }
        auto block_rows = block->rows();
        if (_filter_delete && block_rows > 0) {
            int ori_delete_sign_idx = _reader_context.tablet_schema->field_index(DELETE_SIGN);
            if (ori_delete_sign_idx < 0) {
                *eof = (res.is_end_of_file());
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
            for (int i = 0; i < block_rows; ++i) {
                bool sign = (delete_data[i] == 0);
                filter_data[i] = sign;
                if (UNLIKELY(!sign)) {
                    _row_sources_buffer->set_agg_flag(row_source_idx + i, true);
                }
            }

            ColumnWithTypeAndName column_with_type_and_name {_delete_filter_column,
                                                             std::make_shared<DataTypeUInt8>(),
                                                             "__DORIS_COMPACTION_FILTER__"};
            block->insert(column_with_type_and_name);
            Block::filter_block(block, target_columns.size(), target_columns.size());
            _stats.rows_del_filtered += block_rows - block->rows();
            DCHECK(block->try_get_by_name("__DORIS_COMPACTION_FILTER__") == nullptr);
        }
        *eof = (res.is_end_of_file());
        _eof = *eof;
        return Status::OK();
    }
    int target_block_row = 0;
    auto target_columns = block->mutate_columns();
    size_t column_count = block->columns();
    do {
        Status res = _vcollect_iter->unique_key_next_row(&_next_row);
        if (UNLIKELY(!res.ok())) {
            if (UNLIKELY(res.is_end_of_file())) {
                *eof = true;
                _eof = true;
                break;
            }
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        const auto& src_block = _next_row.block;
        assert(src_block->columns() == column_count);
        for (size_t i = 0; i < column_count; ++i) {
            target_columns[i]->insert_from(*(src_block->get_by_position(i).column),
                                           _next_row.row_pos);
        }
        ++target_block_row;
    } while (target_block_row < _batch_size);
    return Status::OK();
}

} // namespace doris::vectorized
