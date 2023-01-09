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

#include "vec/olap/block_reader.h"

#include "common/status.h"
#include "olap/like_column_predicate.h"
#include "olap/olap_common.h"
#include "runtime/mem_pool.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/olap/vcollect_iterator.h"

namespace doris::vectorized {
using namespace ErrorCode;

BlockReader::~BlockReader() {
    for (int i = 0; i < _agg_functions.size(); ++i) {
        _agg_functions[i]->destroy(_agg_places[i]);
        delete[] _agg_places[i];
    }
}

Status BlockReader::_init_collect_iter(const ReaderParams& read_params,
                                       std::vector<RowsetReaderSharedPtr>* valid_rs_readers) {
    _vcollect_iter.init(this, read_params.read_orderby_key, read_params.read_orderby_key_reverse);
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
    _reader_context.push_down_agg_type_opt = read_params.push_down_agg_type_opt;
    for (auto& rs_reader : rs_readers) {
        RETURN_NOT_OK(rs_reader->init(&_reader_context));
        Status res = _vcollect_iter.add_child(rs_reader);
        if (!res.ok() && !res.is<END_OF_FILE>()) {
            LOG(WARNING) << "failed to add child to iterator, err=" << res;
            return res;
        }
        if (res.ok()) {
            valid_rs_readers->push_back(rs_reader);
        }
    }

    RETURN_IF_ERROR(_vcollect_iter.build_heap(*valid_rs_readers));
    if (_vcollect_iter.is_merge()) {
        auto status = _vcollect_iter.current_row(&_next_row);
        _eof = status.is<END_OF_FILE>();
    }

    return Status::OK();
}

void BlockReader::_init_agg_state(const ReaderParams& read_params) {
    if (_eof) {
        return;
    }

    _stored_data_columns = _next_row.block->create_same_struct_block(_batch_size)->mutate_columns();

    _stored_has_null_tag.resize(_stored_data_columns.size());
    _stored_has_string_tag.resize(_stored_data_columns.size());

    auto& tablet_schema = *_tablet_schema;
    for (auto idx : _agg_columns_idx) {
        AggregateFunctionPtr function =
                tablet_schema
                        .column(read_params.origin_return_columns->at(_return_columns_loc[idx]))
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

Status BlockReader::init(const ReaderParams& read_params) {
    RETURN_NOT_OK(TabletReader::init(read_params));

    int32_t return_column_size = read_params.origin_return_columns->size();
    _return_columns_loc.resize(read_params.return_columns.size());
    for (int i = 0; i < return_column_size; ++i) {
        auto cid = read_params.origin_return_columns->at(i);
        for (int j = 0; j < read_params.return_columns.size(); ++j) {
            if (read_params.return_columns[j] == cid) {
                if (j < _tablet->num_key_columns() || _tablet->keys_type() != AGG_KEYS) {
                    _normal_columns_idx.emplace_back(j);
                } else {
                    _agg_columns_idx.emplace_back(j);
                }
                _return_columns_loc[j] = i;
                break;
            }
        }
    }

    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto status = _init_collect_iter(read_params, &rs_readers);
    if (!status.ok()) {
        return status;
    }

    if (_direct_mode) {
        _next_block_func = &BlockReader::_direct_next_block;
        return Status::OK();
    }

    switch (tablet()->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &BlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        if (_reader_context.enable_unique_key_merge_on_write) {
            _next_block_func = &BlockReader::_direct_next_block;
        } else {
            _next_block_func = &BlockReader::_unique_key_next_block;
            if (_filter_delete) {
                _delete_filter_column = ColumnUInt8::create();
            }
        }
        break;
    case KeysType::AGG_KEYS:
        _next_block_func = &BlockReader::_agg_key_next_block;
        _init_agg_state(read_params);
        break;
    default:
        DCHECK(false) << "No next row function for type:" << tablet()->keys_type();
        break;
    }

    return Status::OK();
}

Status BlockReader::_direct_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                       bool* eof) {
    auto res = _vcollect_iter.next(block);
    if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
        return res;
    }
    *eof = res.is<END_OF_FILE>();
    _eof = *eof;
    if (UNLIKELY(_reader_context.record_rowids)) {
        res = _vcollect_iter.current_block_row_locations(&_block_row_locations);
        if (UNLIKELY(!res.ok() && res != Status::Error<END_OF_FILE>())) {
            return res;
        }
        DCHECK_EQ(_block_row_locations.size(), block->rows());
    }
    return Status::OK();
}

Status BlockReader::_direct_agg_key_next_block(Block* block, MemPool* mem_pool,
                                               ObjectPool* agg_pool, bool* eof) {
    return Status::OK();
}

Status BlockReader::_agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                        bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }

    auto target_block_row = 0;
    auto merged_row = 0;
    auto target_columns = block->mutate_columns();

    _insert_data_normal(target_columns);
    target_block_row++;
    _append_agg_data(target_columns);

    while (true) {
        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
            *eof = true;
            break;
        }
        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }

        if (!_get_next_row_same()) {
            if (target_block_row == _batch_size) {
                break;
            }
            _agg_data_counters.push_back(_last_agg_data_counter);
            _last_agg_data_counter = 0;

            _insert_data_normal(target_columns);
            target_block_row++;
        } else {
            merged_row++;
        }

        _append_agg_data(target_columns);
    }

    _agg_data_counters.push_back(_last_agg_data_counter);
    _last_agg_data_counter = 0;
    _update_agg_data(target_columns);

    _merged_rows += merged_row;
    return Status::OK();
}

Status BlockReader::_unique_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                           bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }

    auto target_block_row = 0;
    auto target_columns = block->mutate_columns();
    if (UNLIKELY(_reader_context.record_rowids)) {
        _block_row_locations.resize(_batch_size);
    }

    do {
        _insert_data_normal(target_columns);
        if (UNLIKELY(_reader_context.record_rowids)) {
            _block_row_locations[target_block_row] = _vcollect_iter.current_row_location();
        }
        target_block_row++;

        // the version is in reverse order, the first row is the highest version,
        // in UNIQUE_KEY highest version is the final result, there is no need to
        // merge the lower versions
        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
            *eof = true;
            if (UNLIKELY(_reader_context.record_rowids)) {
                _block_row_locations.resize(target_block_row);
            }
            break;
        }

        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
    } while (target_block_row < _batch_size);

    // do filter delete row in base compaction, only base compaction need to do the job
    if (_filter_delete) {
        int delete_sign_idx = _reader_context.tablet_schema->field_index(DELETE_SIGN);
        DCHECK(delete_sign_idx > 0);
        if (delete_sign_idx <= 0 || delete_sign_idx >= target_columns.size()) {
            LOG(WARNING) << "tablet_id: " << tablet()->tablet_id() << " delete sign idx "
                         << delete_sign_idx
                         << " not invalid, skip filter delete in base compaction";
            return Status::OK();
        }
        MutableColumnPtr delete_filter_column = (*std::move(_delete_filter_column)).mutate();
        reinterpret_cast<ColumnUInt8*>(delete_filter_column.get())->resize(target_block_row);

        auto* __restrict filter_data =
                reinterpret_cast<ColumnUInt8*>(delete_filter_column.get())->get_data().data();
        auto* __restrict delete_data =
                reinterpret_cast<ColumnInt8*>(target_columns[delete_sign_idx].get())
                        ->get_data()
                        .data();
        for (int i = 0; i < target_block_row; ++i) {
            filter_data[i] = delete_data[i] == 0;
        }

        ColumnWithTypeAndName column_with_type_and_name {_delete_filter_column,
                                                         std::make_shared<DataTypeUInt8>(),
                                                         "__DORIS_COMPACTION_FILTER__"};
        block->insert(column_with_type_and_name);
        Block::filter_block(block, target_columns.size(), target_columns.size());
        _stats.rows_del_filtered += target_block_row - block->rows();
        DCHECK(block->try_get_by_name("__DORIS_COMPACTION_FILTER__") == nullptr);
    }
    return Status::OK();
}

void BlockReader::_insert_data_normal(MutableColumns& columns) {
    auto block = _next_row.block.get();
    for (auto idx : _normal_columns_idx) {
        columns[_return_columns_loc[idx]]->insert_from(*block->get_by_position(idx).column,
                                                       _next_row.row_pos);
    }
}

void BlockReader::_append_agg_data(MutableColumns& columns) {
    _stored_row_ref.push_back(_next_row);
    _last_agg_data_counter++;

    // execute aggregate when have `batch_size` column or some ref invalid soon
    bool is_last = (_next_row.block->rows() == _next_row.row_pos + 1);
    if (is_last || _stored_row_ref.size() == _batch_size) {
        _update_agg_data(columns);
    }
}

void BlockReader::_update_agg_data(MutableColumns& columns) {
    // copy data to stored block
    size_t copy_size = _copy_agg_data();

    // calculate has_null_tag
    for (auto idx : _agg_columns_idx) {
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

size_t BlockReader::_copy_agg_data() {
    size_t copy_size = _stored_row_ref.size();

    for (size_t i = 0; i < copy_size; i++) {
        auto& ref = _stored_row_ref[i];
        _temp_ref_map[ref.block.get()].emplace_back(ref.row_pos, i);
    }

    for (auto idx : _agg_columns_idx) {
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

void BlockReader::_update_agg_value(MutableColumns& columns, int begin, int end, bool is_close) {
    for (int i = 0; i < _agg_columns_idx.size(); i++) {
        auto idx = _agg_columns_idx[i];

        AggregateFunctionPtr function = _agg_functions[i];
        AggregateDataPtr place = _agg_places[i];
        auto column_ptr = _stored_data_columns[idx].get();

        if (begin <= end) {
            function->add_batch_range(begin, end, place, const_cast<const IColumn**>(&column_ptr),
                                      nullptr, _stored_has_null_tag[idx]);
        }

        if (is_close) {
            function->insert_result_into(place, *columns[_return_columns_loc[idx]]);
            // reset aggregate data
            function->destroy(place);
            function->create(place);
        }
    }
}

bool BlockReader::_get_next_row_same() {
    if (_next_row.is_same) {
        return true;
    } else {
        auto block = _next_row.block.get();
        return block->get_same_bit(_next_row.row_pos);
    }
}

ColumnPredicate* BlockReader::_parse_to_predicate(const FunctionFilter& function_filter) {
    int32_t index = _tablet_schema->field_index(function_filter._col_name);
    if (index < 0) {
        return nullptr;
    }

    // currently only support like predicate
    return new LikeColumnPredicate<true>(function_filter._opposite, index, function_filter._fn_ctx,
                                         function_filter._string_param);
}

} // namespace doris::vectorized
