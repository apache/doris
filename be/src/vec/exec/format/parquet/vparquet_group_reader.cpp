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

#include "vparquet_group_reader.h"

#include "schema_desc.h"
#include "util/simd/bits.h"
#include "vec/columns/column_const.h"
#include "vparquet_column_reader.h"

namespace doris::vectorized {

const std::vector<int64_t> RowGroupReader::NO_DELETE = {};

RowGroupReader::RowGroupReader(io::FileReaderSPtr file_reader,
                               const std::vector<ParquetReadColumn>& read_columns,
                               const int32_t row_group_id, const tparquet::RowGroup& row_group,
                               cctz::time_zone* ctz,
                               const PositionDeleteContext& position_delete_ctx,
                               const LazyReadContext& lazy_read_ctx)
        : _file_reader(file_reader),
          _read_columns(read_columns),
          _row_group_id(row_group_id),
          _row_group_meta(row_group),
          _remaining_rows(row_group.num_rows),
          _ctz(ctz),
          _position_delete_ctx(position_delete_ctx),
          _lazy_read_ctx(lazy_read_ctx) {}

RowGroupReader::~RowGroupReader() {
    _column_readers.clear();
}

Status RowGroupReader::init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
                            std::unordered_map<int, tparquet::OffsetIndex>& col_offsets) {
    _merge_read_ranges(row_ranges);
    if (_read_columns.empty()) {
        // Query task that only select columns in path.
        return Status::OK();
    }
    const size_t MAX_GROUP_BUF_SIZE = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t MAX_COLUMN_BUF_SIZE = config::parquet_column_max_buffer_mb << 20;
    size_t max_buf_size = std::min(MAX_COLUMN_BUF_SIZE, MAX_GROUP_BUF_SIZE / _read_columns.size());
    for (auto& read_col : _read_columns) {
        auto field = const_cast<FieldSchema*>(schema.get_column(read_col._file_slot_name));
        std::unique_ptr<ParquetColumnReader> reader;
        RETURN_IF_ERROR(ParquetColumnReader::create(_file_reader, field, _row_group_meta,
                                                    _read_ranges, _ctz, reader, max_buf_size));
        auto col_iter = col_offsets.find(read_col._parquet_col_id);
        if (col_iter != col_offsets.end()) {
            tparquet::OffsetIndex oi = col_iter->second;
            reader->add_offset_index(&oi);
        }
        if (reader == nullptr) {
            VLOG_DEBUG << "Init row group(" << _row_group_id << ") reader failed";
            return Status::Corruption("Init row group reader failed");
        }
        _column_readers[read_col._file_slot_name] = std::move(reader);
    }
    return Status::OK();
}

Status RowGroupReader::next_batch(Block* block, size_t batch_size, size_t* read_rows,
                                  bool* batch_eof) {
    // Process external table query task that select columns are all from path.
    if (_read_columns.empty()) {
        RETURN_IF_ERROR(_read_empty_batch(batch_size, read_rows, batch_eof));
        RETURN_IF_ERROR(
                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));

        Status st =
                VExprContext::filter_block(_lazy_read_ctx.vconjunct_ctx, block, block->columns());
        *read_rows = block->rows();
        return st;
    }
    if (_lazy_read_ctx.can_lazy_read) {
        // call _do_lazy_read recursively when current batch is skipped
        return _do_lazy_read(block, batch_size, read_rows, batch_eof);
    } else {
        ColumnSelectVector run_length_vector;
        RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.all_read_columns, batch_size,
                                          read_rows, batch_eof, run_length_vector));
        RETURN_IF_ERROR(
                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));

        if (block->rows() == 0) {
            *read_rows = block->rows();
            return Status::OK();
        }

        RETURN_IF_ERROR(_build_pos_delete_filter(*read_rows));

        std::vector<uint32_t> columns_to_filter;
        int column_to_keep = block->columns();
        columns_to_filter.resize(column_to_keep);
        for (uint32_t i = 0; i < column_to_keep; ++i) {
            columns_to_filter[i] = i;
        }
        if (_lazy_read_ctx.vconjunct_ctx != nullptr) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_lazy_read_ctx.vconjunct_ctx->execute(block, &result_column_id));
            ColumnPtr& filter_column = block->get_by_position(result_column_id).column;
            RETURN_IF_ERROR(_filter_block(block, filter_column, column_to_keep, columns_to_filter));
        } else {
            RETURN_IF_ERROR(_filter_block(block, column_to_keep, columns_to_filter));
        }

        *read_rows = block->rows();
        return Status::OK();
    }
}

void RowGroupReader::_merge_read_ranges(std::vector<RowRange>& row_ranges) {
    _read_ranges = row_ranges;
}

Status RowGroupReader::_read_column_data(Block* block, const std::vector<std::string>& columns,
                                         size_t batch_size, size_t* read_rows, bool* batch_eof,
                                         ColumnSelectVector& select_vector) {
    size_t batch_read_rows = 0;
    bool has_eof = false;
    for (auto& read_col : columns) {
        auto& column_with_type_and_name = block->get_by_name(read_col);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        size_t col_read_rows = 0;
        bool col_eof = false;
        // Should reset _filter_map_index to 0 when reading next column.
        select_vector.reset();
        while (!col_eof && col_read_rows < batch_size) {
            size_t loop_rows = 0;
            RETURN_IF_ERROR(_column_readers[read_col]->read_column_data(
                    column_ptr, column_type, select_vector, batch_size - col_read_rows, &loop_rows,
                    &col_eof));
            col_read_rows += loop_rows;
        }
        if (batch_read_rows > 0 && batch_read_rows != col_read_rows) {
            return Status::Corruption("Can't read the same number of rows among parquet columns");
        }
        batch_read_rows = col_read_rows;
        if (col_eof) {
            has_eof = true;
        }
    }
    *read_rows = batch_read_rows;
    *batch_eof = has_eof;
    return Status::OK();
}

Status RowGroupReader::_do_lazy_read(Block* block, size_t batch_size, size_t* read_rows,
                                     bool* batch_eof) {
    std::unique_ptr<ColumnSelectVector> select_vector_ptr = nullptr;
    size_t pre_read_rows;
    bool pre_eof;
    size_t origin_column_num = block->columns();
    int filter_column_id = -1;
    while (true) {
        // read predicate columns
        pre_read_rows = 0;
        pre_eof = false;
        ColumnSelectVector run_length_vector;
        RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.predicate_columns, batch_size,
                                          &pre_read_rows, &pre_eof, run_length_vector));
        if (pre_read_rows == 0) {
            DCHECK_EQ(pre_eof, true);
            break;
        }
        RETURN_IF_ERROR(_fill_partition_columns(block, pre_read_rows,
                                                _lazy_read_ctx.predicate_partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, pre_read_rows,
                                              _lazy_read_ctx.predicate_missing_columns));

        RETURN_IF_ERROR(_build_pos_delete_filter(pre_read_rows));

        // generate filter vector
        if (_lazy_read_ctx.resize_first_column) {
            // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
            // The following process may be tricky and time-consuming, but we have no other way.
            block->get_by_position(0).column->assume_mutable()->resize(pre_read_rows);
        }
        RETURN_IF_ERROR(_lazy_read_ctx.vconjunct_ctx->execute(block, &filter_column_id));
        ColumnPtr& sv = block->get_by_position(filter_column_id).column;
        if (_lazy_read_ctx.resize_first_column) {
            // We have to clean the first column to insert right data.
            block->get_by_position(0).column->assume_mutable()->clear();
        }

        // build filter map
        bool can_filter_all = false;
        const uint8_t* filter_map = _build_filter_map(sv, pre_read_rows, &can_filter_all);
        select_vector_ptr.reset(new ColumnSelectVector(filter_map, pre_read_rows, can_filter_all));
        if (select_vector_ptr->filter_all() && !pre_eof) {
            // If continuous batches are skipped, we can cache them to skip a whole page
            _cached_filtered_rows += pre_read_rows;
            for (auto& col : _lazy_read_ctx.predicate_columns) {
                // clean block to read predicate columns
                block->get_by_name(col).column->assume_mutable()->clear();
            }
            for (auto& col : _lazy_read_ctx.predicate_partition_columns) {
                block->get_by_name(col.first).column->assume_mutable()->clear();
            }
            for (auto& col : _lazy_read_ctx.predicate_missing_columns) {
                block->get_by_name(col.first).column->assume_mutable()->clear();
            }
            Block::erase_useless_column(block, origin_column_num);
        } else {
            break;
        }
    }
    if (select_vector_ptr == nullptr) {
        DCHECK_EQ(pre_read_rows + _cached_filtered_rows, 0);
        *read_rows = 0;
        *batch_eof = true;
        return Status::OK();
    }

    ColumnSelectVector& select_vector = *select_vector_ptr;
    std::unique_ptr<uint8_t[]> rebuild_filter_map = nullptr;
    if (_cached_filtered_rows != 0) {
        _rebuild_select_vector(select_vector, rebuild_filter_map, pre_read_rows);
        pre_read_rows += _cached_filtered_rows;
        _cached_filtered_rows = 0;
    }

    // lazy read columns
    size_t lazy_read_rows;
    bool lazy_eof;
    RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.lazy_read_columns, pre_read_rows,
                                      &lazy_read_rows, &lazy_eof, select_vector));
    if (pre_read_rows != lazy_read_rows) {
        return Status::Corruption("Can't read the same number of rows when doing lazy read");
    }
    // pre_eof ^ lazy_eof
    // we set pre_read_rows as batch_size for lazy read columns, so pre_eof != lazy_eof

    // filter data in predicate columns, and remove filter column
    if (select_vector.has_filter()) {
        if (block->columns() == origin_column_num) {
            // the whole row group has been filtered by _lazy_read_ctx.vconjunct_ctx, and batch_eof is
            // generated from next batch, so the filter column is removed ahead.
            DCHECK_EQ(block->rows(), 0);
        } else {
            const auto& filter_column = block->get_by_position(filter_column_id).column;
            RETURN_IF_ERROR(_filter_block(block, filter_column, origin_column_num,
                                          _lazy_read_ctx.all_predicate_col_ids));
        }
    } else {
        Block::erase_useless_column(block, origin_column_num);
    }

    size_t column_num = block->columns();
    size_t column_size = 0;
    for (int i = 0; i < column_num; ++i) {
        size_t cz = block->get_by_position(i).column->size();
        if (column_size != 0 && cz != 0) {
            DCHECK_EQ(column_size, cz);
        }
        if (cz != 0) {
            column_size = cz;
        }
    }
    _lazy_read_filtered_rows += pre_read_rows - column_size;
    *read_rows = column_size;

    *batch_eof = pre_eof;
    RETURN_IF_ERROR(_fill_partition_columns(block, column_size, _lazy_read_ctx.partition_columns));
    return _fill_missing_columns(block, column_size, _lazy_read_ctx.missing_columns);
}

const uint8_t* RowGroupReader::_build_filter_map(ColumnPtr& sv, size_t num_rows,
                                                 bool* can_filter_all) {
    const uint8_t* filter_map = nullptr;
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*sv)) {
        size_t column_size = nullable_column->size();
        if (column_size == 0) {
            *can_filter_all = true;
        } else {
            DCHECK_EQ(column_size, num_rows);
            const auto* __restrict null_map_data = nullable_column->get_null_map_data().data();
            ColumnUInt8* concrete_column = typeid_cast<ColumnUInt8*>(
                    nullable_column->get_nested_column_ptr()->assume_mutable().get());
            auto* __restrict filter_data = concrete_column->get_data().data();
            if (_position_delete_ctx.has_filter) {
                auto* __restrict pos_delete_filter_data = _pos_delete_filter_ptr->data();
                for (size_t i = 0; i < num_rows; ++i) {
                    filter_data[i] &= (!null_map_data[i]) & pos_delete_filter_data[i];
                }
            } else {
                for (size_t i = 0; i < num_rows; ++i) {
                    filter_data[i] &= (!null_map_data[i]);
                }
            }
            filter_map = filter_data;
        }
    } else if (auto* const_column = check_and_get_column<ColumnConst>(*sv)) {
        // filter all
        *can_filter_all = !const_column->get_bool(0);
    } else {
        MutableColumnPtr mutable_holder = sv->assume_mutable();
        ColumnUInt8* mutable_filter_column = typeid_cast<ColumnUInt8*>(mutable_holder.get());
        IColumn::Filter& filter = mutable_filter_column->get_data();
        auto* __restrict filter_data = filter.data();
        const size_t size = filter.size();

        if (_position_delete_ctx.has_filter) {
            auto* __restrict pos_delete_filter_data = _pos_delete_filter_ptr->data();
            for (size_t i = 0; i < size; ++i) {
                filter_data[i] &= pos_delete_filter_data[i];
            }
        }
        filter_map = filter_data;
    }
    return filter_map;
}

void RowGroupReader::_rebuild_select_vector(ColumnSelectVector& select_vector,
                                            std::unique_ptr<uint8_t[]>& filter_map,
                                            size_t pre_read_rows) {
    if (_cached_filtered_rows == 0) {
        return;
    }
    size_t total_rows = _cached_filtered_rows + pre_read_rows;
    if (select_vector.filter_all()) {
        select_vector.build(nullptr, total_rows, true);
        return;
    }

    uint8_t* map = new uint8_t[total_rows];
    filter_map.reset(map);
    for (size_t i = 0; i < _cached_filtered_rows; ++i) {
        map[i] = 0;
    }
    const uint8_t* old_map = select_vector.filter_map();
    if (old_map == nullptr) {
        // select_vector.filter_all() == true is already built.
        for (size_t i = _cached_filtered_rows; i < total_rows; ++i) {
            map[i] = 1;
        }
    } else {
        memcpy(map + _cached_filtered_rows, old_map, pre_read_rows);
    }
    select_vector.build(map, total_rows, false);
}

Status RowGroupReader::_fill_partition_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns) {
    for (auto& kv : partition_columns) {
        auto doris_column = block->get_by_name(kv.first).column;
        IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());
        auto& [value, slot_desc] = kv.second;
        if (!_text_converter->write_vec_column(slot_desc, col_ptr, const_cast<char*>(value.c_str()),
                                               value.size(), true, false, rows)) {
            return Status::InternalError("Failed to fill partition column: {}={}",
                                         slot_desc->col_name(), value);
        }
    }
    return Status::OK();
}

Status RowGroupReader::_fill_missing_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, VExprContext*>& missing_columns) {
    for (auto& kv : missing_columns) {
        if (kv.second == nullptr) {
            // no default column, fill with null
            auto nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    (*std::move(block->get_by_name(kv.first).column)).mutate().get());
            nullable_column->insert_many_defaults(rows);
        } else {
            // fill with default value
            auto* ctx = kv.second;
            auto origin_column_num = block->columns();
            int result_column_id = -1;
            // PT1 => dest primitive type
            RETURN_IF_ERROR(ctx->execute(block, &result_column_id));
            bool is_origin_column = result_column_id < origin_column_num;
            if (!is_origin_column) {
                // call resize because the first column of _src_block_ptr may not be filled by reader,
                // so _src_block_ptr->rows() may return wrong result, cause the column created by `ctx->execute()`
                // has only one row.
                std::move(*block->get_by_position(result_column_id).column).mutate()->resize(rows);
                auto result_column_ptr = block->get_by_position(result_column_id).column;
                // result_column_ptr maybe a ColumnConst, convert it to a normal column
                result_column_ptr = result_column_ptr->convert_to_full_column_if_const();
                auto origin_column_type = block->get_by_name(kv.first).type;
                bool is_nullable = origin_column_type->is_nullable();
                block->replace_by_position(
                        block->get_position_by_name(kv.first),
                        is_nullable ? make_nullable(result_column_ptr) : result_column_ptr);
                block->erase(result_column_id);
            }
        }
    }
    return Status::OK();
}

Status RowGroupReader::_read_empty_batch(size_t batch_size, size_t* read_rows, bool* batch_eof) {
    if (_position_delete_ctx.has_filter) {
        int64_t start_row_id = _position_delete_ctx.current_row_id;
        int64_t end_row_id = std::min(_position_delete_ctx.current_row_id + (int64_t)batch_size,
                                      _position_delete_ctx.last_row_id);
        int64_t num_delete_rows = 0;
        while (_position_delete_ctx.index < _position_delete_ctx.end_index) {
            const int64_t& delete_row_id =
                    _position_delete_ctx.delete_rows[_position_delete_ctx.index];
            if (delete_row_id < start_row_id) {
                _position_delete_ctx.index++;
            } else if (delete_row_id < end_row_id) {
                num_delete_rows++;
                _position_delete_ctx.index++;
            } else { // delete_row_id >= end_row_id
                break;
            }
        }
        *read_rows = end_row_id - start_row_id - num_delete_rows;
        _position_delete_ctx.current_row_id = end_row_id;
        *batch_eof = _position_delete_ctx.current_row_id == _position_delete_ctx.last_row_id;
    } else {
        if (batch_size < _remaining_rows) {
            *read_rows = batch_size;
            _remaining_rows -= batch_size;
            *batch_eof = false;
        } else {
            *read_rows = _remaining_rows;
            _remaining_rows = 0;
            *batch_eof = true;
        }
    }
    return Status::OK();
}

Status RowGroupReader::_build_pos_delete_filter(size_t read_rows) {
    if (!_position_delete_ctx.has_filter) {
        _pos_delete_filter_ptr.reset(nullptr);
        _total_read_rows += read_rows;
        return Status::OK();
    }
    _pos_delete_filter_ptr.reset(new IColumn::Filter(read_rows, 1));
    auto* __restrict _pos_delete_filter_data = _pos_delete_filter_ptr->data();
    while (_position_delete_ctx.index < _position_delete_ctx.end_index) {
        const int64_t delete_row_index_in_row_group =
                _position_delete_ctx.delete_rows[_position_delete_ctx.index] -
                _position_delete_ctx.first_row_id;
        int64_t read_range_rows = 0;
        size_t remaining_read_rows = _total_read_rows + read_rows;
        for (auto& range : _read_ranges) {
            if (delete_row_index_in_row_group < range.first_row) {
                ++_position_delete_ctx.index;
                break;
            } else if (delete_row_index_in_row_group < range.last_row) {
                int64_t index = (delete_row_index_in_row_group - range.first_row) +
                                read_range_rows - _total_read_rows;
                if (index > read_rows - 1) {
                    _total_read_rows += read_rows;
                    return Status::OK();
                }
                _pos_delete_filter_data[index] = 0;
                ++_position_delete_ctx.index;
                break;
            } else { // delete_row >= range.last_row
            }

            int64_t range_size = range.last_row - range.first_row;
            // Don't search next range when there is no remaining_read_rows.
            if (remaining_read_rows <= range_size) {
                _total_read_rows += read_rows;
                return Status::OK();
            } else {
                remaining_read_rows -= range_size;
                read_range_rows += range_size;
            }
        }
    }
    _total_read_rows += read_rows;
    return Status::OK();
}

Status RowGroupReader::_filter_block(Block* block, const ColumnPtr& filter_column,
                                     int column_to_keep, std::vector<uint32_t> columns_to_filter) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
        const auto& nested_column = nullable_column->get_nested_column_ptr();

        MutableColumnPtr mutable_holder =
                nested_column->use_count() == 1
                        ? nested_column->assume_mutable()
                        : nested_column->clone_resized(nested_column->size());

        ColumnUInt8* concrete_column = typeid_cast<ColumnUInt8*>(mutable_holder.get());
        if (!concrete_column) {
            return Status::InvalidArgument(
                    "Illegal type {} of column for filter. Must be UInt8 or Nullable(UInt8).",
                    filter_column->get_name());
        }
        auto* __restrict null_map_data = nullable_column->get_null_map_data().data();
        IColumn::Filter& filter = concrete_column->get_data();
        auto* __restrict filter_data = filter.data();
        const size_t size = filter.size();

        if (_position_delete_ctx.has_filter) {
            auto* __restrict pos_delete_filter_data = _pos_delete_filter_ptr->data();
            for (size_t i = 0; i < size; ++i) {
                filter_data[i] &= (!null_map_data[i]) & pos_delete_filter_data[i];
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                filter_data[i] &= (!null_map_data[i]);
            }
        }
        RETURN_IF_ERROR(_filter_block_internal(block, columns_to_filter, filter));
    } else if (auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
        bool ret = const_column->get_bool(0);
        if (!ret) {
            for (auto& col : columns_to_filter) {
                std::move(*block->get_by_position(col).column).assume_mutable()->clear();
            }
        }
    } else {
        MutableColumnPtr mutable_holder =
                filter_column->use_count() == 1
                        ? filter_column->assume_mutable()
                        : filter_column->clone_resized(filter_column->size());
        ColumnUInt8* mutable_filter_column = typeid_cast<ColumnUInt8*>(mutable_holder.get());
        if (!mutable_filter_column) {
            return Status::InvalidArgument(
                    "Illegal type {} of column for filter. Must be UInt8 or Nullable(UInt8).",
                    filter_column->get_name());
        }

        IColumn::Filter& filter = mutable_filter_column->get_data();
        auto* __restrict filter_data = filter.data();

        if (_position_delete_ctx.has_filter) {
            auto* __restrict pos_delete_filter_data = _pos_delete_filter_ptr->data();
            const size_t size = filter.size();
            for (size_t i = 0; i < size; ++i) {
                filter_data[i] &= pos_delete_filter_data[i];
            }
        }
        RETURN_IF_ERROR(_filter_block_internal(block, columns_to_filter, filter));
    }
    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status RowGroupReader::_filter_block(Block* block, int column_to_keep,
                                     const std::vector<uint32_t>& columns_to_filter) {
    if (_pos_delete_filter_ptr) {
        RETURN_IF_ERROR(
                _filter_block_internal(block, columns_to_filter, (*_pos_delete_filter_ptr)));
    }
    Block::erase_useless_column(block, column_to_keep);

    return Status::OK();
}

Status RowGroupReader::_filter_block_internal(Block* block,
                                              const std::vector<uint32_t>& columns_to_filter,
                                              const IColumn::Filter& filter) {
    size_t filter_size = filter.size();
    size_t count = filter_size - simd::count_zero_num((int8_t*)filter.data(), filter_size);
    if (count == 0) {
        for (auto& col : columns_to_filter) {
            std::move(*block->get_by_position(col).column).assume_mutable()->clear();
        }
    } else {
        for (auto& col : columns_to_filter) {
            size_t size = block->get_by_position(col).column->size();
            if (size != count) {
                auto& column = block->get_by_position(col).column;
                if (column->size() != count) {
                    if (column->use_count() == 1) {
                        const auto result_size = column->assume_mutable()->filter(filter);
                        CHECK_EQ(result_size, count);
                    } else {
                        column = column->filter(filter, count);
                    }
                }
            }
        }
    }
    return Status::OK();
}

ParquetColumnReader::Statistics RowGroupReader::statistics() {
    ParquetColumnReader::Statistics st;
    for (auto& reader : _column_readers) {
        auto ost = reader.second->statistics();
        st.merge(ost);
    }
    return st;
}

} // namespace doris::vectorized
