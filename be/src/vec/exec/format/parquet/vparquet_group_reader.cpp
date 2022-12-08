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
#include "vec/columns/column_const.h"
#include "vparquet_column_reader.h"

namespace doris::vectorized {

RowGroupReader::RowGroupReader(doris::FileReader* file_reader,
                               const std::vector<ParquetReadColumn>& read_columns,
                               const RowGroupIndex& row_group_idx,
                               const tparquet::RowGroup& row_group, cctz::time_zone* ctz,
                               const LazyReadContext& lazy_read_ctx)
        : _file_reader(file_reader),
          _read_columns(read_columns),
          _row_group_idx(row_group_idx),
          _row_group_meta(row_group),
          _remaining_rows(row_group.num_rows),
          _ctz(ctz),
          _lazy_read_ctx(lazy_read_ctx) {}

RowGroupReader::~RowGroupReader() {
    _column_readers.clear();
}

Status RowGroupReader::init(const FieldDescriptor& schema,
                            std::unordered_map<int, tparquet::OffsetIndex>& col_offsets) {
    if (_read_columns.size() == 0) {
        // Query task that only select columns in path.
        return Status::OK();
    }
    const size_t MAX_GROUP_BUF_SIZE = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t MAX_COLUMN_BUF_SIZE = config::parquet_column_max_buffer_mb << 20;
    size_t max_buf_size = std::min(MAX_COLUMN_BUF_SIZE, MAX_GROUP_BUF_SIZE / _read_columns.size());
    for (auto& read_col : _read_columns) {
        auto field = const_cast<FieldSchema*>(schema.get_column(read_col._file_slot_name));
        std::unique_ptr<ParquetColumnReader> reader;
        RETURN_IF_ERROR(ParquetColumnReader::create(_file_reader, field, _row_group_meta, _ctz,
                                                    reader, max_buf_size));
        auto col_iter = col_offsets.find(read_col._parquet_col_id);
        if (col_iter != col_offsets.end()) {
            tparquet::OffsetIndex oi = col_iter->second;
            reader->add_offset_index(&oi);
        }
        if (reader == nullptr) {
            VLOG_DEBUG << "Init row group(" << _row_group_idx.row_group_id << ") reader failed";
            return Status::Corruption("Init row group reader failed");
        }
        _column_readers[read_col._file_slot_name] = std::move(reader);
    }
    return Status::OK();
}

void RowGroupReader::set_row_ranges(const std::vector<doris::vectorized::RowRange>& row_ranges) {
    for (auto& read_col : _read_columns) {
        _column_readers[read_col._file_slot_name]->set_row_ranges(&row_ranges);
    }
}

Status RowGroupReader::next_batch(Block* block, size_t batch_size, size_t* read_rows,
                                  bool* _batch_eof) {
    // Process external table query task that select columns are all from path.
    if (_read_columns.empty()) {
        RETURN_IF_ERROR(_read_empty_batch(batch_size, read_rows, _batch_eof));
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
        return _do_lazy_read(block, batch_size, read_rows, _batch_eof);
    } else {
        ColumnSelectVector run_length_vector;
        RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.all_read_columns, batch_size,
                                          read_rows, _batch_eof, run_length_vector));
        RETURN_IF_ERROR(
                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));

        Status st =
                VExprContext::filter_block(_lazy_read_ctx.vconjunct_ctx, block, block->columns());
        *read_rows = block->rows();
        return st;
    }
}

Status RowGroupReader::_read_column_data(Block* block, const std::vector<std::string>& columns,
                                         size_t batch_size, size_t* read_rows, bool* _batch_eof,
                                         ColumnSelectVector& select_vector) {
    size_t batch_read_rows = 0;
    bool has_eof = false;
    int col_idx = 0;
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
        if (col_idx > 0 && (has_eof ^ col_eof)) {
            return Status::Corruption("The number of rows are not equal among parquet columns");
        }
        if (batch_read_rows > 0 && batch_read_rows != col_read_rows) {
            return Status::Corruption("Can't read the same number of rows among parquet columns");
        }
        batch_read_rows = col_read_rows;
        has_eof = col_eof;
        col_idx++;
    }
    *read_rows = batch_read_rows;
    *_batch_eof = has_eof;
    return Status::OK();
}

Status RowGroupReader::_do_lazy_read(Block* block, size_t batch_size, size_t* read_rows,
                                     bool* batch_eof) {
    // read predicate columns
    size_t pre_read_rows;
    bool pre_eof;
    ColumnSelectVector run_length_vector;
    RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.predicate_columns, batch_size,
                                      &pre_read_rows, &pre_eof, run_length_vector));
    RETURN_IF_ERROR(_fill_partition_columns(block, pre_read_rows,
                                            _lazy_read_ctx.predicate_partition_columns));
    RETURN_IF_ERROR(
            _fill_missing_columns(block, pre_read_rows, _lazy_read_ctx.predicate_missing_columns));
    // generate filter vector
    if (_lazy_read_ctx.resize_first_column) {
        // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
        // The following process may be tricky and time-consuming, but we have no other way.
        block->get_by_position(0).column->assume_mutable()->resize(pre_read_rows);
    }
    size_t origin_column_num = block->columns();
    int filter_column_id = -1;
    RETURN_IF_ERROR(_lazy_read_ctx.vconjunct_ctx->execute(block, &filter_column_id));
    ColumnPtr& sv = block->get_by_position(filter_column_id).column;
    if (_lazy_read_ctx.resize_first_column) {
        // We have to clean the first column to insert right data.
        block->get_by_position(0).column->assume_mutable()->clear();
    }

    // build filter map
    bool can_filter_all = false;
    const uint8_t* filter_map = _build_filter_map(sv, pre_read_rows, &can_filter_all);
    ColumnSelectVector select_vector(filter_map, pre_read_rows, can_filter_all);
    if (select_vector.filter_all() && !pre_eof) {
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
        return _do_lazy_read(block, batch_size, read_rows, batch_eof);
    }
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
        Block::filter_block(block, _lazy_read_ctx.all_predicate_col_ids, filter_column_id,
                            origin_column_num);
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
            const uint8_t* null_map_column = nullable_column->get_null_map_data().data();
            ColumnUInt8* concrete_column = typeid_cast<ColumnUInt8*>(
                    nullable_column->get_nested_column_ptr()->assume_mutable().get());
            uint8_t* filter_data = concrete_column->get_data().data();
            for (int i = 0; i < num_rows; ++i) {
                // filter null if filter_column if nullable
                filter_data[i] &= !null_map_column[i];
            }
            filter_map = filter_data;
        }
    } else if (auto* const_column = check_and_get_column<ColumnConst>(*sv)) {
        // filter all
        *can_filter_all = !const_column->get_bool(0);
    } else {
        filter_map = assert_cast<const ColumnVector<UInt8>&>(*sv).get_data().data();
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

Status RowGroupReader::_read_empty_batch(size_t batch_size, size_t* read_rows, bool* _batch_eof) {
    if (batch_size < _remaining_rows) {
        *read_rows = batch_size;
        _remaining_rows -= batch_size;
        *_batch_eof = false;
    } else {
        *read_rows = _remaining_rows;
        _remaining_rows = 0;
        *_batch_eof = true;
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
