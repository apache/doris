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

namespace doris::vectorized {

RowGroupReader::RowGroupReader(doris::FileReader* file_reader,
                               std::shared_ptr<FileMetaData> file_metadata,
                               const std::vector<int>& column_ids,
                               int64_t split_start_offset,
                               int64_t split_size)
        : _file_reader(file_reader),
          _file_metadata(file_metadata),
          _column_ids(column_ids),
          _current_row_group(0),
          _split_start_offset(split_start_offset),
          _split_size(split_size) {
    DCHECK_LE(column_ids.size(), file_metadata->num_columns());
    _init_column_readers(column_ids);
}

void RowGroupReader::_init_column_readers(const std::vector<int>& column_ids) {
    //    for (int col_id: column_ids) {
    //
    //    }
}

Status RowGroupReader::read_next_row_group(const int32_t* group_id) {
    int32_t total_group = _file_metadata->num_row_groups();
    if (total_group == 0 || _file_metadata->num_rows() == 0) {
        return Status::EOF("");
    }
    while (_current_row_group < total_group) {
        _current_row_group++;
        const parquet::RowGroup& row_group = file_metadata_.row_groups[_current_row_group];
        RETURN_IF_ERROR(ParquetMetadataUtils::ValidateColumnOffsets(
                file_desc->filename, file_desc->file_length, row_group));
        if(!_is_misaligned_range_group(row_group)) {
            continue;
        }
        bool filter_group = false;
        _process_row_group_filter(&filter_group);
        if (!filter_group) {
            group_id = &_current_row_group;
            break;
        }
    }
    return Status::OK();
}

bool RowGroupReader::_is_misaligned_range_group(const parquet::RowGroup& row_group) {
    int64_t start_offset = _get_column_start_offset(row_group.columns[0].meta_data);

    auto last_column = row_group.columns[row_group.columns.size() - 1].meta_data;
    int64_t end_offset =
            _get_column_start_offset(last_column) + last_column.total_compressed_size;

    int64_t row_group_mid = start_offset + (end_offset - start_offset) / 2;
    if (!(row_group_mid >= _split_start_offset &&
            row_group_mid < _split_start_offset + _split_size)) {
        return true;
    }
}

Status RowGroupReader::_process_row_group_filter(bool* filter_group) {
    _process_column_stat_filter();
    _init_chunk_dicts();
    RETURN_IF_ERROR(_process_dict_filter());
    _init_bloom_filter();
    RETURN_IF_ERROR(_process_bloom_filter());
    return Status::OK();
}

Status RowGroupReader::_process_column_stat_filter() {
    return Status();
}

void RowGroupReader::_init_chunk_dicts() {}

Status RowGroupReader::_process_dict_filter() {
    return Status();
}

void RowGroupReader::_init_bloom_filter() {}

Status RowGroupReader::_process_bloom_filter() {
    RETURN_IF_ERROR(_file_reader->seek(0));
    return Status();
}

int64_t RowGroupReader::_get_row_group_start_offset(const tparquet::RowGroup& row_group) {
    if (row_group.__isset.file_offset) {
        return row_group.file_offset;
    }
    return row_group.columns[0].meta_data.data_page_offset;
}

int64_t RowGroupReader::_get_column_start_offset(const tparquet::ColumnMetaData& column) {
    if (column.__isset.dictionary_page_offset) {
        DCHECK_LT(column.dictionary_page_offset, column.data_page_offset);
        return column.dictionary_page_offset;
    }
    return column.data_page_offset;
}
} // namespace doris::vectorized
