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
                               const std::vector<int>& column_ids)
        : _file_reader(file_reader),
          _file_metadata(file_metadata),
          _column_ids(column_ids),
          _current_row_group(0) {
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
    while (_current_row_group < total_group) {
        bool filter_group = false;
        _process_row_group_filter(&filter_group);
        if (!filter_group) {
            group_id = &_current_row_group;
        }
        _current_row_group++;
    }
    return Status::OK();
}

Status RowGroupReader::_process_row_group_filter(bool* filter_group) {
    _init_chunk_dicts();
    RETURN_IF_ERROR(_process_dict_filter());
    _init_bloom_filter();
    RETURN_IF_ERROR(_process_bloom_filter());
    return Status::OK();
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
} // namespace doris::vectorized
