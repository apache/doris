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
#pragma once
#include <common/status.h>

#include "io/file_reader.h"
#include "vparquet_file_metadata.h"

namespace doris::vectorized {
class RowGroupReader {
public:
    RowGroupReader(doris::FileReader* file_reader, std::shared_ptr<FileMetaData> file_metadata,
                   const std::vector<int>& column_ids);

    ~RowGroupReader() = default;

    Status read_next_row_group(const int32_t* group_id);

private:
    void _init_column_readers(const std::vector<int>& column_ids);

    Status _process_row_group_filter(bool* filter_group);

    void _init_chunk_dicts();

    Status _process_dict_filter();

    void _init_bloom_filter();

    Status _process_bloom_filter();

    int64_t _get_row_group_start_offset(const tparquet::RowGroup& row_group);

private:
    doris::FileReader* _file_reader;
    std::shared_ptr<FileMetaData> _file_metadata;
    std::vector<int> _column_ids;
    int32_t _current_row_group;
};
} // namespace doris::vectorized
