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

#include "vparquet_page_index.h"

#include "util/thrift_util.h"

namespace doris::vectorized {

PageIndex::~PageIndex() {
    if (_column_index != nullptr) {
        delete _column_index;
        _column_index = nullptr;
    }
    if (_offset_index != nullptr) {
        delete _offset_index;
        _offset_index = nullptr;
    }
}

Status PageIndex::get_row_range_for_page() {
    return Status();
}

Status PageIndex::collect_skipped_page_range() {
    return Status();
}

bool PageIndex::check_and_get_page_index_ranges(const std::vector<tparquet::ColumnChunk>& columns) {
    int64_t ci_start = std::numeric_limits<int64_t>::max();
    int64_t oi_start = std::numeric_limits<int64_t>::max();
    int64_t ci_end = -1;
    int64_t oi_end = -1;
    for (const tparquet::ColumnChunk& col_chunk : columns) {
        if (col_chunk.__isset.column_index_offset && col_chunk.__isset.column_index_length) {
            ci_start = std::min(ci_start, col_chunk.column_index_offset);
            ci_end =
                    std::max(ci_end, col_chunk.column_index_offset + col_chunk.column_index_length);
        }
        if (col_chunk.__isset.offset_index_offset && col_chunk.__isset.offset_index_length) {
            oi_start = std::min(oi_start, col_chunk.offset_index_offset);
            oi_end =
                    std::max(oi_end, col_chunk.offset_index_offset + col_chunk.offset_index_length);
        }
    }
    bool has_page_index = oi_end != -1 && ci_end != -1;
    if (has_page_index) {
        _column_index_start = ci_start;
        _column_index_size = ci_end - ci_start;
        _offset_index_start = oi_start;
        _offset_index_size = oi_end - oi_start;
    }
    return has_page_index;
}

Status PageIndex::parse_column_index(const tparquet::ColumnChunk& chunk, const uint8_t* buff) {
    int64_t buffer_offset = chunk.column_index_offset - _column_index_start;
    uint32_t length = chunk.column_index_length;
    DCHECK_LE(buffer_offset + length, _column_index_size);
    RETURN_IF_ERROR(deserialize_thrift_msg(buff + buffer_offset, &length, true, _column_index));
    return Status::OK();
}

Status PageIndex::parse_offset_index(const tparquet::ColumnChunk& chunk, const uint8_t* buff,
                                     int64_t buffer_size) {
    int64_t buffer_offset = chunk.offset_index_offset - _offset_index_start + _column_index_size;
    uint32_t length = chunk.offset_index_length;
    DCHECK_LE(buffer_offset + length, buffer_size);
    RETURN_IF_ERROR(deserialize_thrift_msg(buff + buffer_offset, &length, true, _offset_index));
    return Status::OK();
}

} // namespace doris::vectorized