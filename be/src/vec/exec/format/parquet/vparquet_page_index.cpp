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

#include "parquet_pred_cmp.h"
#include "util/thrift_util.h"

namespace doris::vectorized {

Status PageIndex::create_skipped_row_range(tparquet::OffsetIndex& offset_index,
                                           int total_rows_of_group, int page_idx,
                                           RowRange* row_range) {
    const auto& page_locations = offset_index.page_locations;
    DCHECK_LT(page_idx, page_locations.size());
    row_range->first_row = page_locations[page_idx].first_row_index;
    if (page_idx == page_locations.size() - 1) {
        row_range->last_row = total_rows_of_group - 1;
    } else {
        row_range->last_row = page_locations[page_idx + 1].first_row_index - 1;
    }
    return Status::OK();
}

Status PageIndex::collect_skipped_page_range(tparquet::ColumnIndex* column_index,
                                             std::vector<ExprContext*> conjuncts,
                                             std::unordered_set<int> skipped_ranges) {
    //    int first_not_null_page_idx = -1;
    //    for (int page_id = 0; page_id < column_index->null_pages.size(); ++page_id) {
    //        bool is_null_page = column_index->null_pages[page_id];
    //        if (is_null_page) {
    //             skipped_ranges.emplace(page_id);
    //        } else {
    //             if (first_not_null_page_idx == -1) {
    //                  first_not_null_page_idx = page_idx;
    //             }
    //        }
    //    }
    //    int start_page_idx = first_not_null_page_idx;
    //    int end_page_idx = column_index->null_pages.size() - 1;

    //    const vector<std::string>& encoded_min_vals = column_index->min_values;
    //    const vector<std::string>& encoded_max_vals = column_index->max_values;

    //    DCHECK_EQ(encoded_min_vals.size(), encoded_max_vals.size());
    //    DCHECK(skipped_ranges && skipped_ranges.size() == 0);
    //    if (start_page_idx > end_page_idx) return;
    //    DCHECK_LE(0, start_page_idx);
    //    DCHECK_LT(end_page_idx, encoded_max_vals .size());
    //
    //    if (_filter_page_by_min_max(conjuncts, encoded_min_vals[start_page_idx], encoded_max_vals[end_page_idx])) {
    //        skipped_ranges.emplace(PageRange(start_page_idx, end_page_idx));
    //        return;
    //    }
    //
    //    vector<std::string>::const_iterator begin = encoded_max_vals.begin() + start_page_idx;
    //    vector<std::string>::const_iterator end = encoded_max_vals.begin() + end_page_idx + 1;
    //    auto it = std::lower_bound(begin, end, filter_min, compare_less);
    //
    //    int idx = start_page_idx;
    //    if (it != end && it != begin) {
    //        idx = it - begin + start_page_idx;
    //        // skip from start_page_idx to idx - 1
    //        skipped_ranges.emplace(PageRange(start_page_idx, idx - 1));
    //    }
    //
    //    begin = encoded_min_vals.begin() + idx;
    //    end = encoded_min_vals.begin() + end_page_idx + 1;
    //    it = std::upper_bound(begin, end, filter_max, compare_less);
    //
    //    if (it != end) {
    //        idx += it - begin;
    //        // skip from idx to end_page_idx
    //        skipped_ranges.emplace(PageRange(idx, end_page_idx));
    //    }
    //    LOG(WARNING) << "skipped_ranges.size()=" << skipped_ranges.size();
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

Status PageIndex::parse_column_index(const tparquet::ColumnChunk& chunk, const uint8_t* buff,
                                     tparquet::ColumnIndex* column_index) {
    int64_t buffer_offset = chunk.column_index_offset - _column_index_start;
    uint32_t length = chunk.column_index_length;
    DCHECK_GE(buffer_offset, 0);
    DCHECK_LE(buffer_offset + length, _column_index_size);
    RETURN_IF_ERROR(deserialize_thrift_msg(buff + buffer_offset, &length, true, column_index));
    return Status::OK();
}

Status PageIndex::parse_offset_index(const tparquet::ColumnChunk& chunk, const uint8_t* buff,
                                     int64_t buffer_size, tparquet::OffsetIndex* offset_index) {
    int64_t buffer_offset = chunk.offset_index_offset - _offset_index_start + _column_index_size;
    uint32_t length = chunk.offset_index_length;
    DCHECK_GE(buffer_offset, 0);
    DCHECK_LE(buffer_offset + length, buffer_size);
    RETURN_IF_ERROR(deserialize_thrift_msg(buff + buffer_offset, &length, true, offset_index));
    return Status::OK();
}

} // namespace doris::vectorized