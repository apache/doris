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
#include <stdint.h>

#include <vector>

#include "exec/olap_common.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
namespace vectorized {
struct FieldSchema;
struct RowRange;
} // namespace vectorized
} // namespace doris
namespace tparquet {
class ColumnChunk;
class ColumnIndex;
class OffsetIndex;
} // namespace tparquet

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class PageIndex {
public:
    PageIndex() = default;
    ~PageIndex() = default;
    Status create_skipped_row_range(tparquet::OffsetIndex& offset_index,
                                    int64_t total_rows_of_group, int page_idx, RowRange* row_range);
    Status collect_skipped_page_range(tparquet::ColumnIndex* column_index,
                                      const ColumnValueRangeType& col_val_range,
                                      const FieldSchema* col_schema,
                                      std::vector<int>& skipped_ranges, const cctz::time_zone& ctz);
    bool check_and_get_page_index_ranges(const std::vector<tparquet::ColumnChunk>& columns);
    Status parse_column_index(const tparquet::ColumnChunk& chunk, const uint8_t* buff,
                              tparquet::ColumnIndex* column_index);
    Status parse_offset_index(const tparquet::ColumnChunk& chunk, const uint8_t* buff,
                              tparquet::OffsetIndex* offset_index);

private:
    friend class ParquetReader;
    int64_t _column_index_start;
    int64_t _column_index_size;
    int64_t _offset_index_start;
    int64_t _offset_index_size;
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized
