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

#include "format_v2/parquet/reader/scalar_column_reader.h"

#include <parquet/api/reader.h>

#include <algorithm>
#include <exception>
#include <utility>

#include "core/column/column.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "util/simd/bits.h"

namespace doris::parquet {

ScalarColumnReader::ScalarColumnReader(
        const ParquetColumnSchema& column_schema,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader,
        const ParquetPageSkipPlan* page_skip_plan, ParquetColumnReaderProfile profile)
        : ParquetColumnReader(column_schema, column_schema.type, profile),
          _descriptor(column_schema.descriptor),
          _type_descriptor(column_schema.type_descriptor),
          _record_reader(std::move(record_reader)),
          _page_skip_plan(page_skip_plan) {}

Status ScalarColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet column read result pointer for column {}",
                                       _name);
    }
    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }
    ::parquet::internal::RecordReader* record_reader = nullptr;
    const auto context = leaf_context();
    RETURN_IF_ERROR(read_leaf_records(context, rows, &record_reader, rows_read));

    NullMap null_map;
    RETURN_IF_ERROR(build_leaf_null_map(context, *record_reader, *rows_read, &null_map));
    if (record_reader->read_dense_for_nullable() && !null_map.empty()) {
        const int64_t non_null_count = static_cast<int64_t>(simd::count_zero_num(
                reinterpret_cast<const int8_t*>(null_map.data()), null_map.size()));
        const int64_t null_count = *rows_read - non_null_count;
        if (record_reader->values_written() != non_null_count) {
            return Status::Corruption(
                    "Invalid dense nullable parquet record read result for column {}: values={}, "
                    "records={}, nulls={}",
                    _name, record_reader->values_written(), *rows_read, null_count);
        }
    } else if (!record_reader->read_dense_for_nullable() &&
               record_reader->values_written() != *rows_read) {
        return Status::Corruption(
                "Invalid parquet record read result for column {}: values={}, records={}", _name,
                record_reader->values_written(), *rows_read);
    }

    RETURN_IF_ERROR(append_leaf_values(context, *record_reader, *rows_read, &null_map, column));
    advance_rows_read(*rows_read);
    update_reader_read_rows(*rows_read);
    return Status::OK();
}

Status ScalarColumnReader::skip_records(int64_t rows) {
    if (_record_reader == nullptr) {
        return Status::InternalError("Parquet record reader is not initialized for column {}",
                                     _name);
    }
    int64_t skipped_rows = 0;
    try {
        _record_reader->Reset();
        while (skipped_rows < rows) {
            const int64_t skipped = _record_reader->SkipRecords(rows - skipped_rows);
            if (skipped <= 0) {
                return Status::Corruption(
                        "Failed to skip parquet records for column {}: skipped {} of {} rows",
                        _name, skipped_rows, rows);
            }
            skipped_rows += skipped;
        }
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to skip parquet records for column {}: {}", _name,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to skip parquet records for column {}: {}", _name,
                                     e.what());
    }
    return Status::OK();
}

int64_t ScalarColumnReader::page_filtered_rows_to_skip(int64_t rows) const {
    if (_page_skip_plan == nullptr || rows <= 0) {
        return 0;
    }
    const int64_t skip_end = _row_group_rows_read + rows;
    int64_t filtered_rows = 0;
    for (const auto& range : _page_skip_plan->skipped_ranges) {
        const int64_t range_end = range.start + range.length;
        if (range_end <= _row_group_rows_read) {
            continue;
        }
        if (range.start >= skip_end) {
            break;
        }
        const int64_t start = std::max(range.start, _row_group_rows_read);
        const int64_t end = std::min(range_end, skip_end);
        if (start < end) {
            DORIS_CHECK(start == range.start);
            DORIS_CHECK(end == range_end);
            filtered_rows += end - start;
        }
    }
    return filtered_rows;
}

void ScalarColumnReader::advance_rows_read(int64_t rows) {
    DORIS_CHECK(rows >= 0);
    _row_group_rows_read += rows;
}

Status ScalarColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }

    const int64_t page_filtered_rows = page_filtered_rows_to_skip(rows);
    DORIS_CHECK(page_filtered_rows <= rows);
    RETURN_IF_ERROR(skip_records(rows - page_filtered_rows));
    advance_rows_read(rows);
    update_reader_skip_rows(rows);
    return Status::OK();
}

} // namespace doris::parquet
