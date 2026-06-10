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
#include "format_v2/parquet/reader/nested_column_reader.h"
#include "util/simd/bits.h"

namespace doris::parquet {

ScalarColumnReader::ScalarColumnReader(
        const ParquetColumnSchema& column_schema,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader,
        const ParquetPageSkipPlan* page_skip_plan, const cctz::time_zone* timezone,
        bool enable_strict_mode, ParquetColumnReaderProfile profile)
        : ParquetColumnReader(column_schema, column_schema.type, profile),
          _descriptor(column_schema.descriptor),
          _type_descriptor(column_schema.type_descriptor),
          _record_reader(std::move(record_reader)),
          _page_skip_plan(page_skip_plan),
          _timezone(timezone),
          _enable_strict_mode(enable_strict_mode),
          _nested_batch(std::make_unique<NestedScalarBatch>()) {}

ScalarColumnReader::~ScalarColumnReader() = default;

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
    const auto value_kind = decoded_value_kind(_type_descriptor);
    const bool is_binary_value =
            value_kind == DecodedValueKind::BINARY || value_kind == DecodedValueKind::FIXED_BINARY;
    if (!is_binary_value && record_reader->read_dense_for_nullable() && !null_map.empty()) {
        const int64_t non_null_count = static_cast<int64_t>(simd::count_zero_num(
                reinterpret_cast<const int8_t*>(null_map.data()), null_map.size()));
        const int64_t null_count = *rows_read - non_null_count;
        if (record_reader->values_written() != non_null_count) {
            return Status::Corruption(
                    "Invalid dense nullable parquet record read result for column {}: values={}, "
                    "records={}, nulls={}",
                    _name, record_reader->values_written(), *rows_read, null_count);
        }
    } else if (!is_binary_value && !record_reader->read_dense_for_nullable() &&
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
    if (rows <= 0) {
        return Status::OK();
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
    update_reader_skip_rows(rows);
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
            // Scheduler gap skips are derived from page-index selected_ranges. A page-filtered
            // range can only overlap such a gap when the whole data page is outside every selected
            // range, so partial overlap would mean the planner and scheduler are out of sync.
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
    const int64_t record_reader_skip_rows = rows - page_filtered_rows;
    RETURN_IF_ERROR(skip_records(record_reader_skip_rows));
    advance_rows_read(rows);
    return Status::OK();
}

Status ScalarColumnReader::load_nested_batch(int64_t rows) {
    DORIS_CHECK(_nested_batch != nullptr);
    RETURN_IF_ERROR(read_nested_scalar_batch(*this, rows, _definition_level, _nested_batch.get(),
                                             _repetition_level));
    return Status::OK();
}

Status ScalarColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                               int64_t* values_read) {
    if (column.get() == nullptr || values_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet nested scalar build result for column {}",
                                       _name);
    }
    DORIS_CHECK(_nested_batch != nullptr);
    NestedScalarValueCursor value_cursor(_nested_batch.get());
    const int16_t materialized_slot_definition_level = static_cast<int16_t>(
            _nested_batch->value_slot_definition_level - (_type->is_nullable() ? 1 : 0));
    *values_read = 0;
    for (int64_t level_idx = 0;
         level_idx < _nested_batch->levels_written && *values_read < length_upper_bound;
         ++level_idx) {
        const int16_t def_level = _nested_batch->def_levels[level_idx];
        const int16_t rep_level = _nested_batch->rep_levels[level_idx];
        if (def_level < materialized_slot_definition_level || rep_level > _repetition_level) {
            continue;
        }
        if (def_level == _definition_level) {
            RETURN_IF_ERROR(append_scalar_batch_value(*this, *_nested_batch, level_idx,
                                                      &value_cursor, column));
        } else {
            if (!_type->is_nullable() && def_level >= _nullable_definition_level) {
                return Status::Corruption(
                        "Parquet scalar column {} contains null for non-nullable field", _name);
            }
            column->insert_default();
        }
        ++*values_read;
    }
    return Status::OK();
}

const std::vector<int16_t>& ScalarColumnReader::nested_definition_levels() const {
    DORIS_CHECK(_nested_batch != nullptr);
    return _nested_batch->def_levels;
}

const std::vector<int16_t>& ScalarColumnReader::nested_repetition_levels() const {
    DORIS_CHECK(_nested_batch != nullptr);
    return _nested_batch->rep_levels;
}

int64_t ScalarColumnReader::nested_levels_written() const {
    DORIS_CHECK(_nested_batch != nullptr);
    return _nested_batch->levels_written;
}

bool ScalarColumnReader::is_or_has_repeated_child() const {
    return _repetition_level > 0;
}

} // namespace doris::parquet
