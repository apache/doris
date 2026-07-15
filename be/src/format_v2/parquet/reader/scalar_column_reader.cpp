// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/parquet/reader/scalar_column_reader.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <parquet/api/reader.h>

#include <algorithm>
#include <exception>
#include <utility>

#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "util/simd/bits.h"

namespace doris::format::parquet {
namespace {

class ParquetNestedScalarValueCursor {
public:
    explicit ParquetNestedScalarValueCursor(const ParquetNestedScalarBatch* batch) { reset(batch); }

    void reset(const ParquetNestedScalarBatch* batch) {
        DORIS_CHECK(batch != nullptr);
        _batch = batch;
    }

    Status value_index(const std::string& column_name, int64_t level_idx, int64_t* value_idx) {
        DORIS_CHECK(_batch != nullptr);
        DORIS_CHECK(value_idx != nullptr);
        DORIS_CHECK(level_idx < _batch->levels_written);
        DORIS_CHECK(level_idx >= 0);
        DORIS_CHECK(static_cast<size_t>(level_idx) < _batch->value_indices.size());
        const int64_t computed_value_idx = _batch->value_indices[static_cast<size_t>(level_idx)];
        if (computed_value_idx < 0) {
            return Status::Corruption("Nested parquet value is absent for column {}", column_name);
        }
        DORIS_CHECK(_batch->values_column.get() != nullptr);
        if (computed_value_idx >= _batch->values_column->size()) {
            return Status::Corruption("Nested parquet value index is out of range for column {}",
                                      column_name);
        }
        *value_idx = computed_value_idx;
        return Status::OK();
    }

private:
    const ParquetNestedScalarBatch* _batch = nullptr;
};

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const ParquetNestedScalarBatch& batch, int64_t level_idx,
                                 ParquetNestedScalarValueCursor* value_cursor,
                                 MutableColumnPtr& column) {
    DORIS_CHECK(value_cursor != nullptr);
    int64_t value_idx = -1;
    RETURN_IF_ERROR(value_cursor->value_index(column_reader.name(), level_idx, &value_idx));
    auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
    if (nullable_column != nullptr) {
        nullable_column->get_nested_column().insert_from(*batch.values_column,
                                                         static_cast<size_t>(value_idx));
        nullable_column->get_null_map_data().push_back(0);
        return Status::OK();
    }
    column->insert_from(*batch.values_column, static_cast<size_t>(value_idx));
    return Status::OK();
}

Status append_arrow_binary_dictionary_value(const std::string& column_name,
                                            const ::arrow::Array& dictionary,
                                            int64_t dictionary_index,
                                            std::vector<StringRef>* values) {
    DORIS_CHECK(values != nullptr);
    if (dictionary_index < 0 || dictionary_index >= dictionary.length()) {
        return Status::Corruption("Invalid parquet dictionary index {} for column {}",
                                  dictionary_index, column_name);
    }
    if (auto* binary_array = dynamic_cast<const ::arrow::BinaryArray*>(&dictionary)) {
        if (binary_array->IsNull(dictionary_index)) {
            values->emplace_back(static_cast<const char*>(nullptr), 0);
            return Status::OK();
        }
        int32_t length = 0;
        const uint8_t* value = binary_array->GetValue(dictionary_index, &length);
        values->emplace_back(reinterpret_cast<const char*>(value), length);
        return Status::OK();
    }
    if (auto* fixed_array = dynamic_cast<const ::arrow::FixedSizeBinaryArray*>(&dictionary)) {
        if (fixed_array->IsNull(dictionary_index)) {
            values->emplace_back(static_cast<const char*>(nullptr), 0);
            return Status::OK();
        }
        values->emplace_back(reinterpret_cast<const char*>(fixed_array->GetValue(dictionary_index)),
                             fixed_array->byte_width());
        return Status::OK();
    }
    return Status::InternalError("Unexpected Arrow dictionary value array type for column {}",
                                 column_name);
}

} // namespace

ScalarColumnReader::ScalarColumnReader(
        const ParquetColumnSchema& column_schema,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader,
        const ParquetPageSkipPlan* page_skip_plan, const cctz::time_zone* timezone,
        bool enable_strict_mode, ParquetColumnReaderProfile profile)
        : ParquetColumnReader(column_schema, column_schema.type, profile),
          _descriptor(column_schema.descriptor),
          _type_descriptor(column_schema.type_descriptor),
          _record_reader(std::move(record_reader)),
          _leaf_reader(std::make_unique<ParquetLeafReader>(_descriptor, _type_descriptor, _type,
                                                           _name, _record_reader, _profile,
                                                           timezone, enable_strict_mode)),
          _page_skip_plan(page_skip_plan),
          _timezone(timezone),
          _enable_strict_mode(enable_strict_mode),
          _nested_batch(std::make_unique<ParquetNestedScalarBatch>()) {}

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
    auto& reader = leaf_reader();
    RETURN_IF_ERROR(reader.read_batch(rows, &_leaf_batch, rows_read));

    _null_map.clear();
    RETURN_IF_ERROR(reader.build_null_map(_leaf_batch, *rows_read, &_null_map));
    const auto value_kind = decoded_value_kind(_type_descriptor);
    const bool is_binary_value =
            value_kind == DecodedValueKind::BINARY || value_kind == DecodedValueKind::FIXED_BINARY;
    if (!is_binary_value && _leaf_batch.read_dense_for_nullable() && !_null_map.empty()) {
        const int64_t non_null_count = static_cast<int64_t>(simd::count_zero_num(
                reinterpret_cast<const int8_t*>(_null_map.data()), _null_map.size()));
        const int64_t null_count = *rows_read - non_null_count;
        if (_leaf_batch.values_written() != non_null_count) {
            return Status::Corruption(
                    "Invalid dense nullable parquet record read result for column {}: values={}, "
                    "records={}, nulls={}",
                    _name, _leaf_batch.values_written(), *rows_read, null_count);
        }
    } else if (!is_binary_value && !_leaf_batch.read_dense_for_nullable() &&
               _leaf_batch.values_written() != *rows_read) {
        return Status::Corruption(
                "Invalid parquet record read result for column {}: values={}, records={}", _name,
                _leaf_batch.values_written(), *rows_read);
    }

    RETURN_IF_ERROR(reader.append_values(_leaf_batch, *rows_read, &_null_map, column));
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
    SCOPED_TIMER(_profile.arrow_skip_records_time);
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

Status ScalarColumnReader::select_with_dictionary_filter(const SelectionVector& sel,
                                                         uint16_t selected_rows, int64_t batch_rows,
                                                         const IColumn::Filter& dictionary_filter,
                                                         MutableColumnPtr& column,
                                                         IColumn::Filter* row_filter,
                                                         bool* used_filter) {
    DORIS_CHECK(column.get() != nullptr);
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    RETURN_IF_ERROR(sel.verify(selected_rows, batch_rows));
    *used_filter = false;
    row_filter->clear();
    row_filter->reserve(selected_rows);
    DORIS_CHECK(_record_reader != nullptr);
    // A clean fallback is possible only before any range skip or read advances the record reader.
    // Once dictionary selection starts, losing dictionary output is corruption rather than a
    // reason to retry through the ordinary selected-read path with an already advanced stream.
    if (!_record_reader->read_dictionary()) {
        return Status::OK();
    }
    *used_filter = true;

    selection_to_ranges(sel, selected_rows, &_selection_ranges);
    int64_t cursor = 0;
    for (const auto& range : _selection_ranges) {
        if (range.start < cursor || range.start + range.length > batch_rows) {
            return Status::InvalidArgument(
                    "Invalid parquet dictionary selection range [{}, {}) for column {}",
                    range.start, range.start + range.length, _name);
        }
        RETURN_IF_ERROR(skip(range.start - cursor));

        int64_t range_rows_read = 0;
        RETURN_IF_ERROR(read_range_with_dictionary_filter(range.length, dictionary_filter, column,
                                                          row_filter, &range_rows_read,
                                                          used_filter));
        if (!*used_filter) {
            return Status::OK();
        }
        if (range_rows_read != range.length) {
            return Status::Corruption(
                    "Parquet dictionary selected read returned {} rows, expected {} rows for "
                    "column {}",
                    range_rows_read, range.length, _name);
        }
        cursor = range.start + range.length;
    }
    RETURN_IF_ERROR(skip(batch_rows - cursor));
    if (_profile.reader_select_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_select_rows, selected_rows);
    }
    return Status::OK();
}

Status ScalarColumnReader::read_range_with_dictionary_filter(
        int64_t rows, const IColumn::Filter& dictionary_filter, MutableColumnPtr& column,
        IColumn::Filter* row_filter, int64_t* rows_read, bool* used_filter) {
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    DORIS_CHECK(_record_reader != nullptr);
    if (!_record_reader->read_dictionary()) {
        return Status::Corruption(
                "Parquet dictionary reader became unavailable after selected reading started for "
                "column {}",
                _name);
    }

    RETURN_IF_ERROR(leaf_reader().read_batch(rows, &_leaf_batch, rows_read));
    int64_t matched_rows = 0;
    RETURN_IF_ERROR(append_dictionary_filtered_values(_leaf_batch.binary_chunks(),
                                                      dictionary_filter, column, row_filter,
                                                      &matched_rows, used_filter));
    if (!*used_filter) {
        return Status::Corruption(
                "Parquet dictionary reader did not return dictionary batches for column {}", _name);
    }
    if (row_filter->size() < static_cast<size_t>(*rows_read)) {
        return Status::Corruption(
                "Parquet dictionary filter produced too few row decisions for column {}: "
                "filter={}, rows={}",
                _name, row_filter->size(), *rows_read);
    }
    advance_rows_read(*rows_read);
    update_reader_read_rows(*rows_read);
    return Status::OK();
}

Status ScalarColumnReader::append_dictionary_filtered_values(
        const std::vector<std::shared_ptr<::arrow::Array>>& chunks,
        const IColumn::Filter& dictionary_filter, MutableColumnPtr& column,
        IColumn::Filter* row_filter, int64_t* matched_rows, bool* used_filter) {
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(matched_rows != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    *matched_rows = 0;
    *used_filter = false;

    _dictionary_binary_values.clear();
    for (const auto& chunk : chunks) {
        DORIS_CHECK(chunk != nullptr);
        const auto* dict_array = dynamic_cast<const ::arrow::DictionaryArray*>(chunk.get());
        if (dict_array == nullptr) {
            // The caller has already consumed rows from a DictionaryRecordReader. Falling back to a
            // normal selected read would desynchronize the Parquet stream, so absence of a
            // DictionaryArray is reported as corruption by read_range_with_dictionary_filter().
            return Status::OK();
        }
        *used_filter = true;
        const auto& dictionary = dict_array->dictionary();
        if (dictionary == nullptr) {
            return Status::Corruption("Parquet dictionary array has null dictionary for column {}",
                                      _name);
        }

        // Dictionary predicates are evaluated once against the dictionary page and produce a
        // dictionary-entry bitmap. DATA_PAGE rows then only need an integer-index lookup. NULL rows
        // do not have a dictionary entry and cannot satisfy the supported equality/IN predicates.
        for (int64_t row = 0; row < dict_array->length(); ++row) {
            bool keep = false;
            if (!dict_array->IsNull(row)) {
                const int64_t dictionary_index = dict_array->GetValueIndex(row);
                if (dictionary_index < 0 || dictionary_index >= dictionary->length() ||
                    dictionary_index >= static_cast<int64_t>(dictionary_filter.size())) {
                    return Status::Corruption(
                            "Invalid parquet dictionary index {} for column {}: dictionary={}, "
                            "filter={}",
                            dictionary_index, _name, dictionary->length(),
                            dictionary_filter.size());
                }
                keep = dictionary_filter[static_cast<size_t>(dictionary_index)] != 0;
                if (keep) {
                    RETURN_IF_ERROR(append_arrow_binary_dictionary_value(
                            _name, *dictionary, dictionary_index, &_dictionary_binary_values));
                    ++*matched_rows;
                }
            }
            row_filter->push_back(keep ? 1 : 0);
        }
    }

    if (!*used_filter) {
        return Status::OK();
    }
    auto status = append_decoded_binary_values(_dictionary_binary_values, column);
    // StringRef does not own the Arrow dictionary bytes. Drop the logical references immediately
    // after synchronous materialization while keeping vector capacity for the next batch.
    _dictionary_binary_values.clear();
    return status;
}

Status ScalarColumnReader::append_decoded_binary_values(const std::vector<StringRef>& values,
                                                        MutableColumnPtr& column) const {
    DecodedColumnView view;
    view.value_kind = decoded_value_kind(_type_descriptor);
    view.row_count = static_cast<int64_t>(values.size());
    view.logical_integer_bit_width = _type_descriptor.integer_bit_width;
    view.logical_integer_is_signed = !_type_descriptor.is_unsigned_integer;
    view.fixed_length = _type_descriptor.fixed_length;
    view.binary_values = &values;

    SCOPED_TIMER(_profile.materialization_time);
    if (!_type->is_nullable()) {
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
            nullable_column != nullptr) {
            auto& nested_column = nullable_column->get_nested_column();
            auto& null_map = nullable_column->get_null_map_data();
            const auto old_nested_size = nested_column.size();
            const auto old_null_map_size = null_map.size();
            auto st = _type->get_serde()->read_column_from_decoded_values(nested_column, view);
            if (!st.ok()) {
                nested_column.resize(old_nested_size);
                return st;
            }
            null_map.resize(old_null_map_size + nested_column.size() - old_nested_size);
            memset(null_map.data() + old_null_map_size, 0, null_map.size() - old_null_map_size);
            return Status::OK();
        }
        return _type->get_serde()->read_column_from_decoded_values(*column, view);
    }

    NullMap null_map(values.size(), 0);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    return _type->get_serde()->read_column_from_decoded_values(*column, view);
}

// The value index stream must advance on those null slots, otherwise later payload values shift.
Status ScalarColumnReader::load_nested_batch(int64_t rows) {
    DORIS_CHECK(_nested_batch != nullptr);
    reset_nested_build_level_cursor();
    const int16_t materialized_slot_definition_level =
            static_cast<int16_t>(_definition_level - (_type->is_nullable() ? 1 : 0));
    RETURN_IF_ERROR(leaf_reader().read_nested_batch(rows, materialized_slot_definition_level,
                                                    _nested_batch.get(), _repetition_level));
    advance_rows_read(_nested_batch->records_read);
    update_reader_read_rows(_nested_batch->records_read);
    return Status::OK();
}

Status ScalarColumnReader::load_nested_levels_batch(int64_t rows) {
    DORIS_CHECK(_nested_batch != nullptr);
    reset_nested_build_level_cursor();
    RETURN_IF_ERROR(leaf_reader().read_nested_levels_batch(rows, _nested_batch.get()));
    advance_rows_read(_nested_batch->records_read);
    update_reader_read_rows(_nested_batch->records_read);
    return Status::OK();
}

Status ScalarColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                               int64_t* values_read) {
    if (column.get() == nullptr || values_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet nested scalar build result for column {}",
                                       _name);
    }
    DORIS_CHECK(_nested_batch != nullptr);
    ParquetNestedScalarValueCursor value_cursor(_nested_batch.get());
    // The levels-only loader intentionally does not populate value-slot metadata or payload
    // buffers. Derive the logical slot threshold from the schema, exactly as load_nested_batch()
    // does, so this consumer works for both loaded batch forms.
    const int16_t materialized_slot_definition_level =
            static_cast<int16_t>(_definition_level - (_type->is_nullable() ? 1 : 0));
    *values_read = 0;
    int64_t level_idx = nested_build_level_cursor();
    while (level_idx < _nested_batch->levels_written && *values_read < length_upper_bound) {
        const int64_t current_level_idx = level_idx;
        const int16_t def_level = _nested_batch->def_levels[current_level_idx];
        const int16_t rep_level = _nested_batch->rep_levels[current_level_idx];
        ++level_idx;
        if (def_level < materialized_slot_definition_level || rep_level > _repetition_level) {
            continue;
        }
        if (def_level == _definition_level) {
            RETURN_IF_ERROR(append_scalar_batch_value(*this, *_nested_batch, current_level_idx,
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
    set_nested_build_level_cursor(level_idx);
    return Status::OK();
}

Status ScalarColumnReader::consume_nested_column(int64_t length_upper_bound,
                                                 int64_t* values_consumed) {
    if (values_consumed == nullptr) {
        return Status::InvalidArgument("Invalid parquet nested scalar consume result for column {}",
                                       _name);
    }
    DORIS_CHECK(_nested_batch != nullptr);
    // A levels-only batch intentionally has no value-slot metadata. Reconstruct the same logical
    // slot threshold used by load_nested_batch(): a nullable leaf owns a slot at one definition
    // level below a non-null value, while a required leaf owns a slot only at its full definition
    // level. For example, an empty ARRAY<STRING> boundary must not be consumed as a STRING value.
    const int16_t materialized_slot_definition_level =
            static_cast<int16_t>(_definition_level - (_type->is_nullable() ? 1 : 0));
    *values_consumed = 0;
    int64_t level_idx = nested_build_level_cursor();
    while (level_idx < _nested_batch->levels_written && *values_consumed < length_upper_bound) {
        const int64_t current_level_idx = level_idx;
        const int16_t def_level = _nested_batch->def_levels[current_level_idx];
        const int16_t rep_level = _nested_batch->rep_levels[current_level_idx];
        ++level_idx;
        if (def_level < materialized_slot_definition_level || rep_level > _repetition_level) {
            continue;
        }
        RETURN_IF_ERROR(validate_nested_value(current_level_idx, false));
        ++*values_consumed;
    }
    set_nested_build_level_cursor(level_idx);
    return Status::OK();
}

Status ScalarColumnReader::append_nested_value(int64_t level_idx, MutableColumnPtr& column) const {
    if (column.get() == nullptr) {
        return Status::InvalidArgument("Invalid parquet nested scalar append result for column {}",
                                       _name);
    }
    DORIS_CHECK(_nested_batch != nullptr);
    DORIS_CHECK(level_idx >= 0);
    DORIS_CHECK(level_idx < _nested_batch->levels_written);
    ParquetNestedScalarValueCursor value_cursor(_nested_batch.get());
    const int16_t def_level = _nested_batch->def_levels[level_idx];
    if (def_level == _definition_level) {
        return append_scalar_batch_value(*this, *_nested_batch, level_idx, &value_cursor, column);
    }
    if (!_type->is_nullable()) {
        return Status::Corruption("Parquet MAP column {} contains null for non-nullable value",
                                  _name);
    }
    column->insert_default();
    return Status::OK();
}

Status ScalarColumnReader::validate_nested_value(int64_t level_idx, bool require_non_null) const {
    DORIS_CHECK(_nested_batch != nullptr);
    DORIS_CHECK(level_idx >= 0);
    DORIS_CHECK(level_idx < _nested_batch->levels_written);
    const int16_t def_level = _nested_batch->def_levels[level_idx];
    if (def_level == _definition_level) {
        return Status::OK();
    }
    if (require_non_null || !_type->is_nullable()) {
        return Status::Corruption("Parquet scalar column {} contains null for non-nullable field",
                                  _name);
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

} // namespace doris::format::parquet
