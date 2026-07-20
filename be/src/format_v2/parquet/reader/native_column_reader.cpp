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

#include "format_v2/parquet/reader/native_column_reader.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <ranges>
#include <string>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "runtime/runtime_state.h"

namespace doris::format::parquet {
namespace {

constexpr size_t MAX_RETAINED_BATCH_SCRATCH_BYTES = 4UL << 20;

DataTypePtr projected_type(const ParquetColumnSchema& schema,
                           const format::LocalColumnIndex* projection) {
    if (!format::is_partial_projection(projection)) {
        return schema.type;
    }
    switch (schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return schema.type;
    case ParquetColumnSchemaKind::STRUCT: {
        DataTypes child_types;
        Strings child_names;
        child_types.reserve(projection->children.size());
        child_names.reserve(projection->children.size());
        for (const auto& child_projection : projection->children) {
            const auto child_it = std::ranges::find_if(schema.children, [&](const auto& child) {
                return child->local_id == child_projection.local_id();
            });
            DORIS_CHECK(child_it != schema.children.end());
            child_types.push_back(make_nullable(projected_type(**child_it, &child_projection)));
            child_names.push_back((*child_it)->name);
        }
        DataTypePtr type = std::make_shared<DataTypeStruct>(child_types, child_names);
        return schema.type->is_nullable() ? make_nullable(type) : type;
    }
    case ParquetColumnSchemaKind::LIST: {
        DORIS_CHECK(schema.children.size() == 1);
        const auto* child_projection =
                format::find_child_projection(projection, schema.children[0]->local_id);
        DORIS_CHECK(child_projection != nullptr);
        DataTypePtr type = std::make_shared<DataTypeArray>(
                projected_type(*schema.children[0], child_projection));
        return schema.type->is_nullable() ? make_nullable(type) : type;
    }
    case ParquetColumnSchemaKind::MAP: {
        DORIS_CHECK(schema.children.size() == 2);
        const auto* value_projection =
                format::find_child_projection(projection, schema.children[1]->local_id);
        DORIS_CHECK(value_projection != nullptr);
        DataTypePtr type = std::make_shared<DataTypeMap>(
                make_nullable(schema.children[0]->type),
                make_nullable(projected_type(*schema.children[1], value_projection)));
        return schema.type->is_nullable() ? make_nullable(type) : type;
    }
    }
    DORIS_CHECK(false);
    return nullptr;
}

const NativeFieldSchema* find_child_field(const NativeFieldSchema& parent,
                                          const ParquetColumnSchema& child) {
    auto field_it = std::ranges::find_if(parent.children, [&](const NativeFieldSchema& field) {
        return (child.parquet_field_id >= 0 && field.field_id == child.parquet_field_id) ||
               field.name == child.name;
    });
    return field_it == parent.children.end() ? nullptr : &*field_it;
}

void collect_physical_subtree_ids(const NativeFieldSchema& field, std::set<uint64_t>* ids) {
    DORIS_CHECK(ids != nullptr);
    ids->insert(field.get_column_id());
    for (const auto& child : field.children) {
        collect_physical_subtree_ids(child, ids);
    }
}

void collect_projected_ids(const ParquetColumnSchema& schema,
                           const format::LocalColumnIndex* projection,
                           const NativeFieldSchema& native_field, std::set<uint64_t>* ids) {
    DORIS_CHECK(ids != nullptr);
    if (!format::is_partial_projection(projection)) {
        return;
    }
    for (const auto& child_projection : projection->children) {
        const auto schema_it = std::ranges::find_if(schema.children, [&](const auto& child) {
            return child->local_id == child_projection.local_id();
        });
        DORIS_CHECK(schema_it != schema.children.end());
        const NativeFieldSchema* child_field = find_child_field(native_field, **schema_it);
        DORIS_CHECK(child_field != nullptr);
        if (format::is_full_projection(&child_projection)) {
            // A full child path is a request for its complete physical subtree. Keeping only the
            // child group id makes its grandchildren SkipReadingReaders and silently defaults
            // STRUCT fields (or breaks ARRAY/MAP shape invariants).
            collect_physical_subtree_ids(*child_field, ids);
        } else {
            ids->insert(child_field->get_column_id());
            collect_projected_ids(**schema_it, &child_projection, *child_field, ids);
        }
    }
    if (schema.kind == ParquetColumnSchemaKind::MAP) {
        DORIS_CHECK(!native_field.children.empty());
        // MAP entry existence and offsets are owned by the key stream even for value-only
        // projections. Keep the key reader live so the native complex reader can validate
        // key/value entry alignment while constructing offsets.
        ids->insert(native_field.children[0].get_column_id());
    }
}

Status append_non_null_dictionary_values(MutableColumnPtr& target, MutableColumnPtr values) {
    DORIS_CHECK(target);
    DORIS_CHECK(values);
    const size_t value_count = values->size();
    if (auto* nullable = check_and_get_column<ColumnNullable>(*target); nullable != nullptr) {
        nullable->get_nested_column().insert_range_from(*values, 0, value_count);
        auto& null_map = nullable->get_null_map_data();
        null_map.resize_fill(null_map.size() + value_count, 0);
        return Status::OK();
    }
    target->insert_range_from(*values, 0, value_count);
    return Status::OK();
}

} // namespace

NativeColumnReader::NativeColumnReader(const ParquetColumnSchema& schema,
                                       DataTypePtr projected_type,
                                       ParquetColumnReaderProfile profile)
        : ParquetColumnReader(schema, std::move(projected_type), profile),
          _nested(schema.kind != ParquetColumnSchemaKind::PRIMITIVE) {}

NativeColumnReader::~NativeColumnReader() {
    (void)sync_native_profile();
}

Status NativeColumnReader::create(
        const ParquetColumnSchema& column_schema, const format::LocalColumnIndex* projection,
        io::FileReaderSPtr file, const NativeParquetMetadata* metadata, int row_group_id,
        const std::vector<RowRange>& selected_ranges,
        const std::unordered_map<int, tparquet::OffsetIndex>& offset_indexes,
        const cctz::time_zone* timezone, io::IOContext* io_ctx, RuntimeState* runtime_state,
        bool enable_page_cache, const std::string& page_cache_file_key,
        bool enable_dictionary_filter, ParquetColumnReaderProfile profile,
        std::unique_ptr<ParquetColumnReader>* reader) {
    if (reader == nullptr) {
        return Status::InvalidArgument("Native parquet reader result is null");
    }
    if (file == nullptr || metadata == nullptr) {
        return Status::InvalidArgument("Native parquet file context is not initialized");
    }
    if (row_group_id < 0 ||
        row_group_id >= static_cast<int>(metadata->to_thrift().row_groups.size())) {
        return Status::InvalidArgument("Invalid native parquet row group {}", row_group_id);
    }
    const auto& native_schema = metadata->schema();
    if (column_schema.local_id < 0 || column_schema.local_id >= native_schema.size()) {
        return Status::InvalidArgument("Invalid native parquet top-level column id {} for {}",
                                       column_schema.local_id, column_schema.name);
    }
    auto* field = const_cast<NativeFieldSchema*>(native_schema.get_column(column_schema.local_id));
    DORIS_CHECK(field != nullptr);
    if (field->name != column_schema.name &&
        !(field->field_id >= 0 && field->field_id == column_schema.parquet_field_id)) {
        return Status::Corruption(
                "Native/metadata parquet schema mismatch at column {}: native={}, arrow={}",
                column_schema.local_id, field->name, column_schema.name);
    }

    auto type = projected_type(column_schema, projection);
    std::shared_ptr<NativeSchemaNode> schema_node;
    RETURN_IF_ERROR(build_native_schema_node(type, column_schema, &schema_node));
    std::set<uint64_t> projected_ids;
    collect_projected_ids(column_schema, projection, *field, &projected_ids);

    auto native_reader = std::unique_ptr<NativeColumnReader>(
            new NativeColumnReader(column_schema, std::move(type), profile));
    RETURN_IF_ERROR(native_reader->init(
            std::move(file), metadata, row_group_id, field, std::move(schema_node),
            std::move(projected_ids), selected_ranges, offset_indexes, timezone, io_ctx,
            runtime_state, enable_page_cache, page_cache_file_key, enable_dictionary_filter));
    *reader = std::move(native_reader);
    return Status::OK();
}

Status NativeColumnReader::init(
        io::FileReaderSPtr file, const NativeParquetMetadata* metadata, int row_group_id,
        NativeFieldSchema* field, std::shared_ptr<NativeSchemaNode> schema_node,
        std::set<uint64_t> projected_column_ids, const std::vector<RowRange>& selected_ranges,
        const std::unordered_map<int, tparquet::OffsetIndex>& offset_indexes,
        const cctz::time_zone* timezone, io::IOContext* io_ctx, RuntimeState* runtime_state,
        bool enable_page_cache, const std::string& page_cache_file_key,
        bool enable_dictionary_filter) {
    DORIS_CHECK(file != nullptr);
    DORIS_CHECK(metadata != nullptr);
    DORIS_CHECK(field != nullptr);
    DORIS_CHECK(schema_node != nullptr);
    const auto& row_group = metadata->to_thrift().row_groups[row_group_id];
    DORIS_CHECK(row_group.num_rows > 0);
    _row_group_rows = row_group.num_rows;
    _selected_ranges = selected_ranges;
    DORIS_CHECK(!_selected_ranges.empty());
    for (const auto& range : _selected_ranges) {
        DORIS_CHECK(range.start >= 0);
        DORIS_CHECK(range.length > 0);
        DORIS_CHECK(range.start + range.length <= _row_group_rows);
        _row_ranges.add(segment_v2::RowRange(range.start, range.start + range.length));
    }
    // Offset indexes are immutable row-group metadata owned by ParquetScanScheduler. Sharing them
    // avoids retaining the full N-column page-location map once per projected reader.
    _offset_indexes = &offset_indexes;
    _schema_node = std::move(schema_node);
    _projected_column_ids = std::move(projected_column_ids);
    _dictionary_filter_enabled = enable_dictionary_filter;

    const size_t max_group_buffer = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t max_column_buffer = config::parquet_column_max_buffer_mb << 20;
    const size_t max_buffer_size = std::min(max_group_buffer, max_column_buffer);
    RuntimeState* native_runtime_state = runtime_state;
    const bool runtime_page_cache_enabled =
            runtime_state == nullptr ||
            runtime_state->query_options().enable_parquet_file_page_cache;
    if (runtime_page_cache_enabled != enable_page_cache) {
        TQueryOptions query_options =
                runtime_state == nullptr ? TQueryOptions() : runtime_state->query_options();
        query_options.__set_enable_parquet_file_page_cache(enable_page_cache);
        _page_cache_runtime_state = RuntimeState::create_unique(query_options, TQueryGlobals());
        native_runtime_state = _page_cache_runtime_state.get();
    }
    const auto& thrift_metadata = metadata->to_thrift();
    const auto compat = native::parquet_reader_compat(
            thrift_metadata.__isset.created_by ? thrift_metadata.created_by : "");
    RETURN_IF_ERROR(native::ColumnReader::create(
            std::move(file), field, row_group, _row_ranges, timezone, io_ctx, _native_reader,
            max_buffer_size, *_offset_indexes, native_runtime_state, false, _projected_column_ids,
            _filter_column_ids, page_cache_file_key, compat,
            runtime_state != nullptr && runtime_state->enable_strict_mode()));
    DORIS_CHECK(_native_reader != nullptr);
    _skip_column = _type->create_column();
    return Status::OK();
}

Status NativeColumnReader::read_with_filter(int64_t rows, const uint8_t* filter_data,
                                            bool filter_all, MutableColumnPtr& column,
                                            const DataTypePtr& output_type, bool dictionary_ids,
                                            int64_t* rows_read) {
    DORIS_CHECK(rows >= 0);
    DORIS_CHECK(column);
    DORIS_CHECK(output_type != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    *rows_read = 0;
    if (rows == 0) {
        return Status::OK();
    }

    native::FilterMap filter;
    RETURN_IF_ERROR(filter.init(filter_data, static_cast<size_t>(rows), filter_all));
    _native_reader->reset_filter_map_index();
    ColumnPtr native_column(std::move(column));
    bool eof = false;
    int64_t native_calls = 0;
    int64_t consecutive_empty_calls = 0;
    while (*rows_read < rows && !eof) {
        ++native_calls;
        size_t loop_rows = 0;
        RETURN_IF_ERROR(_native_reader->read_column_data(
                native_column, output_type, _schema_node, filter,
                static_cast<size_t>(rows - *rows_read), &loop_rows, &eof, dictionary_ids));
        if (loop_rows == 0 && !eof) {
            // A selected RowRanges plan may reject the current data page completely. V1 advances
            // the page cursor and deliberately returns zero rows so the caller can request the
            // next page. Bound consecutive empty transitions by the Row Group row count to retain
            // a deterministic corruption exit if a decoder ever stops advancing.
            if (++consecutive_empty_calls > _row_group_rows + 1) {
                column = IColumn::mutate(std::move(native_column));
                return Status::Corruption("Native parquet reader made no progress for column {}",
                                          _name);
            }
            continue;
        }
        consecutive_empty_calls = 0;
        *rows_read += static_cast<int64_t>(loop_rows);
    }
    column = IColumn::mutate(std::move(native_column));
    if (_profile.native_read_calls != nullptr) {
        COUNTER_UPDATE(_profile.native_read_calls, native_calls);
    }
    if (_nested && _profile.nested_batches != nullptr) {
        COUNTER_UPDATE(_profile.nested_batches, 1);
    }
    // Retained-capacity inspection walks the native reader tree. Check it periodically instead of
    // on every small batch; row-group destruction is still the hard lifetime bound for scratch.
    constexpr size_t SCRATCH_CHECK_BATCH_INTERVAL = 16;
    if (++_batches_since_scratch_check >= SCRATCH_CHECK_BATCH_INTERVAL) {
        _native_reader->release_batch_scratch(MAX_RETAINED_BATCH_SCRATCH_BYTES);
        _batches_since_scratch_check = 0;
    }
    if (*rows_read != rows) {
        return Status::Corruption("Native parquet reader returned {} rows, expected {} for {}",
                                  *rows_read, rows, _name);
    }
    return Status::OK();
}

Status NativeColumnReader::read_with_plain_filter(int64_t rows, const uint8_t* filter_data,
                                                  bool filter_all, const VExprSPtrs& conjuncts,
                                                  int column_id, IColumn::Filter* row_filter,
                                                  int64_t* rows_read, bool* used_filter) {
    DORIS_CHECK(rows >= 0);
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    row_filter->clear();
    *rows_read = 0;
    *used_filter = false;
    if (rows == 0) {
        return Status::OK();
    }

    native::FilterMap filter;
    RETURN_IF_ERROR(filter.init(filter_data, static_cast<size_t>(rows), filter_all));
    _native_reader->reset_filter_map_index();
    bool eof = false;
    int64_t consecutive_empty_calls = 0;
    while (*rows_read < rows && !eof) {
        size_t loop_rows = 0;
        IColumn::Filter loop_filter;
        bool loop_used = false;
        RETURN_IF_ERROR(_native_reader->read_plain_filter(
                conjuncts, column_id, filter, static_cast<size_t>(rows - *rows_read), &loop_filter,
                &loop_rows, &eof, &loop_used));
        if (!loop_used) {
            if (UNLIKELY(*rows_read != 0)) {
                // Footer encoding lists are untrusted. Once a prior page advanced the cursor, a
                // typed fallback would restart the request at the wrong row, so reject the file
                // instead of terminating the BE or returning shifted results.
                return Status::Corruption(
                        "Parquet PLAIN predicate encoding changed after {} rows for column {}",
                        *rows_read, _name);
            }
            row_filter->clear();
            return Status::OK();
        }
        row_filter->insert(row_filter->end(), loop_filter.begin(), loop_filter.end());
        if (loop_rows == 0 && !eof) {
            if (++consecutive_empty_calls > _row_group_rows + 1) {
                return Status::Corruption(
                        "Native parquet PLAIN predicate made no progress for column {}", _name);
            }
            continue;
        }
        consecutive_empty_calls = 0;
        *rows_read += static_cast<int64_t>(loop_rows);
    }
    if (*rows_read != rows) {
        return Status::Corruption(
                "Native parquet PLAIN predicate returned {} rows, expected {} for {}", *rows_read,
                rows, _name);
    }
    *used_filter = true;
    return Status::OK();
}

Status NativeColumnReader::validate_selected_span(int64_t rows) {
    DORIS_CHECK(rows >= 0);
    while (_selected_range_idx < _selected_ranges.size()) {
        const auto& range = _selected_ranges[_selected_range_idx];
        const int64_t range_end = range.start + range.length;
        if (_logical_row_position < range_end) {
            break;
        }
        ++_selected_range_idx;
    }
    if (_selected_range_idx >= _selected_ranges.size()) {
        return Status::Corruption("Native parquet read past selected ranges for column {}", _name);
    }
    const auto& range = _selected_ranges[_selected_range_idx];
    if (_logical_row_position < range.start ||
        rows > range.start + range.length - _logical_row_position) {
        return Status::Corruption(
                "Native parquet read [{}, {}) crosses selected range [{}, {}) for column {}",
                _logical_row_position, _logical_row_position + rows, range.start,
                range.start + range.length, _name);
    }
    return Status::OK();
}

void NativeColumnReader::advance_selected_span(int64_t rows) {
    _logical_row_position += rows;
    while (_selected_range_idx < _selected_ranges.size() &&
           _logical_row_position >= _selected_ranges[_selected_range_idx].start +
                                            _selected_ranges[_selected_range_idx].length) {
        ++_selected_range_idx;
    }
}

Status NativeColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    RETURN_IF_ERROR(validate_selected_span(rows));
    RETURN_IF_ERROR(read_with_filter(rows, nullptr, false, column, _type, false, rows_read));
    advance_selected_span(*rows_read);
    update_reader_read_rows(*rows_read);
    return Status::OK();
}

Status NativeColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    DORIS_CHECK(_logical_row_position <= _row_group_rows - rows);
    int64_t remaining = rows;
    int64_t native_skipped_rows = 0;
    while (remaining > 0) {
        while (_selected_range_idx < _selected_ranges.size() &&
               _logical_row_position >= _selected_ranges[_selected_range_idx].start +
                                                _selected_ranges[_selected_range_idx].length) {
            ++_selected_range_idx;
        }
        if (_selected_range_idx >= _selected_ranges.size()) {
            _logical_row_position += remaining;
            break;
        }
        const auto& range = _selected_ranges[_selected_range_idx];
        if (_logical_row_position < range.start) {
            const int64_t gap = std::min(remaining, range.start - _logical_row_position);
            _logical_row_position += gap;
            remaining -= gap;
            continue;
        }
        const int64_t selected_rows = detail::bounded_native_lazy_skip_rows(
                std::min(remaining, range.start + range.length - _logical_row_position));
        _skip_column->clear();
        // Pending skips can span many filtered batches and are replayed for every lazy column.
        // Chunking here bounds each dense bitmap while preserving one logical scheduler skip.
        _filter_scratch.assign(static_cast<size_t>(selected_rows), 0);
        int64_t rows_read = 0;
        RETURN_IF_ERROR(read_with_filter(selected_rows, _filter_scratch.data(), true, _skip_column,
                                         _type, false, &rows_read));
        DORIS_CHECK(_skip_column->empty());
        DORIS_CHECK(rows_read == selected_rows);
        _logical_row_position += rows_read;
        native_skipped_rows += rows_read;
        remaining -= rows_read;
    }
    update_reader_skip_rows(native_skipped_rows);
    return Status::OK();
}

Status NativeColumnReader::select(const SelectionVector& selection, uint16_t selected_rows,
                                  int64_t batch_rows, MutableColumnPtr& column) {
    RETURN_IF_ERROR(validate_selected_span(batch_rows));
    const uint8_t* filter_data = nullptr;
    RETURN_IF_ERROR(selection.materialize_filter(selected_rows, batch_rows, &filter_data));
    const size_t old_size = column->size();
    int64_t rows_read = 0;
    RETURN_IF_ERROR(read_with_filter(batch_rows, filter_data, selected_rows == 0, column, _type,
                                     false, &rows_read));
    advance_selected_span(rows_read);
    if (column->size() != old_size + selected_rows) {
        return Status::Corruption(
                "Native parquet selection appended {} rows, expected {} for column {}",
                column->size() - old_size, selected_rows, _name);
    }
    if (_profile.reader_select_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_select_rows, selected_rows);
    }
    update_reader_read_rows(selected_rows);
    update_reader_skip_rows(batch_rows - selected_rows);
    return Status::OK();
}

Status NativeColumnReader::select_with_dictionary_filter(const SelectionVector& selection,
                                                         uint16_t selected_rows, int64_t batch_rows,
                                                         const IColumn::Filter& dictionary_filter,
                                                         MutableColumnPtr& column,
                                                         IColumn::Filter* row_filter,
                                                         bool* used_filter) {
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    RETURN_IF_ERROR(validate_selected_span(batch_rows));
    *used_filter = false;
    row_filter->clear();
    if (!_dictionary_filter_enabled) {
        return Status::OK();
    }
    *used_filter = true;

    const uint8_t* filter_data = nullptr;
    RETURN_IF_ERROR(selection.materialize_filter(selected_rows, batch_rows, &filter_data));
    const bool nullable = _type->is_nullable();
    DataTypePtr id_type = std::make_shared<DataTypeInt32>();
    if (nullable) {
        id_type = make_nullable(id_type);
    }
    if (!_dictionary_id_column) {
        _dictionary_id_column = id_type->create_column();
    }
    _dictionary_id_column->clear();
    int64_t rows_read = 0;
    RETURN_IF_ERROR(read_with_filter(batch_rows, filter_data, selected_rows == 0,
                                     _dictionary_id_column, id_type, true, &rows_read));
    advance_selected_span(rows_read);
    if (_dictionary_id_column->size() != selected_rows) {
        return Status::Corruption(
                "Native parquet dictionary reader appended {} rows, expected {} for {}",
                _dictionary_id_column->size(), selected_rows, _name);
    }

    const ColumnInt32* ids = nullptr;
    const NullMap* null_map = nullptr;
    if (const auto* nullable_ids = check_and_get_column<ColumnNullable>(*_dictionary_id_column);
        nullable_ids != nullptr) {
        ids = check_and_get_column<ColumnInt32>(nullable_ids->get_nested_column());
        null_map = &nullable_ids->get_null_map_data();
    } else {
        ids = check_and_get_column<ColumnInt32>(*_dictionary_id_column);
    }
    DORIS_CHECK(ids != nullptr);

    if (!_matched_dictionary_ids) {
        _matched_dictionary_ids = ColumnInt32::create();
    }
    _matched_dictionary_ids->clear();
    auto& matched_ids = assert_cast<ColumnInt32&>(*_matched_dictionary_ids).get_data();
    row_filter->reserve(selected_rows);
    const auto& id_data = ids->get_data();
    for (size_t row = 0; row < selected_rows; ++row) {
        bool keep = false;
        if (null_map == nullptr || (*null_map)[row] == 0) {
            const int32_t dictionary_id = id_data[row];
            if (dictionary_id < 0 ||
                static_cast<size_t>(dictionary_id) >= dictionary_filter.size()) {
                return Status::Corruption(
                        "Invalid parquet dictionary id {} for column {} with {} entries",
                        dictionary_id, _name, dictionary_filter.size());
            }
            keep = dictionary_filter[static_cast<size_t>(dictionary_id)] != 0;
            if (keep) {
                matched_ids.push_back(dictionary_id);
            }
        }
        row_filter->push_back(keep ? 1 : 0);
    }

    const auto* matched_id_column = check_and_get_column<ColumnInt32>(*_matched_dictionary_ids);
    DORIS_CHECK(matched_id_column != nullptr);
    auto string_values = DORIS_TRY(
            _native_reader->convert_dict_column_to_string_column(matched_id_column, _type));
    RETURN_IF_ERROR(append_non_null_dictionary_values(column, std::move(string_values)));
    if (_profile.reader_select_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_select_rows, selected_rows);
    }
    update_reader_read_rows(cast_set<int64_t>(matched_ids.size()));
    update_reader_skip_rows(batch_rows - cast_set<int64_t>(matched_ids.size()));
    return Status::OK();
}

Status NativeColumnReader::select_with_plain_filter(const SelectionVector& selection,
                                                    uint16_t selected_rows, int64_t batch_rows,
                                                    const VExprSPtrs& conjuncts, int column_id,
                                                    IColumn::Filter* row_filter,
                                                    bool* used_filter) {
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    RETURN_IF_ERROR(validate_selected_span(batch_rows));
    RETURN_IF_ERROR(selection.verify(selected_rows, batch_rows));
    const uint8_t* filter_data = nullptr;
    RETURN_IF_ERROR(selection.materialize_filter(selected_rows, batch_rows, &filter_data));
    int64_t rows_read = 0;
    RETURN_IF_ERROR(read_with_plain_filter(batch_rows, filter_data, selected_rows == 0, conjuncts,
                                           column_id, row_filter, &rows_read, used_filter));
    if (!*used_filter) {
        return Status::OK();
    }
    DORIS_CHECK_EQ(rows_read, batch_rows);
    if (row_filter->size() != selected_rows) {
        return Status::Corruption(
                "Native parquet PLAIN predicate returned {} selected rows, expected {} for {}",
                row_filter->size(), selected_rows, _name);
    }
    advance_selected_span(rows_read);
    update_reader_read_rows(selected_rows);
    update_reader_skip_rows(batch_rows - selected_rows);
    return Status::OK();
}

void NativeColumnReader::flush_profile() {
    record_page_fragments(sync_native_profile());
}

bool NativeColumnReader::crossed_page_since_last_batch() {
    if (_native_reader == nullptr) {
        return false;
    }
    const auto stats = _native_reader->column_statistics();
    bool crossed_page = false;
    if (stats.leaf_page_read_counters.size() == _batch_leaf_page_read_counters.size()) {
        for (size_t leaf = 0; leaf < stats.leaf_page_read_counters.size(); ++leaf) {
            crossed_page |=
                    stats.leaf_page_read_counters[leaf] - _batch_leaf_page_read_counters[leaf] > 1;
        }
    } else {
        // The first snapshot covers the first batch; later snapshots must keep the stable tree shape.
        for (const int64_t page_reads : stats.leaf_page_read_counters) {
            crossed_page |= page_reads > 1;
        }
    }
    _batch_leaf_page_read_counters = stats.leaf_page_read_counters;
    return crossed_page;
}

Result<MutableColumnPtr> NativeColumnReader::dictionary_values() {
    DORIS_CHECK(_native_reader != nullptr);
    return _native_reader->dictionary_values(_type);
}

void NativeColumnReader::record_page_fragments(int64_t page_fragments) {
    if (_profile.native_page_fragments != nullptr) {
        COUNTER_UPDATE(_profile.native_page_fragments, page_fragments);
    }
}

int64_t NativeColumnReader::sync_native_profile() {
    if (_native_reader == nullptr) {
        return 0;
    }
    const auto stats = _native_reader->column_statistics();
    const auto& reported = _reported_native_stats;
    if (_profile.decompress_time != nullptr) {
        COUNTER_UPDATE(_profile.decompress_time, stats.decompress_time - reported.decompress_time);
    }
    if (_profile.decompress_count != nullptr) {
        COUNTER_UPDATE(_profile.decompress_count, stats.decompress_cnt - reported.decompress_cnt);
    }
    if (_profile.decode_header_time != nullptr) {
        COUNTER_UPDATE(_profile.decode_header_time,
                       stats.decode_header_time - reported.decode_header_time);
    }
    if (_profile.decode_value_time != nullptr) {
        COUNTER_UPDATE(_profile.decode_value_time,
                       stats.decode_value_time - reported.decode_value_time);
    }
    if (_profile.decode_dictionary_time != nullptr) {
        COUNTER_UPDATE(_profile.decode_dictionary_time,
                       stats.decode_dict_time - reported.decode_dict_time);
    }
    if (_profile.decode_level_time != nullptr) {
        COUNTER_UPDATE(_profile.decode_level_time,
                       stats.decode_level_time - reported.decode_level_time);
    }
    if (_profile.decode_null_map_time != nullptr) {
        COUNTER_UPDATE(_profile.decode_null_map_time,
                       stats.decode_null_map_time - reported.decode_null_map_time);
    }
    if (_profile.materialization_time != nullptr) {
        COUNTER_UPDATE(_profile.materialization_time,
                       stats.materialization_time - reported.materialization_time);
    }
    if (_profile.hybrid_selection_batches != nullptr) {
        COUNTER_UPDATE(_profile.hybrid_selection_batches,
                       stats.hybrid_selection_batches - reported.hybrid_selection_batches);
    }
    if (_profile.hybrid_selection_ranges != nullptr) {
        COUNTER_UPDATE(_profile.hybrid_selection_ranges,
                       stats.hybrid_selection_ranges - reported.hybrid_selection_ranges);
    }
    if (_profile.hybrid_selection_null_fallback_batches != nullptr) {
        COUNTER_UPDATE(_profile.hybrid_selection_null_fallback_batches,
                       stats.hybrid_selection_null_fallback_batches -
                               reported.hybrid_selection_null_fallback_batches);
    }
    if (_profile.page_index_read_calls != nullptr) {
        COUNTER_UPDATE(_profile.page_index_read_calls,
                       stats.page_index_read_calls - reported.page_index_read_calls);
    }
    if (_profile.skip_page_header_count != nullptr) {
        COUNTER_UPDATE(_profile.skip_page_header_count,
                       stats.skip_page_header_num - reported.skip_page_header_num);
    }
    if (_profile.parse_page_header_count != nullptr) {
        COUNTER_UPDATE(_profile.parse_page_header_count,
                       stats.parse_page_header_num - reported.parse_page_header_num);
    }
    if (_profile.read_page_header_time != nullptr) {
        COUNTER_UPDATE(_profile.read_page_header_time,
                       stats.read_page_header_time - reported.read_page_header_time);
    }
    const int64_t page_read_delta = stats.page_read_counter - reported.page_read_counter;
    if (_profile.page_read_count != nullptr) {
        COUNTER_UPDATE(_profile.page_read_count, page_read_delta);
    }
    if (_profile.page_cache_write_count != nullptr) {
        COUNTER_UPDATE(_profile.page_cache_write_count,
                       stats.page_cache_write_counter - reported.page_cache_write_counter);
    }
    if (_profile.page_cache_compressed_write_count != nullptr) {
        COUNTER_UPDATE(_profile.page_cache_compressed_write_count,
                       stats.page_cache_compressed_write_counter -
                               reported.page_cache_compressed_write_counter);
    }
    if (_profile.page_cache_decompressed_write_count != nullptr) {
        COUNTER_UPDATE(_profile.page_cache_decompressed_write_count,
                       stats.page_cache_decompressed_write_counter -
                               reported.page_cache_decompressed_write_counter);
    }
    if (_profile.page_cache_hit_count != nullptr) {
        COUNTER_UPDATE(_profile.page_cache_hit_count,
                       stats.page_cache_hit_counter - reported.page_cache_hit_counter);
    }
    if (_profile.page_cache_miss_count != nullptr) {
        COUNTER_UPDATE(_profile.page_cache_miss_count,
                       stats.page_cache_missing_counter - reported.page_cache_missing_counter);
    }
    if (_profile.page_cache_compressed_hit_count != nullptr) {
        COUNTER_UPDATE(_profile.page_cache_compressed_hit_count,
                       stats.page_cache_compressed_hit_counter -
                               reported.page_cache_compressed_hit_counter);
    }
    if (_profile.page_cache_decompressed_hit_count != nullptr) {
        COUNTER_UPDATE(_profile.page_cache_decompressed_hit_count,
                       stats.page_cache_decompressed_hit_counter -
                               reported.page_cache_decompressed_hit_counter);
    }
    _reported_native_stats = stats;
    return page_read_delta;
}

} // namespace doris::format::parquet
