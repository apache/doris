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

#include "format_v2/parquet/reader/column_reader.h"

#include <arrow/memory_pool.h>
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>
#include <parquet/level_conversion.h>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/global_rowid_column_reader.h"
#include "format_v2/parquet/reader/list_column_reader.h"
#include "format_v2/parquet/reader/map_column_reader.h"
#include "format_v2/parquet/reader/row_position_column_reader.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"
#include "format_v2/parquet/reader/struct_column_reader.h"
#include "runtime/runtime_profile.h"

namespace doris::format::parquet {
namespace {

class DataPageSkipFilter {
public:
    DataPageSkipFilter(const ParquetPageSkipPlan* page_skip_plan,
                       ParquetPageSkipProfile page_skip_profile)
            : _page_skip_plan(page_skip_plan), _page_skip_profile(page_skip_profile) {
        DORIS_CHECK(_page_skip_plan != nullptr);
    }

    bool operator()(const ::parquet::DataPageStats&) {
        // Arrow invokes this callback once for each DATA_PAGE/DATA_PAGE_V2 and never for
        // dictionary pages, so this ordinal matches Parquet OffsetIndex page locations.
        const size_t page_idx = _next_data_page_idx++;
        const bool skip = _page_skip_plan->should_skip_page(page_idx);
        if (!skip) {
            return false;
        }
        update_skip_profile(page_idx);
        return true;
    }

private:
    void update_skip_profile(size_t page_idx) const {
        if (_page_skip_profile.skipped_pages != nullptr) {
            COUNTER_UPDATE(_page_skip_profile.skipped_pages, 1);
        }
        if (_page_skip_profile.skipped_bytes != nullptr) {
            COUNTER_UPDATE(_page_skip_profile.skipped_bytes,
                           _page_skip_plan->skipped_page_compressed_size(page_idx));
        }
    }

    const ParquetPageSkipPlan* _page_skip_plan = nullptr;
    ParquetPageSkipProfile _page_skip_profile;
    size_t _next_data_page_idx = 0;
};

const ParquetPageSkipPlan* find_page_skip_plan(
        const std::map<int, ParquetPageSkipPlan>* page_skip_plans, int leaf_column_id) {
    if (page_skip_plans == nullptr) {
        return nullptr;
    }
    const auto plan_it = page_skip_plans->find(leaf_column_id);
    return plan_it == page_skip_plans->end() ? nullptr : &plan_it->second;
}

void install_data_page_filter(std::unique_ptr<::parquet::PageReader>& page_reader,
                              const std::map<int, ParquetPageSkipPlan>* page_skip_plans,
                              int leaf_column_id, ParquetPageSkipProfile page_skip_profile) {
    DORIS_CHECK(page_reader != nullptr);
    const ParquetPageSkipPlan* page_skip_plan =
            find_page_skip_plan(page_skip_plans, leaf_column_id);
    if (page_skip_plan == nullptr) {
        return;
    }
    page_reader->set_data_page_filter(DataPageSkipFilter(page_skip_plan, page_skip_profile));
}

bool supports_nested_scalar_record_reader(const ParquetColumnSchema& column_schema) {
    if (column_schema.type_descriptor.supports_record_reader) {
        return true;
    }
    const auto& type_descriptor = column_schema.type_descriptor;
    if ((type_descriptor.extra_type_info != ParquetExtraTypeInfo::NONE &&
         type_descriptor.extra_type_info != ParquetExtraTypeInfo::FLOAT16) ||
        type_descriptor.is_decimal || type_descriptor.is_timestamp ||
        type_descriptor.is_string_like) {
        return false;
    }
    if (type_descriptor.converted_type != ::parquet::ConvertedType::NONE &&
        type_descriptor.converted_type != ::parquet::ConvertedType::UNDEFINED) {
        return false;
    }
    switch (type_descriptor.physical_type) {
    case ::parquet::Type::BOOLEAN:
    case ::parquet::Type::INT32:
    case ::parquet::Type::INT64:
    case ::parquet::Type::FLOAT:
    case ::parquet::Type::DOUBLE:
        return true;
    default:
        return false;
    }
    return true;
}

} // namespace

Status ParquetColumnReader::skip(int64_t rows) {
    return Status::NotSupported("Parquet column skip is not implemented, rows={}", rows);
}

void ParquetColumnReader::advance_nested_build_level_cursor_past_parent(
        int16_t parent_repetition_level) {
    int64_t child_cursor = nested_build_level_cursor();
    const auto& child_rep_levels = nested_repetition_levels();
    const int64_t child_levels_written = nested_levels_written();
    while (child_cursor < child_levels_written) {
        const int16_t child_rep_level = child_rep_levels[child_cursor];
        ++child_cursor;
        if (!is_or_has_repeated_child() || child_rep_level <= parent_repetition_level) {
            break;
        }
    }
    set_nested_build_level_cursor(child_cursor);
}

void ParquetColumnReader::update_reader_read_rows(int64_t rows) const {
    if (_profile.reader_read_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_read_rows, rows);
    }
}

void ParquetColumnReader::update_reader_skip_rows(int64_t rows) const {
    if (_profile.reader_skip_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_skip_rows, rows);
    }
}

Status ParquetColumnReader::select(const SelectionVector& sel, uint16_t selected_rows,
                                   int64_t batch_rows, MutableColumnPtr& column) {
    if (column.get() == nullptr) {
        return Status::InvalidArgument("Parquet selected read result is null for column {}",
                                       name());
    }
    RETURN_IF_ERROR(sel.verify(selected_rows, batch_rows));

    const auto ranges = selection_to_ranges(sel, selected_rows);
    int64_t cursor = 0;
    for (const auto& range : ranges) {
        if (range.start < cursor || range.start + range.length > batch_rows) {
            return Status::InvalidArgument("Invalid parquet selection range [{}, {}) for column {}",
                                           range.start, range.start + range.length, name());
        }
        RETURN_IF_ERROR(skip(range.start - cursor));

        int64_t range_rows_read = 0;
        RETURN_IF_ERROR(read(range.length, column, &range_rows_read));
        if (range_rows_read != range.length) {
            return Status::Corruption(
                    "Parquet selected read returned {} rows, expected {} rows for column {}",
                    range_rows_read, range.length, name());
        }
        cursor = range.start + range.length;
    }
    RETURN_IF_ERROR(skip(batch_rows - cursor));
    if (_profile.reader_select_rows != nullptr) {
        COUNTER_UPDATE(_profile.reader_select_rows, selected_rows);
    }
    return Status::OK();
}

Status ParquetColumnReader::select_with_dictionary_filter(const SelectionVector&, uint16_t, int64_t,
                                                          const IColumn::Filter&, MutableColumnPtr&,
                                                          IColumn::Filter*, bool*) {
    return Status::NotSupported("Parquet dictionary filter is not implemented for column {}",
                                name());
}

ParquetColumnReaderFactory::ParquetColumnReaderFactory(
        std::shared_ptr<::parquet::RowGroupReader> row_group, int num_leaf_columns,
        const std::map<int, ParquetPageSkipPlan>* page_skip_plans,
        ParquetPageSkipProfile page_skip_profile, const cctz::time_zone* timezone,
        bool enable_strict_mode, ParquetColumnReaderProfile column_reader_profile)
        : _row_group(std::move(row_group)),
          _record_readers(static_cast<size_t>(num_leaf_columns)),
          _dictionary_record_readers(static_cast<size_t>(num_leaf_columns)),
          _page_skip_plans(page_skip_plans),
          _page_skip_profile(page_skip_profile),
          _timezone(timezone),
          _enable_strict_mode(enable_strict_mode),
          _column_reader_profile(column_reader_profile) {}

std::unique_ptr<ParquetColumnReader> ParquetColumnReaderFactory::create_row_position_column_reader(
        int64_t row_group_first_row) const {
    return std::make_unique<RowPositionColumnReader>(row_group_first_row, _column_reader_profile);
}

std::unique_ptr<ParquetColumnReader> ParquetColumnReaderFactory::create_global_rowid_column_reader(
        const format::GlobalRowIdContext& context, int64_t row_group_first_row) const {
    return std::make_unique<GlobalRowIdColumnReader>(context, row_group_first_row,
                                                     _column_reader_profile);
}

Status ParquetColumnReaderFactory::make_scalar_column_reader(
        const ParquetColumnSchema& column_schema,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader, bool use_page_skip_plan,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    const auto* page_skip_plan =
            use_page_skip_plan ? find_page_skip_plan(_page_skip_plans, column_schema.leaf_column_id)
                               : nullptr;
    *reader = std::make_unique<ScalarColumnReader>(column_schema, std::move(record_reader),
                                                   page_skip_plan, _timezone, _enable_strict_mode,
                                                   _column_reader_profile);
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_scalar_column_reader(
        const ParquetColumnSchema& column_schema, bool is_nested, bool read_dictionary,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (!column_schema.type_descriptor.unsupported_reason.empty()) {
        return Status::NotSupported("Unsupported parquet column '{}': {}", column_schema.name,
                                    column_schema.type_descriptor.unsupported_reason);
    }
    if (is_nested && column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE) {
        return Status::InvalidArgument("Parquet nested scalar reader requires primitive column {}",
                                       column_schema.name);
    }
    if (column_schema.leaf_column_id < 0 ||
        column_schema.leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       column_schema.leaf_column_id, column_schema.name);
    }
    if (column_schema.descriptor == nullptr) {
        return Status::InvalidArgument("Parquet column descriptor is null for column {}",
                                       column_schema.name);
    }
    if (!is_nested && (column_schema.descriptor->max_repetition_level() != 0 ||
                       column_schema.descriptor->max_definition_level() > 1)) {
        return Status::NotSupported(
                "Current parquet scalar reader only supports flat primitive columns; column {} is "
                "not supported",
                column_schema.name);
    }
    if (is_nested && !supports_nested_scalar_record_reader(column_schema)) {
        return Status::NotSupported(
                "Current parquet nested scalar reader does not support column {}",
                column_schema.name);
    }
    if (!is_nested && !column_schema.type_descriptor.supports_record_reader) {
        return Status::NotSupported("Current parquet scalar reader does not support column {}",
                                    column_schema.name);
    }
    std::shared_ptr<::parquet::internal::RecordReader> record_reader;
    // Nested readers implement skip() by materializing rows into a scratch column. If Arrow
    // page filtering is also installed, those scratch reads can consume the next selected row
    // after a page-index range gap. Keep page filtering on flat scalar readers only.
    RETURN_IF_ERROR(get_record_reader(column_schema.leaf_column_id, column_schema.descriptor,
                                      column_schema.name, !is_nested, read_dictionary,
                                      &record_reader));
    return make_scalar_column_reader(column_schema, std::move(record_reader), !is_nested, reader);
}

//   1. RowGroupReader::GetColumnPageReader(leaf_column_id) -> Arrow PageReader
Status ParquetColumnReaderFactory::get_record_reader(
        int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor, const std::string& name,
        bool install_page_filter, bool read_dictionary,
        std::shared_ptr<::parquet::internal::RecordReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (_row_group == nullptr) {
        return Status::InternalError("Parquet row group reader is not initialized for column {}",
                                     name);
    }
    if (leaf_column_id < 0 || leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       leaf_column_id, name);
    }
    if (descriptor == nullptr) {
        return Status::InvalidArgument("Parquet column descriptor is null for column {}", name);
    }
    auto& record_readers = read_dictionary ? _dictionary_record_readers : _record_readers;
    if (record_readers[leaf_column_id] == nullptr) {
        try {
            auto page_reader = _row_group->GetColumnPageReader(leaf_column_id);
            if (install_page_filter) {
                install_data_page_filter(page_reader, _page_skip_plans, leaf_column_id,
                                         _page_skip_profile);
            }
            const auto level_info = ::parquet::internal::LevelInfo::ComputeLevelInfo(descriptor);
            record_readers[leaf_column_id] = ::parquet::internal::RecordReader::Make(
                    descriptor, level_info, ::arrow::default_memory_pool(),
                    /*read_dictionary=*/read_dictionary,
                    /*read_dense_for_nullable=*/false);
            record_readers[leaf_column_id]->SetPageReader(std::move(page_reader));
        } catch (const ::parquet::ParquetException& e) {
            return Status::Corruption("Failed to create parquet record reader for column {}: {}",
                                      name, e.what());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to create parquet record reader for column {}: {}",
                                         name, e.what());
        }
    }
    if (record_readers[leaf_column_id] == nullptr) {
        return Status::Corruption("Failed to create parquet record reader for column {}", name);
    }
    *reader = record_readers[leaf_column_id];
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_struct_column_reader(
        const ParquetColumnSchema& column_schema, const format::LocalColumnIndex* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    std::vector<std::unique_ptr<ParquetColumnReader>> child_readers;
    child_readers.reserve(column_schema.children.size());
    std::vector<int> child_output_indices;
    child_output_indices.reserve(column_schema.children.size());
    DataTypes projected_child_types;
    Strings projected_child_names;
    for (size_t child_idx = 0; child_idx < column_schema.children.size(); ++child_idx) {
        const auto& child_schema = column_schema.children[child_idx];
        const auto* child_projection =
                format::find_child_projection(projection, child_schema->local_id);
        if (!format::is_child_projected(projection, child_schema->local_id)) {
            continue;
        }
        std::unique_ptr<ParquetColumnReader> child_reader;
        RETURN_IF_ERROR(
                create_column_reader(*child_schema, child_projection, true, false, &child_reader));
        child_output_indices.push_back(static_cast<int>(projected_child_types.size()));
        projected_child_types.push_back(make_nullable(child_reader->type()));
        projected_child_names.push_back(child_reader->name());
        child_readers.push_back(std::move(child_reader));
    }
    if (format::is_partial_projection(projection) &&
        projected_child_types.size() != projection->children.size()) {
        return Status::InvalidArgument(
                "Parquet STRUCT projection for column {} contains invalid child",
                column_schema.name);
    }
    if (projected_child_types.empty() && !column_schema.children.empty()) {
        return Status::NotSupported("Parquet STRUCT projection for column {} contains no children",
                                    column_schema.name);
    }
    DataTypePtr type = column_schema.type;
    if (format::is_partial_projection(projection)) {
        type = std::make_shared<DataTypeStruct>(projected_child_types, projected_child_names);
        if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
            type = make_nullable(type);
        }
    }
    *reader = std::make_unique<StructColumnReader>(
            column_schema, std::move(type), std::move(child_readers),
            std::move(child_output_indices), _column_reader_profile);
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_list_column_reader(
        const ParquetColumnSchema& column_schema, const format::LocalColumnIndex* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.children.size() != 1) {
        return Status::NotSupported("Unsupported parquet LIST layout for column {}",
                                    column_schema.name);
    }
    std::unique_ptr<ParquetColumnReader> element_reader;
    const auto& element_schema = *column_schema.children[0];
    const auto* element_projection =
            format::find_child_projection(projection, element_schema.local_id);
    if (format::is_partial_projection(projection) && element_projection == nullptr) {
        return Status::NotSupported("Parquet LIST projection for column {} contains no element",
                                    column_schema.name);
    }
    RETURN_IF_ERROR(
            create_column_reader(element_schema, element_projection, true, false, &element_reader));
    DataTypePtr type = column_schema.type;
    if (format::is_partial_projection(element_projection)) {
        type = std::make_shared<DataTypeArray>(element_reader->type());
        if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
            type = make_nullable(type);
        }
    }
    *reader = std::make_unique<ListColumnReader>(column_schema, std::move(type),
                                                 std::move(element_reader), _column_reader_profile);
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_map_column_reader(
        const ParquetColumnSchema& column_schema, const format::LocalColumnIndex* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.children.size() != 2) {
        return Status::NotSupported("Unsupported parquet MAP layout for column {}",
                                    column_schema.name);
    }
    const auto& key_schema = *column_schema.children[0];
    const auto& value_schema = *column_schema.children[1];
    const auto* value_projection = format::find_child_projection(projection, value_schema.local_id);
    if (format::is_partial_projection(projection)) {
        if (value_projection == nullptr) {
            return Status::NotSupported("Parquet MAP projection for column {} contains no value",
                                        column_schema.name);
        }
        for (const auto& child_projection : projection->children) {
            if (child_projection.local_id() == key_schema.local_id) {
                continue;
            }
            if (child_projection.local_id() != value_schema.local_id) {
                return Status::InvalidArgument(
                        "Parquet MAP projection for column {} contains invalid child",
                        column_schema.name);
            }
        }
    }
    std::unique_ptr<ParquetColumnReader> key_reader;
    // MAP materialization always needs the full key stream. It owns entry existence, offsets and
    // key equality semantics, so MAP projection is defined only as value-subtree pruning.
    RETURN_IF_ERROR(create_column_reader(key_schema, nullptr, true, false, &key_reader));
    std::unique_ptr<ParquetColumnReader> value_reader;
    RETURN_IF_ERROR(
            create_column_reader(value_schema, value_projection, true, false, &value_reader));
    DataTypePtr type = column_schema.type;
    if (format::is_partial_projection(value_projection)) {
        type = std::make_shared<DataTypeMap>(make_nullable(key_reader->type()),
                                             make_nullable(value_reader->type()));
        if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
            type = make_nullable(type);
        }
    }
    *reader =
            std::make_unique<MapColumnReader>(column_schema, std::move(type), std::move(key_reader),
                                              std::move(value_reader), _column_reader_profile);
    return Status::OK();
}

Status ParquetColumnReaderFactory::create(const ParquetColumnSchema& column_schema,
                                          const format::LocalColumnIndex* projection,
                                          std::unique_ptr<ParquetColumnReader>* reader,
                                          bool read_dictionary) const {
    return create_column_reader(column_schema, projection, false, read_dictionary, reader);
}

Status ParquetColumnReaderFactory::create_count_shape_reader(
        const ParquetColumnSchema& column_schema, const format::LocalColumnIndex* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    return create_count_shape_reader_impl(column_schema, projection, false, reader);
}

Status ParquetColumnReaderFactory::create_count_shape_reader_impl(
        const ParquetColumnSchema& column_schema, const format::LocalColumnIndex* projection,
        bool is_nested, std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    switch (column_schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        if (format::is_partial_projection(projection)) {
            return Status::InvalidArgument("Parquet COUNT projection is invalid for column {}",
                                           column_schema.name);
        }
        return create_scalar_column_reader(column_schema, is_nested, false, reader);
    case ParquetColumnSchemaKind::STRUCT: {
        if (column_schema.children.empty()) {
            return Status::NotSupported("Parquet COUNT shape reader found empty STRUCT column {}",
                                        column_schema.name);
        }
        const ParquetColumnSchema* child_schema = nullptr;
        const format::LocalColumnIndex* child_projection = nullptr;
        if (format::is_partial_projection(projection)) {
            const auto child_id = projection->children[0].local_id();
            const auto child_it = std::ranges::find_if(
                    column_schema.children,
                    [&](const auto& child) { return child->local_id == child_id; });
            if (child_it == column_schema.children.end()) {
                return Status::InvalidArgument(
                        "Parquet COUNT projection for column {} contains invalid child",
                        column_schema.name);
            }
            child_schema = child_it->get();
            child_projection = &projection->children[0];
        } else {
            child_schema = column_schema.children[0].get();
        }
        DORIS_CHECK(child_schema != nullptr);
        return create_count_shape_reader_impl(*child_schema, child_projection, true, reader);
    }
    case ParquetColumnSchemaKind::LIST: {
        if (column_schema.children.size() != 1) {
            return Status::NotSupported("Unsupported parquet LIST layout for COUNT column {}",
                                        column_schema.name);
        }
        const auto& element_schema = *column_schema.children[0];
        const auto* element_projection =
                format::find_child_projection(projection, element_schema.local_id);
        return create_count_shape_reader_impl(element_schema, element_projection, true, reader);
    }
    case ParquetColumnSchemaKind::MAP: {
        if (column_schema.children.empty()) {
            return Status::NotSupported("Unsupported parquet MAP layout for COUNT column {}",
                                        column_schema.name);
        }
        // The key stream defines MAP entry existence and offsets. Counting top-level MAP NULL-ness
        // from it avoids creating a value reader, which is the expensive path for files with huge
        // MAP value strings.
        return create_count_shape_reader_impl(*column_schema.children[0], nullptr, true, reader);
    }
    }
    return Status::NotSupported("Unsupported parquet column schema kind for COUNT column {}",
                                column_schema.name);
}

Status ParquetColumnReaderFactory::create_column_reader(
        const ParquetColumnSchema& column_schema, const format::LocalColumnIndex* projection,
        bool is_nested, bool read_dictionary, std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    switch (column_schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        if (is_nested) {
            if (format::is_partial_projection(projection)) {
                return Status::InvalidArgument("Parquet scalar projection is invalid for column {}",
                                               column_schema.name);
            }
            return create_scalar_column_reader(column_schema, true, false, reader);
        }
        return create_scalar_column_reader(column_schema, false, read_dictionary, reader);
    case ParquetColumnSchemaKind::STRUCT:
        return create_struct_column_reader(column_schema, projection, reader);
    case ParquetColumnSchemaKind::LIST:
        return create_list_column_reader(column_schema, projection, reader);
    case ParquetColumnSchemaKind::MAP:
        return create_map_column_reader(column_schema, projection, reader);
    }
    return Status::NotSupported("Unsupported parquet column schema kind for column {}",
                                column_schema.name);
}

ParquetColumnReader::ParquetColumnReader(const ParquetColumnSchema& schema, const DataTypePtr type,
                                         ParquetColumnReaderProfile profile)
        : _profile(profile),
          _field_id(schema.local_id),
          _leaf_column_id(schema.leaf_column_id),
          _nullable_definition_level(schema.nullable_definition_level),
          _repeated_repetition_level(schema.repeated_repetition_level),
          _definition_level(schema.definition_level),
          _repetition_level(schema.repetition_level),
          _repeated_ancestor_definition_level(schema.repeated_ancestor_definition_level),
          _type(std::move(type)),
          _name(schema.name) {}

Status ParquetColumnReader::load_nested_batch(int64_t) {
    return Status::NotSupported("Parquet nested batch load is not supported for column {}", _name);
}

Status ParquetColumnReader::load_nested_levels_batch(int64_t) {
    return Status::NotSupported("Parquet nested levels batch load is not supported for column {}",
                                _name);
}

Status ParquetColumnReader::build_nested_column(int64_t, MutableColumnPtr&, int64_t*) {
    return Status::NotSupported("Parquet nested column build is not supported for column {}",
                                _name);
}

Status ParquetColumnReader::consume_nested_column(int64_t, int64_t*) {
    return Status::NotSupported("Parquet nested column consume is not supported for column {}",
                                _name);
}

Status ParquetColumnReader::skip_nested_rows(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }

    // A nested parent row may expand to many child values. Capping the number of parent rows per
    // loaded batch bounds that amplification for large holes. The consume interface advances the
    // loaded definition/repetition levels recursively without constructing a discarded Column.
    constexpr int64_t MAX_NESTED_SKIP_BATCH_SIZE = 4096;
    int64_t remaining_rows = rows;
    while (remaining_rows > 0) {
        const int64_t batch_rows = std::min(remaining_rows, MAX_NESTED_SKIP_BATCH_SIZE);
        RETURN_IF_ERROR(load_nested_levels_batch(batch_rows));
        int64_t rows_consumed = 0;
        RETURN_IF_ERROR(consume_nested_column(batch_rows, &rows_consumed));
        if (rows_consumed != batch_rows) {
            return Status::Corruption(
                    "Failed to skip nested parquet column {}: skipped {} of {} rows in batch",
                    _name, rows_consumed, batch_rows);
        }
        remaining_rows -= batch_rows;
    }
    update_reader_skip_rows(rows);
    return Status::OK();
}

const std::vector<int16_t>& ParquetColumnReader::nested_definition_levels() const {
    static const std::vector<int16_t> empty;
    return empty;
}

const std::vector<int16_t>& ParquetColumnReader::nested_repetition_levels() const {
    static const std::vector<int16_t> empty;
    return empty;
}

int64_t ParquetColumnReader::nested_levels_written() const {
    return 0;
}

bool ParquetColumnReader::is_or_has_repeated_child() const {
    return _repetition_level > 0;
}

} // namespace doris::format::parquet
