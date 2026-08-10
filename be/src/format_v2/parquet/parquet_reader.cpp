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

#include "format_v2/parquet/parquet_reader.h"

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/column_mapper.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/parquet_scan.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/count_column_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"

namespace doris::format::parquet {

struct ParquetReaderScanState {
    ParquetFileContext file_context;
    std::vector<std::unique_ptr<ParquetColumnSchema>> file_schema;
    std::shared_ptr<RowGroupScanPlan> scan_plan;
    ParquetScanScheduler scheduler;
    const RuntimeState* runtime_state = nullptr;
    const cctz::time_zone* timezone = nullptr;
    bool enable_bloom_filter = false;
    bool enable_page_cache = false;
    bool enable_strict_mode = false;
};

const ParquetColumnSchema* projected_schema_child(const ParquetColumnSchema& schema,
                                                  int32_t local_id) {
    const auto child_it = std::ranges::find_if(
            schema.children, [local_id](const auto& child) { return child->local_id == local_id; });
    return child_it == schema.children.end() ? nullptr : child_it->get();
}

const ParquetColumnSchema* schema_child_by_name(const ParquetColumnSchema& schema,
                                                std::string_view name) {
    const auto child_it = std::ranges::find_if(
            schema.children, [name](const auto& child) { return child->name == name; });
    return child_it == schema.children.end() ? nullptr : child_it->get();
}

bool collect_variant_terminal_fallback_leaf_ids(const ParquetColumnSchema& schema,
                                                const format::LocalColumnIndex& projection,
                                                std::vector<int>* residual_leaf_ids) {
    DORIS_CHECK(residual_leaf_ids != nullptr);
    const auto* value = schema_child_by_name(schema, "value");
    const auto* typed_value = schema_child_by_name(schema, "typed_value");
    if (value != nullptr && value->kind == ParquetColumnSchemaKind::PRIMITIVE &&
        typed_value != nullptr) {
        if (value->leaf_column_id < 0) {
            return false;
        }
        const auto typed_projection_it =
                std::ranges::find_if(projection.children, [&](const auto& child_projection) {
                    return child_projection.local_id() == typed_value->local_id;
                });
        if (typed_projection_it == projection.children.end()) {
            return false;
        }
        if (typed_value->kind != ParquetColumnSchemaKind::PRIMITIVE) {
            return collect_variant_terminal_fallback_leaf_ids(*typed_value, *typed_projection_it,
                                                              residual_leaf_ids);
        }
        // Object residual keys are disjoint from shredded keys. Only the fallback paired with the
        // terminal typed leaf can supply that projected path; ancestor values contain other keys.
        residual_leaf_ids->push_back(value->leaf_column_id);
        return true;
    }
    for (const auto& child_projection : projection.children) {
        const auto* child = projected_schema_child(schema, child_projection.local_id());
        if (child == nullptr || !collect_variant_terminal_fallback_leaf_ids(
                                        *child, child_projection, residual_leaf_ids)) {
            return false;
        }
    }
    return true;
}

namespace {

bool variant_leaf_projection_is_safe_for_row_group_impl(
        const tparquet::RowGroup& row_group, const ParquetColumnSchema& schema,
        const format::LocalColumnIndex& projection) {
    if (schema.kind != ParquetColumnSchemaKind::VARIANT || schema.max_repetition_level != 0 ||
        !format::is_partial_projection(&projection)) {
        return false;
    }
    std::vector<int> residual_leaf_ids;
    if (!collect_variant_terminal_fallback_leaf_ids(schema, projection, &residual_leaf_ids) ||
        residual_leaf_ids.empty()) {
        return false;
    }
    std::ranges::sort(residual_leaf_ids);
    residual_leaf_ids.erase(std::unique(residual_leaf_ids.begin(), residual_leaf_ids.end()),
                            residual_leaf_ids.end());
    return std::ranges::all_of(residual_leaf_ids, [&](const int leaf_id) {
        if (leaf_id < 0 || leaf_id >= static_cast<int>(row_group.columns.size())) {
            return false;
        }
        const auto& chunk = row_group.columns[leaf_id];
        return row_group.num_rows >= 0 && chunk.__isset.meta_data &&
               chunk.meta_data.num_values == row_group.num_rows &&
               chunk.meta_data.__isset.statistics &&
               chunk.meta_data.statistics.__isset.null_count &&
               chunk.meta_data.statistics.null_count == chunk.meta_data.num_values;
    });
}

} // namespace

bool detail::variant_leaf_projection_is_safe_for_row_group(
        const tparquet::RowGroup& row_group, const ParquetColumnSchema& schema,
        const format::LocalColumnIndex& projection) {
    return variant_leaf_projection_is_safe_for_row_group_impl(row_group, schema, projection);
}

size_t detail::finalize_variant_leaf_projection_for_row_group(const tparquet::RowGroup& row_group,
                                                              const ParquetColumnSchema& schema,
                                                              format::LocalColumnIndex* projection,
                                                              size_t* full_projections) {
    DORIS_CHECK(projection != nullptr);
    if (!format::is_partial_projection(projection)) {
        return 0;
    }
    if (schema.kind == ParquetColumnSchemaKind::VARIANT) {
        if (variant_leaf_projection_is_safe_for_row_group(row_group, schema, *projection)) {
            return 1;
        }
        // The residual null_count is row-group scoped. Restore the complete Variant wrapper for
        // only this reader so another row group can still keep the typed-leaf projection.
        projection->project_all_children = true;
        projection->children.clear();
        if (full_projections != nullptr) {
            ++*full_projections;
        }
        return 0;
    }

    size_t retained = 0;
    for (auto& child_projection : projection->children) {
        const auto* child_schema = projected_schema_child(schema, child_projection.local_id());
        DORIS_CHECK(child_schema != nullptr);
        retained += finalize_variant_leaf_projection_for_row_group(
                row_group, *child_schema, &child_projection, full_projections);
    }
    return retained;
}

size_t count_variant_leaf_projection_candidates(const ParquetColumnSchema& schema,
                                                const format::LocalColumnIndex& projection) {
    if (!format::is_partial_projection(&projection)) {
        return 0;
    }
    if (schema.kind == ParquetColumnSchemaKind::VARIANT) {
        std::vector<int> residual_leaf_ids;
        return schema.max_repetition_level == 0 &&
                               collect_variant_terminal_fallback_leaf_ids(schema, projection,
                                                                          &residual_leaf_ids) &&
                               !residual_leaf_ids.empty()
                       ? 1
                       : 0;
    }
    size_t candidates = 0;
    for (const auto& child_projection : projection.children) {
        const auto* child_schema = projected_schema_child(schema, child_projection.local_id());
        DORIS_CHECK(child_schema != nullptr);
        candidates += count_variant_leaf_projection_candidates(*child_schema, child_projection);
    }
    return candidates;
}

size_t count_variant_leaf_projection_candidates(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const std::vector<format::LocalColumnIndex>& projections) {
    size_t candidates = 0;
    for (const auto& projection : projections) {
        const int32_t local_id = projection.local_id();
        if (local_id < 0 || local_id >= static_cast<int32_t>(file_schema.size()) ||
            !file_schema[local_id]->contains_variant) {
            continue;
        }
        candidates += count_variant_leaf_projection_candidates(*file_schema[local_id], projection);
    }
    return candidates;
}

Status validate_all_projected_leaves_supported(const ParquetColumnSchema& column_schema) {
    if (column_schema.kind == ParquetColumnSchemaKind::PRIMITIVE) {
        if (!column_schema.type_descriptor.unsupported_reason.empty()) {
            return Status::NotSupported("Unsupported parquet column '{}': {}", column_schema.name,
                                        column_schema.type_descriptor.unsupported_reason);
        }
        return Status::OK();
    }
    for (const auto& child : column_schema.children) {
        DORIS_CHECK(child != nullptr);
        RETURN_IF_ERROR(validate_all_projected_leaves_supported(*child));
    }
    return Status::OK();
}

Status validate_projected_leaves_supported(const ParquetColumnSchema& column_schema,
                                           const format::LocalColumnIndex& projection) {
    if (column_schema.kind == ParquetColumnSchemaKind::PRIMITIVE ||
        projection.project_all_children || projection.children.empty()) {
        return validate_all_projected_leaves_supported(column_schema);
    }
    for (const auto& child_projection : projection.children) {
        const auto child_it =
                std::ranges::find_if(column_schema.children, [&](const auto& child_schema) {
                    return child_schema->local_id == child_projection.local_id();
                });
        DORIS_CHECK(child_it != column_schema.children.end());
        RETURN_IF_ERROR(validate_projected_leaves_supported(**child_it, child_projection));
    }
    return Status::OK();
}

Status validate_requested_columns_supported(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request) {
    // Validate the projected native-schema leaves before pruning: checking a physical carrier at
    // a later read site can let an unsupported logical type silently pass when every row is pruned.
    auto validate_scan_column = [&](const format::LocalColumnIndex& projection) -> Status {
        const auto local_id = projection.local_id();
        if (local_id == format::ROW_POSITION_COLUMN_ID ||
            local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            return Status::OK();
        }
        DORIS_CHECK(local_id >= 0 && local_id < static_cast<int32_t>(file_schema.size()));
        DORIS_CHECK(file_schema[local_id] != nullptr);
        return validate_projected_leaves_supported(*file_schema[local_id], projection);
    };
    for (const auto& column : request.predicate_columns) {
        RETURN_IF_ERROR(validate_scan_column(column));
    }
    for (const auto& column : request.non_predicate_columns) {
        if (!request.is_count_star_placeholder(column.column_id())) {
            RETURN_IF_ERROR(validate_scan_column(column));
        }
    }
    return Status::OK();
}

const ParquetColumnSchema& projected_root_schema(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::LocalColumnIndex& projection) {
    const auto local_id = projection.local_id();
    DORIS_CHECK(local_id >= 0 && local_id < static_cast<int32_t>(file_schema.size()));
    DORIS_CHECK(file_schema[local_id] != nullptr);
    return *file_schema[local_id];
}

int64_t count_loaded_non_null_values(const ParquetColumnSchema& root_schema,
                                     const CountColumnReader& shape_reader, int64_t expected_rows) {
    const auto& def_levels = shape_reader.definition_levels();
    const auto& rep_levels = shape_reader.repetition_levels();
    const int64_t levels_written = shape_reader.levels_written();
    DORIS_CHECK(levels_written >= expected_rows);
    if (root_schema.max_repetition_level == 0) {
        DORIS_CHECK(levels_written == expected_rows);
        const int16_t non_null_definition_level = root_schema.nullable_definition_level;
        int64_t count = 0;
        for (int64_t level_idx = 0; level_idx < levels_written; ++level_idx) {
            count += def_levels[level_idx] >= non_null_definition_level ? 1 : 0;
        }
        return count;
    }

    // For repeated encodings, repetition level zero starts a top-level row. Empty MAP/LIST rows
    // have no entries but still carry a level slot; they are non-NULL and must be counted by
    // count(col). The root nullable level distinguishes a NULL top-level value from a non-NULL
    // value regardless of which repeated leaf represents its shape.
    const int16_t non_null_definition_level = root_schema.nullable_definition_level;
    int64_t counted_rows = 0;
    int64_t non_null_rows = 0;
    for (int64_t level_idx = 0; level_idx < levels_written && counted_rows < expected_rows;
         ++level_idx) {
        if (rep_levels[level_idx] != 0) {
            continue;
        }
        ++counted_rows;
        non_null_rows += def_levels[level_idx] >= non_null_definition_level ? 1 : 0;
    }
    DORIS_CHECK(counted_rows == expected_rows);
    return non_null_rows;
}

DataTypePtr nullable_like_original(const DataTypePtr& type, DataTypePtr nested_type) {
    return type != nullptr && type->is_nullable() ? make_nullable(nested_type) : nested_type;
}

int timestamp_tz_scale(const ParquetTypeDescriptor& type_descriptor) {
    switch (type_descriptor.time_unit) {
    case ParquetTimeUnit::MILLIS:
        return 3;
    case ParquetTimeUnit::MICROS:
    case ParquetTimeUnit::UNKNOWN:
    default:
        return 6;
    }
}

bool should_map_to_timestamp_tz(const ParquetColumnSchema& column_schema) {
    const auto& type_descriptor = column_schema.type_descriptor;
    return type_descriptor.physical_type == tparquet::Type::INT96 ||
           (type_descriptor.is_timestamp && type_descriptor.timestamp_is_adjusted_to_utc);
}

DataTypePtr apply_timestamp_tz_mapping(ParquetColumnSchema* column_schema) {
    DORIS_CHECK(column_schema != nullptr);
    if (column_schema->kind == ParquetColumnSchemaKind::PRIMITIVE) {
        if (should_map_to_timestamp_tz(*column_schema)) {
            const bool nullable =
                    column_schema->type != nullptr && column_schema->type->is_nullable();
            const auto scale = timestamp_tz_scale(column_schema->type_descriptor);
            column_schema->type = DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ,
                                                                               nullable, 0, scale);
            column_schema->type_descriptor.doris_type = column_schema->type;
        }
        return column_schema->type;
    }

    std::vector<DataTypePtr> child_types;
    child_types.reserve(column_schema->children.size());
    for (auto& child : column_schema->children) {
        child_types.push_back(apply_timestamp_tz_mapping(child.get()));
    }

    if (column_schema->kind == ParquetColumnSchemaKind::LIST) {
        DORIS_CHECK(child_types.size() == 1);
        column_schema->type = nullable_like_original(
                column_schema->type, std::make_shared<DataTypeArray>(child_types[0]));
    } else if (column_schema->kind == ParquetColumnSchemaKind::MAP) {
        DORIS_CHECK(child_types.size() == 2);
        column_schema->type = nullable_like_original(
                column_schema->type, std::make_shared<DataTypeMap>(make_nullable(child_types[0]),
                                                                   make_nullable(child_types[1])));
    } else if (column_schema->kind == ParquetColumnSchemaKind::STRUCT) {
        Strings child_names;
        child_names.reserve(column_schema->children.size());
        for (const auto& child : column_schema->children) {
            child_names.push_back(child->name);
        }
        column_schema->type = nullable_like_original(
                column_schema->type, std::make_shared<DataTypeStruct>(child_types, child_names));
    } else if (column_schema->kind == ParquetColumnSchemaKind::VARIANT) {
        Strings child_names;
        child_names.reserve(column_schema->children.size());
        for (const auto& child : column_schema->children) {
            child_names.push_back(child->name);
        }
        column_schema->variant_physical_type =
                nullable_like_original(column_schema->variant_physical_type,
                                       std::make_shared<DataTypeStruct>(child_types, child_names));
    }
    return column_schema->type;
}

void apply_timestamp_tz_mapping_in_variants(ParquetColumnSchema* column_schema) {
    DORIS_CHECK(column_schema != nullptr);
    if (column_schema->kind == ParquetColumnSchemaKind::VARIANT) {
        // Shredded Variant timestamps always represent instants, even when the surrounding table
        // did not request catalog-level TIMESTAMPTZ mapping for ordinary Parquet columns.
        apply_timestamp_tz_mapping(column_schema);
        return;
    }
    for (auto& child : column_schema->children) {
        apply_timestamp_tz_mapping_in_variants(child.get());
    }
}

static Status find_projected_minmax_leaf(const ParquetColumnSchema& column_schema,
                                         const format::LocalColumnIndex& projection,
                                         const ParquetColumnSchema** leaf_schema) {
    DORIS_CHECK(leaf_schema != nullptr);
    if (projection.project_all_children || projection.children.empty()) {
        if (column_schema.leaf_column_id < 0) {
            return Status::NotSupported(
                    "Parquet aggregate pushdown only supports primitive column {}",
                    column_schema.name);
        }
        if (column_schema.max_repetition_level > 0) {
            return Status::NotSupported(
                    "Parquet aggregate pushdown does not support repeated column {}",
                    column_schema.name);
        }
        *leaf_schema = &column_schema;
        return Status::OK();
    }
    if (projection.children.size() != 1) {
        return Status::NotSupported(
                "Parquet aggregate pushdown only supports a single nested leaf under column {}",
                column_schema.name);
    }
    const auto& child_projection = projection.children[0];
    const auto child_schema_it =
            std::ranges::find_if(column_schema.children, [&](const auto& child_schema) {
                return child_schema->local_id == child_projection.local_id();
            });
    if (child_schema_it != column_schema.children.end()) {
        return find_projected_minmax_leaf(**child_schema_it, child_projection, leaf_schema);
    }
    return Status::InvalidArgument("Invalid parquet aggregate projection local id {} for column {}",
                                   child_projection.local_id(), column_schema.name);
}

static Status validate_minmax_aggregate_statistics(const ParquetColumnSchema& column_schema) {
    switch (column_schema.type_descriptor.physical_type) {
    case tparquet::Type::BYTE_ARRAY:
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        // Arrow 17 does not expose Parquet's min/max exactness flags. Binary statistics may be
        // truncated bounds rather than values present in the file, so they are safe for pruning
        // but cannot be returned as exact aggregate results.
        return Status::NotSupported(
                "Parquet MIN/MAX aggregate pushdown requires exact statistics for column {}",
                column_schema.name);
    default:
        return Status::OK();
    }
}

void ParquetReader::_fill_column_definition(const ParquetColumnSchema& column_schema,
                                            format::ColumnDefinition* field) const {
    if (column_schema.parquet_field_id >= 0) {
        field->identifier = Field::create_field<TYPE_INT>(column_schema.parquet_field_id);
    } else {
        field->identifier = Field::create_field<TYPE_STRING>(column_schema.name);
    }
    field->local_id = column_schema.local_id;
    field->name = column_schema.name;
    field->type = column_schema.type != nullptr && !column_schema.type->is_nullable()
                          ? make_nullable(column_schema.type)
                          : column_schema.type;
    field->children.clear();
    field->children.reserve(column_schema.children.size());
    for (const auto& child : column_schema.children) {
        format::ColumnDefinition child_field;
        _fill_column_definition(*child, &child_field);
        field->children.push_back(std::move(child_field));
    }
}

ParquetReader::ParquetReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                             std::unique_ptr<io::FileDescription>& file_description,
                             std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                             std::optional<format::GlobalRowIdContext> global_rowid_context,
                             bool enable_mapping_timestamp_tz, bool enable_mapping_varbinary,
                             std::shared_ptr<const FileScanSplitContext> split_context)
        : FileReader(system_properties, file_description, io_ctx, profile),
          _global_rowid_context(global_rowid_context),
          _enable_mapping_timestamp_tz(enable_mapping_timestamp_tz),
          _enable_mapping_varbinary(enable_mapping_varbinary),
          _split_context(std::move(split_context)) {}

ParquetReader::~ParquetReader() = default;

#ifdef BE_TEST
int64_t ParquetReader::TEST_footer_read_calls() const {
    return _state == nullptr ? 0 : _state->file_context.native_footer_read_calls;
}
#endif

Status ParquetReader::init(RuntimeState* state) {
    _init_profile();
    SCOPED_TIMER(_parquet_profile.total_time);
    if (_io_ctx != nullptr && _io_ctx->should_stop) {
        return Status::EndOfFile("stop");
    }
    RETURN_IF_ERROR(format::FileReader::init(state));
    if (_profile != nullptr) {
        COUNTER_UPDATE(_parquet_profile.file_reader_create_time,
                       _reader_statistics.file_reader_create_time);
        COUNTER_UPDATE(_parquet_profile.open_file_num, _reader_statistics.open_file_num);
    }
    _state = std::make_unique<ParquetReaderScanState>();
    _state->enable_bloom_filter =
            state != nullptr && state->query_options().enable_parquet_filter_by_bloom_filter;
    _state->enable_page_cache =
            state != nullptr && state->query_options().enable_parquet_file_page_cache;
    if (state != nullptr) {
        _state->runtime_state = state;
        _state->timezone = &state->timezone_obj();
        _state->enable_strict_mode = state->enable_strict_mode();
        _state->scheduler.set_timezone(&state->timezone_obj());
        _state->scheduler.set_enable_strict_mode(_state->enable_strict_mode);
        _state->scheduler.set_runtime_state(state);
    }
    int64_t merge_read_slice_size = -1;
    if (state != nullptr && state->query_options().__isset.merge_read_slice_size) {
        merge_read_slice_size = state->query_options().merge_read_slice_size;
    }
    _state->scheduler.set_merge_read_options(_profile, merge_read_slice_size);
    _state->scheduler.set_batch_size(_batch_size);
    if (_split_context != nullptr) {
        auto parquet_context =
                std::dynamic_pointer_cast<const ParquetFileSplitContext>(_split_context);
        if (parquet_context == nullptr || parquet_context->metadata == nullptr) {
            return Status::InvalidArgument("Invalid Parquet child split context");
        }
        _state->file_context.set_shared_metadata(parquet_context->metadata);
    }
    // Opening the file parses the footer before any row group can be scheduled. Keep this timer
    // around the whole operation so footer/cache latency cannot disappear from a slow profile.
    {
        SCOPED_TIMER(_parquet_profile.parse_footer_time);
        RETURN_IF_ERROR(_state->file_context.open(
                _tracing_file_reader, _io_ctx.get(), _state->enable_page_cache, *_file_description,
                _enable_mapping_timestamp_tz, _enable_mapping_varbinary));
    }
    if (_profile != nullptr) {
        COUNTER_UPDATE(_parquet_profile.file_footer_read_calls,
                       _state->file_context.native_footer_read_calls);
        COUNTER_UPDATE(_parquet_profile.file_footer_hit_cache,
                       _state->file_context.native_footer_cache_hits);
    }
    // Build file schema from parquet metadata.
    // A file reader may expose raw file identifiers, such as Parquet field_id, through ColumnDefinition::identifier
    {
        SCOPED_TIMER(_parquet_profile.parse_meta_time);
        RETURN_IF_ERROR(build_parquet_column_schema(_state->file_context.native_metadata->schema(),
                                                    &_state->file_schema));
        _state->file_context.contains_variant =
                std::ranges::any_of(_state->file_schema, [](const auto& column) {
                    DORIS_CHECK(column != nullptr);
                    return column->contains_variant;
                });
        if (_enable_mapping_timestamp_tz) {
            for (auto& column_schema : _state->file_schema) {
                apply_timestamp_tz_mapping(column_schema.get());
            }
        }
    }
    return Status::OK();
}

Status ParquetReader::build_split_tasks(const TFileRangeDesc& parent,
                                        std::vector<FileScanSplitTask>* children) {
    DORIS_CHECK(children != nullptr);
    children->clear();
    if (_state == nullptr || _state->file_context.native_metadata == nullptr ||
        _state->file_context.shared_metadata == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    const auto& metadata = _state->file_context.native_metadata->to_thrift();
    const auto compat = native::parquet_reader_compat(
            metadata.__isset.created_by ? metadata.created_by : std::string {});
    const size_t file_size = _file_description->file_size < 0
                                     ? _state->file_context.native_file->size()
                                     : cast_set<size_t>(_file_description->file_size);
    auto context = std::make_shared<ParquetFileSplitContext>(_state->file_context.shared_metadata);
    children->reserve(metadata.row_groups.size());
    for (size_t row_group_idx = 0; row_group_idx < metadata.row_groups.size(); ++row_group_idx) {
        const auto& row_group = metadata.row_groups[row_group_idx];
        if (row_group.columns.empty()) {
            return Status::Corruption("Parquet row group {} has no column chunks", row_group_idx);
        }
        size_t group_start = std::numeric_limits<size_t>::max();
        size_t group_end = 0;
        for (size_t column_idx = 0; column_idx < row_group.columns.size(); ++column_idx) {
            const auto& chunk = row_group.columns[column_idx];
            if (!chunk.__isset.meta_data) {
                return Status::Corruption("Parquet row group {} column {} has no metadata",
                                          row_group_idx, column_idx);
            }
            native::ColumnChunkRange chunk_range;
            RETURN_IF_ERROR(native::compute_column_chunk_range(
                    chunk.meta_data, file_size, compat.parquet_816_padding, &chunk_range));
            group_start = std::min(group_start, chunk_range.offset);
            group_end = std::max(group_end, chunk_range.offset + chunk_range.length);
        }
        TFileRangeDesc child = parent;
        child.__set_start_offset(cast_set<int64_t>(group_start));
        child.__set_size(cast_set<int64_t>(group_end - group_start));
        child.__set_is_file_parent(false);
        children->push_back({.range = std::move(child), .context = context});
    }
    return Status::OK();
}

void ParquetReader::set_batch_size(size_t batch_size) {
    _batch_size = std::max<size_t>(1, batch_size);
    if (_state != nullptr) {
        _state->scheduler.set_batch_size(_batch_size);
    }
}

Status ParquetReader::get_schema(std::vector<format::ColumnDefinition>* file_schema) const {
    SCOPED_TIMER(_parquet_profile.total_time);
    if (file_schema == nullptr) {
        return Status::InvalidArgument("file_schema is null");
    }
    file_schema->clear();
    if (_state == nullptr || _state->file_context.native_metadata == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }

    file_schema->reserve(_state->file_schema.size());
    for (size_t column_idx = 0; column_idx < _state->file_schema.size(); ++column_idx) {
        format::ColumnDefinition field;
        _fill_column_definition(*_state->file_schema[column_idx], &field);
        DORIS_CHECK(field.local_id == static_cast<int32_t>(column_idx));
        file_schema->push_back(std::move(field));
    }
    if (_global_rowid_context.has_value()) {
        file_schema->push_back(format::global_rowid_column_definition());
    }
    return Status::OK();
}

std::unique_ptr<format::TableColumnMapper> ParquetReader::create_column_mapper(
        format::TableColumnMapperOptions options) const {
    return std::make_unique<format::ParquetColumnMapper>(std::move(options));
}

Status ParquetReader::open(std::shared_ptr<format::FileScanRequest> request) {
    SCOPED_TIMER(_parquet_profile.total_time);
    if (_state == nullptr || _state->file_context.native_metadata == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    auto request_snapshot = request;
    DORIS_CHECK(request_snapshot != nullptr);
    if (!request_snapshot->variant_schema_overrides.empty()) {
        // Apply table-format semantics before Variant projection planning. The override is the
        // explicit proof that an otherwise ordinary Parquet group is a Variant carrier.
        RETURN_IF_ERROR(apply_variant_schema_overrides(
                _state->file_context.native_metadata->schema(),
                request_snapshot->variant_schema_overrides, &_state->file_schema));
        for (auto& column_schema : _state->file_schema) {
            apply_timestamp_tz_mapping_in_variants(column_schema.get());
        }
        _state->file_context.contains_variant =
                std::ranges::any_of(_state->file_schema, [](const auto& column) {
                    DORIS_CHECK(column != nullptr);
                    return column->contains_variant;
                });
    }
    size_t variant_leaf_projection_candidates = 0;
    if (_state->file_context.contains_variant) {
        // This counter describes leaf candidates without scanning every row group's statistics.
        // The scheduler profiles the actual per-row-group leaf/full decision separately.
        variant_leaf_projection_candidates =
                count_variant_leaf_projection_candidates(_state->file_schema,
                                                         request_snapshot->predicate_columns) +
                count_variant_leaf_projection_candidates(_state->file_schema,
                                                         request_snapshot->non_predicate_columns);
    }
    if (_parquet_profile.variant_leaf_projections != nullptr) {
        COUNTER_UPDATE(_parquet_profile.variant_leaf_projections,
                       variant_leaf_projection_candidates);
    }
    RETURN_IF_ERROR(format::FileReader::open(std::move(request)));

    // `local_positions.empty()` means all columns are needed by table reader
    // TODO(gabriel): It will happen only for TVF `select *` query.
    if (request_snapshot->local_positions.empty()) {
        for (const auto& col : request_snapshot->predicate_columns) {
            request_snapshot->local_positions.emplace(col.column_id(),
                                                      format::LocalIndex(col.column_id().value()));
        }
        for (const auto& col : request_snapshot->non_predicate_columns) {
            request_snapshot->local_positions.emplace(col.column_id(),
                                                      format::LocalIndex(col.column_id().value()));
        }
    }

    const auto num_fields = static_cast<int32_t>(_state->file_schema.size());
    for (const auto& col : request_snapshot->predicate_columns) {
        DORIS_CHECK(request_snapshot->local_positions.count(col.column_id()) > 0);
        const auto local_id = col.local_id();
        if (local_id == format::ROW_POSITION_COLUMN_ID ||
            local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            continue;
        }
        DORIS_CHECK(local_id >= 0 && local_id < num_fields);
    }
    for (const auto& col : request_snapshot->non_predicate_columns) {
        DORIS_CHECK(request_snapshot->local_positions.count(col.column_id()) > 0);
        const auto local_id = col.local_id();
        if (local_id == format::ROW_POSITION_COLUMN_ID ||
            local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            continue;
        }
        DORIS_CHECK(local_id >= 0 && local_id < num_fields);
    }

    // Reject requested unsupported logical leaves before row-group statistics, dictionaries,
    // bloom filters or page indexes inspect their physical fallback type. For example, a predicate
    // on TIME_MILLIS must fail here even when its INT32 statistics would prune every row group;
    // otherwise the same unsupported SELECT could fail or silently succeed depending on data.
    RETURN_IF_ERROR(validate_requested_columns_supported(_state->file_schema, *request_snapshot));

    auto row_group_plan = std::make_shared<RowGroupScanPlan>();
    ParquetScanRange scan_range;
    scan_range.start_offset = _file_description->range_start_offset;
    scan_range.size = _file_description->range_size;
    scan_range.file_size = _file_description->file_size;
    // Get selected ranges in row groups according to metadata (Row-Group level index and Page Index including Zonemap, Dictionary, Bloom Filter).
    RETURN_IF_ERROR(plan_parquet_row_groups(
            *_state->file_context.native_metadata, _state->file_schema, *request_snapshot,
            scan_range, _state->enable_bloom_filter, row_group_plan.get(), _state->timezone,
            _state->runtime_state, &_state->file_context,
            _parquet_profile.column_reader_profile()));
    if (_profile != nullptr) {
        _parquet_profile.update_pruning_stats(row_group_plan->pruning_stats);
    }
    // Native page readers admit exact validated page payloads to cache. Do not pre-register whole
    // column chunks here: footer offsets are untrusted and this obsolete range map is not consumed.
    _state->scan_plan = row_group_plan;
    _state->scheduler.set_page_skip_profile(_parquet_profile.page_skip_profile());
    if (_profile != nullptr) {
        _state->scheduler.set_pruning_profile(&_parquet_profile);
    }
    _state->scheduler.set_global_rowid_context(_global_rowid_context);
    _state->scheduler.set_scan_profile(_parquet_profile.scan_profile());
    _state->scheduler.set_plan(std::move(row_group_plan));
    _state->scheduler.set_scan_request(request_snapshot);
    _eof = _state->scheduler.empty();
    return Status::OK();
}

Status ParquetReader::queue_scan_request(std::shared_ptr<format::FileScanRequest> request) {
    SCOPED_TIMER(_parquet_profile.total_time);
    SCOPED_TIMER(_parquet_profile.refresh_scan_request_time);
    if (_state == nullptr || _state->file_context.native_metadata == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    DORIS_CHECK(request != nullptr);
    RETURN_IF_ERROR(validate_requested_columns_supported(_state->file_schema, *request));
    _state->scheduler.queue_scan_request(request);
    _request = std::move(request);
    return Status::OK();
}

Status ParquetReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    SCOPED_TIMER(_parquet_profile.total_time);
    if (_state == nullptr || _state->file_context.native_metadata == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    *rows = 0;
    if (_io_ctx != nullptr && _io_ctx->should_stop) {
        *eof = true;
        return Status::OK();
    }
    if (_eof) {
        *eof = true;
        return Status::OK();
    }
    if (_request == nullptr) {
        return Status::Cancelled("ParquetReader is closed");
    }

    const auto predicate_filtered_rows_before = _state->scheduler.predicate_filtered_rows();
    const auto raw_rows_read_before = _state->scheduler.raw_rows_read();
    Status st = _state->scheduler.read_next_batch(_state->file_context, _state->file_schema,
                                                  file_block, rows, eof);
    if (!st.ok()) {
        if (_io_ctx != nullptr && _io_ctx->should_stop) {
            *rows = 0;
            *eof = true;
            return Status::OK();
        }
        return st;
    }
    _sync_page_cache_profile();
    if (_io_ctx != nullptr) {
        _io_ctx->predicate_filtered_rows +=
                _state->scheduler.predicate_filtered_rows() - predicate_filtered_rows_before;
    }
    const auto raw_rows_read = _state->scheduler.raw_rows_read();
    DORIS_CHECK(raw_rows_read >= raw_rows_read_before);
    _record_scan_rows(raw_rows_read - raw_rows_read_before);
    _eof = *eof;
    return Status::OK();
}

bool ParquetReader::_should_stop() const {
    return _io_ctx != nullptr && _io_ctx->should_stop;
}

Status ParquetReader::_stop_status_if_requested(const Status& status) const {
    if (!status.ok() && _should_stop()) {
        return Status::EndOfFile("stop");
    }
    return status;
}

void ParquetReader::_sync_page_cache_profile() {
    if (_profile == nullptr || _state == nullptr) {
        return;
    }
    const auto stats = _state->file_context.page_cache_stats();
    COUNTER_UPDATE(_parquet_profile.page_read_counter,
                   stats.read_count - _reported_page_cache_stats.read_count);
    COUNTER_UPDATE(_parquet_profile.page_cache_write_counter,
                   stats.write_count - _reported_page_cache_stats.write_count);
    COUNTER_UPDATE(
            _parquet_profile.page_cache_compressed_write_counter,
            stats.compressed_write_count - _reported_page_cache_stats.compressed_write_count);
    COUNTER_UPDATE(_parquet_profile.page_cache_hit_counter,
                   stats.hit_count - _reported_page_cache_stats.hit_count);
    COUNTER_UPDATE(_parquet_profile.page_cache_missing_counter,
                   stats.miss_count - _reported_page_cache_stats.miss_count);
    COUNTER_UPDATE(_parquet_profile.page_cache_compressed_hit_counter,
                   stats.compressed_hit_count - _reported_page_cache_stats.compressed_hit_count);
    _reported_page_cache_stats = stats;
}

void ParquetReader::set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) {
    if (_state == nullptr || _state->scan_plan == nullptr) {
        return;
    }
    _state->scheduler.set_condition_cache_context(std::move(ctx));
    if (_io_ctx != nullptr) {
        // Condition-cache HIT filters row ranges before batch reading, so skipped rows never belong
        // to a later get_block() batch. Report the plan-level skipped rows at the same point where
        // the scan plan is rewritten.
        _io_ctx->condition_cache_filtered_rows += _state->scheduler.condition_cache_filtered_rows();
    }
}

int64_t ParquetReader::get_total_rows() const {
    if (_state == nullptr || _state->scan_plan == nullptr) {
        return 0;
    }
    int64_t rows = 0;
    for (const auto& row_group_plan : _state->scan_plan->row_groups) {
        rows += row_group_plan.row_group_rows;
    }
    return rows;
}

Status ParquetReader::get_aggregate_result(const format::FileAggregateRequest& request,
                                           format::FileAggregateResult* result) {
    SCOPED_TIMER(_parquet_profile.total_time);
    DORIS_CHECK(result != nullptr);
    if (_state == nullptr || _state->file_context.native_metadata == nullptr ||
        _state->scan_plan == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    if (_should_stop()) {
        return Status::EndOfFile("stop");
    }
    result->count = 0;
    result->columns.clear();
    if (request.agg_type != TPushAggOp::type::COUNT &&
        request.agg_type != TPushAggOp::type::MINMAX) {
        return Status::NotSupported("Unsupported parquet aggregate pushdown type {}",
                                    request.agg_type);
    }

    // Aggregate pushdown bypasses the scheduler but still requires the exact pruned row-group set.
    // Finish lazy remote probes here; normal scans keep them at current-row-group granularity.
    RETURN_IF_ERROR(finalize_parquet_row_group_plans(
            *_state->file_context.native_metadata, _state->file_schema, *_request,
            _state->enable_bloom_filter, _state->scan_plan.get(), _state->timezone,
            _state->runtime_state, &_state->file_context, _parquet_profile.column_reader_profile(),
            _profile == nullptr ? nullptr : &_parquet_profile));

    for (const auto& aggregate_column : request.columns) {
        const auto local_id = aggregate_column.projection.local_id();
        if (local_id < 0 || local_id >= static_cast<int32_t>(_state->file_schema.size())) {
            return Status::InvalidArgument("Invalid parquet aggregate column id {}", local_id);
        }
        DORIS_CHECK(_state->file_schema[local_id] != nullptr);
        // Aggregate pushdown can return directly from footer statistics without constructing a
        // column reader. Validate first so MIN/MAX(TIME_MILLIS), or an all-pruned COUNT request,
        // cannot expose the physical INT32 fallback as a supported logical value.
        RETURN_IF_ERROR(validate_projected_leaves_supported(*_state->file_schema[local_id],
                                                            aggregate_column.projection));
    }

    // Aggregate row count in all selected row groups. For MIN/MAX aggregate, this is used to determine whether there is no row group selected.
    for (const auto& row_group_plan : _state->scan_plan->row_groups) {
        const auto& row_group_metadata = _state->file_context.native_metadata->to_thrift()
                                                 .row_groups[row_group_plan.row_group_id];
        result->count += row_group_metadata.num_rows;
    }
    if (request.agg_type == TPushAggOp::type::COUNT) {
        if (request.columns.empty()) {
            return Status::OK();
        }
        if (request.columns.size() != 1) {
            return Status::NotSupported("Parquet COUNT pushdown only supports one count column");
        }
        const auto& count_projection = request.columns[0].projection;
        const auto& root_schema = projected_root_schema(_state->file_schema, count_projection);
        // A required primitive COUNT(col) still carries its projection so the unsupported-type
        // validation above cannot be bypassed. Once validated, its definition level proves that
        // every selected row is non-NULL, so preserve the already-computed footer row count and
        // avoid reading definition levels merely to rediscover COUNT(col) == COUNT(*). Complex
        // roots continue through the shape reader because their count semantics and read-row
        // accounting are derived from nested levels.
        if (root_schema.kind == ParquetColumnSchemaKind::PRIMITIVE &&
            root_schema.max_definition_level == 0) {
            return Status::OK();
        }
        result->count = 0;
        for (const auto& row_group_plan : _state->scan_plan->row_groups) {
            std::unique_ptr<CountColumnReader> shape_reader;
            RETURN_IF_ERROR(CountColumnReader::create(
                    _state->file_context.native_data_file(), _state->file_context.native_metadata,
                    row_group_plan.row_group_id, root_schema, &count_projection,
                    _state->file_context.native_io_ctx,
                    _state->file_context.native_page_cache_enabled,
                    _state->file_context.native_page_cache_file_key,
                    _parquet_profile.scan_profile().column_reader_profile, &shape_reader));
            DORIS_CHECK(shape_reader != nullptr);

            int64_t row_group_cursor = 0;
            for (const auto& selected_range : row_group_plan.selected_ranges) {
                DORIS_CHECK(selected_range.start >= row_group_cursor);
                RETURN_IF_ERROR(_stop_status_if_requested(
                        shape_reader->skip(selected_range.start - row_group_cursor)));
                row_group_cursor = selected_range.start;

                int64_t range_rows_read = 0;
                while (range_rows_read < selected_range.length) {
                    const int64_t batch_rows =
                            std::min<int64_t>(_batch_size, selected_range.length - range_rows_read);
                    int64_t rows_read = 0;
                    RETURN_IF_ERROR(_stop_status_if_requested(
                            shape_reader->read_levels(batch_rows, &rows_read)));
                    if (rows_read != batch_rows) {
                        return Status::Corruption(
                                "Parquet COUNT reader returned {} rows, expected {}", rows_read,
                                batch_rows);
                    }
                    _record_scan_rows(rows_read);
                    result->count +=
                            count_loaded_non_null_values(root_schema, *shape_reader, rows_read);
                    range_rows_read += rows_read;
                    row_group_cursor += rows_read;
                }
            }
        }
        return Status::OK();
    }

    result->columns.resize(request.columns.size());
    for (size_t request_column_idx = 0; request_column_idx < request.columns.size();
         ++request_column_idx) {
        const auto file_column_id = request.columns[request_column_idx].projection.local_id();
        if (file_column_id < 0 ||
            file_column_id >= static_cast<int32_t>(_state->file_schema.size())) {
            return Status::InvalidArgument("Invalid parquet aggregate column id {}",
                                           file_column_id);
        }
        const auto& column_schema = _state->file_schema[file_column_id];
        DORIS_CHECK(column_schema != nullptr);
        const ParquetColumnSchema* leaf_schema = nullptr;
        RETURN_IF_ERROR(find_projected_minmax_leaf(
                *column_schema, request.columns[request_column_idx].projection, &leaf_schema));
        DORIS_CHECK(leaf_schema != nullptr);
        RETURN_IF_ERROR(validate_minmax_aggregate_statistics(*leaf_schema));

        auto& aggregate_column = result->columns[request_column_idx];
        aggregate_column.projection = request.columns[request_column_idx].projection;
        for (const auto& row_group_plan : _state->scan_plan->row_groups) {
            const auto& row_group_metadata = _state->file_context.native_metadata->to_thrift()
                                                     .row_groups[row_group_plan.row_group_id];
            DORIS_CHECK(leaf_schema->leaf_column_id >= 0 &&
                        leaf_schema->leaf_column_id <
                                static_cast<int>(row_group_metadata.columns.size()));
            const auto& column_chunk = row_group_metadata.columns[leaf_schema->leaf_column_id];
            DORIS_CHECK(column_chunk.__isset.meta_data);
            const auto& column_metadata = column_chunk.meta_data;
            std::optional<tparquet::Statistics> safe_statistics;
            if (column_metadata.__isset.statistics &&
                detail::can_use_native_footer_min_max(
                        leaf_schema->type_descriptor, column_metadata.statistics,
                        detail::has_supported_type_defined_order(
                                _state->file_context.native_metadata->to_thrift(),
                                leaf_schema->leaf_column_id))) {
                safe_statistics = detail::sanitize_native_footer_statistics(
                        leaf_schema->type_descriptor, column_metadata.statistics,
                        detail::has_supported_type_defined_order(
                                _state->file_context.native_metadata->to_thrift(),
                                leaf_schema->leaf_column_id));
            }
            const auto statistics = ParquetStatisticsUtils::TransformColumnStatistics(
                    *leaf_schema, safe_statistics.has_value() ? &*safe_statistics : nullptr,
                    column_metadata.num_values, _state->timezone);
            if (!statistics.has_min_max) {
                return Status::NotSupported("Missing parquet min/max statistics for column {}",
                                            leaf_schema->name);
            }
            if (!aggregate_column.has_min || statistics.min_value < aggregate_column.min_value) {
                aggregate_column.min_value = statistics.min_value;
                aggregate_column.has_min = true;
            }
            if (!aggregate_column.has_max || aggregate_column.max_value < statistics.max_value) {
                aggregate_column.max_value = statistics.max_value;
                aggregate_column.has_max = true;
            }
        }
        if (!aggregate_column.has_min || !aggregate_column.has_max) {
            return Status::NotSupported("No parquet row group selected for min/max pushdown");
        }
    }
    return Status::OK();
}

Status ParquetReader::close() {
    SCOPED_TIMER(_parquet_profile.total_time);
    if (_state != nullptr) {
        _state->scheduler.close();
        _sync_page_cache_profile();
        RETURN_IF_ERROR(_state->file_context.close());
    }
    return FileReader::close();
}

void ParquetReader::_init_profile() {
    _parquet_profile.init(_profile);
}

} // namespace doris::format::parquet
