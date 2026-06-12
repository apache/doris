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
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/parquet_scan.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "runtime/runtime_state.h"

namespace doris::parquet {

struct ParquetReaderScanState {
    ParquetFileContext file_context;
    std::vector<std::unique_ptr<ParquetColumnSchema>> file_schema;
    RowGroupScanPlan scan_plan;
    ParquetScanScheduler scheduler;
    const cctz::time_zone* timezone = nullptr;
    bool enable_bloom_filter = false;
};

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

// Convert the semantic projection accepted by FileReader::open() back to the physical Parquet
// schema tree consumed by Parquet internals.
//
// ParquetReader::get_schema() exposes a semantic file-local ColumnDefinition tree:
//
//   MAP children = [key, value]
//
// The private ParquetColumnSchema tree still mirrors the physical Parquet layout:
//
//   MAP children = [key_value/entry]
//   key_value/entry children = [key, value]
//
// Row group pruning, page pruning and ParquetColumnReaderFactory all walk ParquetColumnSchema and
// match LocalColumnIndex children against that physical tree. Therefore a table-layer request such
// as `m.value.b` must be translated from semantic form:
//
//   m -> value -> b
//
// to physical Parquet form:
//
//   m -> key_value/entry -> key, value -> b
//
// The key child is always included for MAP because Doris MAP materialization requires both key and
// value columns even when the projection only prunes inside the value type.
static Status translate_projection_to_physical(const ParquetColumnSchema& column_schema,
                                               const format::LocalColumnIndex& semantic_projection,
                                               format::LocalColumnIndex* physical_projection) {
    DORIS_CHECK(physical_projection != nullptr);
    *physical_projection = semantic_projection;
    if (semantic_projection.project_all_children || semantic_projection.children.empty()) {
        physical_projection->project_all_children = true;
        physical_projection->children.clear();
        return Status::OK();
    }

    physical_projection->project_all_children = false;
    physical_projection->children.clear();
    switch (column_schema.kind) {
    case ParquetColumnSchemaKind::STRUCT:
    case ParquetColumnSchemaKind::LIST: {
        physical_projection->children.reserve(semantic_projection.children.size());
        for (const auto& semantic_child_projection : semantic_projection.children) {
            const auto child_schema_it =
                    std::ranges::find_if(column_schema.children, [&](const auto& child_schema) {
                        return child_schema->local_id == semantic_child_projection.local_id();
                    });
            if (child_schema_it == column_schema.children.end()) {
                return Status::InvalidArgument(
                        "Invalid parquet projection child id {} for column {}",
                        semantic_child_projection.local_id(), column_schema.name);
            }
            format::LocalColumnIndex physical_child_projection;
            RETURN_IF_ERROR(translate_projection_to_physical(
                    **child_schema_it, semantic_child_projection, &physical_child_projection));
            physical_projection->children.push_back(std::move(physical_child_projection));
        }
        return Status::OK();
    }
    case ParquetColumnSchemaKind::MAP: {
        if (column_schema.children.size() != 1 || column_schema.children[0]->children.size() != 2) {
            return Status::NotSupported("Unsupported parquet MAP layout for column {}",
                                        column_schema.name);
        }
        const auto& entry_schema = *column_schema.children[0];
        const auto& key_schema = *entry_schema.children[0];
        const auto& value_schema = *entry_schema.children[1];
        const auto* value_projection =
                format::find_child_projection(&semantic_projection, value_schema.local_id);
        if (value_projection == nullptr) {
            return Status::NotSupported("Parquet MAP projection for column {} contains no value",
                                        column_schema.name);
        }

        format::LocalColumnIndex entry_projection =
                format::LocalColumnIndex::partial_local(entry_schema.local_id);
        entry_projection.children.push_back(format::LocalColumnIndex::local(key_schema.local_id));
        format::LocalColumnIndex physical_value_projection;
        RETURN_IF_ERROR(translate_projection_to_physical(value_schema, *value_projection,
                                                         &physical_value_projection));
        entry_projection.children.push_back(std::move(physical_value_projection));
        physical_projection->children.push_back(std::move(entry_projection));
        return Status::OK();
    }
    case ParquetColumnSchemaKind::PRIMITIVE:
        return Status::InvalidArgument("Cannot project children from primitive parquet column {}",
                                       column_schema.name);
    }
    return Status::InvalidArgument("Unknown parquet schema kind for column {}", column_schema.name);
}

// Translate every column projection in a scan request from the semantic FileReader API shape to the
// physical Parquet reader shape. local_positions and expressions stay unchanged because they are
// keyed by top-level file-local column ids; only nested projection paths need physical wrappers.
static Status translate_request_to_physical(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& semantic_request,
        std::shared_ptr<format::FileScanRequest>* physical_request) {
    DORIS_CHECK(physical_request != nullptr);
    auto translated = std::make_shared<format::FileScanRequest>(semantic_request);
    auto translate_root_projection = [&](format::LocalColumnIndex* projection) -> Status {
        DORIS_CHECK(projection != nullptr);
        const auto local_id = projection->local_id();
        if (local_id == format::ROW_POSITION_COLUMN_ID ||
            local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            return Status::OK();
        }
        if (local_id < 0 || local_id >= static_cast<int32_t>(file_schema.size())) {
            return Status::InvalidArgument("Invalid parquet projection top-level local id {}",
                                           local_id);
        }
        format::LocalColumnIndex physical_projection;
        RETURN_IF_ERROR(translate_projection_to_physical(*file_schema[local_id], *projection,
                                                         &physical_projection));
        *projection = std::move(physical_projection);
        return Status::OK();
    };
    for (auto& projection : translated->predicate_columns) {
        RETURN_IF_ERROR(translate_root_projection(&projection));
    }
    for (auto& projection : translated->non_predicate_columns) {
        RETURN_IF_ERROR(translate_root_projection(&projection));
    }
    *physical_request = std::move(translated);
    return Status::OK();
}

// Aggregate pushdown resolves a single primitive Parquet leaf from the projection. It shares the
// same physical ParquetColumnSchema traversal as normal scans, so nested aggregate projections need
// the same semantic-to-physical translation before min/max leaf lookup.
static Status translate_aggregate_request_to_physical(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileAggregateRequest& semantic_request,
        format::FileAggregateRequest* physical_request) {
    DORIS_CHECK(physical_request != nullptr);
    *physical_request = semantic_request;
    for (auto& column : physical_request->columns) {
        const auto local_id = column.projection.local_id();
        if (local_id < 0 || local_id >= static_cast<int32_t>(file_schema.size())) {
            return Status::InvalidArgument("Invalid parquet aggregate column id {}", local_id);
        }
        format::LocalColumnIndex physical_projection;
        RETURN_IF_ERROR(translate_projection_to_physical(*file_schema[local_id], column.projection,
                                                         &physical_projection));
        column.projection = std::move(physical_projection);
    }
    return Status::OK();
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
    field->type = column_schema.type;
    field->children.clear();
    if (column_schema.kind == ParquetColumnSchemaKind::MAP) {
        // Expose semantic Doris MAP children through FileReader::get_schema(). The Parquet
        // key_value/entry wrapper remains private in ParquetColumnSchema and is restored only when
        // translating FileScanRequest projections for the physical reader path.
        if (column_schema.children.empty()) {
            return;
        }
        const auto& entry_schema = *column_schema.children[0];
        DORIS_CHECK(entry_schema.children.size() == 2);
        field->children.reserve(entry_schema.children.size());
        for (const auto& child : entry_schema.children) {
            format::ColumnDefinition child_field;
            _fill_column_definition(*child, &child_field);
            field->children.push_back(std::move(child_field));
        }
        return;
    }
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
                             std::optional<format::GlobalRowIdContext> global_rowid_context)
        : FileReader(system_properties, file_description, io_ctx, profile),
          _global_rowid_context(global_rowid_context) {}

ParquetReader::~ParquetReader() = default;

Status ParquetReader::init(RuntimeState* state) {
    RETURN_IF_ERROR(format::FileReader::init(state));
    if (_profile != nullptr) {
        COUNTER_UPDATE(_parquet_profile.file_reader_create_time,
                       _reader_statistics.file_reader_create_time);
        COUNTER_UPDATE(_parquet_profile.open_file_num, _reader_statistics.open_file_num);
    }
    _state = std::make_unique<ParquetReaderScanState>();
    _state->enable_bloom_filter =
            state != nullptr && state->query_options().enable_parquet_filter_by_bloom_filter;
    if (state != nullptr) {
        _state->timezone = &state->timezone_obj();
        _state->scheduler.set_timezone(&state->timezone_obj());
        _state->scheduler.set_enable_strict_mode(state->enable_strict_mode());
    }
    // Open parquet file and parse metadata to get file schema.
    RETURN_IF_ERROR(_state->file_context.open(_tracing_file_reader, _io_ctx.get()));
    // Build file schema from parquet metadata.
    // A file reader may expose raw file identifiers, such as Parquet field_id, through ColumnDefinition::identifier
    RETURN_IF_ERROR(
            build_parquet_column_schema(*_state->file_context.schema, &_state->file_schema));
    return Status::OK();
}

Status ParquetReader::get_schema(std::vector<format::ColumnDefinition>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("file_schema is null");
    }
    file_schema->clear();
    if (_state == nullptr || _state->file_context.schema == nullptr) {
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

Status ParquetReader::open(std::shared_ptr<format::FileScanRequest> request) {
    if (_state == nullptr || _state->file_context.metadata == nullptr ||
        _state->file_context.schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    auto request_snapshot = request;
    DORIS_CHECK(request_snapshot != nullptr);
    RETURN_IF_ERROR(format::FileReader::open(std::move(request)));
    // The table layer builds FileScanRequest from FileReader::get_schema(), so nested paths use the
    // semantic ColumnDefinition shape. Parquet scan planning and column readers consume the private
    // ParquetColumnSchema physical shape, so keep the public request contract semantic and adapt it
    // at this boundary.
    std::shared_ptr<format::FileScanRequest> physical_request;
    RETURN_IF_ERROR(translate_request_to_physical(_state->file_schema, *request_snapshot,
                                                  &physical_request));
    request_snapshot = std::move(physical_request);
    _request = request_snapshot;

    const int num_fields = static_cast<int>(_state->file_schema.size());
    for (const auto& column_filter : request_snapshot->column_predicate_filters) {
        const auto file_column_id = column_filter.effective_file_column_id();
        if (!file_column_id.is_valid() || file_column_id.value() >= num_fields) {
            return Status::InvalidArgument("Invalid parquet filter top-level local id {}",
                                           file_column_id.value());
        }
    }

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

    RowGroupScanPlan row_group_plan;
    ParquetScanRange scan_range;
    scan_range.start_offset = _file_description->range_start_offset;
    scan_range.size = _file_description->range_size;
    scan_range.file_size = _file_description->file_size;
    // Get selected ranges in row groups according to metadata (Row-Group level index and Page Index including Zonemap, Dictionary, Bloom Filter).
    RETURN_IF_ERROR(plan_parquet_row_groups(
            *_state->file_context.metadata, _state->file_context.file_reader.get(),
            _state->file_schema, *request_snapshot, scan_range, _state->enable_bloom_filter,
            &row_group_plan, _state->timezone));
    if (_profile != nullptr) {
        const auto& pruning_stats = row_group_plan.pruning_stats;
        COUNTER_UPDATE(_parquet_profile.filtered_row_groups,
                       pruning_stats.total_row_groups - pruning_stats.selected_row_groups);
        COUNTER_UPDATE(_parquet_profile.filtered_row_groups_by_min_max,
                       pruning_stats.filtered_row_groups_by_statistics);
        COUNTER_UPDATE(_parquet_profile.filtered_row_groups_by_dictionary,
                       pruning_stats.filtered_row_groups_by_dictionary);
        COUNTER_UPDATE(_parquet_profile.filtered_row_groups_by_bloom_filter,
                       pruning_stats.filtered_row_groups_by_bloom_filter);
        COUNTER_UPDATE(_parquet_profile.to_read_row_groups, pruning_stats.selected_row_groups);
        COUNTER_UPDATE(_parquet_profile.total_row_groups, pruning_stats.total_row_groups);
        COUNTER_UPDATE(_parquet_profile.selected_row_ranges, pruning_stats.selected_row_ranges);
        COUNTER_UPDATE(_parquet_profile.filtered_group_rows, pruning_stats.filtered_group_rows);
        COUNTER_UPDATE(_parquet_profile.filtered_page_rows, pruning_stats.filtered_page_rows);
        COUNTER_UPDATE(_parquet_profile.page_index_read_calls, pruning_stats.page_index_read_calls);
        COUNTER_UPDATE(_parquet_profile.bloom_filter_read_time,
                       pruning_stats.bloom_filter_read_time);
        COUNTER_UPDATE(_parquet_profile.row_group_filter_time, pruning_stats.row_group_filter_time);
        COUNTER_UPDATE(_parquet_profile.page_index_filter_time,
                       pruning_stats.page_index_filter_time);
        COUNTER_UPDATE(_parquet_profile.read_page_index_time, pruning_stats.read_page_index_time);
    }
    _state->scan_plan = row_group_plan;
    _state->scheduler.set_page_skip_profile(
            {.skipped_pages = _parquet_profile.pages_skipped_by_data_page_filter,
             .skipped_bytes = _parquet_profile.data_page_filter_skip_bytes});
    _state->scheduler.set_global_rowid_context(_global_rowid_context);
    _state->scheduler.set_scan_profile({
            .raw_rows_read = _parquet_profile.raw_rows_read,
            .selected_rows = _parquet_profile.selected_rows,
            .rows_filtered_by_conjunct = _parquet_profile.rows_filtered_by_conjunct,
            .total_batches = _parquet_profile.total_batches,
            .empty_selection_batches = _parquet_profile.empty_selection_batches,
            .range_gap_skipped_rows = _parquet_profile.range_gap_skipped_rows,
            .column_read_time = _parquet_profile.column_read_time,
            .predicate_filter_time = _parquet_profile.predicate_filter_time,
            .column_reader_profile =
                    {
                            .reader_read_rows = _parquet_profile.reader_read_rows,
                            .reader_skip_rows = _parquet_profile.reader_skip_rows,
                            .reader_select_rows = _parquet_profile.reader_select_rows,
                            .arrow_read_records_time = _parquet_profile.arrow_read_records_time,
                            .materialization_time = _parquet_profile.materialization_time,
                    },
    });
    _state->scheduler.set_plan(std::move(row_group_plan));
    _eof = _state->scheduler.empty();
    return Status::OK();
}

Status ParquetReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    if (_state == nullptr || _state->file_context.file_reader == nullptr ||
        _state->file_context.schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    *rows = 0;
    if (_eof) {
        *eof = true;
        return Status::OK();
    }
    auto request_snapshot = _request;
    if (request_snapshot == nullptr) {
        return Status::Cancelled("ParquetReader is closed");
    }

    RETURN_IF_ERROR(_state->scheduler.read_next_batch(_state->file_context, _state->file_schema,
                                                      *request_snapshot, file_block, rows, eof));
    _eof = *eof;
    return Status::OK();
}

Status ParquetReader::get_aggregate_result(const format::FileAggregateRequest& request,
                                           format::FileAggregateResult* result) {
    DORIS_CHECK(result != nullptr);
    if (_state == nullptr || _state->file_context.metadata == nullptr ||
        _state->file_context.schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    result->count = 0;
    result->columns.clear();
    if (request.agg_type != TPushAggOp::type::COUNT &&
        request.agg_type != TPushAggOp::type::MINMAX) {
        return Status::NotSupported("Unsupported parquet aggregate pushdown type {}",
                                    request.agg_type);
    }

    // Aggregate row count in all selected row groups. For MIN/MAX aggregate, this is used to determine whether there is no row group selected.
    for (const auto& row_group_plan : _state->scan_plan.row_groups) {
        auto row_group_metadata =
                _state->file_context.metadata->RowGroup(row_group_plan.row_group_id);
        DORIS_CHECK(row_group_metadata != nullptr);
        result->count += row_group_metadata->num_rows();
    }
    if (request.agg_type == TPushAggOp::type::COUNT) {
        return Status::OK();
    }

    format::FileAggregateRequest physical_request;
    RETURN_IF_ERROR(translate_aggregate_request_to_physical(_state->file_schema, request,
                                                            &physical_request));

    result->columns.resize(physical_request.columns.size());
    for (size_t request_column_idx = 0; request_column_idx < physical_request.columns.size();
         ++request_column_idx) {
        const auto file_column_id =
                physical_request.columns[request_column_idx].projection.local_id();
        if (file_column_id < 0 ||
            file_column_id >= static_cast<int32_t>(_state->file_schema.size())) {
            return Status::InvalidArgument("Invalid parquet aggregate column id {}",
                                           file_column_id);
        }
        const auto& column_schema = _state->file_schema[file_column_id];
        DORIS_CHECK(column_schema != nullptr);
        const ParquetColumnSchema* leaf_schema = nullptr;
        RETURN_IF_ERROR(find_projected_minmax_leaf(
                *column_schema, physical_request.columns[request_column_idx].projection,
                &leaf_schema));
        DORIS_CHECK(leaf_schema != nullptr);

        auto& aggregate_column = result->columns[request_column_idx];
        aggregate_column.projection = request.columns[request_column_idx].projection;
        for (const auto& row_group_plan : _state->scan_plan.row_groups) {
            auto row_group_metadata =
                    _state->file_context.metadata->RowGroup(row_group_plan.row_group_id);
            DORIS_CHECK(row_group_metadata != nullptr);
            auto column_chunk = row_group_metadata->ColumnChunk(leaf_schema->leaf_column_id);
            DORIS_CHECK(column_chunk != nullptr);
            const auto statistics = ParquetStatisticsUtils::TransformColumnStatistics(
                    *leaf_schema, column_chunk->statistics(), _state->timezone);
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
    if (_state != nullptr) {
        RETURN_IF_ERROR(_state->file_context.close());
    }
    return FileReader::close();
}

void ParquetReader::_init_profile() {
    if (_profile != nullptr) {
        static const char* parquet_profile = "ParquetReader";
        ADD_TIMER_WITH_LEVEL(_profile, parquet_profile, 1);

        _parquet_profile.filtered_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsFiltered", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_row_groups_by_min_max = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsFilteredByMinMax", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_row_groups_by_dictionary = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsFilteredByDictionary", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_row_groups_by_bloom_filter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsFilteredByBloomFilter", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.to_read_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsReadNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.total_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowGroupsTotalNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.selected_row_ranges = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "SelectedRowRanges", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_group_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByGroup", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_page_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByPage", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.pages_skipped_by_data_page_filter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PagesSkippedByDataPageFilter", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.data_page_filter_skip_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "DataPageFilterSkipBytes", TUnit::BYTES, parquet_profile, 1);
        _parquet_profile.selected_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "SelectedRows", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.rows_filtered_by_conjunct = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RowsFilteredByConjunct", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.total_batches = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "TotalBatches", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.empty_selection_batches = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "EmptySelectionBatches", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.range_gap_skipped_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RangeGapSkippedRows", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.reader_read_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ReaderReadRows", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.reader_skip_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ReaderSkipRows", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.reader_select_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ReaderSelectRows", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.arrow_read_records_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ArrowReadRecordsTime", parquet_profile, 1);
        _parquet_profile.materialization_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "MaterializationTime", parquet_profile, 1);
        _parquet_profile.lazy_read_filtered_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByLazyRead", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredBytes", TUnit::BYTES, parquet_profile, 1);
        _parquet_profile.raw_rows_read = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RawRowsRead", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.column_read_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ColumnReadTime", parquet_profile, 1);
        _parquet_profile.parse_meta_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ParseMetaTime", parquet_profile, 1);
        _parquet_profile.parse_footer_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ParseFooterTime", parquet_profile, 1);
        _parquet_profile.file_reader_create_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "FileReaderCreateTime", parquet_profile, 1);
        _parquet_profile.open_file_num =
                ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "FileNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_index_read_calls =
                ADD_COUNTER_WITH_LEVEL(_profile, "PageIndexReadCalls", TUnit::UNIT, 1);
        _parquet_profile.page_index_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexFilterTime", parquet_profile, 1);
        _parquet_profile.read_page_index_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexReadTime", parquet_profile, 1);
        _parquet_profile.parse_page_index_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexParseTime", parquet_profile, 1);
        _parquet_profile.row_group_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RowGroupFilterTime", parquet_profile, 1);
        _parquet_profile.file_footer_read_calls =
                ADD_COUNTER_WITH_LEVEL(_profile, "FileFooterReadCalls", TUnit::UNIT, 1);
        _parquet_profile.file_footer_hit_cache =
                ADD_COUNTER_WITH_LEVEL(_profile, "FileFooterHitCache", TUnit::UNIT, 1);
        _parquet_profile.decompress_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecompressTime", parquet_profile, 1);
        _parquet_profile.decompress_cnt = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "DecompressCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_read_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageReadCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheWriteCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_compressed_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheCompressedWriteCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_decompressed_write_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheDecompressedWriteCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheHitCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_missing_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheMissingCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_compressed_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheCompressedHitCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_cache_decompressed_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "PageCacheDecompressedHitCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.decode_header_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageHeaderDecodeTime", parquet_profile, 1);
        _parquet_profile.read_page_header_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageHeaderReadTime", parquet_profile, 1);
        _parquet_profile.decode_value_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeValueTime", parquet_profile, 1);
        _parquet_profile.decode_dict_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeDictTime", parquet_profile, 1);
        _parquet_profile.decode_level_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeLevelTime", parquet_profile, 1);
        _parquet_profile.decode_null_map_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeNullMapTime", parquet_profile, 1);
        _parquet_profile.skip_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "SkipPageHeaderNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.parse_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ParsePageHeaderNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.predicate_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PredicateFilterTime", parquet_profile, 1);
        _parquet_profile.dict_filter_rewrite_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DictFilterRewriteTime", parquet_profile, 1);
        _parquet_profile.convert_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ConvertTime", parquet_profile, 1);
        _parquet_profile.bloom_filter_read_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "BloomFilterReadTime", parquet_profile, 1);
    }
}

} // namespace doris::parquet
