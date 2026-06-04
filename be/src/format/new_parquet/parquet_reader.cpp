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

#include "format/new_parquet/parquet_reader.h"

#include <algorithm>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/parquet_file_context.h"
#include "format/new_parquet/parquet_scan.h"
#include "format/new_parquet/parquet_statistics.h"
#include "format/new_parquet/reader/column_reader.h"
#include "runtime/runtime_state.h"

namespace doris::parquet {

struct ParquetReaderScanState {
    ParquetFileContext file_context;
    std::vector<std::unique_ptr<ParquetColumnSchema>> file_schema;
    RowGroupScanPlan scan_plan;
    ParquetScanScheduler scheduler;
    bool enable_bloom_filter = false;
};

static Status find_projected_minmax_leaf(const ParquetColumnSchema& column_schema,
                                         const reader::FieldProjection& projection,
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
    for (const auto& child_schema : column_schema.children) {
        if (child_schema->field_id == child_projection.field_id) {
            return find_projected_minmax_leaf(*child_schema, child_projection, leaf_schema);
        }
    }
    return Status::InvalidArgument("Invalid parquet aggregate projection field id {} for column {}",
                                   child_projection.field_id, column_schema.name);
}

void ParquetReader::_fill_schema_field(const ParquetColumnSchema& column_schema,
                                       reader::SchemaField* field) const {
    field->id = column_schema.field_id;
    field->name = column_schema.name;
    field->type = column_schema.type;
    field->children.clear();
    field->children.reserve(column_schema.children.size());
    for (const auto& child : column_schema.children) {
        reader::SchemaField child_field;
        _fill_schema_field(*child, &child_field);
        field->children.push_back(std::move(child_field));
    }
}

ParquetReader::ParquetReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                             std::unique_ptr<io::FileDescription>& file_description,
                             std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile)
        : FileReader(system_properties, file_description, io_ctx, profile) {}

ParquetReader::~ParquetReader() = default;

Status ParquetReader::init(RuntimeState* state) {
    RETURN_IF_ERROR(reader::FileReader::init(state));
    _state = std::make_unique<ParquetReaderScanState>();
    _state->enable_bloom_filter =
            state != nullptr && state->query_options().enable_parquet_filter_by_bloom_filter;
    // Open parquet file and parse metadata and file schema.
    RETURN_IF_ERROR(_state->file_context.open(_tracing_file_reader, _io_ctx.get()));
    RETURN_IF_ERROR(
            build_parquet_column_schema(*_state->file_context.schema, &_state->file_schema));
    return Status::OK();
}

Status ParquetReader::get_schema(std::vector<reader::SchemaField>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("file_schema is null");
    }
    file_schema->clear();
    if (_state == nullptr || _state->file_context.schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }

    file_schema->reserve(_state->file_schema.size());
    for (size_t column_idx = 0; column_idx < _state->file_schema.size(); ++column_idx) {
        reader::SchemaField field;
        _fill_schema_field(*_state->file_schema[column_idx], &field);
        field.id = static_cast<reader::ColumnId>(column_idx);
        file_schema->push_back(std::move(field));
    }
    return Status::OK();
}

Status ParquetReader::open(std::unique_ptr<reader::FileScanRequest>& request) {
    if (_state == nullptr || _state->file_context.metadata == nullptr ||
        _state->file_context.schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    RETURN_IF_ERROR(reader::FileReader::open(request));

    const int num_fields = static_cast<int>(_state->file_schema.size());
    for (const auto& column_filter : _request->column_predicate_filters) {
        const auto file_column_id = column_filter.file_column_id;
        if (file_column_id < 0 || file_column_id >= num_fields) {
            return Status::InvalidArgument("Invalid parquet filter top-level field id {}",
                                           file_column_id);
        }
    }

    // `_request->column_positions.empty()` means all columns are needed by table reader
    if (_request->column_positions.empty()) {
        for (const auto& col : _request->predicate_columns) {
            _request->column_positions.emplace(col.field_id, col.field_id);
        }
        for (const auto& col : _request->non_predicate_columns) {
            _request->column_positions.emplace(col.field_id, col.field_id);
        }
    }

    // Column validation for .
    for (const auto& col : _request->predicate_columns) {
        DORIS_CHECK(_request->column_positions.count(col.field_id) > 0);
        if (col.field_id == ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID) {
            continue;
        }
        DORIS_CHECK(col.field_id >= 0 && col.field_id < num_fields);
    }
    for (const auto& col : _request->non_predicate_columns) {
        DORIS_CHECK(_request->column_positions.count(col.field_id) > 0);
        if (col.field_id == ParquetColumnReaderFactory::ROW_POSITION_COLUMN_ID) {
            continue;
        }
        DORIS_CHECK(col.field_id >= 0 && col.field_id < num_fields);
    }
    // Validation complete

    RowGroupScanPlan row_group_plan;
    ParquetScanRange scan_range;
    scan_range.start_offset = _file_description->range_start_offset;
    scan_range.size = _file_description->range_size;
    scan_range.file_size = _file_description->file_size;
    // Get selected ranges in row groups according to metadata (Row-Group level index and Page Index including Zonemap, Dictionary, Bloom Filter).
    RETURN_IF_ERROR(plan_parquet_row_groups(*_state->file_context.metadata,
                                            _state->file_context.file_reader.get(),
                                            _state->file_schema, *_request, scan_range,
                                            _state->enable_bloom_filter, &row_group_plan));
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
    }
    _state->scan_plan = row_group_plan;
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

    RETURN_IF_ERROR(_state->scheduler.read_next_batch(_state->file_context, _state->file_schema,
                                                      *_request, file_block, rows, eof));
    _eof = *eof;
    return Status::OK();
}

Status ParquetReader::get_aggregate_result(const reader::FileAggregateRequest& request,
                                           reader::FileAggregateResult* result) {
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

    result->columns.resize(request.columns.size());
    for (size_t request_column_idx = 0; request_column_idx < request.columns.size();
         ++request_column_idx) {
        const auto file_column_id = request.columns[request_column_idx].projection.field_id;
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

        auto& aggregate_column = result->columns[request_column_idx];
        aggregate_column.projection = request.columns[request_column_idx].projection;
        for (const auto& row_group_plan : _state->scan_plan.row_groups) {
            auto row_group_metadata =
                    _state->file_context.metadata->RowGroup(row_group_plan.row_group_id);
            DORIS_CHECK(row_group_metadata != nullptr);
            auto column_chunk = row_group_metadata->ColumnChunk(leaf_schema->leaf_column_id);
            DORIS_CHECK(column_chunk != nullptr);
            const auto statistics = ParquetStatisticsUtils::TransformColumnStatistics(
                    *leaf_schema, column_chunk->statistics());
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
        _state = std::make_unique<ParquetReaderScanState>();
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
