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

#include "format_v2/parquet/parquet_scan.h"

#include <parquet/encoding.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <ranges>
#include <set>
#include <unordered_set>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr_context.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/parquet_statistics.h"
#include "format_v2/parquet/reader/global_rowid_column_reader.h"
#include "format_v2/parquet/reader/native_column_reader.h"
#include "format_v2/parquet/reader/row_position_column_reader.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::format::parquet {

namespace detail {

std::vector<size_t> order_adaptive_predicates(
        const std::vector<size_t>& positions,
        const std::unordered_map<size_t, AdaptivePredicateStats>& stats) {
    if (std::ranges::any_of(positions, [&](size_t position) {
            const auto it = stats.find(position);
            return it == stats.end() || it->second.samples == 0;
        })) {
        return positions;
    }
    auto ordered = positions;
    std::stable_sort(ordered.begin(), ordered.end(), [&](size_t left, size_t right) {
        const auto score = [&](size_t position) {
            const auto& sample = stats.at(position);
            return sample.cost_per_input_row_ns / std::max(1.0 - sample.survival_ratio, 0.01);
        };
        return score(left) < score(right);
    });
    return ordered;
}

std::vector<size_t> adaptive_prefetch_prefix(
        const std::vector<size_t>& ordered_positions,
        const std::unordered_map<size_t, AdaptivePredicateStats>& stats,
        double minimum_reach_probability) {
    if (std::ranges::any_of(ordered_positions, [&](size_t position) {
            const auto it = stats.find(position);
            return it == stats.end() || it->second.samples == 0;
        })) {
        return ordered_positions;
    }
    std::vector<size_t> result;
    double reach_probability = 1;
    for (const size_t position : ordered_positions) {
        if (!result.empty() && reach_probability < minimum_reach_probability) {
            break;
        }
        result.push_back(position);
        reach_probability *= stats.at(position).survival_ratio;
    }
    return result;
}

bool should_sample_adaptive_predicate(size_t samples, size_t batch_sequence) {
    constexpr size_t WARMUP_SAMPLES = 8;
    constexpr size_t STEADY_STATE_INTERVAL = 16;
    return samples < WARMUP_SAMPLES || batch_sequence % STEADY_STATE_INTERVAL == 0;
}

} // namespace detail

namespace {

detail::PredicateConjunctSchedule build_predicate_conjunct_schedule(
        const format::FileScanRequest& request);

int64_t column_start_offset(const ::parquet::ColumnChunkMetaData& column_metadata) {
    return column_metadata.has_dictionary_page()
                   ? cast_set<int64_t>(column_metadata.dictionary_page_offset())
                   : cast_set<int64_t>(column_metadata.data_page_offset());
}

bool is_dictionary_data_encoding(tparquet::Encoding::type encoding) {
    return encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
           encoding == tparquet::Encoding::RLE_DICTIONARY;
}

bool is_level_encoding(tparquet::Encoding::type encoding) {
    return encoding == tparquet::Encoding::RLE || encoding == tparquet::Encoding::BIT_PACKED;
}

bool is_data_page_type(tparquet::PageType::type page_type) {
    return page_type == tparquet::PageType::DATA_PAGE ||
           page_type == tparquet::PageType::DATA_PAGE_V2;
}

bool is_fully_dictionary_encoded_chunk(const tparquet::ColumnMetaData& column_metadata) {
    if (!column_metadata.__isset.dictionary_page_offset ||
        column_metadata.dictionary_page_offset < 0) {
        return false;
    }

    const auto& encoding_stats = column_metadata.encoding_stats;
    if (!encoding_stats.empty()) {
        bool has_dictionary_data_page = false;
        for (const auto& encoding_stat : encoding_stats) {
            if (!is_data_page_type(encoding_stat.page_type) || encoding_stat.count <= 0) {
                continue;
            }
            if (!is_dictionary_data_encoding(encoding_stat.encoding)) {
                return false;
            }
            has_dictionary_data_page = true;
        }
        return has_dictionary_data_page;
    }

    bool has_dictionary_encoding = false;
    for (const auto encoding : column_metadata.encodings) {
        if (is_dictionary_data_encoding(encoding)) {
            has_dictionary_encoding = true;
            continue;
        }
        if (!is_level_encoding(encoding)) {
            return false;
        }
    }
    return has_dictionary_encoding;
}

bool supports_row_level_dictionary_filter(const ParquetColumnSchema& column_schema,
                                          const tparquet::ColumnMetaData& column_metadata) {
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE || column_schema.type == nullptr ||
        column_schema.max_repetition_level > 0) {
        return false;
    }
    if (!column_schema.type_descriptor.is_string_like ||
        column_metadata.type != tparquet::Type::BYTE_ARRAY) {
        return false;
    }
    // Row-level dictionary filtering consumes dictionary ids from DATA_PAGE payloads. It is exact
    // only when every data page is dictionary encoded. Mixed dictionary/plain chunks are left on
    // the normal decoded-value path, matching the safety rule used by StarRocks and Doris v1.
    return is_fully_dictionary_encoded_chunk(column_metadata);
}

void collect_all_leaf_column_ids(const ParquetColumnSchema& column_schema,
                                 std::unordered_set<int>* leaf_column_ids) {
    DORIS_CHECK(leaf_column_ids != nullptr);
    if (column_schema.kind == ParquetColumnSchemaKind::PRIMITIVE) {
        if (column_schema.leaf_column_id >= 0) {
            leaf_column_ids->insert(column_schema.leaf_column_id);
        }
        return;
    }
    for (const auto& child : column_schema.children) {
        DORIS_CHECK(child != nullptr);
        collect_all_leaf_column_ids(*child, leaf_column_ids);
    }
}

void collect_projected_leaf_column_ids(const ParquetColumnSchema& column_schema,
                                       const format::LocalColumnIndex& projection,
                                       std::unordered_set<int>* leaf_column_ids) {
    DORIS_CHECK(leaf_column_ids != nullptr);
    if (projection.project_all_children || projection.children.empty()) {
        collect_all_leaf_column_ids(column_schema, leaf_column_ids);
        return;
    }
    for (const auto& child_projection : projection.children) {
        const auto child_it =
                std::ranges::find_if(column_schema.children, [&](const auto& child_schema) {
                    return child_schema->local_id == child_projection.local_id();
                });
        DORIS_CHECK(child_it != column_schema.children.end());
        collect_projected_leaf_column_ids(**child_it, child_projection, leaf_column_ids);
    }
}

bool is_row_group_outside_range(const ::parquet::FileMetaData& metadata,
                                const ParquetScanRange& scan_range, int row_group_idx) {
    if (scan_range.size < 0) {
        return false;
    }
    const int64_t range_start_offset = scan_range.start_offset;
    const int64_t range_end_offset = range_start_offset + scan_range.size;
    DORIS_CHECK(range_start_offset >= 0);
    DORIS_CHECK(range_end_offset >= range_start_offset);
    if (range_start_offset == 0 &&
        (scan_range.file_size < 0 || range_end_offset >= scan_range.file_size)) {
        return false;
    }

    auto row_group_metadata = metadata.RowGroup(row_group_idx);
    DORIS_CHECK(row_group_metadata != nullptr);
    DORIS_CHECK(row_group_metadata->num_columns() > 0);
    const auto first_column = row_group_metadata->ColumnChunk(0);
    const auto last_column = row_group_metadata->ColumnChunk(row_group_metadata->num_columns() - 1);
    DORIS_CHECK(first_column != nullptr);
    DORIS_CHECK(last_column != nullptr);
    const int64_t row_group_start_offset = column_start_offset(*first_column);
    const int64_t row_group_end_offset =
            column_start_offset(*last_column) + last_column->total_compressed_size();
    const int64_t row_group_mid_offset =
            row_group_start_offset + (row_group_end_offset - row_group_start_offset) / 2;
    return row_group_mid_offset < range_start_offset || row_group_mid_offset >= range_end_offset;
}

std::vector<format::LocalColumnIndex> request_scan_columns(const format::FileScanRequest& request) {
    std::vector<format::LocalColumnIndex> scan_columns;
    scan_columns.reserve(request.predicate_columns.size() + request.non_predicate_columns.size());
    scan_columns.insert(scan_columns.end(), request.predicate_columns.begin(),
                        request.predicate_columns.end());
    for (const auto& column : request.non_predicate_columns) {
        if (!request.is_count_star_placeholder(column.column_id())) {
            scan_columns.push_back(column);
        }
    }
    return scan_columns;
}

std::vector<format::LocalColumnIndex> physical_non_predicate_columns(
        const format::FileScanRequest& request) {
    std::vector<format::LocalColumnIndex> columns;
    columns.reserve(request.non_predicate_columns.size());
    for (const auto& column : request.non_predicate_columns) {
        if (!request.is_count_star_placeholder(column.column_id())) {
            columns.push_back(column);
        }
    }
    return columns;
}

void materialize_count_star_placeholders(const format::FileScanRequest& request, size_t rows,
                                         Block* file_block) {
    DORIS_CHECK(file_block != nullptr);
    for (const auto& column : request.non_predicate_columns) {
        if (!request.is_count_star_placeholder(column.column_id())) {
            continue;
        }
        const auto block_position = request.local_positions.at(column.column_id()).value();
        auto placeholder = file_block->get_by_position(block_position).column->assert_mutable();
        DCHECK(placeholder->empty());
        placeholder->insert_many_defaults(rows);
        file_block->replace_by_position(block_position, std::move(placeholder));
    }
}

std::vector<ParquetPageCacheRange> build_row_group_prefetch_ranges(
        const tparquet::FileMetaData& metadata,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const std::vector<format::LocalColumnIndex>& scan_columns, int row_group_idx) {
    std::unordered_set<int> leaf_column_ids;
    for (const auto& projection : scan_columns) {
        const auto local_id = projection.local_id();
        if (local_id == format::ROW_POSITION_COLUMN_ID ||
            local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            continue;
        }
        DORIS_CHECK(local_id >= 0 && local_id < static_cast<int32_t>(file_schema.size()));
        DORIS_CHECK(file_schema[local_id] != nullptr);
        // Prefetch and merge-reader ranges must be physical leaf chunks, not Doris logical slots.
        // Example: for a struct column s<a:int,b:string>, projecting only s.a should include only
        // the Parquet leaf chunk of a. Projecting the whole struct includes both a and b.
        collect_projected_leaf_column_ids(*file_schema[local_id], projection, &leaf_column_ids);
    }

    DORIS_CHECK(row_group_idx >= 0 && row_group_idx < static_cast<int>(metadata.row_groups.size()));
    const auto& row_group_metadata = metadata.row_groups[row_group_idx];
    std::vector<int> ordered_leaf_column_ids(leaf_column_ids.begin(), leaf_column_ids.end());
    std::ranges::sort(ordered_leaf_column_ids);

    std::vector<ParquetPageCacheRange> ranges;
    ranges.reserve(ordered_leaf_column_ids.size());
    for (const auto leaf_column_id : ordered_leaf_column_ids) {
        DORIS_CHECK(leaf_column_id >= 0 &&
                    leaf_column_id < static_cast<int>(row_group_metadata.columns.size()));
        const auto& chunk = row_group_metadata.columns[leaf_column_id];
        if (!chunk.__isset.meta_data) {
            continue;
        }
        const auto& column_metadata = chunk.meta_data;
        const int64_t offset = column_metadata.__isset.dictionary_page_offset
                                       ? column_metadata.dictionary_page_offset
                                       : column_metadata.data_page_offset;
        const int64_t size = column_metadata.total_compressed_size;
        DORIS_CHECK(offset >= 0);
        if (size > 0) {
            ranges.push_back(ParquetPageCacheRange {.offset = offset, .size = size});
        }
    }
    return ranges;
}

Status select_row_groups_by_scan_range(const ::parquet::FileMetaData& metadata,
                                       const ParquetScanRange& scan_range,
                                       std::vector<int64_t>* row_group_first_rows,
                                       std::vector<int>* selected_row_groups) {
    DORIS_CHECK(row_group_first_rows != nullptr);
    DORIS_CHECK(selected_row_groups != nullptr);
    row_group_first_rows->assign(metadata.num_row_groups(), 0);
    selected_row_groups->clear();
    selected_row_groups->reserve(metadata.num_row_groups());
    int64_t next_row_group_first_row = 0;
    for (int row_group_idx = 0; row_group_idx < metadata.num_row_groups(); ++row_group_idx) {
        (*row_group_first_rows)[row_group_idx] = next_row_group_first_row;
        auto row_group_metadata = metadata.RowGroup(row_group_idx);
        DORIS_CHECK(row_group_metadata != nullptr);
        const int64_t row_group_rows = row_group_metadata->num_rows();
        if (row_group_rows < 0) {
            return Status::Corruption("Invalid negative row count in parquet row group {}",
                                      row_group_idx);
        }
        next_row_group_first_row += row_group_rows;
        if (!is_row_group_outside_range(metadata, scan_range, row_group_idx)) {
            selected_row_groups->push_back(row_group_idx);
        }
    }
    return Status::OK();
}

Status build_row_group_read_plans(
        const ::parquet::FileMetaData& metadata, ::parquet::ParquetFileReader* file_reader,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, const std::vector<int>& selected_row_groups,
        const std::vector<int64_t>& row_group_first_rows, RowGroupScanPlan* plan,
        const cctz::time_zone* timezone, const RuntimeState* runtime_state,
        ParquetFileContext* file_context) {
    DORIS_CHECK(plan != nullptr);
    plan->row_groups.reserve(selected_row_groups.size());
    std::unordered_set<int> requested_leaf_ids;
    if (file_context != nullptr) {
        for (const auto& projection : request_scan_columns(request)) {
            const auto local_id = projection.local_id();
            if (local_id < 0 || local_id >= static_cast<int32_t>(file_schema.size())) {
                continue;
            }
            collect_projected_leaf_column_ids(*file_schema[local_id], projection,
                                              &requested_leaf_ids);
        }
    }
    for (const auto row_group_idx : selected_row_groups) {
        DORIS_CHECK(row_group_idx >= 0);
        DORIS_CHECK(static_cast<size_t>(row_group_idx) < row_group_first_rows.size());
        auto row_group_metadata = metadata.RowGroup(row_group_idx);
        DORIS_CHECK(row_group_metadata != nullptr);
        const int64_t row_group_rows = row_group_metadata->num_rows();
        if (row_group_rows == 0) {
            continue;
        }

        RowGroupReadPlan row_group_plan;
        row_group_plan.row_group_id = row_group_idx;
        row_group_plan.first_file_row = row_group_first_rows[row_group_idx];
        row_group_plan.row_group_rows = row_group_rows;
        if (file_context != nullptr) {
            std::unordered_map<int, NativeParquetPageIndex> page_indexes;
            if (can_use_parquet_page_index(request, runtime_state)) {
                RETURN_IF_ERROR(file_context->load_native_page_indexes(
                        row_group_idx, requested_leaf_ids, &page_indexes,
                        &plan->pruning_stats.read_page_index_time,
                        &plan->pruning_stats.parse_page_index_time));
            }
            RETURN_IF_ERROR(select_row_group_ranges_by_native_page_index(
                    page_indexes, file_schema, request, row_group_rows,
                    &row_group_plan.selected_ranges, &row_group_plan.page_skip_plans,
                    &plan->pruning_stats, timezone, runtime_state));
            for (auto& [leaf_column_id, indexes] : page_indexes) {
                row_group_plan.offset_indexes.emplace(leaf_column_id,
                                                      std::move(indexes.offset_index));
            }
        } else {
            RETURN_IF_ERROR(select_row_group_ranges_by_page_index(
                    file_reader, file_schema, request, row_group_idx, row_group_rows,
                    &row_group_plan.selected_ranges, &row_group_plan.page_skip_plans,
                    &plan->pruning_stats, timezone, runtime_state));
        }
        if (row_group_plan.selected_ranges.empty()) {
            continue;
        }
        plan->pruning_stats.selected_row_ranges += row_group_plan.selected_ranges.size();
        plan->row_groups.push_back(std::move(row_group_plan));
    }
    return Status::OK();
}

} // namespace

Status plan_parquet_row_groups(const ::parquet::FileMetaData& metadata,
                               ::parquet::ParquetFileReader* file_reader,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request,
                               const ParquetScanRange& scan_range, bool enable_bloom_filter,
                               RowGroupScanPlan* plan, const cctz::time_zone* timezone,
                               const RuntimeState* runtime_state,
                               ParquetFileContext* file_context) {
    DORIS_CHECK(plan != nullptr);
    plan->row_groups.clear();
    plan->pruning_stats = ParquetPruningStats {};

    // Row-group planning flow:
    //
    //     parquet footer row groups
    //              |
    //              v
    //     split byte-range candidates
    //              |
    //              v
    //     row-group metadata pruning
    //     statistics/ZoneMap -> dictionary -> bloom filter
    //              |
    //              v
    //     page-index pruning per selected row group
    //              |
    //              v
    //     RowGroupReadPlan with selected row ranges
    //
    // Metadata pruning removes whole row groups before readers are opened. Page index pruning runs
    // only for remaining row groups and produces selected row ranges; the scan scheduler later skips
    // gaps between those ranges, while row-level VExpr conjuncts still run on loaded batches for
    // correctness.
    std::vector<int64_t> row_group_first_rows;
    std::vector<int> scan_range_selected_row_groups;
    RETURN_IF_ERROR(select_row_groups_by_scan_range(metadata, scan_range, &row_group_first_rows,
                                                    &scan_range_selected_row_groups));

    std::vector<int> metadata_selected_row_groups;
    RETURN_IF_ERROR(select_row_groups_by_metadata(
            metadata, file_reader, file_schema, request, &scan_range_selected_row_groups,
            &metadata_selected_row_groups, enable_bloom_filter, &plan->pruning_stats, timezone,
            runtime_state));

    RETURN_IF_ERROR(build_row_group_read_plans(metadata, file_reader, file_schema, request,
                                               metadata_selected_row_groups, row_group_first_rows,
                                               plan, timezone, runtime_state, file_context));
    plan->pruning_stats.selected_row_groups = plan->row_groups.size();
    return Status::OK();
}

namespace {

int64_t native_column_start_offset(const tparquet::ColumnMetaData& column_metadata) {
    return column_metadata.__isset.dictionary_page_offset ? column_metadata.dictionary_page_offset
                                                          : column_metadata.data_page_offset;
}

bool is_native_row_group_outside_range(const tparquet::RowGroup& row_group,
                                       const ParquetScanRange& scan_range) {
    if (scan_range.size < 0) {
        return false;
    }
    const int64_t range_start = scan_range.start_offset;
    const int64_t range_end = range_start + scan_range.size;
    DORIS_CHECK(range_start >= 0 && range_end >= range_start);
    if (range_start == 0 && (scan_range.file_size < 0 || range_end >= scan_range.file_size)) {
        return false;
    }
    if (row_group.columns.empty() || !row_group.columns.front().__isset.meta_data ||
        !row_group.columns.back().__isset.meta_data) {
        return false;
    }
    const auto& first = row_group.columns.front().meta_data;
    const auto& last = row_group.columns.back().meta_data;
    const int64_t group_start = native_column_start_offset(first);
    const int64_t group_end = native_column_start_offset(last) + last.total_compressed_size;
    const int64_t group_mid = group_start + (group_end - group_start) / 2;
    return group_mid < range_start || group_mid >= range_end;
}

Status select_native_row_groups_by_scan_range(const tparquet::FileMetaData& metadata,
                                              const ParquetScanRange& scan_range,
                                              std::vector<int64_t>* row_group_first_rows,
                                              std::vector<int>* selected_row_groups) {
    DORIS_CHECK(row_group_first_rows != nullptr && selected_row_groups != nullptr);
    row_group_first_rows->assign(metadata.row_groups.size(), 0);
    selected_row_groups->clear();
    int64_t next_first_row = 0;
    for (size_t row_group_idx = 0; row_group_idx < metadata.row_groups.size(); ++row_group_idx) {
        (*row_group_first_rows)[row_group_idx] = next_first_row;
        const auto& row_group = metadata.row_groups[row_group_idx];
        if (row_group.num_rows < 0) {
            return Status::Corruption("Invalid negative row count in parquet row group {}",
                                      row_group_idx);
        }
        next_first_row += row_group.num_rows;
        if (!is_native_row_group_outside_range(row_group, scan_range)) {
            selected_row_groups->push_back(cast_set<int>(row_group_idx));
        }
    }
    return Status::OK();
}

Status build_native_row_group_read_plans(
        const NativeParquetMetadata& metadata,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, const std::vector<int>& selected_row_groups,
        const std::vector<int64_t>& row_group_first_rows, RowGroupScanPlan* plan,
        const cctz::time_zone* timezone, const RuntimeState* runtime_state,
        ParquetFileContext* file_context) {
    DORIS_CHECK(plan != nullptr && file_context != nullptr);
    const auto& thrift = metadata.to_thrift();
    std::unordered_set<int> requested_leaf_ids;
    for (const auto& projection : request_scan_columns(request)) {
        const auto local_id = projection.local_id();
        if (local_id < 0 || local_id >= static_cast<int32_t>(file_schema.size())) {
            continue;
        }
        collect_projected_leaf_column_ids(*file_schema[local_id], projection, &requested_leaf_ids);
    }
    plan->row_groups.reserve(selected_row_groups.size());
    for (const int row_group_idx : selected_row_groups) {
        const auto& row_group = thrift.row_groups[row_group_idx];
        if (row_group.num_rows == 0) {
            continue;
        }
        RowGroupReadPlan row_group_plan;
        row_group_plan.row_group_id = row_group_idx;
        row_group_plan.first_file_row = row_group_first_rows[row_group_idx];
        row_group_plan.row_group_rows = row_group.num_rows;
        std::unordered_map<int, NativeParquetPageIndex> page_indexes;
        if (can_use_parquet_page_index(request, runtime_state)) {
            RETURN_IF_ERROR(file_context->load_native_page_indexes(
                    row_group_idx, requested_leaf_ids, &page_indexes,
                    &plan->pruning_stats.read_page_index_time,
                    &plan->pruning_stats.parse_page_index_time));
        }
        RETURN_IF_ERROR(select_row_group_ranges_by_native_page_index(
                page_indexes, file_schema, request, row_group.num_rows,
                &row_group_plan.selected_ranges, &row_group_plan.page_skip_plans,
                &plan->pruning_stats, timezone, runtime_state));
        for (auto& [leaf_column_id, indexes] : page_indexes) {
            row_group_plan.offset_indexes.emplace(leaf_column_id, std::move(indexes.offset_index));
        }
        if (row_group_plan.selected_ranges.empty()) {
            continue;
        }
        plan->pruning_stats.selected_row_ranges += row_group_plan.selected_ranges.size();
        plan->row_groups.push_back(std::move(row_group_plan));
    }
    return Status::OK();
}

} // namespace

Status plan_parquet_row_groups(const NativeParquetMetadata& metadata,
                               const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
                               const format::FileScanRequest& request,
                               const ParquetScanRange& scan_range, bool enable_bloom_filter,
                               RowGroupScanPlan* plan, const cctz::time_zone* timezone,
                               const RuntimeState* runtime_state,
                               ParquetFileContext* file_context) {
    DORIS_CHECK(plan != nullptr && file_context != nullptr);
    plan->row_groups.clear();
    plan->pruning_stats = {};
    std::vector<int64_t> row_group_first_rows;
    std::vector<int> scan_range_selected;
    RETURN_IF_ERROR(select_native_row_groups_by_scan_range(
            metadata.to_thrift(), scan_range, &row_group_first_rows, &scan_range_selected));
    std::vector<int> metadata_selected;
    RETURN_IF_ERROR(select_row_groups_by_metadata(
            metadata.to_thrift(), file_schema, request, &scan_range_selected, &metadata_selected,
            enable_bloom_filter, &plan->pruning_stats, timezone, runtime_state, file_context));
    RETURN_IF_ERROR(build_native_row_group_read_plans(metadata, file_schema, request,
                                                      metadata_selected, row_group_first_rows, plan,
                                                      timezone, runtime_state, file_context));
    plan->pruning_stats.selected_row_groups = plan->row_groups.size();
    return Status::OK();
}

namespace {

using DictionaryResidualConjunct = std::pair<VExprContextSPtr, VExprSPtr>;
using DictionaryResidualConjuncts = std::vector<DictionaryResidualConjunct>;

void update_counter_if_not_null(RuntimeProfile::Counter* counter, int64_t value) {
    if (counter != nullptr) {
        COUNTER_UPDATE(counter, value);
    }
}

uint16_t apply_filter_to_selection(const IColumn::Filter& filter, SelectionVector* selection,
                                   uint16_t selected_rows) {
    uint16_t new_selected_rows = 0;
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        const auto row_idx = selection->get_index(selection_idx);
        if (filter[row_idx] != 0) {
            selection->set_index(new_selected_rows++, static_cast<SelectionVector::Index>(row_idx));
        }
    }
    return new_selected_rows;
}

Status execute_compact_filter_conjuncts(const VExprContextSPtrs& conjuncts, size_t rows,
                                        Block* file_block, IColumn::Filter* compact_filter,
                                        bool* can_filter_all) {
    DORIS_CHECK(compact_filter != nullptr);
    DORIS_CHECK(can_filter_all != nullptr);
    compact_filter->resize_fill(rows, 1);
    *can_filter_all = false;
    for (const auto& conjunct : conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        IColumn::Filter filter(rows, 1);
        bool conjunct_can_filter_all = false;
        RETURN_IF_ERROR(conjunct->execute_filter(file_block, filter.data(), rows, false,
                                                 &conjunct_can_filter_all));
        if (conjunct_can_filter_all) {
            std::ranges::fill(*compact_filter, 0);
            *can_filter_all = true;
            break;
        }
        for (size_t row = 0; row < rows; ++row) {
            (*compact_filter)[row] &= filter[row];
        }
    }
    return Status::OK();
}

Status execute_compact_dictionary_residual_conjuncts(const DictionaryResidualConjuncts& conjuncts,
                                                     size_t rows, Block* file_block,
                                                     IColumn::Filter* compact_filter,
                                                     bool* can_filter_all) {
    DORIS_CHECK(compact_filter != nullptr);
    DORIS_CHECK(can_filter_all != nullptr);
    compact_filter->resize_fill(rows, 1);
    *can_filter_all = false;
    for (const auto& [owner_context, residual_expr] : conjuncts) {
        DORIS_CHECK(owner_context != nullptr);
        DORIS_CHECK(residual_expr != nullptr);
        IColumn::Filter filter(rows, 1);
        bool conjunct_can_filter_all = false;
        RETURN_IF_ERROR(residual_expr->execute_filter(owner_context.get(), file_block,
                                                      filter.data(), rows, false,
                                                      &conjunct_can_filter_all));
        if (conjunct_can_filter_all) {
            std::ranges::fill(*compact_filter, 0);
            *can_filter_all = true;
            break;
        }
        for (size_t row = 0; row < rows; ++row) {
            (*compact_filter)[row] &= filter[row];
        }
    }
    return Status::OK();
}

Status execute_compact_delete_conjuncts(const VExprContextSPtrs& delete_conjuncts, size_t rows,
                                        Block* file_block, IColumn::Filter* compact_filter,
                                        bool* can_filter_all) {
    DORIS_CHECK(compact_filter != nullptr);
    DORIS_CHECK(can_filter_all != nullptr);
    compact_filter->resize_fill(rows, 1);
    *can_filter_all = false;
    for (const auto& delete_conjunct : delete_conjuncts) {
        DORIS_CHECK(delete_conjunct != nullptr);
        int result_column_id = -1;
        RETURN_IF_ERROR(delete_conjunct->root()->execute(delete_conjunct.get(), file_block,
                                                         &result_column_id));
        DORIS_CHECK(result_column_id >= 0 &&
                    result_column_id < static_cast<int>(file_block->columns()));
        const auto& delete_filter = assert_cast<const ColumnUInt8&>(
                                            *file_block->get_by_position(result_column_id).column)
                                            .get_data();
        DORIS_CHECK(delete_filter.size() == rows);
        bool has_kept_row = false;
        for (size_t row = 0; row < rows; ++row) {
            (*compact_filter)[row] &= !delete_filter[row];
            has_kept_row |= (*compact_filter)[row] != 0;
        }
        file_block->erase(result_column_id);
        if (!has_kept_row) {
            *can_filter_all = true;
            break;
        }
    }
    return Status::OK();
}

Status execute_filter_conjuncts(const format::FileScanRequest& request, int64_t batch_rows,
                                Block* file_block, SelectionVector* selection,
                                uint16_t* selected_rows) {
    for (const auto& conjunct : request.conjuncts) {
        if (*selected_rows == 0) {
            break;
        }
        DORIS_CHECK(conjunct != nullptr);
        IColumn::Filter filter(static_cast<size_t>(batch_rows), 1);
        bool can_filter_all = false;
        RETURN_IF_ERROR(conjunct->execute_filter(file_block, filter.data(),
                                                 static_cast<size_t>(batch_rows), false,
                                                 &can_filter_all));
        *selected_rows =
                can_filter_all ? 0 : apply_filter_to_selection(filter, selection, *selected_rows);
    }
    return Status::OK();
}

Status execute_delete_conjuncts(const format::FileScanRequest& request, int64_t batch_rows,
                                Block* file_block, SelectionVector* selection,
                                uint16_t* selected_rows) {
    for (const auto& delete_conjunct : request.delete_conjuncts) {
        if (*selected_rows == 0) {
            break;
        }
        DORIS_CHECK(delete_conjunct != nullptr);
        int result_column_id = -1;
        RETURN_IF_ERROR(delete_conjunct->root()->execute(delete_conjunct.get(), file_block,
                                                         &result_column_id));
        DORIS_CHECK(result_column_id >= 0 &&
                    result_column_id < static_cast<int>(file_block->columns()));
        const auto& delete_filter = assert_cast<const ColumnUInt8&>(
                                            *file_block->get_by_position(result_column_id).column)
                                            .get_data();
        DORIS_CHECK(delete_filter.size() == static_cast<size_t>(batch_rows));
        IColumn::Filter keep_filter(static_cast<size_t>(batch_rows), 1);
        bool has_kept_row = false;
        for (size_t row = 0; row < static_cast<size_t>(batch_rows); ++row) {
            keep_filter[row] = !delete_filter[row];
            has_kept_row |= keep_filter[row] != 0;
        }
        file_block->erase(result_column_id);
        *selected_rows =
                !has_kept_row ? 0
                              : apply_filter_to_selection(keep_filter, selection, *selected_rows);
    }
    return Status::OK();
}

} // namespace

uint16_t apply_compact_filter_to_selection(const IColumn::Filter& filter,
                                           SelectionVector* selection, uint16_t selected_rows) {
    DORIS_CHECK(selection != nullptr);
    DORIS_CHECK(filter.size() == selected_rows);
    uint16_t new_selected_rows = 0;
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        if (filter[selection_idx] != 0) {
            selection->set_index(new_selected_rows++, static_cast<SelectionVector::Index>(
                                                              selection->get_index(selection_idx)));
        }
    }
    return new_selected_rows;
}

IColumn::Filter selection_to_filter(const SelectionVector& selection, uint16_t selected_rows,
                                    int64_t batch_rows) {
    IColumn::Filter filter(static_cast<size_t>(batch_rows), 0);
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        filter[selection.get_index(selection_idx)] = 1;
    }
    return filter;
}

Status execute_batch_filters(const format::FileScanRequest& request, int64_t batch_rows,
                             Block* file_block, SelectionVector* selection, uint16_t* selected_rows,
                             int64_t* conjunct_filtered_rows) {
    if (request.conjuncts.empty() && request.delete_conjuncts.empty()) {
        return Status::OK();
    }
    const auto selected_rows_before_conjunct = *selected_rows;
    RETURN_IF_ERROR(
            execute_filter_conjuncts(request, batch_rows, file_block, selection, selected_rows));
    if (conjunct_filtered_rows != nullptr) {
        *conjunct_filtered_rows += static_cast<int64_t>(selected_rows_before_conjunct) -
                                   static_cast<int64_t>(*selected_rows);
    }
    if (*selected_rows == 0) {
        return Status::OK();
    }
    return execute_delete_conjuncts(request, batch_rows, file_block, selection, selected_rows);
}

namespace {
int64_t count_range_rows(const std::vector<RowRange>& ranges) {
    int64_t rows = 0;
    for (const auto& range : ranges) {
        rows += range.length;
    }
    return rows;
}

void append_intersection(const RowRange& left, const RowRange& right,
                         std::vector<RowRange>* result) {
    const int64_t start = std::max(left.start, right.start);
    const int64_t end = std::min(left.start + left.length, right.start + right.length);
    if (start < end) {
        result->push_back(RowRange {.start = start, .length = end - start});
    }
}

std::vector<RowRange> filter_ranges_by_condition_cache(const std::vector<RowRange>& ranges,
                                                       const std::vector<bool>& cache,
                                                       int64_t row_group_first_row,
                                                       int64_t base_granule) {
    std::vector<RowRange> result;
    if (cache.empty()) {
        return ranges;
    }

    // Cache coordinates are file-global granules; RowRange coordinates are row-group-relative.
    // Walk every selected range in order and split it by granule. Granules covered by the bitmap
    // are kept only when the bit is true. Granules outside the bitmap are kept conservatively, so
    // an undersized or old-format cache entry cannot skip valid rows.
    for (const auto& range : ranges) {
        const int64_t global_start = row_group_first_row + range.start;
        const int64_t global_end = global_start + range.length;
        for (int64_t granule = global_start / ConditionCacheContext::GRANULE_SIZE;
             granule <= (global_end - 1) / ConditionCacheContext::GRANULE_SIZE; ++granule) {
            const int64_t cache_idx = granule - base_granule;
            const bool keep = cache_idx < 0 || static_cast<size_t>(cache_idx) >= cache.size() ||
                              cache[static_cast<size_t>(cache_idx)];
            if (!keep) {
                continue;
            }
            const int64_t granule_start = granule * ConditionCacheContext::GRANULE_SIZE;
            const int64_t granule_end = granule_start + ConditionCacheContext::GRANULE_SIZE;
            const RowRange file_granule_range {.start = granule_start - row_group_first_row,
                                               .length = granule_end - granule_start};
            append_intersection(range, file_granule_range, &result);
        }
    }
    return result;
}

} // namespace

void ParquetScanScheduler::set_plan(RowGroupScanPlan plan) {
    _row_group_plans = std::move(plan.row_groups);
    _condition_cache_filtered_rows = 0;
    _predicate_filtered_rows = 0;
    reset();
}

void ParquetScanScheduler::set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) {
    _condition_cache_ctx = std::move(ctx);
    if (!_condition_cache_ctx || !_condition_cache_ctx->filter_result || _row_group_plans.empty()) {
        return;
    }

    if (!_condition_cache_ctx->is_hit) {
        _condition_cache_ctx->base_granule =
                _row_group_plans.front().first_file_row / ConditionCacheContext::GRANULE_SIZE;
        const auto& last_plan = _row_group_plans.back();
        const int64_t end_granule = (last_plan.first_file_row + last_plan.row_group_rows +
                                     ConditionCacheContext::GRANULE_SIZE - 1) /
                                    ConditionCacheContext::GRANULE_SIZE;
        DORIS_CHECK(end_granule > _condition_cache_ctx->base_granule);
        _condition_cache_ctx->num_granules =
                std::min(_condition_cache_ctx->filter_result->size(),
                         static_cast<size_t>(end_granule - _condition_cache_ctx->base_granule));
        return;
    }

    std::vector<RowGroupReadPlan> filtered_plans;
    filtered_plans.reserve(_row_group_plans.size());
    for (auto& plan : _row_group_plans) {
        const int64_t old_rows = count_range_rows(plan.selected_ranges);
        plan.selected_ranges = filter_ranges_by_condition_cache(
                plan.selected_ranges, *_condition_cache_ctx->filter_result, plan.first_file_row,
                _condition_cache_ctx->base_granule);
        const int64_t new_rows = count_range_rows(plan.selected_ranges);
        _condition_cache_filtered_rows += old_rows - new_rows;
        if (!plan.selected_ranges.empty()) {
            filtered_plans.push_back(std::move(plan));
        }
    }
    _row_group_plans = std::move(filtered_plans);
    reset();
}

void ParquetScanScheduler::reset() {
    _next_row_group_plan_idx = 0;
    _raw_rows_read = 0;
    _predicate_schedule_request = nullptr;
    _predicate_schedule = {};
    _predicate_positions_scratch.clear();
    _predicate_indices_by_position_scratch.clear();
    _ordered_predicate_positions_scratch.clear();
    _predicate_batch_sequence = 0;
    reset_current_row_group();
}

void ParquetScanScheduler::reset_current_row_group() {
    // RuntimeProfile updates are amortized on the batch path, but a row-group transition destroys
    // the reader tree. Force the final delta out before clearing it so short row groups and early
    // EOF paths cannot lose their last decode/IO timings.
    flush_current_reader_profiles();
    _batches_since_profile_flush = 0;
    _has_current_row_group = false;
    _current_predicate_columns.clear();
    _current_non_predicate_columns.clear();
    _current_dictionary_filters.clear();
    _current_dictionary_residual_conjuncts.clear();
    _current_row_group_rows = 0;
    _current_row_group_id = -1;
    _current_row_group_rows_read = 0;
    _current_row_group_first_row = 0;
    _current_selected_ranges.clear();
    _current_offset_indexes.clear();
    _current_range_idx = 0;
    _current_range_rows_read = 0;
    // Readers are row-group scoped. If every remaining row was filtered, no future output can
    // observe the non-predicate readers' position, so dropping them together with their pending lag
    // avoids a useless end-of-row-group SkipRecords call. Example: predicate readers advance from 0
    // to 10,000 while lazy readers stay at 0; clearing both readers here is sufficient because the
    // next row group constructs a new set starting at its own row 0.
    _pending_non_predicate_skip_rows = 0;
    _current_predicate_prefetched = false;
    _current_non_predicate_prefetched = false;
    _current_merge_range_active = false;
}

void ParquetScanScheduler::flush_current_reader_profiles() {
    for (const auto& reader : _current_predicate_columns | std::views::values) {
        reader->flush_profile();
    }
    for (const auto& reader : _current_non_predicate_columns | std::views::values) {
        reader->flush_profile();
    }
}

const detail::PredicateConjunctSchedule& ParquetScanScheduler::predicate_conjunct_schedule(
        const format::FileScanRequest& request) {
    if (_predicate_schedule_request == &request) {
        return _predicate_schedule;
    }

    // FileScanRequest is frozen by ParquetReader::open(). Its address therefore identifies both
    // the conjunct set and local-position mapping for the scheduler lifetime.
    _predicate_schedule = build_predicate_conjunct_schedule(request);
    _predicate_schedule_request = &request;
    _predicate_positions_scratch.clear();
    _predicate_indices_by_position_scratch.clear();
    _predicate_positions_scratch.reserve(request.predicate_columns.size());
    _predicate_indices_by_position_scratch.reserve(request.predicate_columns.size());
    for (size_t idx = 0; idx < request.predicate_columns.size(); ++idx) {
        const auto position_it =
                request.local_positions.find(request.predicate_columns[idx].column_id());
        DORIS_CHECK(position_it != request.local_positions.end());
        const size_t position = position_it->second.value();
        _predicate_positions_scratch.push_back(position);
        _predicate_indices_by_position_scratch.emplace(position, idx);
    }
    return _predicate_schedule;
}

std::vector<format::LocalColumnIndex> ParquetScanScheduler::adaptive_predicate_prefetch_columns(
        const format::FileScanRequest& request) const {
    std::vector<size_t> positions;
    std::unordered_map<size_t, const format::LocalColumnIndex*> columns_by_position;
    positions.reserve(request.predicate_columns.size());
    columns_by_position.reserve(request.predicate_columns.size());
    for (const auto& column : request.predicate_columns) {
        const auto position_it = request.local_positions.find(column.column_id());
        DORIS_CHECK(position_it != request.local_positions.end());
        const size_t position = position_it->second.value();
        positions.push_back(position);
        columns_by_position.emplace(position, &column);
    }
    auto ordered = detail::order_adaptive_predicates(positions, _predicate_runtime_stats);
    ordered = detail::adaptive_prefetch_prefix(ordered, _predicate_runtime_stats, 0.25);
    std::vector<format::LocalColumnIndex> result;
    result.reserve(ordered.size());
    for (const size_t position : ordered) {
        result.push_back(*columns_by_position.at(position));
    }
    return result;
}

Status ParquetScanScheduler::open_next_row_group(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, bool* has_row_group) {
    *has_row_group = false;
    if (_next_row_group_plan_idx >= _row_group_plans.size()) {
        // The last row group's native readers have already been released by
        // reset_current_row_group(). Flush the shared merge reader now so its counters are visible
        // when EOF is returned and its bounded scratch does not survive until file close.
        file_context.reset_random_access_ranges();
        _current_merge_range_active = false;
        return Status::OK();
    }
    RowGroupReadPlan& row_group_plan = _row_group_plans[_next_row_group_plan_idx++];
    const int row_group_idx = row_group_plan.row_group_id;
    // Dictionary probes and data-page readers share the native metadata tree. Reset the previous
    // row-group merge reader before probing because dictionary-page offsets are not scan ordered.
    file_context.reset_random_access_ranges();
    _current_merge_range_active = false;

    const auto& row_group_metadata =
            file_context.native_metadata->to_thrift().row_groups[row_group_idx];
    _current_row_group_rows = row_group_metadata.num_rows;
    DORIS_CHECK(_current_row_group_rows == row_group_plan.row_group_rows);
    DORIS_CHECK(_current_row_group_rows > 0);
    _current_row_group_id = row_group_idx;
    _has_current_row_group = true;
    DORIS_CHECK(!row_group_plan.selected_ranges.empty());
    _current_row_group_first_row = row_group_plan.first_file_row;
    _current_row_group_rows_read = 0;
    _current_selected_ranges = row_group_plan.selected_ranges;
    _current_offset_indexes = std::move(row_group_plan.offset_indexes);
    // Condition Cache and split planning can narrow logical ranges without a physical OffsetIndex.
    // Native readers must keep the sequential level/value cursor path valid in that case; only a
    // PageIndex-derived skip plan requires the transferred indexes below.
    for (const auto& [leaf_column_id, skip_plan] : row_group_plan.page_skip_plans) {
        if (!_current_offset_indexes.contains(leaf_column_id)) {
            continue;
        }
        for (size_t page = 0; page < skip_plan.skipped_pages.size(); ++page) {
            if (!skip_plan.should_skip_page(page)) {
                continue;
            }
            if (_page_skip_profile.skipped_pages != nullptr) {
                COUNTER_UPDATE(_page_skip_profile.skipped_pages, 1);
            }
            if (_page_skip_profile.skipped_bytes != nullptr) {
                COUNTER_UPDATE(_page_skip_profile.skipped_bytes,
                               skip_plan.skipped_page_compressed_size(page));
            }
        }
    }
    _current_range_idx = 0;
    _current_range_rows_read = 0;
    _current_predicate_columns.clear();
    _current_non_predicate_columns.clear();
    _current_dictionary_filters.clear();
    RETURN_IF_ERROR(prepare_current_dictionary_filters(file_context, file_schema, request,
                                                       row_group_idx, row_group_metadata));
    // Dictionary probing is complete, so the native data-page readers can now share the same
    // row-group-scoped MergeRangeFileReader policy as v1. Sharing one wrapper is important: a
    // separate merge reader per leaf would duplicate its 128MB scratch capacity and defeat lazy
    // materialization for wide schemas.
    const auto native_ranges =
            build_row_group_prefetch_ranges(file_context.native_metadata->to_thrift(), file_schema,
                                            request_scan_columns(request), row_group_idx);
    _current_merge_range_active = file_context.set_native_random_access_ranges(
            native_ranges, detail::average_prefetch_range_size(native_ranges), _profile,
            _merge_read_slice_size);

    for (const auto& col : request.predicate_columns) {
        const auto local_id = col.local_id();
        if (_current_predicate_columns.contains(local_id)) {
            continue;
        }
        if (local_id == format::ROW_POSITION_COLUMN_ID) {
            _current_predicate_columns[local_id] = std::make_unique<RowPositionColumnReader>(
                    _current_row_group_first_row, _scan_profile.column_reader_profile);
            continue;
        }
        if (local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            DORIS_CHECK(_global_rowid_context.has_value());
            _current_predicate_columns[local_id] = std::make_unique<GlobalRowIdColumnReader>(
                    *_global_rowid_context, _current_row_group_first_row,
                    _scan_profile.column_reader_profile);
            continue;
        }

        DORIS_CHECK(local_id >= 0 && local_id < static_cast<int32_t>(file_schema.size()));
        const auto& column_schema = file_schema[local_id];
        DORIS_CHECK(column_schema != nullptr);
        std::unique_ptr<ParquetColumnReader> column_reader;
        RETURN_IF_ERROR(NativeColumnReader::create(
                *column_schema, &col, file_context.native_data_file(), file_context.native_metadata,
                row_group_idx, _current_selected_ranges, _current_offset_indexes, _timezone,
                file_context.native_io_ctx, _runtime_state, file_context.native_page_cache_enabled,
                file_context.native_page_cache_file_key,
                _current_dictionary_filters.contains(local_id), _scan_profile.column_reader_profile,
                &column_reader));
        _current_predicate_columns[local_id] = std::move(column_reader);
    }
    // Start warming filter-column chunks as soon as their row group is selected. The native
    // BufferedFileStreamReader later consumes the same Doris file-cache blocks; prefetch never
    // changes row/column materialization order.
    if (!_current_merge_range_active) {
        const auto prefetch_columns = adaptive_predicate_prefetch_columns(request);
        prefetch_current_row_group_columns(file_context, file_schema, prefetch_columns,
                                           &_current_predicate_prefetched);
    }
    for (const auto& col : request.non_predicate_columns) {
        const auto local_id = col.local_id();
        if (request.is_count_star_placeholder(col.column_id())) {
            continue;
        }
        if (local_id == format::ROW_POSITION_COLUMN_ID) {
            _current_non_predicate_columns[local_id] = std::make_unique<RowPositionColumnReader>(
                    _current_row_group_first_row, _scan_profile.column_reader_profile);
            continue;
        }
        if (local_id == format::GLOBAL_ROWID_COLUMN_ID) {
            DORIS_CHECK(_global_rowid_context.has_value());
            _current_non_predicate_columns[local_id] = std::make_unique<GlobalRowIdColumnReader>(
                    *_global_rowid_context, _current_row_group_first_row,
                    _scan_profile.column_reader_profile);
            continue;
        }
        DORIS_CHECK(local_id >= 0 && local_id < static_cast<int32_t>(file_schema.size()));
        const auto& column_schema = file_schema[local_id];
        DORIS_CHECK(column_schema != nullptr);
        std::unique_ptr<ParquetColumnReader> column_reader;
        RETURN_IF_ERROR(NativeColumnReader::create(
                *column_schema, &col, file_context.native_data_file(), file_context.native_metadata,
                row_group_idx, _current_selected_ranges, _current_offset_indexes, _timezone,
                file_context.native_io_ctx, _runtime_state, file_context.native_page_cache_enabled,
                file_context.native_page_cache_file_key, false, _scan_profile.column_reader_profile,
                &column_reader));
        _current_non_predicate_columns[local_id] = std::move(column_reader);
    }
    if (!_current_merge_range_active &&
        ((request.conjuncts.empty() && request.delete_conjuncts.empty()) ||
         _predicate_survival_ratio >= 0.8)) {
        // With no row-level filters there is no lazy-read decision to wait for, so start warming
        // output chunks immediately after their readers are created. Filtered scans still defer
        // this until at least one row survives the predicate phase.
        prefetch_current_row_group_columns(file_context, file_schema,
                                           physical_non_predicate_columns(request),
                                           &_current_non_predicate_prefetched);
    }
    *has_row_group = true;
    return Status::OK();
}

Status ParquetScanScheduler::skip_current_row_group_rows(int64_t rows) {
    DORIS_CHECK(rows >= 0);
    if (rows == 0) {
        return Status::OK();
    }
    if (_scan_profile.range_gap_skipped_rows != nullptr) {
        COUNTER_UPDATE(_scan_profile.range_gap_skipped_rows, rows);
    }
    for (const auto& column_reader : _current_predicate_columns | std::views::values) {
        RETURN_IF_ERROR(column_reader->skip(rows));
    }
    // Keep page-index/condition-cache gaps pending for lazy columns as well. For example, after a
    // fully filtered [0, 32) batch and a pruned [32, 96) gap, predicate readers are at 96 while lazy
    // readers remain at 0; one later skip(96) is cheaper than skip(32) followed by skip(64).
    DORIS_CHECK(_pending_non_predicate_skip_rows <= std::numeric_limits<int64_t>::max() - rows);
    _pending_non_predicate_skip_rows += rows;
    _current_row_group_rows_read += rows;
    return Status::OK();
}

Status ParquetScanScheduler::flush_pending_non_predicate_skip_rows() {
    if (_pending_non_predicate_skip_rows == 0) {
        return Status::OK();
    }
    for (const auto& column_reader : _current_non_predicate_columns | std::views::values) {
        RETURN_IF_ERROR(column_reader->skip(_pending_non_predicate_skip_rows));
    }
    _pending_non_predicate_skip_rows = 0;
    return Status::OK();
}

namespace {

detail::PredicateConjunctSchedule build_predicate_conjunct_schedule(
        const format::FileScanRequest& request) {
    std::unordered_set<size_t> predicate_block_positions;
    predicate_block_positions.reserve(request.predicate_columns.size());
    for (const auto& col : request.predicate_columns) {
        const auto position_it = request.local_positions.find(col.column_id());
        DORIS_CHECK(position_it != request.local_positions.end());
        predicate_block_positions.insert(position_it->second.value());
    }

    detail::PredicateConjunctSchedule schedule;
    for (const auto& conjunct : request.conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        DORIS_CHECK(conjunct->root() != nullptr);
        if (!conjunct->root()->is_safe_to_execute_on_selected_rows()) {
            // Round-by-round filtering can compact later predicate columns before evaluating
            // remaining expressions. Stateful functions such as random(1) and error-preserving
            // functions such as assert_true() must see the same full batch they saw before this
            // optimization, so any unsafe conjunct disables the per-column schedule for the batch.
            schedule.remaining_conjuncts = request.conjuncts;
            schedule.single_column_conjuncts.clear();
            return schedule;
        }
        std::set<int> referenced_positions;
        conjunct->root()->collect_slot_column_ids(referenced_positions);
        if (referenced_positions.size() != 1) {
            schedule.remaining_conjuncts.push_back(conjunct);
            continue;
        }
        const auto block_position = static_cast<size_t>(*referenced_positions.begin());
        if (!predicate_block_positions.contains(block_position)) {
            schedule.remaining_conjuncts.push_back(conjunct);
            continue;
        }
        schedule.single_column_conjuncts[block_position].push_back(conjunct);
    }
    return schedule;
}

bool can_evaluate_all_with_dictionary(const VExprContextSPtrs& conjuncts) {
    if (conjuncts.empty()) {
        return false;
    }
    return std::ranges::all_of(conjuncts, [](const auto& conjunct) {
        return conjunct != nullptr && conjunct->root() != nullptr &&
               conjunct->root()->can_evaluate_dictionary_filter();
    });
}

bool can_evaluate_dictionary_exactly(const VExprSPtr& expr) {
    DORIS_CHECK(expr != nullptr);
    const auto* compound_pred = dynamic_cast<const VCompoundPred*>(expr.get());
    if (compound_pred == nullptr) {
        return expr->can_evaluate_dictionary_filter();
    }
    if (compound_pred->op() != TExprOpcode::COMPOUND_AND &&
        compound_pred->op() != TExprOpcode::COMPOUND_OR) {
        return false;
    }
    return !expr->children().empty() &&
           std::ranges::all_of(expr->children(), [](const auto& child) {
               return can_evaluate_dictionary_exactly(child);
           });
}

void collect_dictionary_residual_exprs(const VExprContextSPtr& owner_context, const VExprSPtr& expr,
                                       DictionaryResidualConjuncts* residual_conjuncts) {
    DORIS_CHECK(owner_context != nullptr);
    DORIS_CHECK(expr != nullptr);
    DORIS_CHECK(residual_conjuncts != nullptr);

    if (can_evaluate_dictionary_exactly(expr)) {
        return;
    }

    // VCompoundPred dictionary evaluation is a conservative prefilter for AND when only some
    // children are dictionary-aware. Split AND so exact dictionary children are not executed again
    // on materialized rows. Do not split a non-exact OR: its branches cannot be evaluated
    // independently after a dictionary prefilter without changing the original boolean semantics.
    const auto* compound_pred = dynamic_cast<const VCompoundPred*>(expr.get());
    if (compound_pred != nullptr && compound_pred->op() == TExprOpcode::COMPOUND_AND) {
        for (const auto& child : expr->children()) {
            collect_dictionary_residual_exprs(owner_context, child, residual_conjuncts);
        }
        return;
    }

    residual_conjuncts->emplace_back(owner_context, expr);
}

DictionaryResidualConjuncts build_dictionary_residual_conjuncts(
        const VExprContextSPtrs& conjuncts) {
    DictionaryResidualConjuncts residual_conjuncts;
    for (const auto& conjunct : conjuncts) {
        DORIS_CHECK(conjunct != nullptr);
        collect_dictionary_residual_exprs(conjunct, conjunct->root(), &residual_conjuncts);
    }
    return residual_conjuncts;
}

uint16_t count_selected_rows(const IColumn::Filter& filter) {
    uint16_t selected_rows = 0;
    for (const auto value : filter) {
        selected_rows += value != 0;
    }
    return selected_rows;
}

IColumn::Filter build_dictionary_entry_filter(size_t block_position,
                                              const ParquetColumnSchema& column_schema,
                                              const VExprContextSPtrs& conjuncts,
                                              const IColumn& dictionary) {
    IColumn::Filter dictionary_filter(dictionary.size(), 1);
    DictionaryEvalContext ctx;
    auto& slot = ctx.slots
                         .emplace(static_cast<int>(block_position),
                                  DictionaryEvalContext::SlotDictionary {
                                          .data_type = column_schema.type, .values = {}})
                         .first->second;
    slot.values.reserve(1);
    for (size_t dictionary_id = 0; dictionary_id < dictionary.size(); ++dictionary_id) {
        Field value;
        dictionary.get(dictionary_id, value);
        slot.values.clear();
        slot.values.push_back(std::move(value));
        dictionary_filter[dictionary_id] = VExprContext::evaluate_dictionary_filter(
                                                   conjuncts, ctx) == ZoneMapFilterResult::kNoMatch
                                                   ? 0
                                                   : 1;
    }
    return dictionary_filter;
}

} // namespace

Status ParquetScanScheduler::prepare_current_dictionary_filters(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, int row_group_idx,
        const tparquet::RowGroup& row_group_metadata) {
    _current_dictionary_filters.clear();
    _current_dictionary_residual_conjuncts.clear();
    if (request.conjuncts.empty()) {
        return Status::OK();
    }
    detail::PredicateConjunctSchedule schedule;
    {
        SCOPED_TIMER(_scan_profile.dict_filter_expr_rewrite_time);
        schedule = predicate_conjunct_schedule(request);
    }
    if (schedule.single_column_conjuncts.empty()) {
        return Status::OK();
    }

    SCOPED_TIMER(_scan_profile.dict_filter_rewrite_time);
    for (const auto& col : request.predicate_columns) {
        const auto local_id = col.local_id();
        if (local_id < 0 || local_id >= static_cast<int32_t>(file_schema.size())) {
            continue;
        }
        const auto position_it = request.local_positions.find(col.column_id());
        DORIS_CHECK(position_it != request.local_positions.end());
        const auto block_position = static_cast<size_t>(position_it->second.value());
        const auto conjunct_it = schedule.single_column_conjuncts.find(block_position);
        if (conjunct_it == schedule.single_column_conjuncts.end() ||
            !can_evaluate_all_with_dictionary(conjunct_it->second)) {
            continue;
        }
        update_counter_if_not_null(_scan_profile.dict_filter_candidate_columns, 1);

        // This optimization is deliberately limited to single-column predicates with a dictionary
        // evaluable part. Mixed AND predicates are split so dictionary-covered children run as a
        // dict-id prefilter and residual children keep the normal row-level expression path.
        const auto& column_schema = file_schema[local_id];
        DORIS_CHECK(column_schema != nullptr);
        if (column_schema->leaf_column_id < 0 ||
            column_schema->leaf_column_id >= static_cast<int>(row_group_metadata.columns.size())) {
            update_counter_if_not_null(_scan_profile.dict_filter_unsupported_columns, 1);
            continue;
        }
        const auto& column_chunk = row_group_metadata.columns[column_schema->leaf_column_id];
        if (!column_chunk.__isset.meta_data ||
            !supports_row_level_dictionary_filter(*column_schema, column_chunk.meta_data)) {
            update_counter_if_not_null(_scan_profile.dict_filter_unsupported_columns, 1);
            continue;
        }

        std::unique_ptr<ParquetColumnReader> column_reader;
        RETURN_IF_ERROR(NativeColumnReader::create(
                *column_schema, &col, file_context.native_file, file_context.native_metadata,
                row_group_idx, _current_selected_ranges, _current_offset_indexes, _timezone,
                file_context.native_io_ctx, _runtime_state, file_context.native_page_cache_enabled,
                file_context.native_page_cache_file_key, true, _scan_profile.column_reader_profile,
                &column_reader));
        MutableColumnPtr dictionary_values;
        {
            SCOPED_TIMER(_scan_profile.dict_filter_read_dict_time);
            auto dictionary_result = column_reader->dictionary_values();
            if (!dictionary_result.has_value()) {
                update_counter_if_not_null(_scan_profile.dict_filter_read_failures, 1);
                // Dictionary filtering is optional: a probe failure must not reject a file that
                // the normal native read path can still decode.
                continue;
            }
            dictionary_values = std::move(dictionary_result).value();
        }

        // Build a safe dictionary prefilter from the dictionary-filter interface instead of
        // executing the row expression on a temporary dictionary block. For compound AND,
        // VCompoundPred intentionally evaluates only dictionary-capable children, so residual
        // predicates still run later on surviving rows.
        IColumn::Filter dictionary_filter;
        DictionaryResidualConjuncts residual_conjuncts;
        {
            SCOPED_TIMER(_scan_profile.dict_filter_build_time);
            dictionary_filter = build_dictionary_entry_filter(
                    block_position, *column_schema, conjunct_it->second, *dictionary_values);
            residual_conjuncts = build_dictionary_residual_conjuncts(conjunct_it->second);
        }

        // The bitmap is keyed by Parquet dictionary id. Later data-page reads evaluate the
        // predicate with an integer lookup and only materialize STRING values for surviving rows.
        _current_dictionary_filters.emplace(local_id, std::move(dictionary_filter));
        _current_dictionary_residual_conjuncts.emplace(local_id, std::move(residual_conjuncts));
        _current_predicate_columns.emplace(local_id, std::move(column_reader));
        update_counter_if_not_null(_scan_profile.dict_filter_columns, 1);
    }
    return Status::OK();
}

Status ParquetScanScheduler::read_filter_columns(int64_t batch_rows,
                                                 const format::FileScanRequest& request,
                                                 Block* file_block, SelectionVector* selection,
                                                 uint16_t* selected_rows,
                                                 int64_t* conjunct_filtered_rows,
                                                 bool* predicate_columns_filtered) {
    DORIS_CHECK(predicate_columns_filtered != nullptr);
    *predicate_columns_filtered = false;
    if (!request.conjuncts.empty() || !request.delete_conjuncts.empty()) {
        selection->resize(static_cast<size_t>(batch_rows));
    }
    const auto& schedule = predicate_conjunct_schedule(request);
    const size_t predicate_batch_sequence = _predicate_batch_sequence++;
    const bool can_read_predicate_columns_round_by_round =
            !schedule.single_column_conjuncts.empty();
    auto& read_column_positions = _read_column_positions_scratch;
    read_column_positions.clear();
    read_column_positions.reserve(request.predicate_columns.size());
    for (auto& rows : _predicate_column_selection_scratch | std::views::values) {
        rows.clear();
    }

    auto remember_column_selection = [&](uint32_t position) {
        auto& rows = _predicate_column_selection_scratch[position];
        rows.resize(*selected_rows);
        for (uint16_t row = 0; row < *selected_rows; ++row) {
            // SelectionVector and the scanner batch contract both bound row ordinals to uint16_t;
            // keep the checked conversion explicit when persisting the coordinate mapping.
            rows[row] = cast_set<uint16_t>(selection->get_index(row));
        }
    };

    auto compact_predicate_columns = [&]() -> Status {
        bool compacted = false;
        int64_t compacted_bytes = 0;
        for (const uint32_t position : read_column_positions) {
            auto& source_rows = _predicate_column_selection_scratch[position];
            const auto& old_column = file_block->get_by_position(position).column;
            if (old_column->size() != source_rows.size()) {
                return Status::Corruption(
                        "Predicate column {} has {} values but {} remembered source rows", position,
                        old_column->size(), source_rows.size());
            }
            bool already_compact = source_rows.size() == *selected_rows &&
                                   old_column->size() == static_cast<size_t>(*selected_rows);
            for (uint16_t row = 0; already_compact && row < *selected_rows; ++row) {
                already_compact = source_rows[row] == selection->get_index(row);
            }
            if (already_compact) {
                continue;
            }
            auto& filter = _predicate_compaction_filter_scratch;
            // resize_fill() preserves bytes when the next predicate column is smaller. Clear the
            // whole reusable mask so survivors from an earlier coordinate space cannot reappear.
            filter.resize(source_rows.size());
            std::ranges::fill(filter, 0);
            size_t source_idx = 0;
            uint16_t selected_idx = 0;
            while (source_idx < source_rows.size() && selected_idx < *selected_rows) {
                const auto source_row = source_rows[source_idx];
                const auto selected_row = selection->get_index(selected_idx);
                if (source_row < selected_row) {
                    ++source_idx;
                    continue;
                }
                DORIS_CHECK_EQ(source_row, selected_row);
                filter[source_idx++] = 1;
                ++selected_idx;
            }
            DORIS_CHECK_EQ(selected_idx, *selected_rows);
            compacted_bytes += static_cast<int64_t>(old_column->byte_size());
            RETURN_IF_CATCH_EXCEPTION(file_block->replace_by_position(
                    position, old_column->filter(filter, *selected_rows)));
            remember_column_selection(position);
            compacted = true;
        }
        if (compacted) {
            update_counter_if_not_null(_scan_profile.predicate_compaction_bytes, compacted_bytes);
            update_counter_if_not_null(_scan_profile.predicate_compaction_count, 1);
        }
        // The output path must not apply a batch-coordinate filter to columns that now use compact
        // coordinates. The loop above establishes this invariant even when no bytes moved because
        // every column was already aligned.
        *predicate_columns_filtered = !read_column_positions.empty();
        return Status::OK();
    };

    auto read_predicate_column = [&](ParquetColumnReader* column_reader, size_t block_position,
                                     ColumnId local_id, bool* used_dictionary_filter) -> Status {
        DORIS_CHECK(used_dictionary_filter != nullptr);
        *used_dictionary_filter = false;
        DCHECK(remove_nullable(column_reader->type())
                       ->equals(*remove_nullable(file_block->get_by_position(block_position).type)))
                << column_reader->type()->get_name() << " "
                << file_block->get_by_position(block_position).type->get_name() << " "
                << column_reader->name() << " " << file_block->get_by_position(block_position).name;
        auto column = file_block->get_by_position(block_position).column->assert_mutable();
        SCOPED_TIMER(_scan_profile.column_read_time);
        const auto dictionary_filter_it = _current_dictionary_filters.find(local_id);
        if (dictionary_filter_it != _current_dictionary_filters.end()) {
            const uint16_t selected_rows_before = *selected_rows;
            IColumn::Filter compact_filter;
            bool used_filter = false;
            RETURN_IF_ERROR(column_reader->select_with_dictionary_filter(
                    *selection, *selected_rows, batch_rows, dictionary_filter_it->second, column,
                    &compact_filter, &used_filter));
            if (used_filter) {
                DORIS_CHECK(compact_filter.size() == selected_rows_before);
                const uint16_t new_selected_rows = count_selected_rows(compact_filter);
                const auto filtered_rows = static_cast<int64_t>(selected_rows_before) -
                                           static_cast<int64_t>(new_selected_rows);
                if (conjunct_filtered_rows != nullptr) {
                    *conjunct_filtered_rows += filtered_rows;
                }
                update_counter_if_not_null(_scan_profile.rows_filtered_by_dict_filter,
                                           filtered_rows);
                if (new_selected_rows != selected_rows_before) {
                    // The dictionary reader already appended only survivors for this column. Keep
                    // older predicate columns in their original coordinate spaces and compact all
                    // of them once at the expression/output boundary below.
                    *selected_rows = apply_compact_filter_to_selection(compact_filter, selection,
                                                                       selected_rows_before);
                }
                file_block->replace_by_position(block_position, std::move(column));
                read_column_positions.push_back(cast_set<uint32_t>(block_position));
                remember_column_selection(cast_set<uint32_t>(block_position));
                *used_dictionary_filter = true;
                return Status::OK();
            }
        }

        if (*selected_rows == batch_rows) {
            int64_t column_rows = 0;
            RETURN_IF_ERROR(column_reader->read(batch_rows, column, &column_rows));
            if (column_rows != batch_rows) {
                return Status::Corruption(
                        "Parquet filter column {} returned {} rows, expected {} rows",
                        column_reader->name(), column_rows, batch_rows);
            }
        } else {
            [[maybe_unused]] auto old_size = column->size();
            RETURN_IF_ERROR(column_reader->select(*selection, *selected_rows, batch_rows, column));
            if (column->size() != old_size + *selected_rows) {
                return Status::Corruption(
                        "Parquet selected filter column {} returned {} rows, expected {} rows",
                        column_reader->name(), column->size(), old_size + *selected_rows);
            }
            *predicate_columns_filtered = true;
        }
        file_block->replace_by_position(block_position, std::move(column));
        read_column_positions.push_back(cast_set<uint32_t>(block_position));
        remember_column_selection(cast_set<uint32_t>(block_position));
        return Status::OK();
    };

    auto execute_scheduled_conjuncts = [&](const VExprContextSPtrs& conjuncts) -> Status {
        if (conjuncts.empty() || *selected_rows == 0) {
            return Status::OK();
        }
        const uint16_t selected_rows_before = *selected_rows;
        IColumn::Filter compact_filter;
        bool can_filter_all = false;
        RETURN_IF_ERROR(execute_compact_filter_conjuncts(
                conjuncts, selected_rows_before, file_block, &compact_filter, &can_filter_all));
        if (can_filter_all) {
            compact_filter.resize_fill(selected_rows_before, 0);
        }
        const uint16_t new_selected_rows = can_filter_all ? 0 : count_selected_rows(compact_filter);
        if (conjunct_filtered_rows != nullptr) {
            *conjunct_filtered_rows += static_cast<int64_t>(selected_rows_before) -
                                       static_cast<int64_t>(new_selected_rows);
        }
        if (new_selected_rows != selected_rows_before) {
            *selected_rows = can_filter_all
                                     ? 0
                                     : apply_compact_filter_to_selection(compact_filter, selection,
                                                                         selected_rows_before);
        }
        return Status::OK();
    };

    auto execute_scheduled_dictionary_residual_conjuncts =
            [&](const DictionaryResidualConjuncts& conjuncts) -> Status {
        if (conjuncts.empty() || *selected_rows == 0) {
            return Status::OK();
        }
        const uint16_t selected_rows_before = *selected_rows;
        IColumn::Filter compact_filter;
        bool can_filter_all = false;
        RETURN_IF_ERROR(execute_compact_dictionary_residual_conjuncts(
                conjuncts, selected_rows_before, file_block, &compact_filter, &can_filter_all));
        if (can_filter_all) {
            compact_filter.resize_fill(selected_rows_before, 0);
        }
        const uint16_t new_selected_rows = can_filter_all ? 0 : count_selected_rows(compact_filter);
        if (conjunct_filtered_rows != nullptr) {
            *conjunct_filtered_rows += static_cast<int64_t>(selected_rows_before) -
                                       static_cast<int64_t>(new_selected_rows);
        }
        if (new_selected_rows != selected_rows_before) {
            *selected_rows = can_filter_all
                                     ? 0
                                     : apply_compact_filter_to_selection(compact_filter, selection,
                                                                         selected_rows_before);
        }
        return Status::OK();
    };

    auto execute_scheduled_conjuncts_with_profile =
            [&](const VExprContextSPtrs& conjuncts) -> Status {
        if (_scan_profile.predicate_filter_time == nullptr) {
            return execute_scheduled_conjuncts(conjuncts);
        }
        SCOPED_TIMER(_scan_profile.predicate_filter_time);
        return execute_scheduled_conjuncts(conjuncts);
    };

    auto execute_scheduled_dictionary_residual_conjuncts_with_profile =
            [&](const DictionaryResidualConjuncts& conjuncts) -> Status {
        if (_scan_profile.predicate_filter_time == nullptr) {
            return execute_scheduled_dictionary_residual_conjuncts(conjuncts);
        }
        SCOPED_TIMER(_scan_profile.predicate_filter_time);
        return execute_scheduled_dictionary_residual_conjuncts(conjuncts);
    };

    auto execute_scheduled_delete_conjuncts = [&]() -> Status {
        if (request.delete_conjuncts.empty() || *selected_rows == 0) {
            return Status::OK();
        }
        const uint16_t selected_rows_before = *selected_rows;
        IColumn::Filter compact_filter;
        bool can_filter_all = false;
        RETURN_IF_ERROR(execute_compact_delete_conjuncts(request.delete_conjuncts,
                                                         selected_rows_before, file_block,
                                                         &compact_filter, &can_filter_all));
        if (can_filter_all) {
            compact_filter.resize_fill(selected_rows_before, 0);
        }
        if (can_filter_all || count_selected_rows(compact_filter) != selected_rows_before) {
            *selected_rows = can_filter_all
                                     ? 0
                                     : apply_compact_filter_to_selection(compact_filter, selection,
                                                                         selected_rows_before);
        }
        return Status::OK();
    };

    auto read_all_predicate_columns = [&]() -> Status {
        for (const auto& [fid, column_reader] : _current_predicate_columns) {
            auto position_it = request.local_positions.find(format::LocalColumnId(fid));
            DORIS_CHECK(position_it != request.local_positions.end());
            bool used_dictionary_filter = false;
            RETURN_IF_ERROR(read_predicate_column(column_reader.get(), position_it->second.value(),
                                                  fid, &used_dictionary_filter));
        }
        return Status::OK();
    };

    if (!can_read_predicate_columns_round_by_round) {
        RETURN_IF_ERROR(read_all_predicate_columns());
        if (_scan_profile.predicate_filter_time == nullptr) {
            return execute_batch_filters(request, batch_rows, file_block, selection, selected_rows,
                                         conjunct_filtered_rows);
        }
        SCOPED_TIMER(_scan_profile.predicate_filter_time);
        return execute_batch_filters(request, batch_rows, file_block, selection, selected_rows,
                                     conjunct_filtered_rows);
    }

    auto read_round_by_round = [&]() -> Status {
        // Single-column conjuncts can be evaluated immediately after their column is read. Once
        // selection shrinks, later predicate columns use ParquetColumnReader::select() so the
        // reader skips rows already rejected by earlier predicates instead of materializing them.
        _ordered_predicate_positions_scratch = detail::order_adaptive_predicates(
                _predicate_positions_scratch, _predicate_runtime_stats);
        const auto& ordered_positions = _ordered_predicate_positions_scratch;
        for (size_t order_idx = 0; order_idx < ordered_positions.size(); ++order_idx) {
            const size_t position = ordered_positions[order_idx];
            const size_t idx = _predicate_indices_by_position_scratch.at(position);
            const auto& col = request.predicate_columns[idx];
            const auto fid = col.local_id();
            auto reader_it = _current_predicate_columns.find(fid);
            DORIS_CHECK(reader_it != _current_predicate_columns.end());
            auto position_it = request.local_positions.find(col.column_id());
            DORIS_CHECK(position_it != request.local_positions.end());
            const auto block_position = position_it->second.value();
            const uint16_t rows_before = *selected_rows;
            auto& stats = _predicate_runtime_stats[position];
            const bool sample = detail::should_sample_adaptive_predicate(stats.samples,
                                                                         predicate_batch_sequence);
            const int64_t start_ns = sample ? MonotonicNanos() : 0;
            bool used_dictionary_filter = false;
            RETURN_IF_ERROR(read_predicate_column(reader_it->second.get(), block_position, fid,
                                                  &used_dictionary_filter));
            const auto conjunct_it = schedule.single_column_conjuncts.find(block_position);
            if (*selected_rows != 0 && conjunct_it != schedule.single_column_conjuncts.end()) {
                if (used_dictionary_filter) {
                    const auto residual_it = _current_dictionary_residual_conjuncts.find(fid);
                    DORIS_CHECK(residual_it != _current_dictionary_residual_conjuncts.end());
                    RETURN_IF_ERROR(execute_scheduled_dictionary_residual_conjuncts_with_profile(
                            residual_it->second));
                } else {
                    RETURN_IF_ERROR(execute_scheduled_conjuncts_with_profile(conjunct_it->second));
                }
            }
            if (sample) {
                const double cost_per_row = static_cast<double>(MonotonicNanos() - start_ns) /
                                            std::max<uint16_t>(rows_before, 1);
                const double survival =
                        static_cast<double>(*selected_rows) / std::max<uint16_t>(rows_before, 1);
                constexpr double ADAPTIVE_ALPHA = 0.25;
                if (stats.samples == 0) {
                    stats.cost_per_input_row_ns = cost_per_row;
                    stats.survival_ratio = survival;
                } else {
                    stats.cost_per_input_row_ns =
                            ADAPTIVE_ALPHA * cost_per_row +
                            (1 - ADAPTIVE_ALPHA) * stats.cost_per_input_row_ns;
                    stats.survival_ratio =
                            ADAPTIVE_ALPHA * survival + (1 - ADAPTIVE_ALPHA) * stats.survival_ratio;
                }
                ++stats.samples;
            }
            if (*selected_rows != 0) {
                continue;
            }
            for (size_t remaining_order_idx = order_idx + 1;
                 remaining_order_idx < ordered_positions.size(); ++remaining_order_idx) {
                const size_t remaining_idx = _predicate_indices_by_position_scratch.at(
                        ordered_positions[remaining_order_idx]);
                const auto remaining_fid = request.predicate_columns[remaining_idx].local_id();
                auto remaining_reader_it = _current_predicate_columns.find(remaining_fid);
                DORIS_CHECK(remaining_reader_it != _current_predicate_columns.end());
                RETURN_IF_ERROR(remaining_reader_it->second->skip(batch_rows));
            }
            return Status::OK();
        }
        return Status::OK();
    };

    auto compact_predicate_columns_with_profile = [&]() -> Status {
        const int64_t start_ns = MonotonicNanos();
        auto status = compact_predicate_columns();
        update_counter_if_not_null(_scan_profile.predicate_compaction_time,
                                   MonotonicNanos() - start_ns);
        return status;
    };

    RETURN_IF_ERROR(read_round_by_round());
    // Single-column expressions only touch the just-read column, so earlier columns can retain
    // their own row mappings. Compact once before a multi-column/output boundary.
    RETURN_IF_ERROR(compact_predicate_columns_with_profile());
    RETURN_IF_ERROR(execute_scheduled_conjuncts_with_profile(schedule.remaining_conjuncts));
    RETURN_IF_ERROR(compact_predicate_columns_with_profile());
    if (_scan_profile.predicate_filter_time == nullptr) {
        RETURN_IF_ERROR(execute_scheduled_delete_conjuncts());
    } else {
        SCOPED_TIMER(_scan_profile.predicate_filter_time);
        RETURN_IF_ERROR(execute_scheduled_delete_conjuncts());
    }
    return compact_predicate_columns_with_profile();
}

void ParquetScanScheduler::prefetch_current_row_group_columns(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const std::vector<format::LocalColumnIndex>& scan_columns, bool* prefetched) {
    DORIS_CHECK(prefetched != nullptr);
    if (_current_merge_range_active || *prefetched || scan_columns.empty() ||
        _current_row_group_id < 0 || file_context.native_metadata == nullptr) {
        return;
    }
    *prefetched = true;
    // The scanner request separates predicate and non-predicate columns so Parquet can read
    // predicate columns first and lazily materialize the rest. Keep the same contract for
    // prefetch: callers decide which side to warm, and this helper only translates that selected
    // projection into physical column-chunk byte ranges for the current row group.
    file_context.prefetch_ranges(
            build_row_group_prefetch_ranges(file_context.native_metadata->to_thrift(), file_schema,
                                            scan_columns, _current_row_group_id),
            nullptr);
}

Status ParquetScanScheduler::read_current_row_group_batch(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema, int64_t batch_rows,
        const format::FileScanRequest& request, int64_t batch_first_file_row, Block* file_block,
        size_t* rows) {
    // Reader statistics are cumulative plain integers. Publishing their delta recursively for
    // every tiny batch is measurable on wide/nested scans, so flush periodically and force the
    // tail at row-group reset/close.
    Defer profile_flush {[this, batch_rows]() {
        const bool finishes_row_group = _current_range_idx + 1 == _current_selected_ranges.size() &&
                                        _current_range_rows_read + batch_rows ==
                                                _current_selected_ranges[_current_range_idx].length;
        if (++_batches_since_profile_flush >= PROFILE_FLUSH_BATCH_INTERVAL || finishes_row_group) {
            flush_current_reader_profiles();
            _batches_since_profile_flush = 0;
        }
    }};
    if (_scan_profile.total_batches != nullptr) {
        COUNTER_UPDATE(_scan_profile.total_batches, 1);
    }
    if (_scan_profile.raw_rows_read != nullptr) {
        COUNTER_UPDATE(_scan_profile.raw_rows_read, batch_rows);
    }
    _raw_rows_read += batch_rows;
    if (_current_predicate_columns.empty() && _current_non_predicate_columns.empty()) {
        *rows = static_cast<size_t>(batch_rows);
        materialize_count_star_placeholders(request, *rows, file_block);
        if (_scan_profile.selected_rows != nullptr) {
            COUNTER_UPDATE(_scan_profile.selected_rows, batch_rows);
        }
        return Status::OK();
    }
    auto& selection = _selection;
    DORIS_CHECK(batch_rows <= std::numeric_limits<uint16_t>::max());
    uint16_t selected_rows = static_cast<uint16_t>(batch_rows);
    int64_t conjunct_filtered_rows = 0;
    bool predicate_columns_filtered = false;
    RETURN_IF_ERROR(read_filter_columns(batch_rows, request, file_block, &selection, &selected_rows,
                                        &conjunct_filtered_rows, &predicate_columns_filtered));
    _predicate_filtered_rows += conjunct_filtered_rows;
    mark_condition_cache_granules(selection, selected_rows, batch_first_file_row);

    const bool need_filter_output = selected_rows != batch_rows;
    const double batch_survival = static_cast<double>(selected_rows) / batch_rows;
    _predicate_survival_ratio = _predicate_survival_ratio < 0
                                        ? batch_survival
                                        : 0.25 * batch_survival + 0.75 * _predicate_survival_ratio;
    if (_scan_profile.selected_rows != nullptr) {
        COUNTER_UPDATE(_scan_profile.selected_rows, selected_rows);
    }
    if (_scan_profile.rows_filtered_by_conjunct != nullptr) {
        COUNTER_UPDATE(_scan_profile.rows_filtered_by_conjunct, conjunct_filtered_rows);
    }
    if (!_current_non_predicate_columns.empty() &&
        _scan_profile.lazy_read_filtered_rows != nullptr) {
        COUNTER_UPDATE(_scan_profile.lazy_read_filtered_rows, batch_rows - selected_rows);
    }
    if (selected_rows == 0 && _scan_profile.empty_selection_batches != nullptr) {
        COUNTER_UPDATE(_scan_profile.empty_selection_batches, 1);
    } else if (static_cast<int64_t>(selected_rows) == batch_rows &&
               _scan_profile.dense_batches != nullptr) {
        COUNTER_UPDATE(_scan_profile.dense_batches, 1);
    } else if (_scan_profile.selected_batches != nullptr) {
        COUNTER_UPDATE(_scan_profile.selected_batches, 1);
    }
    if (need_filter_output && !predicate_columns_filtered) {
        IColumn::Filter output_filter = selection_to_filter(selection, selected_rows, batch_rows);
        for (const auto& col : request.predicate_columns) {
            auto position_it = request.local_positions.find(col.column_id());
            DORIS_CHECK(position_it != request.local_positions.end());
            const auto block_position = position_it->second.value();
            RETURN_IF_CATCH_EXCEPTION(file_block->replace_by_position(
                    block_position, file_block->get_by_position(block_position)
                                            .column->filter(output_filter, selected_rows)));
        }
    }
    if (selected_rows == 0) {
        // Predicate readers have consumed this physical batch, but touching every lazy column here
        // turns a long rejected prefix into `empty_batches * lazy_columns` native calls. Record only
        // the positional lag. If [0, 32), [32, 64), and [64, 96) are empty, the first surviving
        // batch performs one skip(96) per lazy column. If the row group ends instead, reset drops the
        // lazy readers without flushing because no value from them can be observed.
        DORIS_CHECK(_pending_non_predicate_skip_rows <=
                    std::numeric_limits<int64_t>::max() - batch_rows);
        _pending_non_predicate_skip_rows += batch_rows;
        *rows = 0;
        return Status::OK();
    }
    if (!_current_merge_range_active && selected_rows > 0 &&
        !_current_non_predicate_columns.empty()) {
        // Do not prefetch lazy output columns until at least one row survives filtering. This is
        // the same decision point where the v2 reader switches from predicate-only reads to
        // materializing non-predicate columns, so fully filtered batches avoid unnecessary IO.
        prefetch_current_row_group_columns(file_context, file_schema,
                                           physical_non_predicate_columns(request),
                                           &_current_non_predicate_prefetched);
    }

    {
        SCOPED_TIMER(_scan_profile.column_read_time);
        // Bring lazy readers to the first row of the current physical batch before interpreting its
        // selection vector. This also merges pending range gaps with fully filtered batches.
        RETURN_IF_ERROR(flush_pending_non_predicate_skip_rows());
        for (const auto& [fid, column_reader] : _current_non_predicate_columns) {
            auto position_it = request.local_positions.find(format::LocalColumnId(fid));
            DORIS_CHECK(position_it != request.local_positions.end());
            const auto block_position = position_it->second.value();
            auto column = file_block->get_by_position(block_position).column->assert_mutable();
            DCHECK_EQ(file_block->get_by_position(block_position).type->get_primitive_type(),
                      column_reader->type()->get_primitive_type())
                    << type_to_string(file_block->get_by_position(block_position)
                                              .type->get_primitive_type())
                    << " " << type_to_string(column_reader->type()->get_primitive_type()) << " "
                    << column_reader->name() << " " << fid << " " << block_position;
            if (need_filter_output) {
                [[maybe_unused]] auto old_size = column->size();
                RETURN_IF_ERROR(
                        column_reader->select(selection, selected_rows, batch_rows, column));
                if (column->size() != old_size + selected_rows) {
                    return Status::Corruption(
                            "Parquet selected output column {} returned {} rows, expected {} rows",
                            column_reader->name(), column->size(), old_size + selected_rows);
                }
            } else {
                int64_t column_rows = 0;
                RETURN_IF_ERROR(column_reader->read(batch_rows, column, &column_rows));
                if (column_rows != batch_rows) {
                    return Status::Corruption(
                            "Parquet output column {} returned {} rows, expected {} rows",
                            column_reader->name(), column_rows, batch_rows);
                }
            }
            file_block->replace_by_position(block_position, std::move(column));
        }
    }
    materialize_count_star_placeholders(request, selected_rows, file_block);
    *rows = static_cast<size_t>(selected_rows);
    return Status::OK();
}

void ParquetScanScheduler::mark_condition_cache_granules(const SelectionVector& selection,
                                                         uint16_t selected_rows,
                                                         int64_t batch_first_file_row) {
    if (!_condition_cache_ctx || _condition_cache_ctx->is_hit ||
        !_condition_cache_ctx->filter_result) {
        return;
    }
    auto& cache = *_condition_cache_ctx->filter_result;
    for (uint16_t selection_idx = 0; selection_idx < selected_rows; ++selection_idx) {
        const int64_t file_row = batch_first_file_row + selection.get_index(selection_idx);
        const int64_t granule = file_row / ConditionCacheContext::GRANULE_SIZE;
        const int64_t cache_idx = granule - _condition_cache_ctx->base_granule;
        if (cache_idx >= 0 && static_cast<size_t>(cache_idx) < cache.size()) {
            cache[static_cast<size_t>(cache_idx)] = true;
        }
    }
}

Status ParquetScanScheduler::read_next_batch(
        ParquetFileContext& file_context,
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const format::FileScanRequest& request, Block* file_block, size_t* rows, bool* eof) {
    *rows = 0;
    while (true) {
        if (!_has_current_row_group) {
            bool has_row_group = false;
            RETURN_IF_ERROR(
                    open_next_row_group(file_context, file_schema, request, &has_row_group));
            if (!has_row_group) {
                *eof = true;
                return Status::OK();
            }
        }

        if (_current_range_idx >= _current_selected_ranges.size()) {
            // Current row group finished, try next row group.
            reset_current_row_group();
            continue;
        }

        const RowRange& current_range = _current_selected_ranges[_current_range_idx];
        DORIS_CHECK(current_range.start >= 0);
        DORIS_CHECK(current_range.length > 0);
        DORIS_CHECK(current_range.start + current_range.length <= _current_row_group_rows);

        if (_current_row_group_rows_read < current_range.start) {
            // Skip filtered rows according to row group level pruning.
            RETURN_IF_ERROR(skip_current_row_group_rows(current_range.start -
                                                        _current_row_group_rows_read));
        }
        DORIS_CHECK(_current_row_group_rows_read == current_range.start + _current_range_rows_read);
        const int64_t remaining_rows = current_range.length - _current_range_rows_read;
        if (remaining_rows <= 0) {
            // Current range finished, try next range in the same row group.
            ++_current_range_idx;
            _current_range_rows_read = 0;
            continue;
        }

        const int64_t batch_rows = std::min<int64_t>(_batch_size, remaining_rows);
        const int64_t physical_rows_read = batch_rows;
        const int64_t batch_first_file_row =
                _current_row_group_first_row + _current_row_group_rows_read;
        RETURN_IF_ERROR(read_current_row_group_batch(file_context, file_schema, batch_rows, request,
                                                     batch_first_file_row, file_block, rows));
        _current_row_group_rows_read += physical_rows_read;
        _current_range_rows_read += physical_rows_read;
        if (_current_range_rows_read >= current_range.length) {
            ++_current_range_idx;
            _current_range_rows_read = 0;
        }
        if (*rows == 0) {
            continue;
        }
        *eof = false;
        return Status::OK();
    }
}

} // namespace doris::format::parquet
