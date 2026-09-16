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

#include "format/transformer/viceberg_parquet_transformer.h"

#include <parquet/api/reader.h>
#include <parquet/schema.h>

#include <unordered_set>

#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
#include "format/table/parquet_utils.h"
#include "runtime/runtime_state.h"

namespace doris {
#include "common/compile_check_begin.h"

VIcebergParquetTransformer::VIcebergParquetTransformer(
        RuntimeState* state, io::FileWriter* file_writer,
        const VExprContextSPtrs& output_vexpr_ctxs, std::vector<std::string> column_names,
        bool output_object_data, const ParquetFileOptions& parquet_options,
        const std::string* iceberg_schema_json, const iceberg::Schema& iceberg_schema)
        : VParquetTransformer(state, file_writer, output_vexpr_ctxs, std::move(column_names),
                              output_object_data, parquet_options,
                              std::make_unique<iceberg::IcebergArrowBlockConvertor>(
                                      iceberg_schema, iceberg_schema_json)) {}

Status VIcebergParquetTransformer::_parse_schema(std::shared_ptr<arrow::Schema>* schema) {
    return static_cast<const iceberg::IcebergArrowBlockConvertor&>(*_arrow_block_convertor)
            .arrow_schema(_state->timezone(), schema);
}

Status VIcebergParquetTransformer::collect_file_statistics_after_close(TIcebergColumnStats* stats) {
    std::shared_ptr<::parquet::FileMetaData> file_metadata = _file_metadata();
    if (file_metadata == nullptr) {
        return Status::InternalError("File metadata is not available");
    }
    std::map<int, int64_t> column_sizes;
    std::map<int, int64_t> value_counts;
    std::map<int, int64_t> null_value_counts;
    std::map<int, std::string> lower_bounds;
    std::map<int, std::string> upper_bounds;
    std::map<int, std::shared_ptr<::parquet::Statistics>> merged_column_stats;
    std::unordered_set<int> variant_field_ids;

    const int num_row_groups = file_metadata->num_row_groups();
    const int num_columns = file_metadata->num_columns();
    for (int col_idx = 0; col_idx < num_columns; ++col_idx) {
        const auto& schema_node = file_metadata->schema()->Column(col_idx)->schema_node();
        const auto* parent = schema_node->parent();
        const bool is_variant_child = parent != nullptr && parent->logical_type() != nullptr &&
                                      parent->logical_type()->is_variant();
        if (is_variant_child && schema_node->name() != "metadata") {
            // The value leaf has no independent Iceberg field and its byte statistics are not
            // logical Variant statistics.
            continue;
        }
        const int field_id = is_variant_child ? parent->field_id() : schema_node->field_id();
        if (field_id < 0) {
            // Parquet structural leaves (including Variant children) may intentionally omit an
            // Iceberg field id. Never publish them under the synthetic -1 key.
            continue;
        }
        if (is_variant_child) {
            variant_field_ids.insert(field_id);
        }

        for (int rg_idx = 0; rg_idx < num_row_groups; ++rg_idx) {
            auto row_group = file_metadata->RowGroup(rg_idx);
            auto column_chunk = row_group->ColumnChunk(col_idx);
            if (!is_variant_child) {
                column_sizes[field_id] += column_chunk->total_compressed_size();
            }

            if (column_chunk->is_stats_set()) {
                auto column_stat = column_chunk->statistics();
                if (!merged_column_stats.contains(field_id)) {
                    merged_column_stats[field_id] = column_stat;
                } else {
                    parquet_utils::merge_stats(merged_column_stats[field_id], column_stat);
                }
            }
        }
    }

    bool has_any_null_count = false;
    bool has_any_min_max = false;
    for (const auto& [field_id, column_stat] : merged_column_stats) {
        value_counts[field_id] = column_stat->num_values();
        if (column_stat->HasNullCount()) {
            has_any_null_count = true;
            int64_t null_count = column_stat->null_count();
            null_value_counts[field_id] = null_count;
            value_counts[field_id] += null_count;
        }
        if (!variant_field_ids.contains(field_id) && column_stat->HasMinMax()) {
            has_any_min_max = true;
            lower_bounds[field_id] = column_stat->EncodeMin();
            upper_bounds[field_id] = column_stat->EncodeMax();
        }
    }

    stats->__set_column_sizes(column_sizes);
    stats->__set_value_counts(value_counts);
    if (has_any_null_count) {
        stats->__set_null_value_counts(null_value_counts);
    }
    if (has_any_min_max) {
        stats->__set_lower_bounds(lower_bounds);
        stats->__set_upper_bounds(upper_bounds);
    }
    return Status::OK();
}

} // namespace doris
