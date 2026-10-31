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

#include "format/transformer/viceberg_parquet_writer.h"

#include <parquet/api/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

#include <algorithm>
#include <boost/algorithm/string/predicate.hpp>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_set>

#include "exprs/function/geo/spatial_wkb_validation.h"
#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
#include "format/table/iceberg/spatial_parquet_schema.h"
#include "format/table/parquet_utils.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris {

static std::string encode_iceberg_bound(const ::parquet::Statistics& stats, std::string encoded) {
    if (!stats.descr()->logical_type()->is_decimal()) {
        return encoded;
    }
    // Parquet integer statistics are little-endian, while binary decimals already use big-endian.
    // Iceberg bounds require the unscaled value's shortest signed big-endian representation.
    if (stats.physical_type() == ::parquet::Type::INT32 ||
        stats.physical_type() == ::parquet::Type::INT64) {
        std::reverse(encoded.begin(), encoded.end());
    }
    size_t start = 0;
    while (start + 1 < encoded.size()) {
        const auto byte = static_cast<uint8_t>(encoded[start]);
        const bool next_is_negative = (static_cast<uint8_t>(encoded[start + 1]) & 0x80) != 0;
        if ((byte == 0 && !next_is_negative) || (byte == 0xff && next_is_negative)) {
            ++start;
        } else {
            break;
        }
    }
    encoded.erase(0, start);
    return encoded;
}

namespace {

// The Iceberg field whose metrics a Parquet leaf column feeds.
struct IcebergMetricColumn {
    int field_id;
    bool is_variant_metadata;
};

// The leaves of a Variant carry no Iceberg field id. Its metadata leaf counts the values and nulls
// of the Variant field, and its value leaf feeds no metric: its byte statistics are no logical
// Variant statistics.
std::optional<IcebergMetricColumn> iceberg_metric_column(const ::parquet::schema::Node& leaf) {
    const auto* parent = leaf.parent();
    const bool is_variant_leaf = parent != nullptr && parent->logical_type() != nullptr &&
                                 parent->logical_type()->is_variant();
    if (is_variant_leaf && leaf.name() != "metadata") {
        return std::nullopt;
    }
    const int field_id = is_variant_leaf ? parent->field_id() : leaf.field_id();
    // Every field of the Iceberg schema the file is written with carries its id.
    DORIS_CHECK_GE(field_id, 0);
    return IcebergMetricColumn {field_id, is_variant_leaf};
}

} // namespace

VIcebergParquetWriter::VIcebergParquetWriter(RuntimeState* state, io::FileWriter* file_writer,
                                             const VExprContextSPtrs& output_vexpr_ctxs,
                                             std::vector<std::string> column_names,
                                             bool output_object_data,
                                             const ParquetFileOptions& parquet_options,
                                             const std::string* iceberg_schema_json,
                                             const iceberg::Schema& iceberg_schema,
                                             const std::vector<int32_t>& nan_count_field_ids)
        : VParquetWriter(state, file_writer, output_vexpr_ctxs, std::move(column_names),
                         output_object_data, parquet_options),
          _iceberg_schema(iceberg_schema),
          _iceberg_schema_json(iceberg_schema_json == nullptr ? "" : *iceberg_schema_json),
          _nan_count_field_ids(nan_count_field_ids) {}

arrow::Status VIcebergParquetWriter::_open_file_writer() {
    bool has_spatial_column = false;
    for (const auto& column : _iceberg_schema.columns()) {
        has_spatial_column = has_spatial_column || contains_spatial_type(column.field_type());
    }
    if (!has_spatial_column) {
        return VParquetWriter::_open_file_writer();
    }

    ARROW_ASSIGN_OR_RAISE(
            auto parquet_schema,
            make_iceberg_parquet_schema(_arrow_convertor()->arrow_schema(), _iceberg_schema,
                                        _writer_properties(), _arrow_writer_properties()));
    auto parquet_writer = ::parquet::ParquetFileWriter::Open(
            _output_stream(), std::move(parquet_schema), _writer_properties());
    ARROW_RETURN_NOT_OK(::parquet::arrow::FileWriter::Make(
            ExecEnv::GetInstance()->arrow_memory_pool(), std::move(parquet_writer),
            _arrow_convertor()->arrow_schema(), _arrow_writer_properties(),
            &_parquet_file_writer()));
    return arrow::Status::OK();
}

Status VIcebergParquetWriter::open() {
    RETURN_IF_ERROR(VParquetWriter::open());
    _nan_value_counter.emplace(_iceberg_schema, _nan_count_field_ids);
    return Status::OK();
}

Status VIcebergParquetWriter::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }
    ColumnNumbers column_numbers(block.columns());
    std::iota(column_numbers.begin(), column_numbers.end(), 0);
    RETURN_IF_ERROR(validate_spatial_wkb_inputs(block, column_numbers));
    _nan_value_counter->count(block);
    return VParquetWriter::write(block);
}

std::unique_ptr<ArrowBlockConvertor> VIcebergParquetWriter::_create_arrow_block_convertor(
        DataTypes types, std::vector<std::string> names, const std::string& timezone_name,
        const cctz::time_zone& timezone, bool enable_int96_timestamps) const {
    return std::make_unique<iceberg::IcebergArrowBlockConvertor>(
            _iceberg_schema, &_iceberg_schema_json, timezone_name, timezone);
}

Status VIcebergParquetWriter::collect_split_offsets_after_close(std::vector<int64_t>* offsets) {
    auto metadata = _file_metadata();
    // Arrow publishes file metadata only after Close(), so offsets cannot be collected earlier.
    if (metadata == nullptr) {
        return Status::InternalError("File metadata is not available");
    }
    offsets->clear();
    offsets->reserve(metadata->num_row_groups());
    for (int i = 0; i < metadata->num_row_groups(); ++i) {
        auto row_group = metadata->RowGroup(i);
        // Empty files/groups have no page boundary to advertise as a scan split.
        if (row_group->num_rows() == 0 || row_group->num_columns() == 0) {
            continue;
        }
        auto first_column = row_group->ColumnChunk(0);
        int64_t offset = first_column->data_page_offset();
        // A split must include the dictionary page, not just the first data page that references it.
        if (first_column->has_dictionary_page()) {
            offset = std::min(offset, first_column->dictionary_page_offset());
        }
        offsets->push_back(offset);
    }
    std::sort(offsets->begin(), offsets->end());
    return Status::OK();
}

Status VIcebergParquetWriter::collect_file_statistics_after_close(TIcebergColumnStats* stats) {
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
    // Variant fields get value and null counts, but no column size and no bounds.
    std::unordered_set<int> variant_field_ids;

    const int num_row_groups = file_metadata->num_row_groups();
    const int num_columns = file_metadata->num_columns();
    for (int col_idx = 0; col_idx < num_columns; ++col_idx) {
        const auto metric_column =
                iceberg_metric_column(*file_metadata->schema()->Column(col_idx)->schema_node());
        if (!metric_column.has_value()) {
            continue;
        }
        const auto [field_id, is_variant_metadata] = *metric_column;
        if (is_variant_metadata) {
            variant_field_ids.insert(field_id);
        }

        for (int rg_idx = 0; rg_idx < num_row_groups; ++rg_idx) {
            auto row_group = file_metadata->RowGroup(rg_idx);
            auto column_chunk = row_group->ColumnChunk(col_idx);
            if (!is_variant_metadata) {
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
            lower_bounds[field_id] = encode_iceberg_bound(*column_stat, column_stat->EncodeMin());
            upper_bounds[field_id] = encode_iceberg_bound(*column_stat, column_stat->EncodeMax());
        }
    }

    stats->__set_column_sizes(column_sizes);
    stats->__set_value_counts(value_counts);
    // Left unset when no column was counted, so FE keeps reporting "unknown" rather than an empty
    // claim -- the same shape an older BE produces.
    if (!_nan_value_counter->empty()) {
        stats->__set_nan_value_counts(_nan_value_counter->counts());
    }
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
