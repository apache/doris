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
#include <parquet/schema.h>

#include <cmath>

#include "common/check.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
#include "format/table/parquet_utils.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

// Branchless so the compiler can vectorize it: this runs over every floating value written.
template <typename Container>
int64_t count_nan_values(const Container& data, const NullMap* null_map) {
    int64_t nan_count = 0;
    const size_t rows = data.size();
    if (null_map == nullptr) {
        for (size_t i = 0; i < rows; ++i) {
            nan_count += static_cast<int64_t>(std::isnan(data[i]));
        }
        return nan_count;
    }
    for (size_t i = 0; i < rows; ++i) {
        nan_count += static_cast<int64_t>((*null_map)[i] == 0 && std::isnan(data[i]));
    }
    return nan_count;
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
          _nan_count_field_ids(nan_count_field_ids.begin(), nan_count_field_ids.end()) {}

Status VIcebergParquetWriter::open() {
    RETURN_IF_ERROR(VParquetWriter::open());
    _init_nan_value_counts();
    return Status::OK();
}

// Iceberg excludes NaN from a column's bounds by spec ("NaNs are not permitted as lower or upper
// bounds"), so nan_value_counts is the ONLY metadata that can prove a file holds no NaN. Without it
// InclusiveMetricsEvaluator.isNaN must assume NaN may be present and a float range predicate cannot
// prune the file at all (see the FLOAT/DOUBLE leaves in FE IcebergPredicateConverter).
//
// A field listed here is a claim that every one of its values was counted, which is what makes a
// reported zero trustworthy. Two independent narrowings apply, and a field excluded by either one stays
// absent from the map -- "unknown", which iceberg reads conservatively -- rather than wrongly claimed
// NaN-free:
//   - policy: _nan_count_field_ids, the fields whose count FE would keep under the table's metrics
//     config. Counting one FE drops is a pure waste of a data pass, and iceberg disables metrics for
//     everything past the first 100 fields by default, so a wide table hits this with no property set.
//   - capability: only top-level columns, because a floating field nested in a struct/list/map is not a
//     block column of its own.
void VIcebergParquetWriter::_init_nan_value_counts() {
    const auto& columns = _iceberg_schema.columns();
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto type_id = columns[i].field_type()->type_id();
        if (type_id != iceberg::TypeID::FLOAT && type_id != iceberg::TypeID::DOUBLE) {
            continue;
        }
        const int32_t field_id = columns[i].field_id();
        if (_nan_count_field_ids.contains(field_id)) {
            _nan_counted_columns.emplace_back(i, field_id);
            _nan_value_counts[field_id] = 0;
        }
    }
}

// Block column i maps to iceberg column i: IcebergArrowBlockConvertor builds the arrow fields straight
// from _iceberg_schema.columns(), and ArrowBlockConvertor::convert_to_arrow is positional and rejects a
// column-count mismatch outright.
void VIcebergParquetWriter::_count_nan_values(const Block& block) {
    for (const auto& [column_position, field_id] : _nan_counted_columns) {
        const IColumn* column = block.get_by_position(column_position).column.get();
        const NullMap* null_map = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
            null_map = &nullable->get_null_map_data();
            column = &nullable->get_nested_column();
        }
        if (const auto* float64 = check_and_get_column<ColumnFloat64>(column)) {
            _nan_value_counts[field_id] += count_nan_values(float64->get_data(), null_map);
            continue;
        }
        const auto* float32 = check_and_get_column<ColumnFloat32>(column);
        DORIS_CHECK(float32 != nullptr);
        _nan_value_counts[field_id] += count_nan_values(float32->get_data(), null_map);
    }
}

Status VIcebergParquetWriter::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }
    _count_nan_values(block);
    return VParquetWriter::write(block);
}

std::unique_ptr<ArrowBlockConvertor> VIcebergParquetWriter::_create_arrow_block_convertor(
        DataTypes types, std::vector<std::string> names, const std::string& timezone_name,
        const cctz::time_zone& timezone) const {
    return std::make_unique<iceberg::IcebergArrowBlockConvertor>(
            _iceberg_schema, &_iceberg_schema_json, timezone_name, timezone);
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

    const int num_row_groups = file_metadata->num_row_groups();
    const int num_columns = file_metadata->num_columns();
    for (int col_idx = 0; col_idx < num_columns; ++col_idx) {
        auto field_id = file_metadata->schema()->Column(col_idx)->schema_node()->field_id();

        for (int rg_idx = 0; rg_idx < num_row_groups; ++rg_idx) {
            auto row_group = file_metadata->RowGroup(rg_idx);
            auto column_chunk = row_group->ColumnChunk(col_idx);
            column_sizes[field_id] += column_chunk->total_compressed_size();

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
        if (column_stat->HasMinMax()) {
            has_any_min_max = true;
            lower_bounds[field_id] = column_stat->EncodeMin();
            upper_bounds[field_id] = column_stat->EncodeMax();
        }
    }

    stats->__set_column_sizes(column_sizes);
    stats->__set_value_counts(value_counts);
    // Left unset when no column was counted, so FE keeps reporting "unknown" rather than an empty
    // claim -- the same shape an older BE produces.
    if (!_nan_value_counts.empty()) {
        stats->__set_nan_value_counts(_nan_value_counts);
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
