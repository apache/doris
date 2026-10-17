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
#include <string>

#include "exprs/function/geo/functions_geo.h"
#include "format/table/iceberg/iceberg_arrow_block_convertor.h"
#include "format/table/parquet_utils.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

::parquet::schema::NodePtr clone_parquet_node(const ::parquet::schema::Node& node) {
    if (node.is_primitive()) {
        const auto& primitive = static_cast<const ::parquet::schema::PrimitiveNode&>(node);
        return ::parquet::schema::PrimitiveNode::Make(
                primitive.name(), primitive.repetition(), primitive.logical_type(),
                primitive.physical_type(), primitive.type_length(), primitive.field_id());
    }

    const auto& group = static_cast<const ::parquet::schema::GroupNode&>(node);
    ::parquet::schema::NodeVector fields;
    fields.reserve(group.field_count());
    for (int index = 0; index < group.field_count(); ++index) {
        fields.push_back(clone_parquet_node(*group.field(index)));
    }
    return ::parquet::schema::GroupNode::Make(
            group.name(), group.repetition(), fields,
            group.logical_type()->is_none() ? nullptr : group.logical_type(), group.field_id());
}

arrow::Status validate_spatial_crs_for_write(const std::string& crs) {
    // projjson:my_crs needs a my_crs definition in Parquet file metadata. The sink currently
    // carries only the schema, so writing this annotation would leave a dangling CRS reference.
    if (boost::istarts_with(crs, "projjson:")) {
        return arrow::Status::NotImplemented(
                "Referenced spatial CRS is not supported for Parquet writes without its file "
                "metadata definition: ",
                crs);
    }
    return arrow::Status::OK();
}

arrow::Result<::parquet::LogicalType::EdgeInterpolationAlgorithm> geography_algorithm(
        const std::string& algorithm) {
    using Algorithm = ::parquet::LogicalType::EdgeInterpolationAlgorithm;
    if (algorithm == "spherical") {
        return Algorithm::SPHERICAL;
    }
    if (algorithm == "vincenty") {
        return Algorithm::VINCENTY;
    }
    if (algorithm == "thomas") {
        return Algorithm::THOMAS;
    }
    if (algorithm == "andoyer") {
        return Algorithm::ANDOYER;
    }
    if (algorithm == "karney") {
        return Algorithm::KARNEY;
    }
    return arrow::Status::Invalid("Unsupported Iceberg geography edge algorithm: ", algorithm);
}

bool contains_spatial_type(iceberg::Type* type) {
    switch (type->type_id()) {
    case iceberg::TypeID::GEOMETRY:
    case iceberg::TypeID::GEOGRAPHY:
        return true;
    case iceberg::TypeID::STRUCT:
        for (const auto& field : type->as_struct_type()->fields()) {
            if (contains_spatial_type(field.field_type())) {
                return true;
            }
        }
        return false;
    case iceberg::TypeID::LIST:
        return contains_spatial_type(type->as_list_type()->element_field().field_type());
    case iceberg::TypeID::MAP:
        return contains_spatial_type(type->as_map_type()->key_field().field_type()) ||
               contains_spatial_type(type->as_map_type()->value_field().field_type());
    default:
        return false;
    }
}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> make_iceberg_parquet_schema(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const iceberg::Schema& iceberg_schema,
        const std::shared_ptr<::parquet::WriterProperties>& writer_properties,
        const std::shared_ptr<::parquet::ArrowWriterProperties>& arrow_properties) {
    std::shared_ptr<::parquet::SchemaDescriptor> parquet_schema;
    ARROW_RETURN_NOT_OK(::parquet::arrow::ToParquetSchema(arrow_schema.get(), *writer_properties,
                                                          *arrow_properties, &parquet_schema));

    const auto* source_root = parquet_schema->group_node();
    ::parquet::schema::NodeVector fields;
    fields.reserve(source_root->field_count());
    for (int index = 0; index < source_root->field_count(); ++index) {
        const auto& source_field = source_root->field(index);
        const auto& iceberg_field = iceberg_schema.columns()[index];
        if (iceberg_field.field_type()->type_id() == iceberg::TypeID::GEOMETRY) {
            const auto& geometry =
                    static_cast<const iceberg::GeometryType&>(*iceberg_field.field_type());
            ARROW_RETURN_NOT_OK(validate_spatial_crs_for_write(geometry.crs()));
            fields.push_back(::parquet::schema::PrimitiveNode::Make(
                    source_field->name(), source_field->repetition(),
                    ::parquet::LogicalType::Geometry(geometry.crs()), ::parquet::Type::BYTE_ARRAY,
                    -1, source_field->field_id()));
        } else if (iceberg_field.field_type()->type_id() == iceberg::TypeID::GEOGRAPHY) {
            const auto& geography =
                    static_cast<const iceberg::GeographyType&>(*iceberg_field.field_type());
            ARROW_RETURN_NOT_OK(validate_spatial_crs_for_write(geography.crs()));
            ARROW_ASSIGN_OR_RAISE(auto algorithm, geography_algorithm(geography.algorithm()));
            fields.push_back(::parquet::schema::PrimitiveNode::Make(
                    source_field->name(), source_field->repetition(),
                    ::parquet::LogicalType::Geography(geography.crs(), algorithm),
                    ::parquet::Type::BYTE_ARRAY, -1, source_field->field_id()));
        } else if (contains_spatial_type(iceberg_field.field_type())) {
            return arrow::Status::NotImplemented(
                    "Nested Iceberg spatial columns are not supported for Parquet writes: ",
                    iceberg_field.field_name());
        } else {
            fields.push_back(clone_parquet_node(*source_field));
        }
    }
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make(source_root->name(), source_root->repetition(),
                                               fields, nullptr, source_root->field_id()));
}

} // namespace

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
