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

#include "format/transformer/vparquet_transformer.h"

#include <arrow/io/type_fwd.h>
#include <arrow/memory_pool.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <glog/logging.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/platform.h>
#include <parquet/schema.h>
#include <parquet/type_fwd.h>
#include <parquet/types.h>

#include <ctime>
#include <exception>
#include <numeric>
#include <ostream>
#include <string>
#include <unordered_set>

#include "common/config.h"
#include "common/status.h"
#include "exprs/function/geo/functions_geo.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"
#include "format/table/iceberg/arrow_schema_util.h"
#include "format/table/parquet_utils.h"
#include "io/fs/file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

arrow::MemoryPool* get_arrow_memory_pool() {
    auto* pool = ExecEnv::GetInstance()->arrow_memory_pool();
    return pool != nullptr ? pool : arrow::default_memory_pool();
}

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
    return ::parquet::schema::GroupNode::Make(group.name(), group.repetition(), fields,
                                               group.logical_type(), group.field_id());
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
            fields.push_back(::parquet::schema::PrimitiveNode::Make(
                    source_field->name(), source_field->repetition(),
                    ::parquet::LogicalType::Geometry(geometry.crs()), ::parquet::Type::BYTE_ARRAY,
                    -1, source_field->field_id()));
        } else if (iceberg_field.field_type()->type_id() == iceberg::TypeID::GEOGRAPHY) {
            const auto& geography =
                    static_cast<const iceberg::GeographyType&>(*iceberg_field.field_type());
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
                                                fields, source_root->logical_type(),
                                                source_root->field_id()));
}

} // namespace

ParquetOutputStream::ParquetOutputStream(doris::io::FileWriter* file_writer)
        : _file_writer(file_writer), _cur_pos(0), _written_len(0) {
    set_mode(arrow::io::FileMode::WRITE);
}

ParquetOutputStream::~ParquetOutputStream() {
    arrow::Status st = Close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet file error: " << st.ToString();
    }
}

arrow::Status ParquetOutputStream::Write(const void* data, int64_t nbytes) {
    if (_is_closed) {
        return arrow::Status::OK();
    }
    size_t written_len = nbytes;
    Status st = _file_writer->append({static_cast<const uint8_t*>(data), written_len});
    if (!st.ok()) {
        return arrow::Status::IOError(st.to_string());
    }
    _cur_pos += written_len;
    _written_len += written_len;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetOutputStream::Tell() const {
    return _cur_pos;
}

arrow::Status ParquetOutputStream::Close() {
    if (!_is_closed) {
        Defer defer {[this] { _is_closed = true; }};
        Status st = _file_writer->close();
        if (!st.ok()) {
            LOG(WARNING) << "close parquet output stream failed: " << st;
            return arrow::Status::IOError(st.to_string());
        }
    }
    return arrow::Status::OK();
}

int64_t ParquetOutputStream::get_written_len() const {
    return _written_len;
}

void ParquetOutputStream::set_written_len(int64_t written_len) {
    _written_len = written_len;
}

void ParquetBuildHelper::build_compression_type(
        ::parquet::WriterProperties::Builder& builder,
        const TParquetCompressionType::type& compression_type) {
    switch (compression_type) {
    case TParquetCompressionType::SNAPPY: {
        builder.compression(arrow::Compression::SNAPPY);
        break;
    }
    case TParquetCompressionType::GZIP: {
        builder.compression(arrow::Compression::GZIP);
        break;
    }
    case TParquetCompressionType::BROTLI: {
        builder.compression(arrow::Compression::BROTLI);
        break;
    }
    case TParquetCompressionType::ZSTD: {
        builder.compression(arrow::Compression::ZSTD);
        break;
    }
    case TParquetCompressionType::LZ4: {
        builder.compression(arrow::Compression::LZ4);
        break;
    }
    case TParquetCompressionType::LZ4_HADOOP: {
        constexpr int64_t HADOOP_LZ4_DEFAULT_BUFFER_SIZE = 256 * 1024;
        // Hadoop-framed LZ4 -> Parquet thrift codec "LZ4" (deprecated). This matches what
        // Spark/Iceberg writes for `write.parquet.compression-codec=lz4`. Arrow 17 emits one
        // Hadoop LZ4 block per Parquet page/dictionary page, while Hadoop JVM readers default to
        // a 256 KiB LZ4 codec buffer, so keep page targets below that buffer size.
        builder.compression(arrow::Compression::LZ4_HADOOP);
        builder.data_pagesize(HADOOP_LZ4_DEFAULT_BUFFER_SIZE / 2);
        builder.dictionary_pagesize_limit(HADOOP_LZ4_DEFAULT_BUFFER_SIZE / 2);
        break;
    }
    // arrow do not support lzo and bz2 compression type.
    // case TParquetCompressionType::LZO: {
    //     builder.compression(arrow::Compression::LZO);
    //     break;
    // }
    // case TParquetCompressionType::BZ2: {
    //     builder.compression(arrow::Compression::BZ2);
    //     break;
    // }
    case TParquetCompressionType::UNCOMPRESSED: {
        builder.compression(arrow::Compression::UNCOMPRESSED);
        break;
    }
    default:
        builder.compression(arrow::Compression::SNAPPY);
    }
}

void ParquetBuildHelper::build_version(::parquet::WriterProperties::Builder& builder,
                                       const TParquetVersion::type& parquet_version) {
    switch (parquet_version) {
    case TParquetVersion::PARQUET_1_0: {
        builder.version(::parquet::ParquetVersion::PARQUET_1_0);
        break;
    }
    case TParquetVersion::PARQUET_2_LATEST: {
        builder.version(::parquet::ParquetVersion::PARQUET_2_LATEST);
        break;
    }
    default:
        builder.version(::parquet::ParquetVersion::PARQUET_1_0);
    }
}

VParquetTransformer::VParquetTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                                         const VExprContextSPtrs& output_vexpr_ctxs,
                                         std::vector<std::string> column_names,
                                         bool output_object_data,
                                         const ParquetFileOptions& parquet_options,
                                         const std::string* iceberg_schema_json,
                                         const iceberg::Schema* iceberg_schema)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _column_names(std::move(column_names)),
          _parquet_options(parquet_options),
          _iceberg_schema_json(iceberg_schema_json),
          _iceberg_schema(iceberg_schema) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
}

VParquetTransformer::VParquetTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                                         const VExprContextSPtrs& output_vexpr_ctxs,
                                         std::vector<TParquetSchema> parquet_schemas,
                                         bool output_object_data,
                                         const ParquetFileOptions& parquet_options,
                                         const std::string* iceberg_schema_json)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _parquet_schemas(std::move(parquet_schemas)),
          _parquet_options(parquet_options),
          _iceberg_schema_json(iceberg_schema_json) {
    _iceberg_schema = nullptr;
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
}

Status VParquetTransformer::_parse_properties() {
    try {
        arrow::MemoryPool* pool = get_arrow_memory_pool();

        //build parquet writer properties
        ::parquet::WriterProperties::Builder builder;
        ParquetBuildHelper::build_compression_type(builder, _parquet_options.compression_type);
        ParquetBuildHelper::build_version(builder, _parquet_options.parquet_version);
        if (_parquet_options.parquet_disable_dictionary) {
            builder.disable_dictionary();
        } else {
            builder.enable_dictionary();
        }
        builder.created_by(
                fmt::format("{}({})", doris::get_short_version(), ::parquet::DEFAULT_CREATED_BY));
        builder.max_row_group_length(std::numeric_limits<int64_t>::max());
        builder.memory_pool(pool);
        _parquet_writer_properties = builder.build();

        //build arrow  writer properties
        ::parquet::ArrowWriterProperties::Builder arrow_builder;
        if (_parquet_options.enable_int96_timestamps) {
            arrow_builder.enable_force_write_int96_timestamps();
        }
        arrow_builder.store_schema();
        _arrow_properties = arrow_builder.build();
    } catch (const ::parquet::ParquetException& e) {
        return Status::InternalError("parquet writer parse properties error: {}", e.what());
    }
    return Status::OK();
}

Status VParquetTransformer::_parse_schema() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    if (_iceberg_schema != nullptr) {
        RETURN_IF_ERROR(
                iceberg::ArrowSchemaUtil::convert(_iceberg_schema, _state->timezone(), fields));
    } else {
        for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
            std::shared_ptr<arrow::DataType> type;
            RETURN_IF_ERROR(convert_to_arrow_type(_output_vexpr_ctxs[i]->root()->data_type(), &type,
                                                  _state->timezone()));
            if (!_parquet_schemas.empty()) {
                std::shared_ptr<arrow::Field> field =
                        arrow::field(_parquet_schemas[i].schema_column_name, type,
                                     _output_vexpr_ctxs[i]->root()->is_nullable());
                fields.emplace_back(field);
            } else {
                std::shared_ptr<arrow::Field> field = arrow::field(
                        _column_names[i], type, _output_vexpr_ctxs[i]->root()->is_nullable());
                fields.emplace_back(field);
            }
        }
    }

    if (_iceberg_schema_json != nullptr) {
        std::shared_ptr<arrow::KeyValueMetadata> schema_metadata =
                arrow::KeyValueMetadata::Make({"iceberg.schema"}, {*_iceberg_schema_json});
        _arrow_schema = arrow::schema(std::move(fields), std::move(schema_metadata));
    } else {
        _arrow_schema = arrow::schema(std::move(fields));
    }
    return Status::OK();
}

Status VParquetTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    if (_iceberg_schema != nullptr) {
        ColumnNumbers column_numbers(block.columns());
        std::iota(column_numbers.begin(), column_numbers.end(), 0);
        RETURN_IF_ERROR(validate_spatial_wkb_inputs(block, column_numbers));
    }

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(convert_to_arrow_batch(block, _arrow_schema, get_arrow_memory_pool(), &result,
                                           _state->timezone_obj()));
    if (_write_size == 0) {
        RETURN_DORIS_STATUS_IF_ERROR(_writer->NewBufferedRowGroup());
    }
    RETURN_DORIS_STATUS_IF_ERROR(_writer->WriteRecordBatch(*result));
    _write_size += block.bytes();
    if (_write_size >= doris::config::min_row_group_size) {
        _write_size = 0;
    }
    return Status::OK();
}

arrow::Status VParquetTransformer::_open_file_writer() {
    if (_iceberg_schema != nullptr) {
        bool has_spatial_column = false;
        for (const auto& column : _iceberg_schema->columns()) {
            has_spatial_column = has_spatial_column || contains_spatial_type(column.field_type());
        }
        if (has_spatial_column) {
            ARROW_ASSIGN_OR_RAISE(auto parquet_schema,
                                  make_iceberg_parquet_schema(_arrow_schema, *_iceberg_schema,
                                                              _parquet_writer_properties,
                                                              _arrow_properties));
            auto parquet_writer = ::parquet::ParquetFileWriter::Open(
                    _outstream, std::move(parquet_schema), _parquet_writer_properties);
            ARROW_RETURN_NOT_OK(::parquet::arrow::FileWriter::Make(
                    get_arrow_memory_pool(), std::move(parquet_writer), _arrow_schema,
                    _arrow_properties, &_writer));
            return arrow::Status::OK();
        }
    }
    ARROW_ASSIGN_OR_RAISE(_writer, ::parquet::arrow::FileWriter::Open(
                                           *_arrow_schema, get_arrow_memory_pool(), _outstream,
                                           _parquet_writer_properties, _arrow_properties));
    return arrow::Status::OK();
}

Status VParquetTransformer::open() {
    RETURN_IF_ERROR(_parse_properties());
    RETURN_IF_ERROR(_parse_schema());
    try {
        RETURN_DORIS_STATUS_IF_ERROR(_open_file_writer());
    } catch (const ::parquet::ParquetStatusException& e) {
        LOG(WARNING) << "parquet file writer open error: " << e.what();
        return Status::InternalError("parquet file writer open error: {}", e.what());
    }
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

int64_t VParquetTransformer::written_len() {
    return _outstream->get_written_len();
}

Status VParquetTransformer::close() {
    try {
        if (_writer != nullptr) {
            RETURN_DORIS_STATUS_IF_ERROR(_writer->Close());
        }
        RETURN_DORIS_STATUS_IF_ERROR(_outstream->Close());

    } catch (const std::exception& e) {
        LOG(WARNING) << "Parquet writer close error: " << e.what();
        return Status::IOError(e.what());
    }

    return Status::OK();
}

Status VParquetTransformer::collect_file_statistics_after_close(TIcebergColumnStats* stats) {
    std::shared_ptr<::parquet::FileMetaData> file_metadata = _writer->metadata();
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
