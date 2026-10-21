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

#include "exec/sink/writer/iceberg/viceberg_partition_writer.h"

#include <memory>
#include <sstream>

#include "core/block/materialize_block.h"
#include "core/column/column_map.h"
#include "format/table/iceberg/schema.h"
#include "format/transformer/viceberg_parquet_writer.h"
#include "format/transformer/vorc_transformer.h"
#include "format/transformer/vparquet_writer.h"
#include "io/file_factory.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {
bool contains_nanosecond_timestamp(const iceberg::Type& type) {
    switch (type.type_id()) {
    case iceberg::TypeID::TIMESTAMP_NS:
    case iceberg::TypeID::TIMESTAMPTZ_NS:
        return true;
    case iceberg::TypeID::STRUCT:
        for (const auto& field : static_cast<const iceberg::StructType&>(type).fields()) {
            if (contains_nanosecond_timestamp(*field.field_type())) {
                return true;
            }
        }
        return false;
    case iceberg::TypeID::LIST:
        return contains_nanosecond_timestamp(
                *static_cast<const iceberg::ListType&>(type).element_field().field_type());
    case iceberg::TypeID::MAP: {
        const auto& map = static_cast<const iceberg::MapType&>(type);
        return contains_nanosecond_timestamp(*map.key_field().field_type()) ||
               contains_nanosecond_timestamp(*map.value_field().field_type());
    }
    default:
        return false;
    }
}
} // namespace

VIcebergPartitionWriter::VIcebergPartitionWriter(
        const TDataSink& t_sink, std::vector<std::string> partition_values,
        const VExprContextSPtrs& write_output_expr_ctxs, const iceberg::Schema& schema,
        const std::string* iceberg_schema_json, std::vector<std::string> write_column_names,
        WriteInfo write_info, std::string file_name, int file_name_index,
        TFileFormatType::type file_format_type, TFileCompressType::type compress_type,
        const std::map<std::string, std::string>& hadoop_conf,
        ClosedFileCallback closed_file_callback)
        : _partition_values(std::move(partition_values)),
          _write_output_expr_ctxs(write_output_expr_ctxs),
          _schema(schema),
          _iceberg_schema_json(iceberg_schema_json),
          _write_column_names(std::move(write_column_names)),
          _write_info(std::move(write_info)),
          _file_name(std::move(file_name)),
          _file_name_index(file_name_index),
          _file_format_type(file_format_type),
          _compress_type(compress_type),
          _hadoop_conf(hadoop_conf),
          _closed_file_callback(std::move(closed_file_callback)) {
    if (t_sink.iceberg_table_sink.__isset.collect_column_stats) {
        _collect_column_stats = t_sink.iceberg_table_sink.collect_column_stats;
    }
    if (t_sink.iceberg_table_sink.__isset.nan_count_field_ids) {
        _nan_count_field_ids = t_sink.iceberg_table_sink.nan_count_field_ids;
    }
}

Status VIcebergPartitionWriter::open(RuntimeState* state, RuntimeProfile* profile,
                                     const RowDescriptor* row_desc) {
    _state = state;
    io::FSPropertiesRef fs_properties(_write_info.file_type);
    fs_properties.properties = &_hadoop_conf;
    if (!_write_info.broker_addresses.empty()) {
        fs_properties.broker_addresses = &(_write_info.broker_addresses);
    }
    _path = fmt::format("{}/{}", _write_info.write_path, _get_target_file_name());
    io::FileDescription file_description = {.path = _path, .fs_name {}};
    _fs = DORIS_TRY(FileFactory::create_fs(fs_properties, file_description));
    io::FileWriterOptions file_writer_options = {.used_by_s3_committer = false};
    auto create_file = [&]() {
        return _fs->create_file(file_description.path, &_file_writer, &file_writer_options);
    };

    Status open_status;
    switch (_file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        TParquetCompressionType::type parquet_compression_type =
                DORIS_TRY(ParquetBuildHelper::to_parquet_compression_type(_compress_type));
        RETURN_IF_ERROR(create_file());
        // Parquet 1.0 coerces Arrow nanoseconds to microseconds, including nested fields.
        ParquetFileOptions parquet_options = {
                .compression_type = parquet_compression_type,
                .parquet_version = contains_nanosecond_timestamp(_schema.root_struct())
                                           ? TParquetVersion::PARQUET_2_LATEST
                                           : TParquetVersion::PARQUET_1_0,
                .parquet_disable_dictionary = false,
                .enable_int96_timestamps = false};
        _file_format_transformer = std::make_unique<VIcebergParquetWriter>(
                state, _file_writer.get(), _write_output_expr_ctxs, _write_column_names, false,
                parquet_options, _iceberg_schema_json, _schema, _nan_count_field_ids);
        open_status = _file_format_transformer->open();
        break;
    }
    case TFileFormatType::FORMAT_ORC: {
        RETURN_IF_ERROR(create_file());
        _file_format_transformer = std::make_unique<VOrcTransformer>(
                state, _file_writer.get(), _write_output_expr_ctxs, "", _write_column_names, false,
                _compress_type, &_schema, _fs, _nan_count_field_ids);
        open_status = _file_format_transformer->open();
        break;
    }
    default: {
        open_status = Status::InternalError("Unsupported file format type {}",
                                            to_string(_file_format_type));
        break;
    }
    }
    if (!open_status.ok()) {
        // A transformer failure happens after writer creation. Defer deletion until the output
        // stream has finished closing so an asynchronous object upload cannot publish it later.
        _defer_file_cleanup();
    }
    return open_status;
}

void VIcebergPartitionWriter::_defer_file_cleanup() {
    if (_fs == nullptr || _file_cleanup_deferred) {
        return;
    }
    _file_cleanup_deferred = true;
    // Destroy the format writer and its output stream before deleting the physical object. In
    // particular, a failed close may leave an asynchronous upload running, and the object must not
    // be visible after the rollback has completed.
    _file_format_transformer.reset();
    _file_writer.reset();
    if (_closed_file_callback) {
        _closed_file_callback(_fs, _path);
    } else {
        WARN_IF_ERROR(_fs->delete_file(_path), "failed to delete Iceberg file");
    }
}

Status VIcebergPartitionWriter::close(const Status& status) {
    if (_file_cleanup_deferred) {
        return _terminal_close_status;
    }
    Status result_status;
    if (_file_format_transformer != nullptr) {
        result_status = _file_format_transformer->close();
        if (!result_status.ok()) {
            LOG(WARNING) << fmt::format("_file_format_transformer close failed, reason: {}",
                                        result_status.to_string());
        }
    }
    bool status_ok = result_status.ok() && status.ok();
    if (!status_ok && _fs != nullptr) {
        _terminal_close_status = result_status;
        _defer_file_cleanup();
    }
    if (status_ok) {
        TIcebergCommitData commit_data;
        Status commit_status = _build_iceberg_commit_data(&commit_data);
        if (!commit_status.ok()) {
            _terminal_close_status = commit_status;
            _defer_file_cleanup();
            return commit_status;
        }
        Status report_status = _state->add_iceberg_commit_datas(std::move(commit_data));
        if (!report_status.ok()) {
            _terminal_close_status = report_status;
            _defer_file_cleanup();
            return report_status;
        }
        if (_closed_file_callback) {
            _closed_file_callback(_fs, _path);
        }
    }
    return result_status;
}

Status VIcebergPartitionWriter::write(Block& block) {
    RETURN_IF_ERROR(_file_format_transformer->write(block));
    _row_count += block.rows();
    return Status::OK();
}

Status VIcebergPartitionWriter::_build_iceberg_commit_data(TIcebergCommitData* commit_data) {
    DCHECK(commit_data != nullptr);
    DCHECK(_file_format_transformer != nullptr);

    commit_data->__set_file_path(
            fmt::format("{}/{}", _write_info.original_write_path, _get_target_file_name()));
    commit_data->__set_row_count(_row_count);
    commit_data->__set_affected_rows(_row_count);
    commit_data->__set_file_size(_file_format_transformer->written_len());
    commit_data->__set_file_content(TFileContent::DATA);
    commit_data->__set_partition_values(_partition_values);
    // Split offsets describe the physical file, even when the table disables all column metrics.
    if (_file_format_type == TFileFormatType::FORMAT_PARQUET) {
        std::vector<int64_t> offsets;
        RETURN_IF_ERROR(static_cast<VIcebergParquetWriter*>(_file_format_transformer.get())
                                ->collect_split_offsets_after_close(&offsets));
        // Empty files have no splits; leave the optional field absent, as with an older BE.
        if (!offsets.empty()) {
            commit_data->__set_split_offsets(offsets);
        }
    }
    // ORC collection reopens the file, so honor the FE policy before any footer work.
    if (!_collect_column_stats) {
        return Status::OK();
    }
    if (_file_format_type == TFileFormatType::FORMAT_PARQUET) {
        TIcebergColumnStats column_stats;
        RETURN_IF_ERROR(static_cast<VIcebergParquetWriter*>(_file_format_transformer.get())
                                ->collect_file_statistics_after_close(&column_stats));
        commit_data->__set_column_stats(column_stats);
    } else if (_file_format_type == TFileFormatType::FORMAT_ORC) {
        TIcebergColumnStats column_stats;
        RETURN_IF_ERROR(static_cast<VOrcTransformer*>(_file_format_transformer.get())
                                ->collect_file_statistics_after_close(&column_stats));
        commit_data->__set_column_stats(column_stats);
    }
    return Status::OK();
}

std::string VIcebergPartitionWriter::_get_file_extension(
        TFileFormatType::type file_format_type, TFileCompressType::type write_compress_type) {
    std::string compress_name;
    switch (write_compress_type) {
    case TFileCompressType::SNAPPYBLOCK: {
        compress_name = ".snappy";
        break;
    }
    case TFileCompressType::ZLIB: {
        compress_name = ".zlib";
        break;
    }
    case TFileCompressType::ZSTD: {
        compress_name = ".zstd";
        break;
    }
    case TFileCompressType::LZ4BLOCK: {
        compress_name = ".lz4";
        break;
    }
    default: {
        compress_name = "";
        break;
    }
    }

    std::string file_format_name;
    switch (file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        file_format_name = ".parquet";
        break;
    }
    case TFileFormatType::FORMAT_ORC: {
        file_format_name = ".orc";
        break;
    }
    default: {
        file_format_name = "";
        break;
    }
    }
    return fmt::format("{}{}", compress_name, file_format_name);
}

std::string VIcebergPartitionWriter::_get_target_file_name() {
    return fmt::format("{}-{}{}", _file_name, _file_name_index,
                       _get_file_extension(_file_format_type, _compress_type));
}

} // namespace doris
