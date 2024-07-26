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

#include "viceberg_partition_writer.h"

#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_map.h"
#include "vec/core/materialize_block.h"
#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace vectorized {

VIcebergPartitionWriter::VIcebergPartitionWriter(
        const TDataSink& t_sink, std::vector<std::string> partition_values,
        const VExprContextSPtrs& write_output_expr_ctxs, const iceberg::Schema& schema,
        const std::string* iceberg_schema_json, std::vector<std::string> write_column_names,
        WriteInfo write_info, std::string file_name, int file_name_index,
        TFileFormatType::type file_format_type, TFileCompressType::type compress_type,
        const std::map<std::string, std::string>& hadoop_conf)
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
          _hadoop_conf(hadoop_conf) {}

Status VIcebergPartitionWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    io::FSPropertiesRef fs_properties(_write_info.file_type);
    fs_properties.properties = &_hadoop_conf;
    io::FileDescription file_description = {
            .path = fmt::format("{}/{}", _write_info.write_path, _get_target_file_name()),
            .fs_name {}};
    _fs = DORIS_TRY(FileFactory::create_fs(fs_properties, file_description));
    io::FileWriterOptions file_writer_options = {.used_by_s3_committer = false};
    RETURN_IF_ERROR(_fs->create_file(file_description.path, &_file_writer, &file_writer_options));

    switch (_file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        bool parquet_disable_dictionary = false;
        TParquetCompressionType::type parquet_compression_type;
        switch (_compress_type) {
        case TFileCompressType::PLAIN: {
            parquet_compression_type = TParquetCompressionType::UNCOMPRESSED;
            break;
        }
        case TFileCompressType::SNAPPYBLOCK: {
            parquet_compression_type = TParquetCompressionType::SNAPPY;
            break;
        }
        case TFileCompressType::ZSTD: {
            parquet_compression_type = TParquetCompressionType::ZSTD;
            break;
        }
        default: {
            return Status::InternalError("Unsupported compress type {} with parquet",
                                         to_string(_compress_type));
        }
        }
        _file_format_transformer.reset(new VParquetTransformer(
                state, _file_writer.get(), _write_output_expr_ctxs, _write_column_names,
                parquet_compression_type, parquet_disable_dictionary, TParquetVersion::PARQUET_1_0,
                false, _iceberg_schema_json));
        return _file_format_transformer->open();
    }
    case TFileFormatType::FORMAT_ORC: {
        _file_format_transformer.reset(
                new VOrcTransformer(state, _file_writer.get(), _write_output_expr_ctxs, "",
                                    _write_column_names, false, _compress_type, &_schema));
        return _file_format_transformer->open();
    }
    default: {
        return Status::InternalError("Unsupported file format type {}",
                                     to_string(_file_format_type));
    }
    }
}

Status VIcebergPartitionWriter::close(const Status& status) {
    if (_file_format_transformer != nullptr) {
        Status st = _file_format_transformer->close();
        if (!st.ok()) {
            LOG(WARNING) << fmt::format("_file_format_transformer close failed, reason: {}",
                                        st.to_string());
        }
    }
    if (!status.ok() && _fs != nullptr) {
        auto path = fmt::format("{}/{}", _write_info.write_path, _file_name);
        Status st = _fs->delete_file(path);
        if (!st.ok()) {
            LOG(WARNING) << fmt::format("Delete file {} failed, reason: {}", path, st.to_string());
        }
    }
    if (status.ok()) {
        _state->iceberg_commit_datas().emplace_back(_build_iceberg_commit_data());
    }
    return Status::OK();
}

Status VIcebergPartitionWriter::write(vectorized::Block& block) {
    RETURN_IF_ERROR(_file_format_transformer->write(block));
    _row_count += block.rows();
    return Status::OK();
}

TIcebergCommitData VIcebergPartitionWriter::_build_iceberg_commit_data() {
    TIcebergCommitData iceberg_commit_data;
    iceberg_commit_data.__set_file_path(
            fmt::format("{}/{}", _write_info.original_write_path, _get_target_file_name()));
    iceberg_commit_data.__set_row_count(_row_count);
    DCHECK(_file_format_transformer != nullptr);
    iceberg_commit_data.__set_file_size(_file_format_transformer->written_len());
    iceberg_commit_data.__set_file_content(TFileContent::DATA);
    iceberg_commit_data.__set_partition_values(_partition_values);
    return iceberg_commit_data;
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

} // namespace vectorized
} // namespace doris
