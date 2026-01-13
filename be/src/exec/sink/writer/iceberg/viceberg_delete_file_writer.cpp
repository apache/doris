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

#include "viceberg_delete_file_writer.h"

#include <fmt/format.h>

#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace vectorized {

VIcebergDeleteFileWriter::VIcebergDeleteFileWriter(TFileContent::type delete_type,
                                                   const std::string& output_path,
                                                   TFileFormatType::type file_format,
                                                   TFileCompressType::type compress_type)
        : _delete_type(delete_type),
          _output_path(output_path),
          _file_format(file_format),
          _compress_type(compress_type) {}

VIcebergDeleteFileWriter::~VIcebergDeleteFileWriter() = default;

Status VIcebergDeleteFileWriter::open(RuntimeState* state, RuntimeProfile* profile,
                                      const VExprContextSPtrs& output_exprs,
                                      const std::vector<std::string>& column_names,
                                      const std::map<std::string, std::string>& hadoop_config,
                                      TFileType::type file_type,
                                      const std::vector<TNetworkAddress>& broker_addresses) {
    if (_delete_type != TFileContent::POSITION_DELETES) {
        return Status::NotSupported("Iceberg delete file writer only supports position deletes");
    }

    _state = state;

    // Create file system and file writer
    io::FSPropertiesRef fs_properties(file_type);
    fs_properties.properties = &hadoop_config;
    if (!broker_addresses.empty()) {
        fs_properties.broker_addresses = &broker_addresses;
    }

    io::FileDescription file_description = {.path = _output_path, .fs_name {}};
    _fs = DORIS_TRY(FileFactory::create_fs(fs_properties, file_description));

    io::FileWriterOptions file_writer_options = {.used_by_s3_committer = false};
    RETURN_IF_ERROR(_fs->create_file(file_description.path, &_file_writer, &file_writer_options));

    // Create file format transformer based on format type
    switch (_file_format) {
    case TFileFormatType::FORMAT_PARQUET: {
        TParquetCompressionType::type parquet_compression_type;
        switch (_compress_type) {
        case TFileCompressType::PLAIN:
            parquet_compression_type = TParquetCompressionType::UNCOMPRESSED;
            break;
        case TFileCompressType::SNAPPYBLOCK:
            parquet_compression_type = TParquetCompressionType::SNAPPY;
            break;
        case TFileCompressType::ZSTD:
            parquet_compression_type = TParquetCompressionType::ZSTD;
            break;
        default:
            return Status::InternalError("Unsupported compress type {} with parquet",
                                         to_string(_compress_type));
        }

        ParquetFileOptions parquet_options = {parquet_compression_type,
                                              TParquetVersion::PARQUET_1_0, false, false};
        _file_format_transformer.reset(new VParquetTransformer(state, _file_writer.get(),
                                                               output_exprs, column_names, false,
                                                               parquet_options, nullptr, nullptr));
        return _file_format_transformer->open();
    }
    case TFileFormatType::FORMAT_ORC: {
        _file_format_transformer.reset(new VOrcTransformer(state, _file_writer.get(), output_exprs,
                                                           "", column_names, false, _compress_type,
                                                           nullptr));
        return _file_format_transformer->open();
    }
    default:
        return Status::InternalError("Unsupported file format type {}", to_string(_file_format));
    }

    return Status::OK();
}

Status VIcebergDeleteFileWriter::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // Write block using file format transformer
    RETURN_IF_ERROR(_file_format_transformer->write(const_cast<Block&>(block)));
    _written_rows += block.rows();

    return Status::OK();
}

Status VIcebergDeleteFileWriter::close(TIcebergCommitData& commit_data) {
    Status result_status;

    if (_file_format_transformer != nullptr) {
        result_status = _file_format_transformer->close();
        if (!result_status.ok()) {
            LOG(WARNING) << fmt::format("_file_format_transformer close failed, reason: {}",
                                        result_status.to_string());
        }
        _file_size = _file_format_transformer->written_len();
    }

    // Fill commit data (use __set_ to mark optional fields as present)
    commit_data.__set_file_path(_output_path);
    commit_data.__set_row_count(_written_rows);
    commit_data.__set_file_size(_file_size);
    commit_data.__set_file_content(_delete_type);

    // Set partition information
    if (!_partition_spec_json.empty()) {
        commit_data.__set_partition_spec_id(0); // TODO: Parse from partition_spec_json
        commit_data.__set_partition_data_json(_partition_data_json);
    }

    if (!_partition_values.empty()) {
        commit_data.__set_partition_values(_partition_values);
    }

    return result_status;
}

// Factory method
std::unique_ptr<VIcebergDeleteFileWriter> VIcebergDeleteFileWriterFactory::create_writer(
        TFileContent::type delete_type, const std::string& output_path,
        TFileFormatType::type file_format, TFileCompressType::type compress_type) {
    return std::make_unique<VIcebergDeleteFileWriter>(delete_type, output_path, file_format,
                                                      compress_type);
}

} // namespace vectorized
} // namespace doris
