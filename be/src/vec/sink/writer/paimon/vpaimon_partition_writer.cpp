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

#include "vpaimon_partition_writer.h"

#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace vectorized {

VPaimonPartitionWriter::VPaimonPartitionWriter(
        std::vector<std::string> partition_values,
        const VExprContextSPtrs& write_output_expr_ctxs,
        std::vector<std::string> write_column_names, WriteInfo write_info, std::string file_name,
        int file_name_index, TFileFormatType::type file_format_type,
        TFileCompressType::type compress_type,
        const std::map<std::string, std::string>& hadoop_conf)
        : _partition_values(std::move(partition_values)),
          _write_output_expr_ctxs(write_output_expr_ctxs),
          _write_column_names(std::move(write_column_names)),
          _write_info(std::move(write_info)),
          _file_name(std::move(file_name)),
          _file_name_index(file_name_index),
          _file_format_type(file_format_type),
          _compress_type(compress_type),
          _hadoop_conf(hadoop_conf) {}

Status VPaimonPartitionWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    io::FSPropertiesRef fs_properties(_write_info.file_type);
    fs_properties.properties = &_hadoop_conf;
    if (!_write_info.broker_addresses.empty()) {
        fs_properties.broker_addresses = &(_write_info.broker_addresses);
    }
    // Files go into bucket-0 subdirectory
    std::string target_file = fmt::format("{}/bucket-0/{}", _write_info.write_path,
                                          _get_target_file_name());
    io::FileDescription file_description = {.path = target_file, .fs_name {}};
    _fs = DORIS_TRY(FileFactory::create_fs(fs_properties, file_description));
    io::FileWriterOptions file_writer_options = {.used_by_s3_committer = false};
    RETURN_IF_ERROR(_fs->create_file(file_description.path, &_file_writer, &file_writer_options));

    switch (_file_format_type) {
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
        ParquetFileOptions parquet_options = {.compression_type = parquet_compression_type,
                                              .parquet_version = TParquetVersion::PARQUET_1_0,
                                              .parquet_disable_dictionary = false,
                                              .enable_int96_timestamps = false};
        _file_format_transformer = std::make_unique<VParquetTransformer>(
                state, _file_writer.get(), _write_output_expr_ctxs, _write_column_names, false,
                parquet_options, nullptr, nullptr);
        return _file_format_transformer->open();
    }
    case TFileFormatType::FORMAT_ORC: {
        _file_format_transformer = std::make_unique<VOrcTransformer>(
                state, _file_writer.get(), _write_output_expr_ctxs, "", _write_column_names, false,
                _compress_type, nullptr, _fs);
        return _file_format_transformer->open();
    }
    default:
        return Status::InternalError("Unsupported file format type {}", to_string(_file_format_type));
    }
}

Status VPaimonPartitionWriter::write(vectorized::Block& block) {
    RETURN_IF_ERROR(_file_format_transformer->write(block));
    _row_count += block.rows();
    return Status::OK();
}

Status VPaimonPartitionWriter::close(const Status& status) {
    Status result_status;
    if (_file_format_transformer != nullptr) {
        result_status = _file_format_transformer->close();
        if (!result_status.ok()) {
            LOG(WARNING) << fmt::format("paimon partition writer close failed: {}",
                                        result_status.to_string());
        }
    }
    bool status_ok = result_status.ok() && status.ok();
    LOG(INFO) << fmt::format("VPaimonPartitionWriter::close - result_status.ok()={}, status.ok()={}, status_ok={}, row_count={}, file={}",
                             result_status.ok(), status.ok(), status_ok, _row_count,
                             _get_target_file_name());
    if (!status_ok && _fs != nullptr) {
        auto path = fmt::format("{}/bucket-0/{}", _write_info.write_path, _get_target_file_name());
        Status st = _fs->delete_file(path);
        if (!st.ok()) {
            LOG(WARNING) << fmt::format("Delete paimon file {} failed: {}", path, st.to_string());
        }
    }
    if (status_ok) {
        TPaimonCommitData commit_data;
        _build_paimon_commit_data(&commit_data);
        _state->add_paimon_commit_datas(commit_data);
        LOG(INFO) << fmt::format("Added paimon commit data: file_path={}, row_count={}, file_size={}",
                                 commit_data.file_path, commit_data.row_count, commit_data.file_size);
    } else {
        LOG(WARNING) << fmt::format("Did NOT add paimon commit data due to status_ok=false");
    }
    return result_status;
}

void VPaimonPartitionWriter::_build_paimon_commit_data(TPaimonCommitData* commit_data) {
    DCHECK(commit_data != nullptr);
    DCHECK(_file_format_transformer != nullptr);
    // Full path: original_write_path/bucket-0/filename
    commit_data->__set_file_path(fmt::format("{}/bucket-0/{}", _write_info.original_write_path,
                                             _get_target_file_name()));
    commit_data->__set_row_count(_row_count);
    commit_data->__set_file_size(_file_format_transformer->written_len());
    commit_data->__set_partition_values(_partition_values);
}

std::string VPaimonPartitionWriter::_get_file_extension(TFileFormatType::type file_format_type,
                                                        TFileCompressType::type compress_type) {
    std::string compress_name;
    switch (compress_type) {
    case TFileCompressType::SNAPPYBLOCK:
        compress_name = ".snappy";
        break;
    case TFileCompressType::ZSTD:
        compress_name = ".zstd";
        break;
    case TFileCompressType::ZLIB:
        compress_name = ".zlib";
        break;
    default:
        compress_name = "";
        break;
    }
    std::string format_name;
    switch (file_format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        format_name = ".parquet";
        break;
    case TFileFormatType::FORMAT_ORC:
        format_name = ".orc";
        break;
    default:
        format_name = "";
        break;
    }
    return fmt::format("{}{}", compress_name, format_name);
}

std::string VPaimonPartitionWriter::_get_target_file_name() {
    return fmt::format("data-{}-{}{}", _file_name, _file_name_index,
                       _get_file_extension(_file_format_type, _compress_type));
}

} // namespace vectorized
} // namespace doris
