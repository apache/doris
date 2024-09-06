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

#include "vhive_partition_writer.h"

#include <aws/s3/model/CompletedPart.h>

#include "io/file_factory.h"
#include "io/fs/s3_file_writer.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_map.h"
#include "vec/core/materialize_block.h"
#include "vec/runtime/vcsv_transformer.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace vectorized {

VHivePartitionWriter::VHivePartitionWriter(const TDataSink& t_sink, std::string partition_name,
                                           TUpdateMode::type update_mode,
                                           const VExprContextSPtrs& write_output_expr_ctxs,
                                           std::vector<std::string> write_column_names,
                                           WriteInfo write_info, std::string file_name,
                                           int file_name_index,
                                           TFileFormatType::type file_format_type,
                                           TFileCompressType::type hive_compress_type,
                                           const THiveSerDeProperties* hive_serde_properties,
                                           const std::map<std::string, std::string>& hadoop_conf)
        : _partition_name(std::move(partition_name)),
          _update_mode(update_mode),
          _write_output_expr_ctxs(write_output_expr_ctxs),
          _write_column_names(std::move(write_column_names)),
          _write_info(std::move(write_info)),
          _file_name(std::move(file_name)),
          _file_name_index(file_name_index),
          _file_format_type(file_format_type),
          _hive_compress_type(hive_compress_type),
          _hive_serde_properties(hive_serde_properties),
          _hadoop_conf(hadoop_conf) {}

Status VHivePartitionWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    io::FSPropertiesRef fs_properties(_write_info.file_type);
    fs_properties.properties = &_hadoop_conf;
    io::FileDescription file_description = {
            .path = fmt::format("{}/{}", _write_info.write_path, _get_target_file_name()),
            .fs_name {}};
    // If the destination path contains a schema, use the schema directly.
    // If not, use defaultFS.
    // Otherwise a write error will occur.
    // example:
    //    hdfs://host:port/path1/path2  --> hdfs://host:port
    //    hdfs://nameservice/path1/path2 --> hdfs://nameservice
    string::size_type idx = file_description.path.find("://");
    if (idx != string::npos) {
        idx = file_description.path.find("/", idx + 3);
        if (idx != string::npos) {
            file_description.fs_name = file_description.path.substr(0, idx);
        }
    }
    _fs = DORIS_TRY(FileFactory::create_fs(fs_properties, file_description));
    io::FileWriterOptions file_writer_options = {.used_by_s3_committer = true};
    RETURN_IF_ERROR(_fs->create_file(file_description.path, &_file_writer, &file_writer_options));

    switch (_file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        bool parquet_disable_dictionary = false;
        TParquetCompressionType::type parquet_compression_type;
        switch (_hive_compress_type) {
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
            return Status::InternalError("Unsupported hive compress type {} with parquet",
                                         to_string(_hive_compress_type));
        }
        }
        _file_format_transformer = std::make_unique<VParquetTransformer>(
                state, _file_writer.get(), _write_output_expr_ctxs, _write_column_names,
                parquet_compression_type, parquet_disable_dictionary, TParquetVersion::PARQUET_1_0,
                false);
        return _file_format_transformer->open();
    }
    case TFileFormatType::FORMAT_ORC: {
        _file_format_transformer = std::make_unique<VOrcTransformer>(
                state, _file_writer.get(), _write_output_expr_ctxs, "", _write_column_names, false,
                _hive_compress_type);
        return _file_format_transformer->open();
    }
    case TFileFormatType::FORMAT_CSV_PLAIN: {
        // TODO(syt): support hive csv table, only hive text file is supportted now
        _file_format_transformer = std::make_unique<VCSVTransformer>(
                state, _file_writer.get(), _write_output_expr_ctxs, false, "csv", "",
                _hive_serde_properties->field_delim, _hive_serde_properties->line_delim, false,
                _hive_compress_type, _hive_serde_properties);
        return _file_format_transformer->open();
    }
    default: {
        return Status::InternalError("Unsupported file format type {}",
                                     to_string(_file_format_type));
    }
    }
}

Status VHivePartitionWriter::close(const Status& status) {
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
        auto path = fmt::format("{}/{}", _write_info.write_path, _file_name);
        Status st = _fs->delete_file(path);
        if (!st.ok()) {
            LOG(WARNING) << fmt::format("Delete file {} failed, reason: {}", path, st.to_string());
        }
    }
    if (status_ok) {
        _state->hive_partition_updates().emplace_back(_build_partition_update());
    }
    return result_status;
}

Status VHivePartitionWriter::write(vectorized::Block& block) {
    RETURN_IF_ERROR(_file_format_transformer->write(block));
    _row_count += block.rows();
    return Status::OK();
}

THivePartitionUpdate VHivePartitionWriter::_build_partition_update() {
    THivePartitionUpdate hive_partition_update;
    hive_partition_update.__set_name(_partition_name);
    hive_partition_update.__set_update_mode(_update_mode);
    THiveLocationParams location;
    location.__set_write_path(_write_info.original_write_path);
    location.__set_target_path(_write_info.target_path);
    hive_partition_update.__set_location(location);
    hive_partition_update.__set_file_names({_get_target_file_name()});
    hive_partition_update.__set_row_count(_row_count);
    DCHECK(_file_format_transformer != nullptr);
    hive_partition_update.__set_file_size(_file_format_transformer->written_len());

    if (_write_info.file_type == TFileType::FILE_S3) {
        DCHECK(_file_writer != nullptr);
        doris::io::S3FileWriter* s3_mpu_file_writer =
                dynamic_cast<doris::io::S3FileWriter*>(_file_writer.get());
        DCHECK(s3_mpu_file_writer != nullptr);
        TS3MPUPendingUpload s3_mpu_pending_upload;
        s3_mpu_pending_upload.__set_bucket(s3_mpu_file_writer->bucket());
        s3_mpu_pending_upload.__set_key(s3_mpu_file_writer->key());
        s3_mpu_pending_upload.__set_upload_id(s3_mpu_file_writer->upload_id());

        std::map<int, std::string> etags;
        for (auto& completed_part : s3_mpu_file_writer->completed_parts()) {
            etags.insert({completed_part.part_num, completed_part.etag});
        }
        s3_mpu_pending_upload.__set_etags(etags);
        hive_partition_update.__set_s3_mpu_pending_uploads({s3_mpu_pending_upload});
    }
    return hive_partition_update;
}

std::string VHivePartitionWriter::_get_file_extension(TFileFormatType::type file_format_type,
                                                      TFileCompressType::type write_compress_type) {
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
        compress_name = ".zst";
        break;
    }
    case TFileCompressType::GZ: {
        compress_name = ".gz";
        break;
    }
    case TFileCompressType::BZ2: {
        compress_name = ".bz2";
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

std::string VHivePartitionWriter::_get_target_file_name() {
    return fmt::format("{}-{}{}", _file_name, _file_name_index,
                       _get_file_extension(_file_format_type, _hive_compress_type));
}

} // namespace vectorized
} // namespace doris
