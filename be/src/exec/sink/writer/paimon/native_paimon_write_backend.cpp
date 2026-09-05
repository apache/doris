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

#include "exec/sink/writer/paimon/native_paimon_write_backend.h"

#include <arrow/type.h>
#include <fmt/format.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/block/materialize_block.h"
#include "format/table/paimon/arrow_schema_util.h"
#include "format/transformer/vparquet_transformer.h"
#include "io/file_factory.h"
#include "io/fs/file_system.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

Status parquet_compression(TFileCompressType::type compression,
                           TParquetCompressionType::type* result) {
    switch (compression) {
    case TFileCompressType::PLAIN:
        *result = TParquetCompressionType::UNCOMPRESSED;
        return Status::OK();
    case TFileCompressType::SNAPPYBLOCK:
        *result = TParquetCompressionType::SNAPPY;
        return Status::OK();
    case TFileCompressType::ZSTD:
        *result = TParquetCompressionType::ZSTD;
        return Status::OK();
    case TFileCompressType::LZ4BLOCK:
        *result = TParquetCompressionType::LZ4_HADOOP;
        return Status::OK();
    case TFileCompressType::GZ:
        *result = TParquetCompressionType::GZIP;
        return Status::OK();
    default:
        return Status::NotSupported("Unsupported Paimon Parquet compression {}",
                                    to_string(compression));
    }
}

class NativePaimonWriter final : public IPaimonWriter {
public:
    NativePaimonWriter(TPaimonTableSink sink, RuntimeState* state)
            : _sink(std::move(sink)),
              _info(_sink.native_write_info),
              _state(state),
              _writer_uuid(generate_uuid_string()) {}

    Status write(RuntimeState*, Block& block) override {
        if (_prepared) {
            return Status::InternalError("Cannot write after preparing a Paimon native commit");
        }
        if (block.rows() == 0) {
            return Status::OK();
        }
        materialize_block_inplace(block);
        if (_arrow_schema == nullptr) {
            RETURN_IF_ERROR(paimon::ArrowSchemaUtil::convert(_info.schema, block,
                                                             _state->timezone(), &_arrow_schema));
        }
        if (_transformer != nullptr &&
            _transformer->written_len() >= _info.target_file_size_bytes) {
            RETURN_IF_ERROR(_close_current_file());
        }
        if (_transformer == nullptr) {
            RETURN_IF_ERROR(_open_next_file());
        }
        RETURN_IF_ERROR(_transformer->write(block));
        _current_row_count += block.rows();
        _next_sequence_number += block.rows();
        return Status::OK();
    }

    Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override {
        if (_prepared) {
            return Status::InternalError("Paimon native commit was already prepared");
        }
        RETURN_IF_ERROR(_close_current_file());
        messages.reserve(messages.size() + _commit_data.size());
        for (const auto& data : _commit_data) {
            TPaimonCommitMessage message;
            message.__set_native_commit_data(data);
            messages.emplace_back(std::move(message));
        }
        _prepared = true;
        return Status::OK();
    }

    Status abort() override {
        Status result = Status::OK();
        _transformer.reset();
        _file_writer.reset();
        if (_fs != nullptr) {
            for (const auto& path : _created_files) {
                Status st = _fs->delete_file(path);
                if (!st.ok()) {
                    LOG(WARNING) << "Failed to delete uncommitted native Paimon file " << path
                                 << ": " << st;
                    if (result.ok()) {
                        result = st;
                    }
                }
            }
        }
        _created_files.clear();
        _commit_data.clear();
        return result;
    }

private:
    Status _ensure_file_system() {
        if (_fs != nullptr) {
            return Status::OK();
        }
        io::FSPropertiesRef fs_properties(_info.file_type);
        fs_properties.properties = &_sink.hadoop_config;
        if (_info.__isset.broker_addresses && !_info.broker_addresses.empty()) {
            fs_properties.broker_addresses = &_info.broker_addresses;
        }
        io::FileDescription description = {.path = _info.output_path, .fs_name = {}};
        _fs = DORIS_TRY(FileFactory::create_fs(fs_properties, description));
        return _fs->create_directory(_info.output_path, false);
    }

    Status _open_next_file() {
        RETURN_IF_ERROR(_ensure_file_system());
        _current_file_name =
                fmt::format("{}{}-{}.parquet", _info.data_file_prefix, _writer_uuid, _file_index++);
        _current_path = fmt::format("{}/{}", _info.output_path, _current_file_name);
        io::FileWriterOptions writer_options = {.used_by_s3_committer = false};
        RETURN_IF_ERROR(_fs->create_file(_current_path, &_file_writer, &writer_options));
        _created_files.emplace_back(_current_path);

        TParquetCompressionType::type compression;
        RETURN_IF_ERROR(parquet_compression(_info.compression_type, &compression));
        ParquetFileOptions parquet_options = {.compression_type = compression,
                                              .parquet_version = TParquetVersion::PARQUET_1_0,
                                              .parquet_disable_dictionary = false,
                                              .enable_int96_timestamps = false,
                                              .store_decimal_as_integer = true};
        _transformer = std::make_unique<VParquetTransformer>(_state, _file_writer.get(),
                                                             VExprContextSPtrs {}, _arrow_schema,
                                                             false, parquet_options);
        Status st = _transformer->open();
        if (!st.ok()) {
            _transformer.reset();
            _file_writer.reset();
            Status delete_st = _fs->delete_file(_current_path);
            if (!delete_st.ok()) {
                LOG(WARNING) << "Failed to clean up Paimon file after open failure: " << delete_st;
            }
            _created_files.pop_back();
            return st;
        }
        _current_row_count = 0;
        _current_min_sequence_number = _next_sequence_number;
        return Status::OK();
    }

    Status _close_current_file() {
        if (_transformer == nullptr) {
            return Status::OK();
        }
        Status st = _transformer->close();
        const int64_t file_size = _transformer->written_len();
        _transformer.reset();
        _file_writer.reset();
        if (!st.ok()) {
            Status delete_st = _fs->delete_file(_current_path);
            if (!delete_st.ok()) {
                LOG(WARNING) << "Failed to clean up Paimon file after close failure: " << delete_st;
            }
            return st;
        }
        if (_current_row_count == 0) {
            RETURN_IF_ERROR(_fs->delete_file(_current_path));
            return Status::OK();
        }

        TPaimonNativeCommitData data;
        data.__set_file_name(_current_file_name);
        data.__set_file_size(file_size);
        data.__set_row_count(_current_row_count);
        data.__set_min_sequence_number(_current_min_sequence_number);
        data.__set_max_sequence_number(_next_sequence_number - 1);
        data.__set_schema_id(_info.schema.schema_id);
        data.__set_bucket(0);
        data.__set_total_buckets(-1);
        _commit_data.emplace_back(std::move(data));
        return Status::OK();
    }

    TPaimonTableSink _sink;
    const TPaimonNativeWriteInfo& _info;
    RuntimeState* _state;
    std::string _writer_uuid;
    int _file_index = 0;
    int64_t _next_sequence_number = 0;
    int64_t _current_min_sequence_number = 0;
    int64_t _current_row_count = 0;
    bool _prepared = false;

    std::shared_ptr<arrow::Schema> _arrow_schema;
    std::shared_ptr<io::FileSystem> _fs;
    std::unique_ptr<io::FileWriter> _file_writer;
    std::unique_ptr<VParquetTransformer> _transformer;
    std::string _current_file_name;
    std::string _current_path;
    std::vector<std::string> _created_files;
    std::vector<TPaimonNativeCommitData> _commit_data;
};

} // namespace

Status NativePaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state,
                                      RuntimeProfile*) {
    if (!sink.__isset.native_write_info) {
        return Status::InvalidArgument("Paimon native backend requires native_write_info");
    }
    const auto& info = sink.native_write_info;
    if (!info.__isset.schema || !info.schema.__isset.schema_id || !info.__isset.output_path ||
        info.output_path.empty() || !info.__isset.file_type || !info.__isset.file_format ||
        info.file_format != TFileFormatType::FORMAT_PARQUET || !info.__isset.compression_type ||
        !info.__isset.target_file_size_bytes || info.target_file_size_bytes <= 0 ||
        !info.__isset.data_file_prefix || info.data_file_prefix.empty()) {
        return Status::InvalidArgument("Incomplete phase-one Paimon native write configuration");
    }
    if (!sink.__isset.write_mode || sink.write_mode != TPaimonWriteMode::APPEND) {
        return Status::NotSupported("Paimon native phase one supports APPEND only");
    }
    _sink = sink;
    _state = state;
    _opened = true;
    return Status::OK();
}

Status NativePaimonWriteBackend::create_writer(std::unique_ptr<IPaimonWriter>* writer) {
    if (!_opened || _state == nullptr) {
        return Status::InternalError("Paimon native backend is not open");
    }
    *writer = std::make_unique<NativePaimonWriter>(_sink, _state);
    return Status::OK();
}

Status NativePaimonWriteBackend::close() {
    _opened = false;
    _state = nullptr;
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
