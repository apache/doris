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

#include "format/transformer/vparquet_writer.h"

#include <arrow/io/type_fwd.h>
#include <arrow/memory_pool.h>
#include <arrow/table.h>
#include <glog/logging.h>
#include <parquet/api/reader.h>
#include <parquet/column_writer.h>
#include <parquet/platform.h>
#include <parquet/schema.h>
#include <parquet/type_fwd.h>
#include <parquet/types.h>

#include <ctime>
#include <exception>
#include <ostream>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"
#include "format/parquet/parquet_arrow_block_convertor.h"
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

VParquetWriter::VParquetWriter(RuntimeState* state, doris::io::FileWriter* file_writer,
                               const VExprContextSPtrs& output_vexpr_ctxs,
                               std::vector<std::string> column_names, bool output_object_data,
                               const ParquetFileOptions& parquet_options)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _column_names(std::move(column_names)),
          _parquet_options(parquet_options) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
}

VParquetWriter::VParquetWriter(RuntimeState* state, doris::io::FileWriter* file_writer,
                               const VExprContextSPtrs& output_vexpr_ctxs,
                               std::vector<TParquetSchema> parquet_schemas, bool output_object_data,
                               const ParquetFileOptions& parquet_options)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _parquet_schemas(std::move(parquet_schemas)),
          _parquet_options(parquet_options) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
}

Status VParquetWriter::_parse_properties() {
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

std::unique_ptr<ArrowBlockConvertor> VParquetWriter::_create_arrow_block_convertor(
        DataTypes types, std::vector<std::string> names, const std::string& timezone_name,
        const cctz::time_zone& timezone) const {
    return std::make_unique<ParquetArrowBlockConvertor>(std::move(types), std::move(names),
                                                        timezone_name, timezone);
}

Status VParquetWriter::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(
            _arrow_block_convertor->convert_to_arrow(block, get_arrow_memory_pool(), &result));
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

arrow::Status VParquetWriter::_open_file_writer() {
    ARROW_ASSIGN_OR_RAISE(_writer,
                          ::parquet::arrow::FileWriter::Open(
                                  *_arrow_block_convertor->arrow_schema(), get_arrow_memory_pool(),
                                  _outstream, _parquet_writer_properties, _arrow_properties));
    return arrow::Status::OK();
}

Status VParquetWriter::open() {
    _timezone = _state->timezone();
    _timezone_obj = _state->timezone_obj();
    RETURN_IF_ERROR(_parse_properties());
    DataTypes types;
    types.reserve(_output_vexpr_ctxs.size());
    for (const auto& context : _output_vexpr_ctxs) {
        types.emplace_back(context->root()->data_type());
    }
    std::vector<std::string> names = _column_names;
    if (!_parquet_schemas.empty()) {
        names.clear();
        names.reserve(_parquet_schemas.size());
        for (const auto& schema : _parquet_schemas) {
            names.emplace_back(schema.schema_column_name);
        }
    }
    _arrow_block_convertor = _create_arrow_block_convertor(std::move(types), std::move(names),
                                                           _timezone, _timezone_obj);
    RETURN_IF_ERROR(_arrow_block_convertor->init());
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

int64_t VParquetWriter::written_len() {
    return _outstream->get_written_len();
}

Status VParquetWriter::close() {
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

} // namespace doris
