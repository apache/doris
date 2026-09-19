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
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"
#include "io/fs/file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/timezone_utils.h"

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

VParquetTransformer::VParquetTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                                         const VExprContextSPtrs& output_vexpr_ctxs,
                                         std::vector<std::string> column_names,
                                         bool output_object_data,
                                         const ParquetFileOptions& parquet_options,
                                         std::unique_ptr<ArrowBlockConvertor> arrow_block_convertor)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _arrow_block_convertor(std::move(arrow_block_convertor)),
          _column_names(std::move(column_names)),
          _parquet_options(parquet_options) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
}

VParquetTransformer::VParquetTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                                         const VExprContextSPtrs& output_vexpr_ctxs,
                                         std::vector<TParquetSchema> parquet_schemas,
                                         bool output_object_data,
                                         const ParquetFileOptions& parquet_options,
                                         std::unique_ptr<ArrowBlockConvertor> arrow_block_convertor)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _arrow_block_convertor(std::move(arrow_block_convertor)),
          _parquet_schemas(std::move(parquet_schemas)),
          _parquet_options(parquet_options) {
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

Status VParquetTransformer::_parse_schema(std::shared_ptr<arrow::Schema>* schema) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    // INT96 has no logical timezone. Its schema and DATETIMEV2 conversion must use
    // the same writer-local timezone, including UTC for a wall-clock carrier.
    const bool datetime_naive = !_parquet_options.enable_int96_timestamps;
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        std::shared_ptr<arrow::DataType> type;
        RETURN_IF_ERROR(convert_to_arrow_type(_output_vexpr_ctxs[i]->root()->data_type(), &type,
                                              _timezone, datetime_naive));
        const auto& name = _parquet_schemas.empty() ? _column_names[i]
                                                    : _parquet_schemas[i].schema_column_name;
        fields.emplace_back(arrow::field(name, type, _output_vexpr_ctxs[i]->root()->is_nullable()));
    }
    *schema = arrow::schema(std::move(fields));
    return Status::OK();
}

Status VParquetTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(_arrow_block_convertor->convert_to_arrow(
            block, _arrow_schema, get_arrow_memory_pool(), &result, _timezone_obj));
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
    ARROW_ASSIGN_OR_RAISE(_writer, ::parquet::arrow::FileWriter::Open(
                                           *_arrow_schema, get_arrow_memory_pool(), _outstream,
                                           _parquet_writer_properties, _arrow_properties));
    return arrow::Status::OK();
}

Status VParquetTransformer::open() {
    _timezone = _state->timezone();
    _timezone_obj = _state->timezone_obj();
    if (_parquet_options.enable_int96_timestamps && _parquet_options.int96_timezone.has_value()) {
        _timezone = *_parquet_options.int96_timezone;
        // Cache the override on this writer, never mutate the shared query RuntimeState.
        if (!TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj)) {
            return Status::InvalidArgument("Invalid Parquet INT96 writer timezone: {}", _timezone);
        }
    }
    RETURN_IF_ERROR(_parse_properties());
    RETURN_IF_ERROR(_parse_schema(&_arrow_schema));
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

} // namespace doris
