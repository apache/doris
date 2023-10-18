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

#include "vec/runtime/vparquet_transformer.h"

#include <arrow/io/type_fwd.h>
#include <arrow/table.h>
#include <glog/logging.h>
#include <math.h>
#include <parquet/column_writer.h>
#include <parquet/platform.h>
#include <parquet/schema.h>
#include <parquet/type_fwd.h>
#include <parquet/types.h>
#include <time.h>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <ostream>
#include <string>

#include "common/status.h"
#include "gutil/endian.h"
#include "io/fs/file_writer.h"
#include "olap/olap_common.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"
#include "util/binary_cast.hpp"
#include "util/mysql_global.h"
#include "util/types.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/function_helpers.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

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
    if (_is_closed) {
        return arrow::Status::OK();
    }
    Status st = _file_writer->close();
    if (!st.ok()) {
        LOG(WARNING) << "close parquet output stream failed: " << st;
        return arrow::Status::IOError(st.to_string());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

int64_t ParquetOutputStream::get_written_len() const {
    return _written_len;
}

void ParquetOutputStream::set_written_len(int64_t written_len) {
    _written_len = written_len;
}

void ParquetBuildHelper::build_schema_repetition_type(
        parquet::Repetition::type& parquet_repetition_type,
        const TParquetRepetitionType::type& column_repetition_type) {
    switch (column_repetition_type) {
    case TParquetRepetitionType::REQUIRED: {
        parquet_repetition_type = parquet::Repetition::REQUIRED;
        break;
    }
    case TParquetRepetitionType::REPEATED: {
        parquet_repetition_type = parquet::Repetition::REPEATED;
        break;
    }
    case TParquetRepetitionType::OPTIONAL: {
        parquet_repetition_type = parquet::Repetition::OPTIONAL;
        break;
    }
    default:
        parquet_repetition_type = parquet::Repetition::UNDEFINED;
    }
}

void ParquetBuildHelper::build_compression_type(
        parquet::WriterProperties::Builder& builder,
        const TParquetCompressionType::type& compression_type) {
    switch (compression_type) {
    case TParquetCompressionType::SNAPPY: {
        builder.compression(parquet::Compression::SNAPPY);
        break;
    }
    case TParquetCompressionType::GZIP: {
        builder.compression(parquet::Compression::GZIP);
        break;
    }
    case TParquetCompressionType::BROTLI: {
        builder.compression(parquet::Compression::BROTLI);
        break;
    }
    case TParquetCompressionType::ZSTD: {
        builder.compression(parquet::Compression::ZSTD);
        break;
    }
    case TParquetCompressionType::LZ4: {
        builder.compression(parquet::Compression::LZ4);
        break;
    }
    case TParquetCompressionType::LZO: {
        builder.compression(parquet::Compression::LZO);
        break;
    }
    case TParquetCompressionType::BZ2: {
        builder.compression(parquet::Compression::BZ2);
        break;
    }
    case TParquetCompressionType::UNCOMPRESSED: {
        builder.compression(parquet::Compression::UNCOMPRESSED);
        break;
    }
    default:
        builder.compression(parquet::Compression::UNCOMPRESSED);
    }
}

void ParquetBuildHelper::build_version(parquet::WriterProperties::Builder& builder,
                                       const TParquetVersion::type& parquet_version) {
    switch (parquet_version) {
    case TParquetVersion::PARQUET_1_0: {
        builder.version(parquet::ParquetVersion::PARQUET_1_0);
        break;
    }
    case TParquetVersion::PARQUET_2_LATEST: {
        builder.version(parquet::ParquetVersion::PARQUET_2_LATEST);
        break;
    }
    default:
        builder.version(parquet::ParquetVersion::PARQUET_1_0);
    }
}

VParquetTransformer::VParquetTransformer(doris::io::FileWriter* file_writer,
                                         const VExprContextSPtrs& output_vexpr_ctxs,
                                         const std::vector<TParquetSchema>& parquet_schemas,
                                         const TParquetCompressionType::type& compression_type,
                                         const bool& parquet_disable_dictionary,
                                         const TParquetVersion::type& parquet_version,
                                         bool output_object_data)
        : VFileFormatTransformer(output_vexpr_ctxs, output_object_data),
          _parquet_schemas(parquet_schemas),
          _compression_type(compression_type),
          _parquet_disable_dictionary(parquet_disable_dictionary),
          _parquet_version(parquet_version) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
}

Status VParquetTransformer::_parse_properties() {
    try {
        parquet::WriterProperties::Builder builder;
        ParquetBuildHelper::build_compression_type(builder, _compression_type);
        ParquetBuildHelper::build_version(builder, _parquet_version);
        if (_parquet_disable_dictionary) {
            builder.disable_dictionary();
        } else {
            builder.enable_dictionary();
        }
        _parquet_writer_properties = builder.build();
        _arrow_properties = parquet::ArrowWriterProperties::Builder().store_schema()->build();
    } catch (const parquet::ParquetException& e) {
        return Status::InternalError("parquet writer parse properties error: {}", e.what());
    }
    return Status::OK();
}

Status VParquetTransformer::_parse_schema() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        std::shared_ptr<arrow::DataType> type;
        RETURN_IF_ERROR(convert_to_arrow_type(_output_vexpr_ctxs[i]->root()->type(), &type));
        std::shared_ptr<arrow::Field> field =
                arrow::field(_parquet_schemas[i].schema_column_name, type,
                             _output_vexpr_ctxs[i]->root()->is_nullable());
        fields.emplace_back(field);
    }
    _arrow_schema = arrow::schema(std::move(fields));
    return Status::OK();
}

Status VParquetTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(convert_to_arrow_batch(block, arrow::default_memory_pool(), &result));

    auto get_table_res = arrow::Table::FromRecordBatches(result->schema(), {result});
    if (!get_table_res.ok()) {
        return Status::InternalError("Error when get arrow table from record batchs");
    }
    auto& table = get_table_res.ValueOrDie();
    RETURN_DORIS_STATUS_IF_ERROR(_writer->WriteTable(*table, block.rows()));
    return Status::OK();
}

arrow::Status VParquetTransformer::_open_file_writer() {
    ARROW_ASSIGN_OR_RAISE(_writer, parquet::arrow::FileWriter::Open(
                                           *_arrow_schema, arrow::default_memory_pool(), _outstream,
                                           _parquet_writer_properties, _arrow_properties));
    return arrow::Status::OK();
}

Status VParquetTransformer::open() {
    RETURN_IF_ERROR(_parse_properties());
    RETURN_IF_ERROR(_parse_schema());
    try {
        RETURN_DORIS_STATUS_IF_ERROR(_open_file_writer());
    } catch (const parquet::ParquetStatusException& e) {
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

} // namespace doris::vectorized
