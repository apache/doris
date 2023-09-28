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

static const std::string epoch_date_str = "1970-01-01";
static const int64_t timestamp_threshold = -2177481943;
static const int64_t timestamp_diff = 343;

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

void ParquetBuildHelper::build_schema_data_type(parquet::Type::type& parquet_data_type,
                                                const TParquetDataType::type& column_data_type) {
    switch (column_data_type) {
    case TParquetDataType::BOOLEAN: {
        parquet_data_type = parquet::Type::BOOLEAN;
        break;
    }
    case TParquetDataType::INT32: {
        parquet_data_type = parquet::Type::INT32;
        break;
    }
    case TParquetDataType::INT64: {
        parquet_data_type = parquet::Type::INT64;
        break;
    }
    case TParquetDataType::INT96: {
        parquet_data_type = parquet::Type::INT96;
        break;
    }
    case TParquetDataType::BYTE_ARRAY: {
        parquet_data_type = parquet::Type::BYTE_ARRAY;
        break;
    }
    case TParquetDataType::FLOAT: {
        parquet_data_type = parquet::Type::FLOAT;
        break;
    }
    case TParquetDataType::DOUBLE: {
        parquet_data_type = parquet::Type::DOUBLE;
        break;
    }
    case TParquetDataType::FIXED_LEN_BYTE_ARRAY: {
        parquet_data_type = parquet::Type::FIXED_LEN_BYTE_ARRAY;
        break;
    }
    default:
        parquet_data_type = parquet::Type::UNDEFINED;
    }
}

void ParquetBuildHelper::build_schema_data_logical_type(
        std::shared_ptr<const parquet::LogicalType>& parquet_data_logical_type_ptr,
        const TParquetDataLogicalType::type& column_data_logical_type, int* primitive_length,
        const TypeDescriptor& type_desc) {
    switch (column_data_logical_type) {
    case TParquetDataLogicalType::DECIMAL: {
        DCHECK(type_desc.precision != -1 && type_desc.scale != -1)
                << "precision and scale: " << type_desc.precision << " " << type_desc.scale;
        if (type_desc.type == TYPE_DECIMAL32) {
            *primitive_length = 4;
        } else if (type_desc.type == TYPE_DECIMAL64) {
            *primitive_length = 8;
        } else if (type_desc.type == TYPE_DECIMAL128I) {
            *primitive_length = 16;
        } else {
            throw parquet::ParquetException(
                    "the logical decimal now only support in decimalv3, maybe error of " +
                    type_desc.debug_string());
        }
        parquet_data_logical_type_ptr =
                parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale);
        break;
    }
    case TParquetDataLogicalType::STRING: {
        parquet_data_logical_type_ptr = parquet::LogicalType::String();
        break;
    }
    case TParquetDataLogicalType::DATE: {
        parquet_data_logical_type_ptr = parquet::LogicalType::Date();
        break;
    }
    case TParquetDataLogicalType::TIMESTAMP: {
        parquet_data_logical_type_ptr =
                parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MILLIS, true);
        break;
    }
    default: {
        parquet_data_logical_type_ptr = parquet::LogicalType::None();
    }
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
          _rg_writer(nullptr),
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
        _properties = builder.build();
        _arrow_properties = parquet::ArrowWriterProperties::Builder().store_schema()->build();
    } catch (const parquet::ParquetException& e) {
        return Status::InternalError("parquet writer parse properties error: {}", e.what());
    }
    return Status::OK();
}

Status VParquetTransformer::_parse_schema2() {
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

Status VParquetTransformer::_parse_schema() {
    parquet::schema::NodeVector fields;
    parquet::Repetition::type parquet_repetition_type;
    parquet::Type::type parquet_physical_type;
    std::shared_ptr<const parquet::LogicalType> parquet_data_logical_type;
    int primitive_length = -1;
    for (int idx = 0; idx < _parquet_schemas.size(); ++idx) {
        primitive_length = -1;
        ParquetBuildHelper::build_schema_repetition_type(
                parquet_repetition_type, _parquet_schemas[idx].schema_repetition_type);
        ParquetBuildHelper::build_schema_data_type(parquet_physical_type,
                                                   _parquet_schemas[idx].schema_data_type);
        ParquetBuildHelper::build_schema_data_logical_type(
                parquet_data_logical_type, _parquet_schemas[idx].schema_data_logical_type,
                &primitive_length, _output_vexpr_ctxs[idx]->root()->type());
        try {
            fields.push_back(parquet::schema::PrimitiveNode::Make(
                    _parquet_schemas[idx].schema_column_name, parquet_repetition_type,
                    parquet_data_logical_type, parquet_physical_type, primitive_length));
        } catch (const parquet::ParquetException& e) {
            LOG(WARNING) << "parquet writer parse schema error: " << e.what();
            return Status::InternalError("parquet writer parse schema error: {}", e.what());
        }
        _schema = std::static_pointer_cast<parquet::schema::GroupNode>(
                parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
    }
    return Status::OK();
}

#define RETURN_WRONG_TYPE \
    return Status::InvalidArgument("Invalid column type: {}", raw_column->get_name());

#define DISPATCH_PARQUET_NUMERIC_WRITER(WRITER, COLUMN_TYPE, NATIVE_TYPE)                         \
    parquet::RowGroupWriter* rgWriter = _get_rg_writer();                                         \
    parquet::WRITER* col_writer = static_cast<parquet::WRITER*>(rgWriter->column(i));             \
    if (null_map != nullptr) {                                                                    \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();                  \
        for (size_t row_id = 0; row_id < sz; row_id++) {                                          \
            def_level[row_id] = null_data[row_id] == 0;                                           \
        }                                                                                         \
        col_writer->WriteBatch(sz, def_level.data(), nullptr,                                     \
                               reinterpret_cast<const NATIVE_TYPE*>(                              \
                                       assert_cast<const COLUMN_TYPE&>(*col).get_data().data())); \
    } else if (const auto* not_nullable_column = check_and_get_column<const COLUMN_TYPE>(col)) {  \
        col_writer->WriteBatch(                                                                   \
                sz, nullable ? def_level.data() : nullptr, nullptr,                               \
                reinterpret_cast<const NATIVE_TYPE*>(not_nullable_column->get_data().data()));    \
    } else {                                                                                      \
        RETURN_WRONG_TYPE                                                                         \
    }

#define DISPATCH_PARQUET_COMPLEX_WRITER(COLUMN_TYPE)                                             \
    parquet::RowGroupWriter* rgWriter = _get_rg_writer();                                        \
    parquet::ByteArrayWriter* col_writer =                                                       \
            static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));                         \
    if (null_map != nullptr) {                                                                   \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();                 \
        for (size_t row_id = 0; row_id < sz; row_id++) {                                         \
            if (null_data[row_id] != 0) {                                                        \
                single_def_level = 0;                                                            \
                parquet::ByteArray value;                                                        \
                col_writer->WriteBatch(1, &single_def_level, nullptr, &value);                   \
                single_def_level = 1;                                                            \
            } else {                                                                             \
                const auto& tmp = col->get_data_at(row_id);                                      \
                parquet::ByteArray value;                                                        \
                value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);                          \
                value.len = tmp.size;                                                            \
                col_writer->WriteBatch(1, &single_def_level, nullptr, &value);                   \
            }                                                                                    \
        }                                                                                        \
    } else if (const auto* not_nullable_column = check_and_get_column<const COLUMN_TYPE>(col)) { \
        for (size_t row_id = 0; row_id < sz; row_id++) {                                         \
            const auto& tmp = not_nullable_column->get_data_at(row_id);                          \
            parquet::ByteArray value;                                                            \
            value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);                              \
            value.len = tmp.size;                                                                \
            col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr, &value);  \
        }                                                                                        \
    } else {                                                                                     \
        RETURN_WRONG_TYPE                                                                        \
    }

Status VParquetTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    convert_to_arrow_batch(block, _arrow_schema, arrow::default_memory_pool(), &result);

    auto get_table_res = arrow::Table::FromRecordBatches(result->schema(), {result});
    if (!get_table_res.ok()) {
        return Status::InternalError("Error when get arrow table from record batchs");
    }
    auto& table = get_table_res.ValueOrDie();
    RETURN_DORIS_STATUS_IF_ERROR(_writer2->WriteTable(*table, block.rows()));
    return Status::OK();
}

Status VParquetTransformer::write2(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }
    size_t sz = block.rows();
    try {
        for (size_t i = 0; i < block.columns(); i++) {
            auto& raw_column = block.get_by_position(i).column;
            auto nullable = raw_column->is_nullable();
            const auto col = nullable ? reinterpret_cast<const ColumnNullable*>(
                                                block.get_by_position(i).column.get())
                                                ->get_nested_column_ptr()
                                                .get()
                                      : block.get_by_position(i).column.get();
            auto null_map = nullable && reinterpret_cast<const ColumnNullable*>(
                                                block.get_by_position(i).column.get())
                                                    ->has_null()
                                    ? reinterpret_cast<const ColumnNullable*>(
                                              block.get_by_position(i).column.get())
                                              ->get_null_map_column_ptr()
                                    : nullptr;
            auto& type = block.get_by_position(i).type;

            std::vector<int16_t> def_level(sz);
            // For scalar type, definition level == 1 means this value is not NULL.
            std::fill(def_level.begin(), def_level.end(), 1);
            int16_t single_def_level = 1;
            switch (_output_vexpr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN: {
                DISPATCH_PARQUET_NUMERIC_WRITER(BoolWriter, ColumnVector<UInt8>, bool)
                break;
            }
            case TYPE_BIGINT: {
                DISPATCH_PARQUET_NUMERIC_WRITER(Int64Writer, ColumnVector<Int64>, int64_t)
                break;
            }
            case TYPE_LARGEINT: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                parquet::ByteArray value;
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            single_def_level = 0;
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                            single_def_level = 1;
                        } else {
                            const int128_t tmp = assert_cast<const ColumnVector<Int128>&>(*col)
                                                         .get_data()[row_id];
                            std::string value_str = fmt::format("{}", tmp);
                            value.ptr = reinterpret_cast<const uint8_t*>(value_str.data());
                            value.len = value_str.length();
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Int128>>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const int128_t tmp = not_nullable_column->get_data()[row_id];
                        std::string value_str = fmt::format("{}", tmp);
                        value.ptr = reinterpret_cast<const uint8_t*>(value_str.data());
                        value.len = value_str.length();
                        col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr,
                                               &value);
                    }
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_FLOAT: {
                DISPATCH_PARQUET_NUMERIC_WRITER(FloatWriter, ColumnVector<Float32>, float_t)
                break;
            }
            case TYPE_DOUBLE: {
                DISPATCH_PARQUET_NUMERIC_WRITER(DoubleWriter, ColumnVector<Float64>, double_t)
                break;
            }
            case TYPE_TINYINT:
            case TYPE_SMALLINT: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(i));
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    if (const auto* int16_column =
                                check_and_get_column<const ColumnVector<Int16>>(col)) {
                        for (size_t row_id = 0; row_id < sz; row_id++) {
                            if (null_data[row_id] != 0) {
                                single_def_level = 0;
                            }
                            const int32_t tmp = int16_column->get_data()[row_id];
                            col_writer->WriteBatch(1, &single_def_level, nullptr,
                                                   reinterpret_cast<const int32_t*>(&tmp));
                            single_def_level = 1;
                        }
                    } else if (const auto* int8_column =
                                       check_and_get_column<const ColumnVector<Int8>>(col)) {
                        for (size_t row_id = 0; row_id < sz; row_id++) {
                            if (null_data[row_id] != 0) {
                                single_def_level = 0;
                            }
                            const int32_t tmp = int8_column->get_data()[row_id];
                            col_writer->WriteBatch(1, &single_def_level, nullptr,
                                                   reinterpret_cast<const int32_t*>(&tmp));
                            single_def_level = 1;
                        }
                    } else {
                        RETURN_WRONG_TYPE
                    }
                } else if (const auto& int16_column =
                                   check_and_get_column<const ColumnVector<Int16>>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const int32_t tmp = int16_column->get_data()[row_id];
                        col_writer->WriteBatch(1, nullable ? def_level.data() : nullptr, nullptr,
                                               reinterpret_cast<const int32_t*>(&tmp));
                    }
                } else if (const auto& int8_column =
                                   check_and_get_column<const ColumnVector<Int8>>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const int32_t tmp = int8_column->get_data()[row_id];
                        col_writer->WriteBatch(1, nullable ? def_level.data() : nullptr, nullptr,
                                               reinterpret_cast<const int32_t*>(&tmp));
                    }
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_INT: {
                DISPATCH_PARQUET_NUMERIC_WRITER(Int32Writer, ColumnVector<Int32>, Int32)
                break;
            }
            case TYPE_DATETIME: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                uint64_t default_int64 = 0;
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        def_level[row_id] = null_data[row_id] == 0;
                    }
                    int64_t tmp_data[sz];
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            tmp_data[row_id] = default_int64;
                        } else {
                            VecDateTimeValue datetime_value = binary_cast<Int64, VecDateTimeValue>(
                                    assert_cast<const ColumnVector<Int64>&>(*col)
                                            .get_data()[row_id]);
                            if (!datetime_value.unix_timestamp(&tmp_data[row_id],
                                                               TimezoneUtils::default_time_zone)) {
                                return Status::InternalError("get unix timestamp error.");
                            }
                            // -2177481943 represent '1900-12-31 23:54:17'
                            // but -2177481944 represent '1900-12-31 23:59:59'
                            // so for timestamp <= -2177481944, we subtract 343 (5min 43s)
                            if (tmp_data[row_id] < timestamp_threshold) {
                                tmp_data[row_id] -= timestamp_diff;
                            }
                            // convert seconds to MILLIS seconds
                            tmp_data[row_id] *= 1000;
                        }
                    }
                    col_writer->WriteBatch(sz, def_level.data(), nullptr,
                                           reinterpret_cast<const int64_t*>(tmp_data));
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Int64>>(col)) {
                    std::vector<int64_t> res(sz);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        VecDateTimeValue datetime_value = binary_cast<Int64, VecDateTimeValue>(
                                not_nullable_column->get_data()[row_id]);

                        if (!datetime_value.unix_timestamp(&res[row_id],
                                                           TimezoneUtils::default_time_zone)) {
                            return Status::InternalError("get unix timestamp error.");
                        };
                        // -2177481943 represent '1900-12-31 23:54:17'
                        // but -2177481944 represent '1900-12-31 23:59:59'
                        // so for timestamp <= -2177481944, we subtract 343 (5min 43s)
                        if (res[row_id] < timestamp_threshold) {
                            res[row_id] -= timestamp_diff;
                        }
                        // convert seconds to MILLIS seconds
                        res[row_id] *= 1000;
                    }
                    col_writer->WriteBatch(sz, nullable ? def_level.data() : nullptr, nullptr,
                                           reinterpret_cast<const int64_t*>(res.data()));
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_DATE: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                uint64_t default_int64 = 0;
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        def_level[row_id] = null_data[row_id] == 0;
                    }
                    VecDateTimeValue epoch_date;
                    if (!epoch_date.from_date_str(epoch_date_str.c_str(),
                                                  epoch_date_str.length())) {
                        return Status::InternalError("create epoch date from string error");
                    }
                    int32_t days_from_epoch = epoch_date.daynr();
                    int32_t tmp_data[sz];
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            tmp_data[row_id] = default_int64;
                        } else {
                            int32_t days = binary_cast<Int64, VecDateTimeValue>(
                                                   assert_cast<const ColumnVector<Int64>&>(*col)
                                                           .get_data()[row_id])
                                                   .daynr();
                            tmp_data[row_id] = days - days_from_epoch;
                        }
                    }
                    col_writer->WriteBatch(sz, def_level.data(), nullptr,
                                           reinterpret_cast<const int64_t*>(tmp_data));
                } else if (check_and_get_column<const ColumnVector<Int64>>(col)) {
                    VecDateTimeValue epoch_date;
                    if (!epoch_date.from_date_str(epoch_date_str.c_str(),
                                                  epoch_date_str.length())) {
                        return Status::InternalError("create epoch date from string error");
                    }
                    int32_t days_from_epoch = epoch_date.daynr();
                    std::vector<int32_t> res(sz);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        int32_t days = binary_cast<Int64, VecDateTimeValue>(
                                               assert_cast<const ColumnVector<Int64>&>(*col)
                                                       .get_data()[row_id])
                                               .daynr();
                        res[row_id] = days - days_from_epoch;
                    }
                    col_writer->WriteBatch(sz, nullable ? def_level.data() : nullptr, nullptr,
                                           reinterpret_cast<const int64_t*>(res.data()));
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_DATEV2: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                parquet::ByteArray value;
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            single_def_level = 0;
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                            single_def_level = 1;
                        } else {
                            char buffer[30];
                            int output_scale = _output_vexpr_ctxs[i]->root()->type().scale;
                            value.ptr = reinterpret_cast<const uint8_t*>(buffer);
                            value.len = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(
                                                assert_cast<const ColumnVector<UInt32>&>(*col)
                                                        .get_data()[row_id])
                                                .to_buffer(buffer, output_scale);
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<UInt32>>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        char buffer[30];
                        int output_scale = _output_vexpr_ctxs[i]->root()->type().scale;
                        value.ptr = reinterpret_cast<const uint8_t*>(buffer);
                        value.len = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(
                                            not_nullable_column->get_data()[row_id])
                                            .to_buffer(buffer, output_scale);
                        col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr,
                                               &value);
                    }
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_DATETIMEV2: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                parquet::ByteArray value;
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            single_def_level = 0;
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                            single_def_level = 1;
                        } else {
                            char buffer[30];
                            int output_scale = _output_vexpr_ctxs[i]->root()->type().scale;
                            value.ptr = reinterpret_cast<const uint8_t*>(buffer);
                            value.len = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(
                                                assert_cast<const ColumnVector<UInt64>&>(*col)
                                                        .get_data()[row_id])
                                                .to_buffer(buffer, output_scale);
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<UInt64>>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        char buffer[30];
                        int output_scale = _output_vexpr_ctxs[i]->root()->type().scale;
                        value.ptr = reinterpret_cast<const uint8_t*>(buffer);
                        value.len = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(
                                            not_nullable_column->get_data()[row_id])
                                            .to_buffer(buffer, output_scale);
                        col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr,
                                               &value);
                    }
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_OBJECT: {
                if (_output_object_data) {
                    DISPATCH_PARQUET_COMPLEX_WRITER(ColumnBitmap)
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_HLL: {
                if (_output_object_data) {
                    DISPATCH_PARQUET_COMPLEX_WRITER(ColumnHLL)
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_STRING: {
                DISPATCH_PARQUET_COMPLEX_WRITER(ColumnString)
                break;
            }
            case TYPE_DECIMALV2: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                parquet::ByteArray value;
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            single_def_level = 0;
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                            single_def_level = 1;
                        } else {
                            const DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(
                                                                     col->get_data_at(row_id).data)
                                                                     ->value);
                            char decimal_buffer[MAX_DECIMAL_WIDTH];
                            int output_scale = _output_vexpr_ctxs[i]->root()->type().scale;
                            value.ptr = reinterpret_cast<const uint8_t*>(decimal_buffer);
                            value.len = decimal_val.to_buffer(decimal_buffer, output_scale);
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnDecimal128>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const DecimalV2Value decimal_val(
                                reinterpret_cast<const PackedInt128*>(
                                        not_nullable_column->get_data_at(row_id).data)
                                        ->value);
                        char decimal_buffer[MAX_DECIMAL_WIDTH];
                        int output_scale = _output_vexpr_ctxs[i]->root()->type().scale;
                        value.ptr = reinterpret_cast<const uint8_t*>(decimal_buffer);
                        value.len = decimal_val.to_buffer(decimal_buffer, output_scale);
                        col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr,
                                               &value);
                    }
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_DECIMAL32: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::FixedLenByteArrayWriter* col_writer =
                        static_cast<parquet::FixedLenByteArrayWriter*>(rgWriter->column(i));
                parquet::FixedLenByteArray value;
                auto decimal_type = check_and_get_data_type<DataTypeDecimal<Decimal32>>(
                        remove_nullable(type).get());
                DCHECK(decimal_type);
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    const auto& data_column = assert_cast<const ColumnDecimal32&>(*col);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            single_def_level = 0;
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                            single_def_level = 1;
                        } else {
                            auto data = data_column.get_element(row_id);
                            auto big_endian = bswap_32(data);
                            value.ptr = reinterpret_cast<const uint8_t*>(&big_endian);
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                        }
                    }
                } else {
                    const auto& data_column = assert_cast<const ColumnDecimal32&>(*col);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        auto data = data_column.get_element(row_id);
                        auto big_endian = bswap_32(data);
                        value.ptr = reinterpret_cast<const uint8_t*>(&big_endian);
                        col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr,
                                               &value);
                    }
                }
                break;
            }
            case TYPE_DECIMAL64: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::FixedLenByteArrayWriter* col_writer =
                        static_cast<parquet::FixedLenByteArrayWriter*>(rgWriter->column(i));
                parquet::FixedLenByteArray value;
                auto decimal_type = check_and_get_data_type<DataTypeDecimal<Decimal64>>(
                        remove_nullable(type).get());
                DCHECK(decimal_type);
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    const auto& data_column = assert_cast<const ColumnDecimal64&>(*col);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            single_def_level = 0;
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                            single_def_level = 1;
                        } else {
                            auto data = data_column.get_element(row_id);
                            auto big_endian = bswap_64(data);
                            value.ptr = reinterpret_cast<const uint8_t*>(&big_endian);
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                        }
                    }
                } else {
                    const auto& data_column = assert_cast<const ColumnDecimal64&>(*col);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        auto data = data_column.get_element(row_id);
                        auto big_endian = bswap_64(data);
                        value.ptr = reinterpret_cast<const uint8_t*>(&big_endian);
                        col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr,
                                               &value);
                    }
                }
                break;
            }
            case TYPE_DECIMAL128I: {
                parquet::RowGroupWriter* rgWriter = _get_rg_writer();
                parquet::FixedLenByteArrayWriter* col_writer =
                        static_cast<parquet::FixedLenByteArrayWriter*>(rgWriter->column(i));
                parquet::FixedLenByteArray value;
                auto decimal_type = check_and_get_data_type<DataTypeDecimal<Decimal128I>>(
                        remove_nullable(type).get());
                DCHECK(decimal_type);
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    const auto& data_column = assert_cast<const ColumnDecimal128I&>(*col);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            single_def_level = 0;
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                            single_def_level = 1;
                        } else {
                            auto data = data_column.get_element(row_id);
                            auto big_endian = gbswap_128(data);
                            value.ptr = reinterpret_cast<const uint8_t*>(&big_endian);
                            col_writer->WriteBatch(1, &single_def_level, nullptr, &value);
                        }
                    }
                } else {
                    const auto& data_column = assert_cast<const ColumnDecimal128I&>(*col);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        auto data = data_column.get_element(row_id);
                        auto big_endian = gbswap_128(data);
                        value.ptr = reinterpret_cast<const uint8_t*>(&big_endian);
                        col_writer->WriteBatch(1, nullable ? &single_def_level : nullptr, nullptr,
                                               &value);
                    }
                }
                break;
            }
            default: {
                return Status::InvalidArgument(
                        "Invalid expression type: {}",
                        _output_vexpr_ctxs[i]->root()->type().debug_string());
            }
            }
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Parquet write error: " << e.what();
        return Status::InternalError(e.what());
    }
    _cur_written_rows += sz;
    return Status::OK();
}

arrow::Status VParquetTransformer::_open_file_writer() {
    ARROW_ASSIGN_OR_RAISE(
            _writer2, parquet::arrow::FileWriter::Open(*_arrow_schema, arrow::default_memory_pool(),
                                                       _outstream, _properties, _arrow_properties));
    return arrow::Status::OK();
}

Status VParquetTransformer::open() {
    RETURN_IF_ERROR(_parse_properties());
    RETURN_IF_ERROR(_parse_schema2());
    try {
        RETURN_DORIS_STATUS_IF_ERROR(_open_file_writer());
    } catch (const parquet::ParquetStatusException& e) {
        LOG(WARNING) << "parquet file writer open error: " << e.what();
        return Status::InternalError("parquet file writer open error: {}", e.what());
    }
    if (_writer2 == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

parquet::RowGroupWriter* VParquetTransformer::_get_rg_writer() {
    if (_rg_writer == nullptr) {
        _rg_writer = _writer->AppendBufferedRowGroup();
    }
    if (_cur_written_rows > _max_row_per_group) {
        _rg_writer->Close();
        _rg_writer = _writer->AppendBufferedRowGroup();
        _cur_written_rows = 0;
    }
    return _rg_writer;
}

int64_t VParquetTransformer::written_len() {
    return _outstream->get_written_len();
}

Status VParquetTransformer::close() {
    try {
        if (_rg_writer != nullptr) {
            _rg_writer->Close();
            _rg_writer = nullptr;
        }
        if (_writer != nullptr) {
            _writer->Close();
        }

        if (_writer2 != nullptr) {
            RETURN_DORIS_STATUS_IF_ERROR(_writer2->Close());
        }
        RETURN_DORIS_STATUS_IF_ERROR(_outstream->Close());

    } catch (const std::exception& e) {
        _rg_writer = nullptr;
        LOG(WARNING) << "Parquet writer close error: " << e.what();
        return Status::IOError(e.what());
    }
    return Status::OK();
}

} // namespace doris::vectorized
