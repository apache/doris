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

#include "vec/runtime/vparquet_writer.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <time.h>

#include "exec/parquet_writer.h"
#include "io/file_writer.h"
#include "util/mysql_global.h"
#include "util/types.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

VParquetWriterWrapper::VParquetWriterWrapper(doris::FileWriter* file_writer,
                                             const std::vector<VExprContext*>& output_vexpr_ctxs,
                                             const std::vector<TParquetSchema>& parquet_schemas,
                                             const TParquetCompressionType::type& compression_type,
                                             const bool& parquet_disable_dictionary,
                                             const TParquetVersion::type& parquet_version,
                                             bool output_object_data)
        : VFileWriterWrapper(output_vexpr_ctxs, output_object_data), _rg_writer(nullptr) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
    parse_properties(compression_type, parquet_disable_dictionary, parquet_version);
    parse_schema(parquet_schemas);
}

void VParquetWriterWrapper::parse_properties(const TParquetCompressionType::type& compression_type,
                                             const bool& parquet_disable_dictionary,
                                             const TParquetVersion::type& parquet_version) {
    parquet::WriterProperties::Builder builder;
    ParquetBuildHelper::build_compression_type(builder, compression_type);
    ParquetBuildHelper::build_version(builder, parquet_version);
    if (parquet_disable_dictionary) {
        builder.disable_dictionary();
    } else {
        builder.enable_dictionary();
    }
    _properties = builder.build();
}

void VParquetWriterWrapper::parse_schema(const std::vector<TParquetSchema>& parquet_schemas) {
    parquet::schema::NodeVector fields;
    parquet::Repetition::type parquet_repetition_type;
    parquet::Type::type parquet_data_type;
    for (int idx = 0; idx < parquet_schemas.size(); ++idx) {
        ParquetBuildHelper::build_schema_repetition_type(
                parquet_repetition_type, parquet_schemas[idx].schema_repetition_type);
        ParquetBuildHelper::build_schema_data_type(parquet_data_type,
                                                   parquet_schemas[idx].schema_data_type);
        fields.push_back(parquet::schema::PrimitiveNode::Make(
                parquet_schemas[idx].schema_column_name, parquet_repetition_type,
                parquet::LogicalType::None(), parquet_data_type));
        _schema = std::static_pointer_cast<parquet::schema::GroupNode>(
                parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
    }
}

#define RETURN_WRONG_TYPE \
    return Status::InvalidArgument("Invalid column type: {}", raw_column->get_name());

#define DISPATCH_PARQUET_NUMERIC_WRITER(WRITER, COLUMN_TYPE, NATIVE_TYPE)                         \
    parquet::RowGroupWriter* rgWriter = get_rg_writer();                                          \
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

#define DISPATCH_PARQUET_DECIMAL_WRITER(DECIMAL_TYPE)                                            \
    parquet::RowGroupWriter* rgWriter = get_rg_writer();                                         \
    parquet::ByteArrayWriter* col_writer =                                                       \
            static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));                         \
    parquet::ByteArray value;                                                                    \
    auto decimal_type =                                                                          \
            check_and_get_data_type<DataTypeDecimal<DECIMAL_TYPE>>(remove_nullable(type).get()); \
    DCHECK(decimal_type);                                                                        \
    if (null_map != nullptr) {                                                                   \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();                 \
        for (size_t row_id = 0; row_id < sz; row_id++) {                                         \
            if (null_data[row_id] != 0) {                                                        \
                single_def_level = 0;                                                            \
                col_writer->WriteBatch(1, &single_def_level, nullptr, &value);                   \
                single_def_level = 1;                                                            \
            } else {                                                                             \
                auto s = decimal_type->to_string(*col, row_id);                                  \
                value.ptr = reinterpret_cast<const uint8_t*>(s.data());                          \
                value.len = s.size();                                                            \
                col_writer->WriteBatch(1, &single_def_level, nullptr, &value);                   \
            }                                                                                    \
        }                                                                                        \
    } else {                                                                                     \
        for (size_t row_id = 0; row_id < sz; row_id++) {                                         \
            auto s = decimal_type->to_string(*col, row_id);                                      \
            value.ptr = reinterpret_cast<const uint8_t*>(s.data());                              \
            value.len = s.size();                                                                \
            col_writer->WriteBatch(1, nullable ? def_level.data() : nullptr, nullptr, &value);   \
        }                                                                                        \
    }

#define DISPATCH_PARQUET_COMPLEX_WRITER(COLUMN_TYPE)                                             \
    parquet::RowGroupWriter* rgWriter = get_rg_writer();                                         \
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

Status VParquetWriterWrapper::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }
    size_t sz = block.rows();
    try {
        for (size_t i = 0; i < block.columns(); i++) {
            auto& raw_column = block.get_by_position(i).column;
            auto nullable = raw_column->is_nullable();
            auto column_ptr = block.get_by_position(i).column->convert_to_full_column_if_const();
            doris::vectorized::ColumnPtr column;
            if (nullable) {
                column = assert_cast<const ColumnNullable&>(*column_ptr).get_nested_column_ptr();
            } else {
                column = column_ptr;
            }
            auto col = column.get();
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
                return Status::InvalidArgument("do not support large int type.");
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
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
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
            case TYPE_DATETIME:
            case TYPE_DATE: {
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                uint64_t default_int64 = 0;
                if (null_map != nullptr) {
                    auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        def_level[row_id] = null_data[row_id] == 0;
                    }
                    uint64_t tmp_data[sz];
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if (null_data[row_id] != 0) {
                            tmp_data[row_id] = default_int64;
                        } else {
                            tmp_data[row_id] = binary_cast<Int64, VecDateTimeValue>(
                                                       assert_cast<const ColumnVector<Int64>&>(*col)
                                                               .get_data()[row_id])
                                                       .to_olap_datetime();
                        }
                    }
                    col_writer->WriteBatch(sz, def_level.data(), nullptr,
                                           reinterpret_cast<const int64_t*>(tmp_data));
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Int64>>(col)) {
                    std::vector<uint64_t> res(sz);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        res[row_id] = binary_cast<Int64, VecDateTimeValue>(
                                              not_nullable_column->get_data()[row_id])
                                              .to_olap_datetime();
                    }
                    col_writer->WriteBatch(sz, nullable ? def_level.data() : nullptr, nullptr,
                                           reinterpret_cast<const int64_t*>(res.data()));
                } else {
                    RETURN_WRONG_TYPE
                }
                break;
            }
            case TYPE_DATEV2: {
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
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
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
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
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
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
                DISPATCH_PARQUET_DECIMAL_WRITER(Decimal32)
                break;
            }
            case TYPE_DECIMAL64: {
                DISPATCH_PARQUET_DECIMAL_WRITER(Decimal64)
                break;
            }
            case TYPE_DECIMAL128I: {
                DISPATCH_PARQUET_DECIMAL_WRITER(Decimal128I)
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

Status VParquetWriterWrapper::prepare() {
    _writer = parquet::ParquetFileWriter::Open(_outstream, _schema, _properties);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

parquet::RowGroupWriter* VParquetWriterWrapper::get_rg_writer() {
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

int64_t VParquetWriterWrapper::written_len() {
    return _outstream->get_written_len();
}

void VParquetWriterWrapper::close() {
    try {
        if (_rg_writer != nullptr) {
            _rg_writer->Close();
            _rg_writer = nullptr;
        }
        _writer->Close();
        arrow::Status st = _outstream->Close();
        if (!st.ok()) {
            LOG(WARNING) << "close parquet file error: " << st.ToString();
        }
    } catch (const std::exception& e) {
        _rg_writer = nullptr;
        LOG(WARNING) << "Parquet writer close error: " << e.what();
    }
}

} // namespace doris::vectorized
