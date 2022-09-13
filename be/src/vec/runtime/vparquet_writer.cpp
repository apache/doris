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

#include "io/file_writer.h"
#include "util/mysql_global.h"
#include "util/types.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VParquetWriterWrapper::VParquetWriterWrapper(doris::FileWriter* file_writer,
                                             const std::vector<VExprContext*>& output_vexpr_ctxs,
                                             const std::map<std::string, std::string>& properties,
                                             const std::vector<std::vector<std::string>>& schema,
                                             bool output_object_data)
        : _output_vexpr_ctxs(output_vexpr_ctxs),
          _str_schema(schema),
          _cur_written_rows(0),
          _rg_writer(nullptr),
          _output_object_data(output_object_data) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
    parse_properties(properties);
    parse_schema(schema);
    init_parquet_writer();
}

void VParquetWriterWrapper::parse_properties(
        const std::map<std::string, std::string>& propertie_map) {
    parquet::WriterProperties::Builder builder;
    for (auto it = propertie_map.begin(); it != propertie_map.end(); it++) {
        std::string property_name = it->first;
        std::string property_value = it->second;
        if (property_name == "compression") {
            // UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, LZ4, LZO, BZ2
            if (property_value == "snappy") {
                builder.compression(parquet::Compression::SNAPPY);
            } else if (property_value == "gzip") {
                builder.compression(parquet::Compression::GZIP);
            } else if (property_value == "brotli") {
                builder.compression(parquet::Compression::BROTLI);
            } else if (property_value == "zstd") {
                builder.compression(parquet::Compression::ZSTD);
            } else if (property_value == "lz4") {
                builder.compression(parquet::Compression::LZ4);
            } else if (property_value == "lzo") {
                builder.compression(parquet::Compression::LZO);
            } else if (property_value == "bz2") {
                builder.compression(parquet::Compression::BZ2);
            } else {
                builder.compression(parquet::Compression::UNCOMPRESSED);
            }
        } else if (property_name == "disable_dictionary") {
            if (property_value == "true") {
                builder.enable_dictionary();
            } else {
                builder.disable_dictionary();
            }
        } else if (property_name == "version") {
            if (property_value == "v1") {
                builder.version(parquet::ParquetVersion::PARQUET_1_0);
            } else {
                builder.version(parquet::ParquetVersion::PARQUET_2_LATEST);
            }
        }
    }
    _properties = builder.build();
}

Status VParquetWriterWrapper::parse_schema(const std::vector<std::vector<std::string>>& schema) {
    parquet::schema::NodeVector fields;
    for (auto column = schema.begin(); column != schema.end(); column++) {
        std::string repetition_type = (*column)[0];
        parquet::Repetition::type parquet_repetition_type = parquet::Repetition::REQUIRED;
        if (repetition_type.find("required") != std::string::npos) {
            parquet_repetition_type = parquet::Repetition::REQUIRED;
        } else if (repetition_type.find("repeated") != std::string::npos) {
            parquet_repetition_type = parquet::Repetition::REPEATED;
        } else if (repetition_type.find("optional") != std::string::npos) {
            parquet_repetition_type = parquet::Repetition::OPTIONAL;
        } else {
            parquet_repetition_type = parquet::Repetition::UNDEFINED;
        }

        std::string data_type = (*column)[1];
        parquet::Type::type parquet_data_type = parquet::Type::BYTE_ARRAY;
        if (data_type == "boolean") {
            parquet_data_type = parquet::Type::BOOLEAN;
        } else if (data_type.find("int32") != std::string::npos) {
            parquet_data_type = parquet::Type::INT32;
        } else if (data_type.find("int64") != std::string::npos) {
            parquet_data_type = parquet::Type::INT64;
        } else if (data_type.find("int96") != std::string::npos) {
            parquet_data_type = parquet::Type::INT96;
        } else if (data_type.find("float") != std::string::npos) {
            parquet_data_type = parquet::Type::FLOAT;
        } else if (data_type.find("double") != std::string::npos) {
            parquet_data_type = parquet::Type::DOUBLE;
        } else if (data_type.find("byte_array") != std::string::npos) {
            parquet_data_type = parquet::Type::BYTE_ARRAY;
        } else if (data_type.find("fixed_len_byte_array") != std::string::npos) {
            parquet_data_type = parquet::Type::FIXED_LEN_BYTE_ARRAY;
        } else {
            parquet_data_type = parquet::Type::UNDEFINED;
        }

        std::string column_name = (*column)[2];
        fields.push_back(parquet::schema::PrimitiveNode::Make(column_name, parquet_repetition_type,
                                                              parquet::LogicalType::None(),
                                                              parquet_data_type));
        _schema = std::static_pointer_cast<parquet::schema::GroupNode>(
                parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
    }
    return Status::OK();
}

Status VParquetWriterWrapper::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }
    size_t sz = block.rows();
    try {
        for (size_t i = 0; i < block.columns(); i++) {
            auto& raw_column = block.get_by_position(i).column;
            const auto col = raw_column->is_nullable()
                                     ? reinterpret_cast<const ColumnNullable*>(
                                               block.get_by_position(i).column.get())
                                               ->get_nested_column_ptr()
                                               .get()
                                     : block.get_by_position(i).column.get();
            auto null_map =
                    raw_column->is_nullable() && reinterpret_cast<const ColumnNullable*>(
                                                         block.get_by_position(i).column.get())
                                                         ->get_null_map_column_ptr()
                                                         ->has_null()
                            ? reinterpret_cast<const ColumnNullable*>(
                                      block.get_by_position(i).column.get())
                                      ->get_null_map_column_ptr()
                            : nullptr;
            auto& type = block.get_by_position(i).type;
            switch (_output_vexpr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN: {
                if (_str_schema[i][1] != "boolean") {
                    return Status::InvalidArgument(
                            "project field type is boolean, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::BoolWriter* col_writer =
                        static_cast<parquet::BoolWriter*>(rgWriter->column(i));
                bool default_bool = false;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        col_writer->WriteBatch(
                                1, nullptr, nullptr,
                                (*null_map)[row_id] != 0
                                        ? &default_bool
                                        : reinterpret_cast<const bool*>(
                                                  assert_cast<const ColumnVector<UInt8>&>(*col)
                                                          .get_data_at(row_id)
                                                          .data));
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<UInt8>>(col)) {
                    col_writer->WriteBatch(
                            sz, nullptr, nullptr,
                            reinterpret_cast<const bool*>(not_nullable_column->get_data().data()));
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_TINYINT:
            case TYPE_SMALLINT:
            case TYPE_INT: {
                if (_str_schema[i][1] != "int32") {
                    return Status::InvalidArgument(
                            "project field type is tiny int/small int/int, should use int32,"
                            " but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(i));
                int32_t default_int32 = 0;
                if (null_map != nullptr) {
                    if (const auto* nested_column =
                                check_and_get_column<const ColumnVector<Int32>>(col)) {
                        for (size_t row_id = 0; row_id < sz; row_id++) {
                            col_writer->WriteBatch(
                                    1, nullptr, nullptr,
                                    (*null_map)[row_id] != 0
                                            ? &default_int32
                                            : reinterpret_cast<const int32_t*>(
                                                      nested_column->get_data_at(row_id).data));
                        }
                    } else if (const auto* int16_column =
                                       check_and_get_column<const ColumnVector<Int16>>(col)) {
                        for (size_t row_id = 0; row_id < sz; row_id++) {
                            const int32_t tmp = int16_column->get_data()[row_id];
                            col_writer->WriteBatch(
                                    1, nullptr, nullptr,
                                    (*null_map)[row_id] != 0
                                            ? &default_int32
                                            : reinterpret_cast<const int32_t*>(&tmp));
                        }
                    } else if (const auto* int8_column =
                                       check_and_get_column<const ColumnVector<Int8>>(col)) {
                        for (size_t row_id = 0; row_id < sz; row_id++) {
                            const int32_t tmp = int8_column->get_data()[row_id];
                            col_writer->WriteBatch(
                                    1, nullptr, nullptr,
                                    (*null_map)[row_id] != 0
                                            ? &default_int32
                                            : reinterpret_cast<const int32_t*>(&tmp));
                        }
                    } else {
                        std::stringstream ss;
                        ss << "Invalid column type: ";
                        ss << raw_column->get_name();
                        return Status::InvalidArgument(ss.str());
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Int32>>(col)) {
                    col_writer->WriteBatch(sz, nullptr, nullptr,
                                           reinterpret_cast<const int32_t*>(
                                                   not_nullable_column->get_data().data()));
                } else if (const auto& int16_column =
                                   check_and_get_column<const ColumnVector<Int16>>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const int32_t tmp = int16_column->get_data()[row_id];
                        col_writer->WriteBatch(1, nullptr, nullptr,
                                               reinterpret_cast<const int32_t*>(&tmp));
                    }
                } else if (const auto& int8_column =
                                   check_and_get_column<const ColumnVector<Int8>>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const int32_t tmp = int8_column->get_data()[row_id];
                        col_writer->WriteBatch(1, nullptr, nullptr,
                                               reinterpret_cast<const int32_t*>(&tmp));
                    }
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_BIGINT: {
                if (_str_schema[i][1] != "int64") {
                    return Status::InvalidArgument(
                            "project field type is big int, should use int64, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                int64_t default_int64 = 0;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        col_writer->WriteBatch(
                                1, nullptr, nullptr,
                                (*null_map)[row_id] != 0
                                        ? &default_int64
                                        : reinterpret_cast<const int64_t*>(
                                                  assert_cast<const ColumnVector<Int64>&>(*col)
                                                          .get_data_at(row_id)
                                                          .data));
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Int64>>(col)) {
                    col_writer->WriteBatch(sz, nullptr, nullptr,
                                           reinterpret_cast<const int64_t*>(
                                                   not_nullable_column->get_data().data()));
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_LARGEINT: {
                // TODO: not support int_128
                // It is better write a default value, because rg_writer need all columns has value before flush to disk.
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                int64_t default_int64 = 0;
                for (size_t row_id = 0; row_id < sz; row_id++) {
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                }
                return Status::InvalidArgument("do not support large int type.");
            }
            case TYPE_FLOAT: {
                if (_str_schema[i][1] != "float") {
                    return Status::InvalidArgument(
                            "project field type is float, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::FloatWriter* col_writer =
                        static_cast<parquet::FloatWriter*>(rgWriter->column(i));
                float_t default_float = 0.0;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        col_writer->WriteBatch(
                                1, nullptr, nullptr,
                                (*null_map)[row_id] != 0
                                        ? &default_float
                                        : reinterpret_cast<const float_t*>(
                                                  assert_cast<const ColumnVector<Float32>&>(*col)
                                                          .get_data_at(row_id)
                                                          .data));
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Float32>>(col)) {
                    col_writer->WriteBatch(sz, nullptr, nullptr,
                                           reinterpret_cast<const float_t*>(
                                                   not_nullable_column->get_data().data()));
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_DOUBLE: {
                if (_str_schema[i][1] != "double") {
                    return Status::InvalidArgument(
                            "project field type is double, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::DoubleWriter* col_writer =
                        static_cast<parquet::DoubleWriter*>(rgWriter->column(i));
                double_t default_double = 0.0;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        col_writer->WriteBatch(
                                1, nullptr, nullptr,
                                (*null_map)[row_id] != 0
                                        ? &default_double
                                        : reinterpret_cast<const double_t*>(
                                                  assert_cast<const ColumnVector<Float64>&>(*col)
                                                          .get_data_at(row_id)
                                                          .data));
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Float64>>(col)) {
                    col_writer->WriteBatch(sz, nullptr, nullptr,
                                           reinterpret_cast<const double_t*>(
                                                   not_nullable_column->get_data().data()));
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_DATETIME:
            case TYPE_DATE: {
                if (_str_schema[i][1] != "int64") {
                    return Status::InvalidArgument(
                            "project field type is date/datetime, should use int64, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                int64_t default_int64 = 0;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                        } else {
                            const auto tmp = binary_cast<Int64, VecDateTimeValue>(
                                                     assert_cast<const ColumnVector<Int64>&>(*col)
                                                             .get_data()[row_id])
                                                     .to_olap_datetime();
                            col_writer->WriteBatch(1, nullptr, nullptr,
                                                   reinterpret_cast<const int64_t*>(&tmp));
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<Int64>>(col)) {
                    std::vector<uint64_t> res(sz);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        res[row_id] = binary_cast<Int64, VecDateTimeValue>(
                                              not_nullable_column->get_data()[row_id])
                                              .to_olap_datetime();
                    }
                    col_writer->WriteBatch(sz, nullptr, nullptr,
                                           reinterpret_cast<const int64_t*>(res.data()));
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_DATEV2: {
                if (_str_schema[i][1] != "int64") {
                    return Status::InvalidArgument(
                            "project field type is datev2, should use int32, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                int64_t default_int64 = 0;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                        } else {
                            uint64_t tmp = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(
                                                   assert_cast<const ColumnVector<UInt32>&>(*col)
                                                           .get_data()[row_id])
                                                   .to_olap_datetime();
                            col_writer->WriteBatch(1, nullptr, nullptr,
                                                   reinterpret_cast<const int64_t*>(&tmp));
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<UInt32>>(col)) {
                    std::vector<uint64_t> res(sz);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        res[row_id] = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(
                                              not_nullable_column->get_data()[row_id])
                                              .to_olap_datetime();
                    }
                    col_writer->WriteBatch(sz, nullptr, nullptr,
                                           reinterpret_cast<const int64_t*>(res.data()));
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_DATETIMEV2: {
                if (_str_schema[i][1] != "int64") {
                    return Status::InvalidArgument(
                            "project field type is datetimev2, should use int64, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(i));
                int64_t default_int64 = 0;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                        } else {
                            uint64_t tmp = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(
                                                   assert_cast<const ColumnVector<UInt64>&>(*col)
                                                           .get_data()[row_id])
                                                   .to_olap_datetime();
                            col_writer->WriteBatch(1, nullptr, nullptr,
                                                   reinterpret_cast<const int64_t*>(&tmp));
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnVector<UInt64>>(col)) {
                    std::vector<uint64_t> res(sz);
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        res[row_id] = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(
                                              not_nullable_column->get_data()[row_id])
                                              .to_olap_datetime();
                    }
                    col_writer->WriteBatch(sz, nullptr, nullptr,
                                           reinterpret_cast<const int64_t*>(res.data()));
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_OBJECT: {
                if (!_output_object_data) {
                    std::stringstream ss;
                    ss << "unsupported file format: " << _output_vexpr_ctxs[i]->root()->type().type;
                    return Status::InvalidArgument(ss.str());
                }
                if (_str_schema[i][1] != "byte_array") {
                    return Status::InvalidArgument(
                            "project field type is bitmap, should use byte_array, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            parquet::ByteArray value;
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        } else {
                            const auto& tmp = col->get_data_at(row_id);
                            parquet::ByteArray value;
                            value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);
                            value.len = tmp.size;
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnBitmap>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const auto& tmp = not_nullable_column->get_data_at(row_id);
                        parquet::ByteArray value;
                        value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);
                        value.len = tmp.size;
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_HLL: {
                if (!_output_object_data) {
                    std::stringstream ss;
                    ss << "unsupported file format: " << _output_vexpr_ctxs[i]->root()->type().type;
                    return Status::InvalidArgument(ss.str());
                }
                if (_str_schema[i][1] != "byte_array") {
                    return Status::InvalidArgument(
                            "project field type is hll, should use byte_array, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            parquet::ByteArray value;
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        } else {
                            const auto& tmp = col->get_data_at(row_id);
                            parquet::ByteArray value;
                            value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);
                            value.len = tmp.size;
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnHLL>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const auto& tmp = not_nullable_column->get_data_at(row_id);
                        parquet::ByteArray value;
                        value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);
                        value.len = tmp.size;
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_STRING: {
                if (_str_schema[i][1] != "byte_array") {
                    return Status::InvalidArgument(
                            "project field type is string, should use byte_array, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            parquet::ByteArray value;
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        } else {
                            const auto& tmp = col->get_data_at(row_id);
                            parquet::ByteArray value;
                            value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);
                            value.len = tmp.size;
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        }
                    }
                } else if (const auto* not_nullable_column =
                                   check_and_get_column<const ColumnString>(col)) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        const auto& tmp = not_nullable_column->get_data_at(row_id);
                        parquet::ByteArray value;
                        value.ptr = reinterpret_cast<const uint8_t*>(tmp.data);
                        value.len = tmp.size;
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_DECIMALV2: {
                if (_str_schema[i][1] != "byte_array") {
                    return Status::InvalidArgument(
                            "project field type is decimal v2, should use byte_array, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                parquet::ByteArray value;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        } else {
                            const DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(
                                                                     col->get_data_at(row_id).data)
                                                                     ->value);
                            char decimal_buffer[MAX_DECIMAL_WIDTH];
                            int output_scale = _output_vexpr_ctxs[i]->root()->type().scale;
                            value.ptr = reinterpret_cast<const uint8_t*>(decimal_buffer);
                            value.len = decimal_val.to_buffer(decimal_buffer, output_scale);
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
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
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                } else {
                    std::stringstream ss;
                    ss << "Invalid column type: ";
                    ss << raw_column->get_name();
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64:
            case TYPE_DECIMAL128: {
                if (_str_schema[i][1] != "byte_array") {
                    return Status::InvalidArgument(
                            "project field type is decimal v2, should use byte_array, "
                            "but the definition type of column {} is {}",
                            _str_schema[i][2], _str_schema[i][1]);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(i));
                parquet::ByteArray value;
                if (null_map != nullptr) {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        if ((*null_map)[row_id] != 0) {
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        } else {
                            auto s = type->to_string(*col, row_id);
                            value.ptr = reinterpret_cast<const uint8_t*>(s.data());
                            value.len = s.size();
                            col_writer->WriteBatch(1, nullptr, nullptr, &value);
                        }
                    }
                } else {
                    for (size_t row_id = 0; row_id < sz; row_id++) {
                        auto s = type->to_string(*col, row_id);
                        value.ptr = reinterpret_cast<const uint8_t*>(s.data());
                        value.len = s.size();
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                }
                break;
            }
            default: {
                std::stringstream ss;
                ss << "unsupported file format: " << _output_vexpr_ctxs[i]->root()->type().type;
                return Status::InvalidArgument(ss.str());
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

Status VParquetWriterWrapper::init_parquet_writer() {
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

VParquetWriterWrapper::~VParquetWriterWrapper() {}

} // namespace doris::vectorized
