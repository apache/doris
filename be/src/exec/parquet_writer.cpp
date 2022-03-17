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

#include "exec/parquet_writer.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <time.h>

#include "common/logging.h"
#include "exec/file_writer.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "util/thrift_util.h"
#include "util/types.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr.h"

namespace doris {

/// ParquetOutputStream
ParquetOutputStream::ParquetOutputStream(FileWriter* file_writer)
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
    size_t written_len = 0;
    Status st = _file_writer->write(static_cast<const uint8_t*>(data), nbytes, &written_len);
    if (!st.ok()) {
        return arrow::Status::IOError(st.get_error_msg());
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
        LOG(WARNING) << "close parquet output stream failed: " << st.get_error_msg();
        return arrow::Status::IOError(st.get_error_msg());
    }
    _is_closed = true;
    return arrow::Status::OK();
}

int64_t ParquetOutputStream::get_written_len() {
    return _written_len;
}

void ParquetOutputStream::set_written_len(int64_t written_len) {
    _written_len = written_len;
}

/// ParquetWriterWrapper
ParquetWriterWrapper::ParquetWriterWrapper(FileWriter* file_writer,
                                           const std::vector<ExprContext*>* output_expr_ctxs,
                                           const std::map<std::string, std::string>& properties,
                                           const std::vector<std::vector<std::string>>& schema,
                                           bool output_object_data)
        : _output_expr_ctxs(output_expr_ctxs),
          _output_vexpr_ctxs(nullptr),
          _str_schema(schema),
          _cur_writed_rows(0),
          _rg_writer(nullptr),
          _output_object_data(output_object_data) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
    parse_properties(properties);
    parse_schema(schema);
    init_parquet_writer();
}

ParquetWriterWrapper::ParquetWriterWrapper(
        FileWriter* file_writer, const std::vector<vectorized::VExprContext*>* output_vexpr_ctxs,
        const std::map<std::string, std::string>& properties,
        const std::vector<std::vector<std::string>>& schema, bool output_object_data)
        : _output_expr_ctxs(nullptr),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _str_schema(schema),
          _cur_writed_rows(0),
          _rg_writer(nullptr),
          _output_object_data(output_object_data) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
    parse_properties(properties);
    parse_schema(schema);
    init_parquet_writer();
}

void ParquetWriterWrapper::parse_properties(
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

Status ParquetWriterWrapper::parse_schema(const std::vector<std::vector<std::string>>& schema) {
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

Status ParquetWriterWrapper::write(const RowBatch& row_batch) {
    int num_rows = row_batch.num_rows();
    for (int i = 0; i < num_rows; ++i) {
        TupleRow* row = row_batch.get_row(i);
        RETURN_IF_ERROR(_write_one_row(row));
        _cur_writed_rows++;
    }
    return Status::OK();
}

Status ParquetWriterWrapper::write(vectorized::Block& block) {
    int num_columns = _output_vexpr_ctxs->size();
    _column_ids.resize(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        int column_id = -1;
        (*_output_vexpr_ctxs)[i]->execute(&block, &column_id);
        _column_ids[i] = column_id;
    }

    int num_rows = block.rows();
    materialize_block_inplace(block, _column_ids.begin(), _column_ids.end());

    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(_write_one_row(block, i));
        _cur_writed_rows++;
    }
    return Status::OK();
}

Status ParquetWriterWrapper::init_parquet_writer() {
    _writer = parquet::ParquetFileWriter::Open(_outstream, _schema, _properties);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

parquet::RowGroupWriter* ParquetWriterWrapper::get_rg_writer() {
    if (_rg_writer == nullptr) {
        _rg_writer = _writer->AppendBufferedRowGroup();
    }
    if (_cur_writed_rows > _max_row_per_group) {
        _rg_writer->Close();
        _rg_writer = _writer->AppendBufferedRowGroup();
        _cur_writed_rows = 0;
    }
    return _rg_writer;
}

Status ParquetWriterWrapper::_write_one_row(TupleRow* row) {
    int num_columns = _output_expr_ctxs->size();
    if (num_columns != _str_schema.size()) {
        return Status::InternalError("project field size is not equal to schema column size");
    }
    try {
        for (int index = 0; index < num_columns; ++index) {
            void* item = (*_output_expr_ctxs)[index]->get_value(row);
            switch ((*_output_expr_ctxs)[index]->root()->type().type) {
            case TYPE_BOOLEAN: {
                if (_str_schema[index][1] != "boolean") {
                    return error_msg("boolean", "boolean", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::BoolWriter* col_writer =
                        static_cast<parquet::BoolWriter*>(rgWriter->column(index));
                if (item != nullptr) {
                    col_writer->WriteBatch(1, nullptr, nullptr, static_cast<bool*>(item));
                } else {
                    bool default_bool = false;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_bool);
                }
                break;
            }
            case TYPE_TINYINT: {
                if (_str_schema[index][1] != "int32") {
                    return error_msg("tiny int", "int32", index);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(index));
                if (item != nullptr) {
                    int32_t data = *(static_cast<int8_t*>(item));
                    col_writer->WriteBatch(1, nullptr, nullptr, &data);
                } else {
                    int32_t default_int32 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int32);
                }
                break;
            }
            case TYPE_SMALLINT: {
                if (_str_schema[index][1] != "int32") {
                    return error_msg("small int", "int32", index);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(index));
                if (item != nullptr) {
                    int32_t data = *(static_cast<int16_t*>(item));
                    col_writer->WriteBatch(1, nullptr, nullptr, &data);
                } else {
                    int32_t default_int32 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int32);
                }
                break;
            }
            case TYPE_INT: {
                if (_str_schema[index][1] != "int32") {
                    return error_msg("int", "int32", index);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(index));
                if (item != nullptr) {
                    col_writer->WriteBatch(1, nullptr, nullptr, static_cast<int32_t*>(item));
                } else {
                    int32_t default_int32 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int32);
                }
                break;
            }
            case TYPE_BIGINT: {
                if (_str_schema[index][1] != "int64") {
                    return error_msg("big int", "int64", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(index));
                if (item != nullptr) {
                    col_writer->WriteBatch(1, nullptr, nullptr, (int64_t*)(item));
                } else {
                    int64_t default_int644 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int644);
                }
                break;
            }
            case TYPE_LARGEINT: {
                // TODO: not support int_128
                // It is better write a default value, because rg_writer need all columns has value before flush to disk.
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(index));
                int64_t default_int64 = 0;
                col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                return Status::InvalidArgument("do not support large int type.");
            }
            case TYPE_FLOAT: {
                if (_str_schema[index][1] != "float") {
                    return error_msg("float", "float", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::FloatWriter* col_writer =
                        static_cast<parquet::FloatWriter*>(rgWriter->column(index));
                if (item != nullptr) {
                    col_writer->WriteBatch(1, nullptr, nullptr, (float_t*)(item));
                } else {
                    float_t default_float = 0.0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_float);
                }
                break;
            }
            case TYPE_DOUBLE: {
                if (_str_schema[index][1] != "double") {
                    return error_msg("double", "double", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::DoubleWriter* col_writer =
                        static_cast<parquet::DoubleWriter*>(rgWriter->column(index));
                if (item != nullptr) {
                    col_writer->WriteBatch(1, nullptr, nullptr, (double_t*)(item));
                } else {
                    double_t default_double = 0.0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_double);
                }
                break;
            }
            case TYPE_DATETIME:
            case TYPE_DATE: {
                if (_str_schema[index][1] != "int64") {
                    return error_msg("date/datetime", "int64", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(index));
                if (item != nullptr) {
                    const DateTimeValue* time_val = (const DateTimeValue*)(item);
                    int64_t timestamp = time_val->to_olap_datetime();
                    col_writer->WriteBatch(1, nullptr, nullptr, &timestamp);
                } else {
                    int64_t default_int64 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                }
                break;
            }

            case TYPE_HLL:
            case TYPE_OBJECT: {
                if (_output_object_data) {
                    if (_str_schema[index][1] != "byte_array") {
                        return error_msg("hll/bitmap", "byte_array", index);
                    }
                    parquet::RowGroupWriter* rgWriter = get_rg_writer();
                    parquet::ByteArrayWriter* col_writer =
                            static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                    if (item != nullptr) {
                        const StringValue* string_val = (const StringValue*)(item);
                        parquet::ByteArray value;
                        value.ptr = reinterpret_cast<const uint8_t*>(string_val->ptr);
                        value.len = string_val->len;
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    } else {
                        parquet::ByteArray value;
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                } else {
                    fmt::memory_buffer err_ss;
                    fmt::format_to(err_ss, "unsupported file format: : {}.",
                                   (*_output_vexpr_ctxs)[index]->root()->type().type);
                    return Status::InvalidArgument(fmt::to_string(err_ss.data()));
                }
                break;
            }
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_STRING: {
                if (_str_schema[index][1] != "byte_array") {
                    return error_msg("char/varchar", "byte_array", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                if (item != nullptr) {
                    const StringValue* string_val = (const StringValue*)(item);
                    parquet::ByteArray value;
                    value.ptr = reinterpret_cast<const uint8_t*>(string_val->ptr);
                    value.len = string_val->len;
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);
                } else {
                    parquet::ByteArray value;
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);
                }
                break;
            }
            case TYPE_DECIMALV2: {
                if (_str_schema[index][1] != "byte_array") {
                    return error_msg("decimal v2", "byte_array", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                if (item != nullptr) {
                    const DecimalV2Value decimal_val(
                            reinterpret_cast<const PackedInt128*>(item)->value);
                    char decimal_buffer[MAX_DECIMAL_WIDTH];
                    int output_scale = (*_output_expr_ctxs)[index]->root()->output_scale();
                    parquet::ByteArray value;
                    value.ptr = reinterpret_cast<const uint8_t*>(decimal_buffer);
                    value.len = decimal_val.to_buffer(decimal_buffer, output_scale);
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);
                } else {
                    parquet::ByteArray value;
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);
                }
                break;
            }
            default: {
                fmt::memory_buffer err_ss;
                fmt::format_to(err_ss, "unsupported file format: : {}.",
                               (*_output_vexpr_ctxs)[index]->root()->type().type);
                return Status::InvalidArgument(fmt::to_string(err_ss.data()));
            }
            }
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Parquet write error: " << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status ParquetWriterWrapper::_write_one_row(vectorized::Block& block, size_t row) {
    int num_columns = _output_vexpr_ctxs->size();
    if (num_columns != _str_schema.size()) {
        return Status::InternalError("project field size is not equal to schema column size");
    }
    try {
        for (int index = 0; index < num_columns; ++index) {
            bool is_null = false;
            auto& column_ptr = block.get_by_position(_column_ids[index]).column;
            auto& type_ptr = block.get_by_position(_column_ids[index]).type;

            vectorized::ColumnPtr column;
            if (type_ptr->is_nullable()) {
                column = assert_cast<const vectorized::ColumnNullable&>(*column_ptr)
                                 .get_nested_column_ptr();
                if (column_ptr->is_null_at(row)) {
                    is_null = true;
                }
            } else {
                is_null = false;
                column = column_ptr;
            }

            switch ((*_output_vexpr_ctxs)[index]->root()->result_type()) {
            case TYPE_BOOLEAN: {
                if (_str_schema[index][1] != "boolean") {
                    return error_msg("boolean", "boolean", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::BoolWriter* col_writer =
                        static_cast<parquet::BoolWriter*>(rgWriter->column(index));

                if (is_null) {
                    bool default_bool = false;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_bool);
                } else {
                    auto& data_column = assert_cast<const vectorized::ColumnUInt8&>(*column);
                    uint8_t data = data_column.get_element(row);
                    col_writer->WriteBatch(1, nullptr, nullptr,
                                           reinterpret_cast<const bool*>(&data));
                }
                break;
            }

            case TYPE_TINYINT: {
                if (_str_schema[index][1] != "int32") {
                    return error_msg("tiny int", "int32", index);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(index));

                if (is_null) {
                    int32_t default_int32 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int32);
                } else {
                    auto& data_column = assert_cast<const vectorized::ColumnInt8&>(*column);
                    int32_t data = data_column.get_element(row);
                    col_writer->WriteBatch(1, nullptr, nullptr, &data);
                }
                break;
            }

            case TYPE_SMALLINT: {
                if (_str_schema[index][1] != "int32") {
                    return error_msg("small int", "int32", index);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(index));

                if (is_null) {
                    int32_t default_int32 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int32);
                } else {
                    auto& data_column = assert_cast<const vectorized::ColumnInt16&>(*column);
                    int32_t data = data_column.get_element(row);
                    col_writer->WriteBatch(1, nullptr, nullptr,
                                           reinterpret_cast<const int32_t*>(&data));
                }
                break;
            }

            case TYPE_INT: {
                if (_str_schema[index][1] != "int32") {
                    return error_msg("int", "int32", index);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int32Writer* col_writer =
                        static_cast<parquet::Int32Writer*>(rgWriter->column(index));

                if (is_null) {
                    int32_t default_int32 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int32);
                } else {
                    auto& data = assert_cast<const vectorized::ColumnInt32&>(*column).get_data();
                    col_writer->WriteBatch(1, nullptr, nullptr, (int32_t*)(&data[row]));
                }
                break;
            }

            case TYPE_BIGINT: {
                if (_str_schema[index][1] != "int64") {
                    return error_msg("big int", "int64", index);
                }

                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(index));

                if (is_null) {
                    int64_t default_int64 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                } else {
                    auto& data = assert_cast<const vectorized::ColumnInt64&>(*column).get_data();
                    col_writer->WriteBatch(1, nullptr, nullptr, (int64_t*)(&data[row]));
                }
                break;
            }

            case TYPE_LARGEINT: {
                // TODO: not support int_128
                // It is better write a default value, because rg_writer need all columns has value before flush to disk.
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(index));
                int64_t default_int64 = 0;
                col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);
                return Status::InvalidArgument("do not support large int type.");
            }

            case TYPE_FLOAT: {
                if (_str_schema[index][1] != "float") {
                    return error_msg("float", "float", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::FloatWriter* col_writer =
                        static_cast<parquet::FloatWriter*>(rgWriter->column(index));
                if (is_null) {
                    float_t default_float = 0.0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_float);

                } else {
                    auto& data = assert_cast<const vectorized::ColumnFloat32&>(*column).get_data();
                    col_writer->WriteBatch(1, nullptr, nullptr, (float_t*)(&data[row]));
                }
                break;
            }

            case TYPE_DOUBLE: {
                if (_str_schema[index][1] != "double") {
                    return error_msg("double", "double", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::DoubleWriter* col_writer =
                        static_cast<parquet::DoubleWriter*>(rgWriter->column(index));
                if (is_null) {
                    double_t default_double = 0.0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_double);

                } else {
                    auto& data = assert_cast<const vectorized::ColumnFloat64&>(*column).get_data();
                    col_writer->WriteBatch(1, nullptr, nullptr, (double_t*)(&data[row]));
                }
                break;
            }

            case TYPE_STRING:
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
                if (_str_schema[index][1] != "byte_array") {
                    return error_msg("char/varchar", "byte_array", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                if (is_null) {
                    parquet::ByteArray value;
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);

                } else {
                    const auto& string_val =
                            assert_cast<const vectorized::ColumnString&>(*column).get_data_at(row);
                    parquet::ByteArray value;
                    value.ptr = reinterpret_cast<const uint8_t*>(string_val.data);
                    value.len = string_val.size;
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);
                }
                break;
            }

            case TYPE_DECIMALV2: {
                if (_str_schema[index][1] != "byte_array") {
                    return error_msg("decimal v2", "byte_array", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                if (is_null) {
                    parquet::ByteArray value;
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);

                } else {
                    DecimalV2Value decimal_val =
                            (DecimalV2Value)assert_cast<
                                    const vectorized::ColumnDecimal<vectorized::Decimal128>&>(
                                    *column)
                                    .get_data()[row];
                    char decimal_buffer[MAX_DECIMAL_WIDTH];
                    // TODO: Support decimal output_scale accuracy 
                    //int output_scale = (*_output_vexpr_ctxs)[index]->root()->output_scale();
                    int output_scale = -1;
                    parquet::ByteArray value;
                    value.ptr = reinterpret_cast<const uint8_t*>(decimal_buffer);
                    value.len = decimal_val.to_buffer(decimal_buffer, output_scale);
                    col_writer->WriteBatch(1, nullptr, nullptr, &value);
                }
                break;
            }

            case TYPE_DATE:
            case TYPE_DATETIME: {
                if (_str_schema[index][1] != "int64") {
                    return error_msg("date/datetime", "int64", index);
                }
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::Int64Writer* col_writer =
                        static_cast<parquet::Int64Writer*>(rgWriter->column(index));
                if (is_null) {
                    int64_t default_int64 = 0;
                    col_writer->WriteBatch(1, nullptr, nullptr, &default_int64);

                } else {
                    int64_t int_val =
                            assert_cast<const vectorized::ColumnInt64&>(*column).get_data()[row];
                    vectorized::VecDateTimeValue value =
                            binary_cast<int64_t, doris::vectorized::VecDateTimeValue>(int_val);
                    int64_t timestamp = value.to_olap_datetime();
                    col_writer->WriteBatch(1, nullptr, nullptr, &timestamp);
                }
                break;
            }

            case TYPE_OBJECT: {
                if (_output_object_data) {
                    if (_str_schema[index][1] != "byte_array") {
                        return error_msg("object", "byte_array", index);
                    }
                    parquet::RowGroupWriter* rgWriter = get_rg_writer();
                    parquet::ByteArrayWriter* col_writer =
                            static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                    if (is_null) {
                        parquet::ByteArray value;
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);

                    } else {
                        auto date_column = assert_cast<const vectorized::ColumnBitmap&>(*column);
                        BitmapValue& data = date_column.get_element(row);
                        std::string bitmap_str(data.getSizeInBytes(), '0');
                        data.write(bitmap_str.data());
                        parquet::ByteArray value;
                        value.ptr = reinterpret_cast<const uint8_t*>(bitmap_str.c_str());
                        value.len = bitmap_str.length();
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                } else {
                    fmt::memory_buffer err_ss;
                    fmt::format_to(err_ss, "unsupported file format: : {}.",
                                   (*_output_vexpr_ctxs)[index]->root()->type().type);
                    return Status::InvalidArgument(fmt::to_string(err_ss.data()));
                }
                break;
            }

            case TYPE_HLL: {
                if (_output_object_data) {
                    if (_str_schema[index][1] != "byte_array") {
                        return error_msg("hll", "byte_array", index);
                    }
                    parquet::RowGroupWriter* rgWriter = get_rg_writer();
                    parquet::ByteArrayWriter* col_writer =
                            static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                    if (is_null) {
                        parquet::ByteArray value;
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);

                    } else {
                        auto& data =
                                assert_cast<const vectorized::ColumnHLL&>(*column).get_data()[row];
                        std::string hll_str(data.max_serialized_size(), '0');
                        size_t actual_size = data.serialize((uint8_t*)hll_str.data());
                        hll_str.resize(actual_size);
                        parquet::ByteArray value;
                        value.ptr = reinterpret_cast<const uint8_t*>(hll_str.c_str());
                        value.len = hll_str.length();
                        col_writer->WriteBatch(1, nullptr, nullptr, &value);
                    }
                } else {
                    fmt::memory_buffer err_ss;
                    fmt::format_to(err_ss, "unsupported file format: : {}.",
                                   (*_output_vexpr_ctxs)[index]->root()->type().type);
                    return Status::InvalidArgument(fmt::to_string(err_ss.data()));
                }
                break;
            }
            default: {
                fmt::memory_buffer err_ss;
                fmt::format_to(err_ss, "unsupported file format: : {}.",
                               (*_output_vexpr_ctxs)[index]->root()->type().type);
                return Status::InvalidArgument(fmt::to_string(err_ss.data()));
            }
            }
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Parquet write error: " << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

int64_t ParquetWriterWrapper::written_len() {
    return _outstream->get_written_len();
}
void ParquetWriterWrapper::close() {
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

Status ParquetWriterWrapper::error_msg(const std::string& field_type,
                                       const std::string& parquert_type, int index) {
    fmt::memory_buffer err_ss;
    fmt::format_to(err_ss,
                   "project field type is {}, should use parquert_type {}, but the definition type "
                   "of column {} is {}.",
                   field_type, parquert_type, _str_schema[index][2], _str_schema[index][1]);
    return Status::InvalidArgument(fmt::to_string(err_ss.data()));
}

ParquetWriterWrapper::~ParquetWriterWrapper() {}

} // namespace doris
