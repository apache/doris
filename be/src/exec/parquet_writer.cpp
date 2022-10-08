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
#include <gen_cpp/DataSinks_types.h>
#include <time.h>

#include "io/file_writer.h"
#include "util/mysql_global.h"
#include "util/types.h"

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

ParquetWriterWrapper::ParquetWriterWrapper(FileWriter* file_writer,
                                           const std::vector<ExprContext*>& output_expr_ctxs,
                                           const std::map<std::string, std::string>& properties,
                                           const std::vector<std::vector<std::string>>& schema,
                                           bool output_object_data)
        : _output_expr_ctxs(output_expr_ctxs),
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

/// ParquetWriterWrapper
ParquetWriterWrapper::ParquetWriterWrapper(FileWriter* file_writer,
                                           const std::vector<ExprContext*>& output_expr_ctxs,
                                           const std::vector<TParquetSchema>& parquet_schemas,
                                           const TParquetCompressionType::type& compression_type,
                                           const bool& parquet_disable_dictionary,
                                           const TParquetVersion::type& parquet_version,
                                           bool output_object_data)
        : _output_expr_ctxs(output_expr_ctxs),
          _cur_writed_rows(0),
          _rg_writer(nullptr),
          _output_object_data(output_object_data) {
    _outstream = std::shared_ptr<ParquetOutputStream>(new ParquetOutputStream(file_writer));
    parse_properties(compression_type, parquet_disable_dictionary, parquet_version);
    parse_schema(parquet_schemas);
    init_parquet_writer();
}

void ParquetWriterWrapper::parse_properties(const TParquetCompressionType::type& compression_type,
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

void ParquetWriterWrapper::parse_schema(const std::vector<TParquetSchema>& parquet_schemas) {
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

Status ParquetWriterWrapper::write(const RowBatch& row_batch) {
    int num_rows = row_batch.num_rows();
    for (int i = 0; i < num_rows; ++i) {
        TupleRow* row = row_batch.get_row(i);
        RETURN_IF_ERROR(_write_one_row(row));
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

template <typename T>
void ParquetWriterWrapper::write_int32_column(int index, T* item) {
    parquet::RowGroupWriter* rgWriter = get_rg_writer();
    parquet::Int32Writer* col_writer = static_cast<parquet::Int32Writer*>(rgWriter->column(index));
    int32_t value = 0;
    if (item != nullptr) {
        value = *item;
    }
    col_writer->WriteBatch(1, nullptr, nullptr, &value);
}

Status ParquetWriterWrapper::_write_one_row(TupleRow* row) {
    int num_columns = _output_expr_ctxs.size();
    try {
        for (int index = 0; index < num_columns; ++index) {
            void* item = _output_expr_ctxs[index]->get_value(row);
            switch (_output_expr_ctxs[index]->root()->type().type) {
            case TYPE_BOOLEAN: {
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
                write_int32_column(index, static_cast<int8_t*>(item));
                break;
            }
            case TYPE_SMALLINT: {
                write_int32_column(index, static_cast<int16_t*>(item));
                break;
            }
            case TYPE_INT: {
                write_int32_column(index, static_cast<int32_t*>(item));
                break;
            }
            case TYPE_BIGINT: {
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
                    std::stringstream ss;
                    ss << "unsupported file format: "
                       << _output_expr_ctxs[index]->root()->type().type;
                    return Status::InvalidArgument(ss.str());
                }
                break;
            }
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_STRING: {
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
                parquet::RowGroupWriter* rgWriter = get_rg_writer();
                parquet::ByteArrayWriter* col_writer =
                        static_cast<parquet::ByteArrayWriter*>(rgWriter->column(index));
                if (item != nullptr) {
                    const DecimalV2Value decimal_val(
                            reinterpret_cast<const PackedInt128*>(item)->value);
                    char decimal_buffer[MAX_DECIMAL_WIDTH];
                    int output_scale = _output_expr_ctxs[index]->root()->output_scale();
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
                std::stringstream ss;
                ss << "unsupported file format: " << _output_expr_ctxs[index]->root()->type().type;
                return Status::InvalidArgument(ss.str());
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

} // namespace doris
