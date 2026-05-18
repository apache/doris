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

#include "format/parquet/parquet_reader.h"

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "io/fs/file_reader.h"
#include "util/slice.h"

namespace doris::parquet {

namespace {

Status arrow_status_to_doris_status(const arrow::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    if (status.IsIOError()) {
        return Status::IOError(status.ToString());
    }
    if (status.IsInvalid()) {
        return Status::InvalidArgument(status.ToString());
    }
    return Status::InternalError(status.ToString());
}

class DorisRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    DorisRandomAccessFile(io::FileReaderSPtr file, io::IOContext* io_ctx)
            : _file(std::move(file)), _io_ctx(io_ctx) {
        set_mode(arrow::io::FileMode::READ);
    }

    arrow::Status Close() override {
        _closed = true;
        return arrow::Status::OK();
    }

    bool closed() const override { return _closed; }

    arrow::Result<int64_t> Tell() const override { return _pos; }

    arrow::Status Seek(int64_t position) override {
        if (position < 0) {
            return arrow::Status::Invalid("negative seek position");
        }
        _pos = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> GetSize() override {
        if (!_file) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        return static_cast<int64_t>(_file->size());
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(_pos, nbytes, out));
        _pos += bytes_read;
        return bytes_read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, Read(nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (!_file) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid("negative read position or length");
        }
        size_t bytes_read = 0;
        Status st = _file->read_at(static_cast<size_t>(position),
                                   Slice(static_cast<uint8_t*>(out), static_cast<size_t>(nbytes)),
                                   &bytes_read, _io_ctx);
        if (!st.ok()) {
            return arrow::Status::IOError(st.to_string_no_stack());
        }
        return static_cast<int64_t>(bytes_read);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                         int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(position, nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

private:
    io::FileReaderSPtr _file;
    io::IOContext* _io_ctx = nullptr;
    int64_t _pos = 0;
    bool _closed = false;
};

DataTypePtr make_nullable_if_needed(DataTypePtr type, const ::parquet::ColumnDescriptor* column) {
    if (type != nullptr && column != nullptr && column->max_definition_level() > 0) {
        return make_nullable(type);
    }
    return type;
}

DataTypePtr create_type(PrimitiveType type, bool nullable, int precision = 0, int scale = 0) {
    return DataTypeFactory::instance().create_data_type(type, nullable, precision, scale);
}

PrimitiveType decimal_primitive_type(int precision) {
    return precision > 38 ? TYPE_DECIMAL256 : TYPE_DECIMAL128I;
}

DataTypePtr converted_type_to_doris_type(const ::parquet::ColumnDescriptor* column) {
    switch (column->converted_type()) {
    case ::parquet::ConvertedType::UTF8:
    case ::parquet::ConvertedType::ENUM:
    case ::parquet::ConvertedType::JSON:
    case ::parquet::ConvertedType::BSON:
        return create_type(TYPE_STRING, column->max_definition_level() > 0);
    case ::parquet::ConvertedType::DECIMAL:
        return create_type(decimal_primitive_type(column->type_precision()),
                           column->max_definition_level() > 0,
                           column->type_precision(), column->type_scale());
    case ::parquet::ConvertedType::DATE:
        return create_type(TYPE_DATEV2, column->max_definition_level() > 0);
    case ::parquet::ConvertedType::TIME_MILLIS:
        return create_type(TYPE_TIMEV2, column->max_definition_level() > 0, 0, 3);
    case ::parquet::ConvertedType::TIME_MICROS:
        return create_type(TYPE_TIMEV2, column->max_definition_level() > 0, 0, 6);
    case ::parquet::ConvertedType::TIMESTAMP_MILLIS:
        return create_type(TYPE_DATETIMEV2, column->max_definition_level() > 0, 0, 3);
    case ::parquet::ConvertedType::TIMESTAMP_MICROS:
        return create_type(TYPE_DATETIMEV2, column->max_definition_level() > 0, 0, 6);
    case ::parquet::ConvertedType::INT_8:
        return create_type(TYPE_TINYINT, column->max_definition_level() > 0);
    case ::parquet::ConvertedType::UINT_8:
    case ::parquet::ConvertedType::INT_16:
        return create_type(TYPE_SMALLINT, column->max_definition_level() > 0);
    case ::parquet::ConvertedType::UINT_16:
    case ::parquet::ConvertedType::INT_32:
        return create_type(TYPE_INT, column->max_definition_level() > 0);
    case ::parquet::ConvertedType::UINT_32:
    case ::parquet::ConvertedType::INT_64:
        return create_type(TYPE_BIGINT, column->max_definition_level() > 0);
    case ::parquet::ConvertedType::UINT_64:
        return create_type(TYPE_LARGEINT, column->max_definition_level() > 0);
    case ::parquet::ConvertedType::NONE:
    default:
        return nullptr;
    }
}

DataTypePtr logical_type_to_doris_type(const ::parquet::ColumnDescriptor* column) {
    const auto& logical_type = column->logical_type();
    if (logical_type == nullptr || !logical_type->is_valid() || logical_type->is_none()) {
        return nullptr;
    }
    const bool nullable = column->max_definition_level() > 0;
    if (logical_type->is_string() || logical_type->is_enum() || logical_type->is_JSON() ||
        logical_type->is_BSON() || logical_type->is_UUID()) {
        return create_type(TYPE_STRING, nullable);
    }
    if (logical_type->is_decimal()) {
        const auto& decimal_type = static_cast<const ::parquet::DecimalLogicalType&>(*logical_type);
        return create_type(decimal_primitive_type(decimal_type.precision()), nullable,
                           decimal_type.precision(), decimal_type.scale());
    }
    if (logical_type->is_date()) {
        return create_type(TYPE_DATEV2, nullable);
    }
    if (logical_type->is_time()) {
        const auto& time_type = static_cast<const ::parquet::TimeLogicalType&>(*logical_type);
        int scale = 0;
        if (time_type.time_unit() == ::parquet::LogicalType::TimeUnit::MILLIS) {
            scale = 3;
        } else if (time_type.time_unit() == ::parquet::LogicalType::TimeUnit::MICROS) {
            scale = 6;
        } else {
            return nullptr;
        }
        return create_type(TYPE_TIMEV2, nullable, 0, scale);
    }
    if (logical_type->is_timestamp()) {
        const auto& timestamp_type =
                static_cast<const ::parquet::TimestampLogicalType&>(*logical_type);
        int scale = 0;
        if (timestamp_type.time_unit() == ::parquet::LogicalType::TimeUnit::MILLIS) {
            scale = 3;
        } else if (timestamp_type.time_unit() == ::parquet::LogicalType::TimeUnit::MICROS) {
            scale = 6;
        } else {
            return nullptr;
        }
        return create_type(TYPE_DATETIMEV2, nullable, 0, scale);
    }
    if (logical_type->is_int()) {
        const auto& int_type = static_cast<const ::parquet::IntLogicalType&>(*logical_type);
        switch (int_type.bit_width()) {
        case 8:
            return create_type(int_type.is_signed() ? TYPE_TINYINT : TYPE_SMALLINT, nullable);
        case 16:
            return create_type(int_type.is_signed() ? TYPE_SMALLINT : TYPE_INT, nullable);
        case 32:
            return create_type(int_type.is_signed() ? TYPE_INT : TYPE_BIGINT, nullable);
        case 64:
            return create_type(int_type.is_signed() ? TYPE_BIGINT : TYPE_LARGEINT, nullable);
        default:
            return nullptr;
        }
    }
    return nullptr;
}

DataTypePtr parquet_column_to_doris_type(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr) {
        return nullptr;
    }

    if (auto logical_type = logical_type_to_doris_type(column); logical_type != nullptr) {
        return logical_type;
    }
    if (auto converted_type = converted_type_to_doris_type(column); converted_type != nullptr) {
        return converted_type;
    }

    DataTypePtr type;
    switch (column->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        type = std::make_shared<DataTypeBool>();
        break;
    case ::parquet::Type::INT32:
        type = std::make_shared<DataTypeInt32>();
        break;
    case ::parquet::Type::INT64:
        type = std::make_shared<DataTypeInt64>();
        break;
    case ::parquet::Type::FLOAT:
        type = std::make_shared<DataTypeFloat32>();
        break;
    case ::parquet::Type::DOUBLE:
        type = std::make_shared<DataTypeFloat64>();
        break;
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        type = std::make_shared<DataTypeString>();
        break;
    case ::parquet::Type::INT96:
        type = std::make_shared<DataTypeString>();
        break;
    default:
        return nullptr;
    }
    return make_nullable_if_needed(type, column);
}

std::string column_name(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr) {
        return {};
    }
    auto path = column->path();
    if (path) {
        return path->ToDotString();
    }
    return column->name();
}

} // namespace

struct ParquetColumnReaderState {
    int file_column_id = -1;
    int parquet_column_ordinal = -1;
    const ::parquet::ColumnDescriptor* descriptor = nullptr;
    std::shared_ptr<::parquet::ColumnReader> reader;
};

struct ParquetFileState {
    // Doris 文件句柄适配成 Arrow RandomAccessFile。该对象只处理随机读，不携带
    // table/global schema 语义。
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;

    // Arrow Parquet core reader 和 footer metadata。ParquetReader 只依赖 core API，
    // 不使用 parquet::arrow reader，也不输出 Arrow Array/RecordBatch。
    std::unique_ptr<::parquet::ParquetFileReader> parquet_reader;
    std::shared_ptr<::parquet::FileMetaData> metadata;
    const ::parquet::SchemaDescriptor* schema = nullptr;

    // 当前 scan 的 file-local projection 和 row group 列表。阶段一先保守选择全部 row
    // groups；后续 row group/page/bloom pruning 只消费 FileLocalFilter。
    std::vector<int> projected_columns;
    std::vector<int> selected_row_groups;
    int next_row_group_idx = 0;
    std::shared_ptr<::parquet::RowGroupReader> current_row_group;
    std::vector<ParquetColumnReaderState> current_columns;
};

namespace {

Status reset_reader_position(ParquetFileState* state) {
    if (state == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    state->next_row_group_idx = 0;
    state->current_row_group.reset();
    state->current_columns.clear();
    return Status::OK();
}

} // namespace

ParquetReader::ParquetReader() : _state(std::make_unique<ParquetFileState>()) {}

ParquetReader::~ParquetReader() = default;

Status ParquetReader::open(io::FileReaderSPtr file, io::IOContext* io_ctx) {
    RETURN_IF_ERROR(reader::FileReader::open(file, io_ctx));
    _state = std::make_unique<ParquetFileState>();
    _state->arrow_file = std::make_shared<DorisRandomAccessFile>(_file, _io_ctx);

    try {
        _state->parquet_reader =
                ::parquet::ParquetFileReader::Open(_state->arrow_file,
                                                   ::parquet::default_reader_properties());
        _state->metadata = _state->parquet_reader->metadata();
        _state->schema = _state->metadata != nullptr ? _state->metadata->schema() : nullptr;
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to open parquet file: {}", e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open parquet file: {}", e.what());
    }

    if (_state->metadata == nullptr || _state->schema == nullptr) {
        return Status::Corruption("Failed to read parquet metadata");
    }
    return Status::OK();
}

Status ParquetReader::get_schema(std::vector<reader::SchemaField>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("file_schema is null");
    }
    file_schema->clear();
    if (_state == nullptr || _state->schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }

    const int num_columns = _state->schema->num_columns();
    file_schema->reserve(num_columns);
    for (int column_idx = 0; column_idx < num_columns; ++column_idx) {
        const auto* column = _state->schema->Column(column_idx);
        reader::SchemaField field;
        field.id = column_idx;
        field.name = column_name(column);
        field.type = parquet_column_to_doris_type(column);
        if (field.type == nullptr) {
            return Status::NotSupported("Unsupported parquet column type for column {}",
                                        field.name);
        }
        file_schema->push_back(std::move(field));
    }
    return Status::OK();
}

Status ParquetReader::init(const ParquetScanRequest& request) {
    return init(static_cast<const reader::FileScanRequest&>(request));
}

Status ParquetReader::init(const reader::FileScanRequest& request) {
    if (_state == nullptr || _state->metadata == nullptr || _state->schema == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    RETURN_IF_ERROR(reader::FileReader::init(request));

    _state->projected_columns.clear();
    const int num_columns = _state->schema->num_columns();
    for (auto column_id : request.projected_file_columns) {
        if (column_id < 0 || column_id >= num_columns) {
            return Status::InvalidArgument("Invalid parquet column id {}", column_id);
        }
        _state->projected_columns.push_back(column_id);
    }

    _state->selected_row_groups.clear();
    const int num_row_groups = _state->metadata->num_row_groups();
    _state->selected_row_groups.reserve(num_row_groups);
    for (int row_group_idx = 0; row_group_idx < num_row_groups; ++row_group_idx) {
        _state->selected_row_groups.push_back(row_group_idx);
    }
    RETURN_IF_ERROR(reset_reader_position(_state.get()));
    _eof = _state->selected_row_groups.empty();
    return Status::OK();
}

Status ParquetReader::next(Block* file_block, size_t* rows, bool* eof) {
    (void)file_block;
    if (rows != nullptr) {
        *rows = 0;
    }
    if (eof != nullptr) {
        *eof = _eof;
    }
    if (_eof) {
        return Status::OK();
    }
    return Status::NotSupported(
            "Arrow-backed ParquetReader stage 1 only supports metadata/schema initialization; "
            "Doris Block decoding will be implemented in the flat primitive read stage");
}

Status ParquetReader::close() {
    if (_state != nullptr) {
        if (_state->parquet_reader != nullptr) {
            try {
                _state->parquet_reader->Close();
            } catch (const std::exception&) {
                // close 需要保持幂等；这里不覆盖此前 scan 路径上的真实错误。
            }
        }
        if (_state->arrow_file != nullptr) {
            static_cast<void>(arrow_status_to_doris_status(_state->arrow_file->Close()));
        }
        _state = std::make_unique<ParquetFileState>();
    }
    return reader::FileReader::close();
}

} // namespace doris::parquet
