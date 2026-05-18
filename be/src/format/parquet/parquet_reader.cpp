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
#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "io/fs/file_reader.h"
#include "util/slice.h"

namespace doris::parquet {

namespace {

constexpr int64_t DEFAULT_PARQUET_READ_BATCH_SIZE = 4096;

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

bool has_non_physical_annotation(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr) {
        return false;
    }
    const auto& logical_type = column->logical_type();
    return column->converted_type() != ::parquet::ConvertedType::NONE ||
           (logical_type != nullptr && logical_type->is_valid() && !logical_type->is_none());
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

DataTypePtr direct_flat_primitive_doris_type(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr || column->max_repetition_level() != 0 ||
        column->max_definition_level() > 1 || has_non_physical_annotation(column)) {
        return nullptr;
    }

    const bool nullable = column->max_definition_level() > 0;
    switch (column->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        return create_type(TYPE_BOOLEAN, nullable);
    case ::parquet::Type::INT32:
        return create_type(TYPE_INT, nullable);
    case ::parquet::Type::INT64:
        return create_type(TYPE_BIGINT, nullable);
    case ::parquet::Type::FLOAT:
        return create_type(TYPE_FLOAT, nullable);
    case ::parquet::Type::DOUBLE:
        return create_type(TYPE_DOUBLE, nullable);
    default:
        return nullptr;
    }
}

} // namespace

struct ParquetColumnReaderState {
    int file_column_id = -1;
    int parquet_column_ordinal = -1;
    const ::parquet::ColumnDescriptor* descriptor = nullptr;
    DataTypePtr type;
    std::string name;
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
    size_t next_row_group_idx = 0;
    std::shared_ptr<::parquet::RowGroupReader> current_row_group;
    std::vector<ParquetColumnReaderState> current_columns;
    int64_t current_row_group_rows = 0;
    int64_t current_row_group_rows_read = 0;
};

namespace {

Status reset_reader_position(ParquetFileState* state) {
    if (state == nullptr) {
        return Status::Uninitialized("ParquetReader is not open");
    }
    state->next_row_group_idx = 0;
    state->current_row_group.reset();
    state->current_columns.clear();
    state->current_row_group_rows = 0;
    state->current_row_group_rows_read = 0;
    return Status::OK();
}

void reset_current_row_group(ParquetFileState* state) {
    state->current_row_group.reset();
    state->current_columns.clear();
    state->current_row_group_rows = 0;
    state->current_row_group_rows_read = 0;
}

template <PrimitiveType DorisType, typename ParquetValueType>
void insert_physical_value(IColumn& column, const ParquetValueType& value) {
    using DorisCppType = typename PrimitiveTypeTraits<DorisType>::CppType;
    DorisCppType doris_value = static_cast<DorisCppType>(value);
    column.insert_data(reinterpret_cast<const char*>(&doris_value), sizeof(DorisCppType));
}

template <typename ParquetReaderType, PrimitiveType DorisType>
Status read_flat_primitive_column(ParquetColumnReaderState& column_state, int64_t batch_rows,
                                  MutableColumnPtr* result_column, int64_t* rows_read) {
    auto* typed_reader = dynamic_cast<ParquetReaderType*>(column_state.reader.get());
    if (typed_reader == nullptr) {
        return Status::InternalError("Unexpected parquet column reader type for column {}",
                                     column_state.name);
    }

    using ParquetValueType = typename ParquetReaderType::T;
    const size_t batch_size = static_cast<size_t>(batch_rows);
    auto values = std::make_unique<ParquetValueType[]>(batch_size);
    std::vector<int16_t> def_levels;
    int16_t* def_levels_ptr = nullptr;
    if (column_state.descriptor->max_definition_level() > 0) {
        def_levels.resize(batch_size);
        def_levels_ptr = def_levels.data();
    }

    int64_t values_read = 0;
    int64_t levels_read = 0;
    try {
        levels_read = typed_reader->ReadBatch(batch_rows, def_levels_ptr, nullptr, values.get(),
                                              &values_read);
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to read parquet column {}: {}", column_state.name,
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to read parquet column {}: {}", column_state.name,
                                     e.what());
    }

    if (levels_read < 0 || values_read < 0 || levels_read > batch_rows || values_read > batch_rows) {
        return Status::Corruption("Invalid parquet read result for column {}", column_state.name);
    }

    auto column = column_state.type->create_column();
    if (column_state.descriptor->max_definition_level() == 0) {
        if (values_read != levels_read) {
            return Status::Corruption(
                    "Invalid required parquet column read result for column {}: levels={}, "
                    "values={}",
                    column_state.name, levels_read, values_read);
        }
        const size_t level_count = static_cast<size_t>(levels_read);
        for (size_t i = 0; i < level_count; ++i) {
            insert_physical_value<DorisType>(*column, values[i]);
        }
    } else {
        size_t value_idx = 0;
        const size_t value_count = static_cast<size_t>(values_read);
        const size_t level_count = static_cast<size_t>(levels_read);
        const int16_t max_definition_level = column_state.descriptor->max_definition_level();
        for (size_t i = 0; i < level_count; ++i) {
            if (def_levels[i] == max_definition_level) {
                if (value_idx >= value_count) {
                    return Status::Corruption(
                            "Parquet definition levels exceed values for column {}",
                            column_state.name);
                }
                insert_physical_value<DorisType>(*column, values[value_idx++]);
            } else {
                column->insert_data(nullptr, 0);
            }
        }
        if (value_idx != value_count) {
            return Status::Corruption(
                    "Parquet values exceed definition levels for column {}: consumed={}, "
                    "values={}",
                    column_state.name, value_idx, values_read);
        }
    }

    *rows_read = levels_read;
    *result_column = std::move(column);
    return Status::OK();
}

Status read_column_batch(ParquetColumnReaderState& column_state, int64_t batch_rows,
                         MutableColumnPtr* result_column, int64_t* rows_read) {
    switch (column_state.descriptor->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        return read_flat_primitive_column<::parquet::BoolReader, TYPE_BOOLEAN>(
                column_state, batch_rows, result_column, rows_read);
    case ::parquet::Type::INT32:
        return read_flat_primitive_column<::parquet::Int32Reader, TYPE_INT>(
                column_state, batch_rows, result_column, rows_read);
    case ::parquet::Type::INT64:
        return read_flat_primitive_column<::parquet::Int64Reader, TYPE_BIGINT>(
                column_state, batch_rows, result_column, rows_read);
    case ::parquet::Type::FLOAT:
        return read_flat_primitive_column<::parquet::FloatReader, TYPE_FLOAT>(
                column_state, batch_rows, result_column, rows_read);
    case ::parquet::Type::DOUBLE:
        return read_flat_primitive_column<::parquet::DoubleReader, TYPE_DOUBLE>(
                column_state, batch_rows, result_column, rows_read);
    default:
        return Status::NotSupported("Unsupported parquet physical type for stage 2 column {}",
                                    column_state.name);
    }
}

Status open_next_row_group(ParquetFileState* state, bool* has_row_group) {
    *has_row_group = false;
    while (state->next_row_group_idx < state->selected_row_groups.size()) {
        const int row_group_idx = state->selected_row_groups[state->next_row_group_idx++];
        try {
            state->current_row_group = state->parquet_reader->RowGroup(row_group_idx);
        } catch (const ::parquet::ParquetException& e) {
            return Status::Corruption("Failed to open parquet row group {}: {}", row_group_idx,
                                      e.what());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to open parquet row group {}: {}", row_group_idx,
                                         e.what());
        }

        auto row_group_metadata = state->metadata->RowGroup(row_group_idx);
        state->current_row_group_rows =
                row_group_metadata == nullptr ? 0 : row_group_metadata->num_rows();
        if (state->current_row_group_rows < 0) {
            return Status::Corruption("Invalid negative row count in parquet row group {}",
                                      row_group_idx);
        }
        state->current_row_group_rows_read = 0;
        state->current_columns.clear();
        state->current_columns.reserve(state->projected_columns.size());

        for (int file_column_id : state->projected_columns) {
            const auto* descriptor = state->schema->Column(file_column_id);
            DataTypePtr type = direct_flat_primitive_doris_type(descriptor);
            if (type == nullptr) {
                return Status::NotSupported(
                        "Stage 2 parquet reader only supports flat physical primitive columns; "
                        "column {} is not supported",
                        column_name(descriptor));
            }

            ParquetColumnReaderState column_state;
            column_state.file_column_id = file_column_id;
            column_state.parquet_column_ordinal = file_column_id;
            column_state.descriptor = descriptor;
            column_state.type = std::move(type);
            column_state.name = column_name(descriptor);
            column_state.reader = state->current_row_group->Column(file_column_id);
            if (column_state.reader == nullptr) {
                return Status::Corruption("Failed to create parquet column reader for column {}",
                                          column_state.name);
            }
            state->current_columns.push_back(std::move(column_state));
        }

        if (state->current_row_group_rows == 0) {
            reset_current_row_group(state);
            continue;
        }
        *has_row_group = true;
        return Status::OK();
    }
    return Status::OK();
}

Status read_current_row_group_batch(ParquetFileState* state, int64_t batch_rows,
                                    Block* file_block, size_t* rows) {
    file_block->clear();

    if (state->current_columns.empty()) {
        *rows = static_cast<size_t>(batch_rows);
        return Status::OK();
    }

    int64_t expected_rows = -1;
    for (auto& column_state : state->current_columns) {
        MutableColumnPtr column;
        int64_t column_rows = 0;
        RETURN_IF_ERROR(read_column_batch(column_state, batch_rows, &column, &column_rows));
        if (expected_rows < 0) {
            expected_rows = column_rows;
        } else if (column_rows != expected_rows) {
            return Status::Corruption(
                    "Parquet columns returned different row counts in the same batch: {} vs {}",
                    expected_rows, column_rows);
        }
        file_block->insert(ColumnWithTypeAndName {std::move(column), column_state.type,
                                                  column_state.name});
    }

    *rows = static_cast<size_t>(std::max<int64_t>(expected_rows, 0));
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
    if (rows != nullptr) {
        *rows = 0;
    }
    if (eof != nullptr) {
        *eof = false;
    }
    if (file_block == nullptr || rows == nullptr || eof == nullptr) {
        return Status::InvalidArgument("ParquetReader::next requires non-null output arguments");
    }
    if (_eof) {
        *eof = true;
        return Status::OK();
    }

    while (true) {
        if (_state == nullptr || _state->parquet_reader == nullptr || _state->schema == nullptr) {
            return Status::Uninitialized("ParquetReader is not open");
        }

        if (_state->current_row_group == nullptr) {
            bool has_row_group = false;
            RETURN_IF_ERROR(open_next_row_group(_state.get(), &has_row_group));
            if (!has_row_group) {
                _eof = true;
                *eof = true;
                return Status::OK();
            }
        }

        const int64_t remaining_rows =
                _state->current_row_group_rows - _state->current_row_group_rows_read;
        if (remaining_rows <= 0) {
            reset_current_row_group(_state.get());
            continue;
        }

        const int64_t batch_rows = std::min<int64_t>(DEFAULT_PARQUET_READ_BATCH_SIZE,
                                                     remaining_rows);
        RETURN_IF_ERROR(read_current_row_group_batch(_state.get(), batch_rows, file_block, rows));
        if (*rows == 0) {
            return Status::Corruption("Parquet row group returned zero rows before EOF");
        }
        _state->current_row_group_rows_read += static_cast<int64_t>(*rows);
        if (_state->current_row_group_rows_read >= _state->current_row_group_rows) {
            reset_current_row_group(_state.get());
        }
        *eof = false;
        return Status::OK();
    }
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
