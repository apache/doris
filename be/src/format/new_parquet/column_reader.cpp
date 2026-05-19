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

#include "format/new_parquet/column_reader.h"

#include <parquet/api/reader.h>
#include <parquet/api/schema.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_decimal.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/value/vdatetime_value.h"
#include "format/new_parquet/parquet_column_schema.h"

namespace doris::parquet {
namespace {

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

bool is_decimal_column(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr) {
        return false;
    }
    const auto& logical_type = column->logical_type();
    return column->converted_type() == ::parquet::ConvertedType::DECIMAL ||
           (logical_type != nullptr && logical_type->is_valid() && logical_type->is_decimal());
}

bool is_timestamp_column(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr) {
        return false;
    }
    const auto& logical_type = column->logical_type();
    return column->converted_type() == ::parquet::ConvertedType::TIMESTAMP_MILLIS ||
           column->converted_type() == ::parquet::ConvertedType::TIMESTAMP_MICROS ||
           (logical_type != nullptr && logical_type->is_valid() && logical_type->is_timestamp());
}

bool is_string_like_column(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr || is_decimal_column(column)) {
        return false;
    }
    return column->physical_type() == ::parquet::Type::BYTE_ARRAY ||
           column->physical_type() == ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
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

template <PrimitiveType DorisType, typename ParquetValueType>
void insert_physical_value(IColumn& column, const ParquetValueType& value) {
    using DorisCppType = typename PrimitiveTypeTraits<DorisType>::CppType;
    DorisCppType doris_value = static_cast<DorisCppType>(value);
    column.insert_data(reinterpret_cast<const char*>(&doris_value), sizeof(DorisCppType));
}

class PrimitiveColumnReader final : public ParquetColumnReader {
public:
    PrimitiveColumnReader(int file_column_id, const ::parquet::ColumnDescriptor* descriptor,
                          DataTypePtr type, std::string name,
                          std::shared_ptr<::parquet::ColumnReader> arrow_reader)
            : _file_column_id(file_column_id),
              _parquet_column_ordinal(file_column_id),
              _descriptor(descriptor),
              _type(std::move(type)),
              _name(std::move(name)),
              _arrow_reader(std::move(arrow_reader)) {}

    int file_column_id() const override { return _file_column_id; }
    int parquet_column_ordinal() const override { return _parquet_column_ordinal; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read_batch(int64_t batch_rows, MutableColumnPtr* result_column,
                      int64_t* rows_read) override;

    const ::parquet::ColumnDescriptor* descriptor() const { return _descriptor; }
    const std::shared_ptr<::parquet::ColumnReader>& arrow_reader() const {
        return _arrow_reader;
    }

private:
    int _file_column_id = -1;
    int _parquet_column_ordinal = -1;
    const ::parquet::ColumnDescriptor* _descriptor = nullptr;
    DataTypePtr _type;
    std::string _name;
    std::shared_ptr<::parquet::ColumnReader> _arrow_reader;
};

class StructColumnReader final : public ParquetColumnReader {
public:
    StructColumnReader(const ParquetColumnSchema& schema,
                       std::vector<std::unique_ptr<ParquetColumnReader>> children)
            : _field_id(schema.field_id),
              _type(schema.type),
              _name(schema.name),
              _children(std::move(children)) {}

    int file_column_id() const override { return _field_id; }
    int parquet_column_ordinal() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read_batch(int64_t batch_rows, MutableColumnPtr* result_column,
                      int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    int _field_id = -1;
    DataTypePtr _type;
    std::string _name;
    std::vector<std::unique_ptr<ParquetColumnReader>> _children;
};

template <typename ParquetReaderType, typename InsertValue>
Status read_typed_column_values(ParquetColumnReader& column_reader, int64_t batch_rows,
                                MutableColumnPtr* result_column, int64_t* rows_read,
                                InsertValue&& insert_value) {
    auto& primitive_reader = static_cast<PrimitiveColumnReader&>(column_reader);
    auto* typed_reader = dynamic_cast<ParquetReaderType*>(primitive_reader.arrow_reader().get());
    if (typed_reader == nullptr) {
        return Status::InternalError("Unexpected parquet column reader type for column {}",
                                     column_reader.name());
    }

    using ParquetValueType = typename ParquetReaderType::T;
    const size_t batch_size = static_cast<size_t>(batch_rows);
    auto values = std::make_unique<ParquetValueType[]>(batch_size);
    std::vector<int16_t> def_levels;
    int16_t* def_levels_ptr = nullptr;
    if (primitive_reader.descriptor()->max_definition_level() > 0) {
        def_levels.resize(batch_size);
        def_levels_ptr = def_levels.data();
    }

    int64_t values_read = 0;
    int64_t levels_read = 0;
    try {
        levels_read = typed_reader->ReadBatch(batch_rows, def_levels_ptr, nullptr, values.get(),
                                              &values_read);
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to read parquet column {}: {}", column_reader.name(),
                                  e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to read parquet column {}: {}", column_reader.name(),
                                     e.what());
    }

    if (levels_read < 0 || values_read < 0 || levels_read > batch_rows || values_read > batch_rows) {
        return Status::Corruption("Invalid parquet read result for column {}", column_reader.name());
    }

    auto column = column_reader.type()->create_column();
    if (primitive_reader.descriptor()->max_definition_level() == 0) {
        if (values_read != levels_read) {
            return Status::Corruption(
                    "Invalid required parquet column read result for column {}: levels={}, "
                    "values={}",
                    column_reader.name(), levels_read, values_read);
        }
        const size_t level_count = static_cast<size_t>(levels_read);
        for (size_t i = 0; i < level_count; ++i) {
            RETURN_IF_ERROR(insert_value(*column, values[i]));
        }
    } else {
        size_t value_idx = 0;
        const size_t value_count = static_cast<size_t>(values_read);
        const size_t level_count = static_cast<size_t>(levels_read);
        const int16_t max_definition_level = primitive_reader.descriptor()->max_definition_level();
        for (size_t i = 0; i < level_count; ++i) {
            if (def_levels[i] == max_definition_level) {
                if (value_idx >= value_count) {
                    return Status::Corruption(
                            "Parquet definition levels exceed values for column {}",
                            column_reader.name());
                }
                RETURN_IF_ERROR(insert_value(*column, values[value_idx++]));
            } else {
                column->insert_data(nullptr, 0);
            }
        }
        if (value_idx != value_count) {
            return Status::Corruption(
                    "Parquet values exceed definition levels for column {}: consumed={}, "
                    "values={}",
                    column_reader.name(), value_idx, values_read);
        }
    }

    *rows_read = levels_read;
    *result_column = std::move(column);
    return Status::OK();
}

template <typename ParquetReaderType, PrimitiveType DorisType>
Status read_flat_primitive_column(ParquetColumnReader& column_reader, int64_t batch_rows,
                                  MutableColumnPtr* result_column, int64_t* rows_read) {
    return read_typed_column_values<ParquetReaderType>(
            column_reader, batch_rows, result_column, rows_read,
            [](IColumn& column, const typename ParquetReaderType::T& value) {
                insert_physical_value<DorisType>(column, value);
                return Status::OK();
            });
}

Status insert_byte_array_value(IColumn& column, const ::parquet::ByteArray& value) {
    column.insert_data(reinterpret_cast<const char*>(value.ptr), value.len);
    return Status::OK();
}

Status insert_fixed_len_byte_array_value(IColumn& column,
                                         const ::parquet::FixedLenByteArray& value,
                                         int type_length) {
    column.insert_data(reinterpret_cast<const char*>(value.ptr), static_cast<size_t>(type_length));
    return Status::OK();
}

template <typename NativeType>
NativeType decode_big_endian_signed_integer(const uint8_t* data, int length) {
    using UnsignedNativeType =
            std::conditional_t<std::is_same_v<NativeType, Int128>, unsigned __int128,
                               std::make_unsigned_t<NativeType>>;
    UnsignedNativeType value =
            data != nullptr && length > 0 && (data[0] & 0x80) != 0
                    ? static_cast<UnsignedNativeType>(-1)
                    : 0;
    for (int i = 0; i < length; ++i) {
        value = static_cast<UnsignedNativeType>((value << 8) | data[i]);
    }
    return static_cast<NativeType>(value);
}

template <PrimitiveType DecimalType, typename NativeType>
Status insert_decimal_value(IColumn& column, NativeType value) {
    using DecimalCppType = typename PrimitiveTypeTraits<DecimalType>::CppType;
    DecimalCppType decimal_value {value};
    column.insert_data(reinterpret_cast<const char*>(&decimal_value), sizeof(DecimalCppType));
    return Status::OK();
}

Status insert_decimal_from_byte_array(IColumn& column, const ::parquet::ByteArray& value) {
    if (value.len > sizeof(Int128)) {
        return Status::NotSupported("Decimal byte array longer than 16 bytes is not supported");
    }
    Int128 decimal_value = decode_big_endian_signed_integer<Int128>(value.ptr,
                                                                    static_cast<int>(value.len));
    return insert_decimal_value<TYPE_DECIMAL128I>(column, decimal_value);
}

Status insert_decimal_from_fixed_len_byte_array(IColumn& column,
                                                const ::parquet::FixedLenByteArray& value,
                                                int type_length) {
    if (type_length > static_cast<int>(sizeof(Int128))) {
        return Status::NotSupported(
                "Fixed length decimal longer than 16 bytes is not supported");
    }
    Int128 decimal_value = decode_big_endian_signed_integer<Int128>(value.ptr, type_length);
    return insert_decimal_value<TYPE_DECIMAL128I>(column, decimal_value);
}

Status insert_int32_decimal(IColumn& column, int32_t value) {
    return insert_decimal_value<TYPE_DECIMAL128I>(column, static_cast<Int128>(value));
}

Status insert_int64_decimal(IColumn& column, int64_t value) {
    return insert_decimal_value<TYPE_DECIMAL128I>(column, static_cast<Int128>(value));
}

int64_t timestamp_second_mask(const ::parquet::ColumnDescriptor* descriptor) {
    const auto& logical_type = descriptor->logical_type();
    if (logical_type != nullptr && logical_type->is_valid() && logical_type->is_timestamp()) {
        const auto& timestamp_type =
                static_cast<const ::parquet::TimestampLogicalType&>(*logical_type);
        if (timestamp_type.time_unit() == ::parquet::LogicalType::TimeUnit::MILLIS) {
            return 1000;
        }
        if (timestamp_type.time_unit() == ::parquet::LogicalType::TimeUnit::MICROS) {
            return 1000000;
        }
    }
    if (descriptor->converted_type() == ::parquet::ConvertedType::TIMESTAMP_MILLIS) {
        return 1000;
    }
    return 1000000;
}

Status insert_int64_timestamp(IColumn& column, int64_t value,
                              const ::parquet::ColumnDescriptor* descriptor) {
    static const cctz::time_zone utc_time_zone = cctz::utc_time_zone();
    const int64_t second_mask = timestamp_second_mask(descriptor);
    int64_t epoch_seconds = value / second_mask;
    int64_t sub_second = value % second_mask;
    if (sub_second < 0) {
        sub_second += second_mask;
        --epoch_seconds;
    }
    const int32_t microsecond =
            static_cast<int32_t>(sub_second * (1000000 / second_mask));
    DateV2Value<DateTimeV2ValueType> datetime_value;
    datetime_value.from_unixtime(epoch_seconds, utc_time_zone);
    datetime_value.set_microsecond(static_cast<uint64_t>(microsecond));
    const auto raw_value = datetime_value.to_date_int_val();
    column.insert_data(reinterpret_cast<const char*>(&raw_value), sizeof(raw_value));
    return Status::OK();
}

} // namespace

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

DataTypePtr supported_flat_column_type(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr || column->max_repetition_level() != 0 ||
        column->max_definition_level() > 1) {
        return nullptr;
    }
    if (auto type = direct_flat_primitive_doris_type(column); type != nullptr) {
        return type;
    }
    if (is_string_like_column(column)) {
        return create_type(TYPE_STRING, column->max_definition_level() > 0);
    }
    if (is_decimal_column(column) && column->type_precision() <= 38) {
        return create_type(TYPE_DECIMAL128I, column->max_definition_level() > 0,
                           column->type_precision(), column->type_scale());
    }
    if (is_timestamp_column(column) && column->physical_type() == ::parquet::Type::INT64) {
        if (auto type = logical_type_to_doris_type(column); type != nullptr) {
            return type;
        }
        return converted_type_to_doris_type(column);
    }
    return nullptr;
}

Status PrimitiveColumnReader::read_batch(int64_t batch_rows, MutableColumnPtr* result_column,
                                         int64_t* rows_read) {
    if (is_decimal_column(_descriptor)) {
        switch (_descriptor->physical_type()) {
        case ::parquet::Type::INT32:
            return read_typed_column_values<::parquet::Int32Reader>(
                    *this, batch_rows, result_column, rows_read,
                    [](IColumn& column, int32_t value) { return insert_int32_decimal(column, value); });
        case ::parquet::Type::INT64:
            return read_typed_column_values<::parquet::Int64Reader>(
                    *this, batch_rows, result_column, rows_read,
                    [](IColumn& column, int64_t value) { return insert_int64_decimal(column, value); });
        case ::parquet::Type::BYTE_ARRAY:
            return read_typed_column_values<::parquet::ByteArrayReader>(
                    *this, batch_rows, result_column, rows_read,
                    [](IColumn& column, const ::parquet::ByteArray& value) {
                        return insert_decimal_from_byte_array(column, value);
                    });
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return read_typed_column_values<::parquet::FixedLenByteArrayReader>(
                    *this, batch_rows, result_column, rows_read,
                    [this](IColumn& column, const ::parquet::FixedLenByteArray& value) {
                        return insert_decimal_from_fixed_len_byte_array(
                                column, value, _descriptor->type_length());
                    });
        default:
            return Status::NotSupported("Unsupported parquet decimal physical type for column {}",
                                        _name);
        }
    }
    if (is_timestamp_column(_descriptor) &&
        _descriptor->physical_type() == ::parquet::Type::INT64) {
        return read_typed_column_values<::parquet::Int64Reader>(
                *this, batch_rows, result_column, rows_read,
                [this](IColumn& column, int64_t value) {
                    return insert_int64_timestamp(column, value, _descriptor);
                });
    }
    if (is_string_like_column(_descriptor)) {
        if (_descriptor->physical_type() == ::parquet::Type::BYTE_ARRAY) {
            return read_typed_column_values<::parquet::ByteArrayReader>(
                    *this, batch_rows, result_column, rows_read,
                    [](IColumn& column, const ::parquet::ByteArray& value) {
                        return insert_byte_array_value(column, value);
                    });
        }
        return read_typed_column_values<::parquet::FixedLenByteArrayReader>(
                *this, batch_rows, result_column, rows_read,
                [this](IColumn& column, const ::parquet::FixedLenByteArray& value) {
                    return insert_fixed_len_byte_array_value(
                            column, value, _descriptor->type_length());
                });
    }

    switch (_descriptor->physical_type()) {
    case ::parquet::Type::BOOLEAN:
        return read_flat_primitive_column<::parquet::BoolReader, TYPE_BOOLEAN>(
                *this, batch_rows, result_column, rows_read);
    case ::parquet::Type::INT32:
        return read_flat_primitive_column<::parquet::Int32Reader, TYPE_INT>(
                *this, batch_rows, result_column, rows_read);
    case ::parquet::Type::INT64:
        return read_flat_primitive_column<::parquet::Int64Reader, TYPE_BIGINT>(
                *this, batch_rows, result_column, rows_read);
    case ::parquet::Type::FLOAT:
        return read_flat_primitive_column<::parquet::FloatReader, TYPE_FLOAT>(
                *this, batch_rows, result_column, rows_read);
    case ::parquet::Type::DOUBLE:
        return read_flat_primitive_column<::parquet::DoubleReader, TYPE_DOUBLE>(
                *this, batch_rows, result_column, rows_read);
    default:
        return Status::NotSupported("Unsupported parquet physical type for column {}", _name);
    }
}

Status StructColumnReader::read_batch(int64_t batch_rows, MutableColumnPtr* result_column,
                                      int64_t* rows_read) {
    if (_children.empty()) {
        auto column = _type->create_column();
        column->resize(static_cast<size_t>(batch_rows));
        *result_column = std::move(column);
        *rows_read = batch_rows;
        return Status::OK();
    }

    MutableColumns child_columns;
    child_columns.reserve(_children.size());
    int64_t expected_rows = -1;
    for (auto& child_reader : _children) {
        MutableColumnPtr child_column;
        int64_t child_rows = 0;
        RETURN_IF_ERROR(child_reader->read_batch(batch_rows, &child_column, &child_rows));
        if (expected_rows < 0) {
            expected_rows = child_rows;
        } else if (child_rows != expected_rows) {
            return Status::Corruption(
                    "Parquet struct children returned different row counts in column {}: {} vs {}",
                    _name, expected_rows, child_rows);
        }
        child_columns.push_back(std::move(child_column));
    }

    *rows_read = std::max<int64_t>(expected_rows, 0);
    *result_column = ColumnStruct::create(std::move(child_columns));
    return Status::OK();
}

Status StructColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    for (auto& child_reader : _children) {
        RETURN_IF_ERROR(child_reader->skip(rows));
    }
    return Status::OK();
}

Status ParquetColumnReader::skip(int64_t rows) {
    return Status::NotSupported("Parquet column skip is not implemented, rows={}", rows);
}

ParquetColumnReaderFactory::ParquetColumnReaderFactory(
        const std::vector<std::shared_ptr<::parquet::ColumnReader>>& arrow_readers)
        : _arrow_readers(arrow_readers) {}

Status ParquetColumnReaderFactory::create_primitive_reader(
        int file_column_id, const ::parquet::ColumnDescriptor* descriptor, DataTypePtr type,
        std::string name, std::shared_ptr<::parquet::ColumnReader> arrow_reader,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (descriptor == nullptr || type == nullptr || arrow_reader == nullptr) {
        return Status::InvalidArgument("Invalid parquet column reader arguments for column {}",
                                       name);
    }
    *reader = std::make_unique<PrimitiveColumnReader>(file_column_id, descriptor, std::move(type),
                                                      std::move(name), std::move(arrow_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_primitive(
        const ParquetColumnSchema& column_schema,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.leaf_column_id < 0 ||
        column_schema.leaf_column_id >= static_cast<int>(_arrow_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       column_schema.leaf_column_id, column_schema.name);
    }
    if (supported_flat_column_type(column_schema.descriptor) == nullptr) {
        return Status::NotSupported(
                "Current parquet reader only supports primitive columns without repetition; "
                "column {} is not supported",
                column_schema.name);
    }
    return create_primitive_reader(column_schema.leaf_column_id, column_schema.descriptor,
                                   column_schema.type, column_schema.name,
                                   _arrow_readers[column_schema.leaf_column_id], reader);
}

Status ParquetColumnReaderFactory::create_struct(
        const ParquetColumnSchema& column_schema,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
        return Status::NotSupported("Nullable parquet STRUCT reader is not implemented for column {}",
                                    column_schema.name);
    }
    std::vector<std::unique_ptr<ParquetColumnReader>> child_readers;
    child_readers.reserve(column_schema.children.size());
    for (const auto& child_schema : column_schema.children) {
        std::unique_ptr<ParquetColumnReader> child_reader;
        RETURN_IF_ERROR(create(*child_schema, &child_reader));
        child_readers.push_back(std::move(child_reader));
    }
    *reader = std::make_unique<StructColumnReader>(column_schema, std::move(child_readers));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create(const ParquetColumnSchema& column_schema,
                                          std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    switch (column_schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return create_primitive(column_schema, reader);
    case ParquetColumnSchemaKind::STRUCT:
        return create_struct(column_schema, reader);
    case ParquetColumnSchemaKind::LIST:
        return Status::NotSupported("Parquet LIST reader is not implemented for column {}",
                                    column_schema.name);
    case ParquetColumnSchemaKind::MAP:
        return Status::NotSupported("Parquet MAP reader is not implemented for column {}",
                                    column_schema.name);
    }
    return Status::NotSupported("Unsupported parquet column schema kind for column {}",
                                column_schema.name);
}

} // namespace doris::parquet
