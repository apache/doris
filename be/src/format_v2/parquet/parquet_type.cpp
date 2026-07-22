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

#include "format_v2/parquet/parquet_type.h"

#include <parquet/api/schema.h>

#include <memory>
#include <string>

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"

namespace doris::format::parquet {
namespace {

DataTypePtr create_type(PrimitiveType type, bool nullable, int precision = 0, int scale = 0) {
    return DataTypeFactory::instance().create_data_type(type, nullable, precision, scale);
}

PrimitiveType decimal_primitive_type(int precision) {
    return precision > 38 ? TYPE_DECIMAL256 : TYPE_DECIMAL128I;
}

void mark_decimal(const ::parquet::ColumnDescriptor* column, int precision, int scale,
                  ParquetTypeDescriptor* result) {
    result->is_decimal = true;
    result->decimal_precision = precision;
    result->decimal_scale = scale;
    switch (column->physical_type()) {
    case ::parquet::Type::INT32:
        result->extra_type_info = ParquetExtraTypeInfo::DECIMAL_INT32;
        break;
    case ::parquet::Type::INT64:
        result->extra_type_info = ParquetExtraTypeInfo::DECIMAL_INT64;
        break;
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        result->extra_type_info = ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY;
        break;
    default:
        result->extra_type_info = ParquetExtraTypeInfo::NONE;
        break;
    }
}

void mark_integer(int bit_width, bool is_signed, ParquetTypeDescriptor* result) {
    result->integer_bit_width = bit_width;
    result->is_unsigned_integer = !is_signed;
}

DataTypePtr converted_type_to_doris_type(const ::parquet::ColumnDescriptor* column,
                                         ParquetTypeDescriptor* result) {
    const bool nullable = column->max_definition_level() > 0;
    switch (column->converted_type()) {
    case ::parquet::ConvertedType::UTF8:
    case ::parquet::ConvertedType::ENUM:
    case ::parquet::ConvertedType::JSON:
    case ::parquet::ConvertedType::BSON:
        return create_type(TYPE_STRING, nullable);
    case ::parquet::ConvertedType::DECIMAL:
        mark_decimal(column, column->type_precision(), column->type_scale(), result);
        return create_type(decimal_primitive_type(column->type_precision()), nullable,
                           column->type_precision(), column->type_scale());
    case ::parquet::ConvertedType::DATE:
        return create_type(TYPE_DATEV2, nullable);
    case ::parquet::ConvertedType::TIME_MILLIS:
        result->unsupported_reason = "Parquet TIME with isAdjustedToUTC=true is not supported";
        return nullptr;
    case ::parquet::ConvertedType::TIME_MICROS:
        result->unsupported_reason = "Parquet TIME with isAdjustedToUTC=true is not supported";
        return nullptr;
    case ::parquet::ConvertedType::TIMESTAMP_MILLIS:
        result->is_timestamp = true;
        result->timestamp_is_adjusted_to_utc = true;
        result->time_unit = ParquetTimeUnit::MILLIS;
        result->extra_type_info = ParquetExtraTypeInfo::UNIT_MS;
        return create_type(TYPE_DATETIMEV2, nullable, 0, 3);
    case ::parquet::ConvertedType::TIMESTAMP_MICROS:
        result->is_timestamp = true;
        result->timestamp_is_adjusted_to_utc = true;
        result->time_unit = ParquetTimeUnit::MICROS;
        result->extra_type_info = ParquetExtraTypeInfo::UNIT_MICROS;
        return create_type(TYPE_DATETIMEV2, nullable, 0, 6);
    // Parquet stores signed and unsigned integer logical annotations on signed physical carriers:
    // INT_8/UINT_8/INT_16/UINT_16/INT_32/UINT_32 use physical INT32, and
    // INT_64/UINT_64 use physical INT64. Doris maps unsigned integers to the next wider
    // signed type so all values in the unsigned range can be represented.
    case ::parquet::ConvertedType::INT_8:
        mark_integer(8, true, result);
        return create_type(TYPE_TINYINT, nullable);
    case ::parquet::ConvertedType::UINT_8:
        mark_integer(8, false, result);
        return create_type(TYPE_SMALLINT, nullable);
    case ::parquet::ConvertedType::INT_16:
        mark_integer(16, true, result);
        return create_type(TYPE_SMALLINT, nullable);
    case ::parquet::ConvertedType::UINT_16:
        mark_integer(16, false, result);
        return create_type(TYPE_INT, nullable);
    case ::parquet::ConvertedType::INT_32:
        mark_integer(32, true, result);
        return create_type(TYPE_INT, nullable);
    case ::parquet::ConvertedType::UINT_32:
        mark_integer(32, false, result);
        return create_type(TYPE_BIGINT, nullable);
    case ::parquet::ConvertedType::INT_64:
        mark_integer(64, true, result);
        return create_type(TYPE_BIGINT, nullable);
    case ::parquet::ConvertedType::UINT_64:
        mark_integer(64, false, result);
        return create_type(TYPE_LARGEINT, nullable);
    case ::parquet::ConvertedType::NONE:
    default:
        return nullptr;
    }
}

DataTypePtr logical_type_to_doris_type(const ::parquet::ColumnDescriptor* column,
                                       ParquetTypeDescriptor* result) {
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
        mark_decimal(column, decimal_type.precision(), decimal_type.scale(), result);
        return create_type(decimal_primitive_type(decimal_type.precision()), nullable,
                           decimal_type.precision(), decimal_type.scale());
    }
    if (logical_type->is_date()) {
        return create_type(TYPE_DATEV2, nullable);
    }
    if (logical_type->is_time()) {
        const auto& time_type = static_cast<const ::parquet::TimeLogicalType&>(*logical_type);
        if (time_type.is_adjusted_to_utc()) {
            result->unsupported_reason = "Parquet TIME with isAdjustedToUTC=true is not supported";
            return nullptr;
        }
        int scale = 0;
        if (time_type.time_unit() == ::parquet::LogicalType::TimeUnit::MILLIS) {
            scale = 3;
            result->time_unit = ParquetTimeUnit::MILLIS;
            result->extra_type_info = ParquetExtraTypeInfo::UNIT_MS;
        } else if (time_type.time_unit() == ::parquet::LogicalType::TimeUnit::MICROS) {
            scale = 6;
            result->time_unit = ParquetTimeUnit::MICROS;
            result->extra_type_info = ParquetExtraTypeInfo::UNIT_MICROS;
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
            result->time_unit = ParquetTimeUnit::MILLIS;
            result->extra_type_info = ParquetExtraTypeInfo::UNIT_MS;
        } else if (timestamp_type.time_unit() == ::parquet::LogicalType::TimeUnit::MICROS) {
            scale = 6;
            result->time_unit = ParquetTimeUnit::MICROS;
            result->extra_type_info = ParquetExtraTypeInfo::UNIT_MICROS;
        } else if (timestamp_type.time_unit() == ::parquet::LogicalType::TimeUnit::NANOS) {
            scale = 6;
            result->time_unit = ParquetTimeUnit::NANOS;
            result->extra_type_info = ParquetExtraTypeInfo::UNIT_NS;
        } else {
            return nullptr;
        }
        result->is_timestamp = true;
        result->timestamp_is_adjusted_to_utc = timestamp_type.is_adjusted_to_utc();
        return create_type(TYPE_DATETIMEV2, nullable, 0, scale);
    }
    if (logical_type->is_int()) {
        const auto& int_type = static_cast<const ::parquet::IntLogicalType&>(*logical_type);
        mark_integer(int_type.bit_width(), int_type.is_signed(), result);
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
    if (logical_type->is_float16()) {
        if (column->physical_type() != ::parquet::Type::FIXED_LEN_BYTE_ARRAY ||
            column->type_length() != 2) {
            return nullptr;
        }
        result->extra_type_info = ParquetExtraTypeInfo::FLOAT16;
        return create_type(TYPE_FLOAT, nullable);
    }
    return nullptr;
}

DataTypePtr physical_type_to_doris_type(const ::parquet::ColumnDescriptor* column) {
    const bool nullable = column->max_definition_level() > 0;
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
        type = create_type(TYPE_DATETIMEV2, nullable, 0, 6);
        break;
    default:
        return nullptr;
    }
    return nullable ? make_nullable(type) : type;
}

bool record_reader_physical_type_supported(::parquet::Type::type physical_type) {
    switch (physical_type) {
    case ::parquet::Type::BOOLEAN:
    case ::parquet::Type::INT32:
    case ::parquet::Type::INT64:
    case ::parquet::Type::INT96:
    case ::parquet::Type::FLOAT:
    case ::parquet::Type::DOUBLE:
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return true;
    default:
        return false;
    }
}

} // namespace

std::string parquet_column_name(const ::parquet::ColumnDescriptor* column) {
    if (column == nullptr) {
        return {};
    }
    auto path = column->path();
    if (path) {
        return path->ToDotString();
    }
    return column->name();
}

ParquetTypeDescriptor resolve_parquet_type(const ::parquet::ColumnDescriptor* column) {
    ParquetTypeDescriptor result;
    if (column == nullptr) {
        return result;
    }

    result.physical_type = column->physical_type();
    result.converted_type = column->converted_type();
    result.fixed_length = column->type_length();
    result.physical_doris_type = physical_type_to_doris_type(column);

    if (auto logical_type = logical_type_to_doris_type(column, &result); logical_type != nullptr) {
        result.doris_type = logical_type;
    } else if (!result.unsupported_reason.empty()) {
        result.doris_type = nullptr;
        result.supports_record_reader = false;
    } else if (auto converted_type = converted_type_to_doris_type(column, &result);
               converted_type != nullptr) {
        result.doris_type = converted_type;
    } else if (!result.unsupported_reason.empty()) {
        result.doris_type = nullptr;
        result.supports_record_reader = false;
    } else {
        result.doris_type = result.physical_doris_type;
        if (result.physical_type == ::parquet::Type::INT96) {
            result.extra_type_info = ParquetExtraTypeInfo::IMPALA_TIMESTAMP;
        }
    }

    result.is_string_like = !result.is_decimal &&
                            result.extra_type_info != ParquetExtraTypeInfo::FLOAT16 &&
                            (result.physical_type == ::parquet::Type::BYTE_ARRAY ||
                             result.physical_type == ::parquet::Type::FIXED_LEN_BYTE_ARRAY);

    if (!record_reader_physical_type_supported(result.physical_type)) {
        result.supports_record_reader = false;
    }
    return result;
}

bool supports_record_reader(const ParquetTypeDescriptor& type_descriptor) {
    return type_descriptor.supports_record_reader;
}

DecodedValueKind decoded_value_kind(const ParquetTypeDescriptor& type_descriptor) {
    switch (type_descriptor.physical_type) {
    case ::parquet::Type::BOOLEAN:
        return DecodedValueKind::BOOL;
    case ::parquet::Type::INT32:
        if (type_descriptor.is_unsigned_integer && type_descriptor.integer_bit_width == 32) {
            return DecodedValueKind::UINT32;
        }
        return DecodedValueKind::INT32;
    case ::parquet::Type::INT64:
        if (type_descriptor.is_unsigned_integer && type_descriptor.integer_bit_width == 64) {
            return DecodedValueKind::UINT64;
        }
        return DecodedValueKind::INT64;
    case ::parquet::Type::INT96:
        return DecodedValueKind::INT96;
    case ::parquet::Type::FLOAT:
        return DecodedValueKind::FLOAT;
    case ::parquet::Type::DOUBLE:
        return DecodedValueKind::DOUBLE;
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return DecodedValueKind::FIXED_BINARY;
    case ::parquet::Type::BYTE_ARRAY:
    default:
        return DecodedValueKind::BINARY;
    }
}

} // namespace doris::format::parquet
