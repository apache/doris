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

#include "vec/exec/format/table/parquet_utils.h"

#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <unordered_map>
#include <utility>

#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/unaligned.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/parquet/parquet_column_convert.h"

namespace doris::vectorized::parquet_utils {
namespace {

template <typename ColumnType, typename T>
void insert_numeric_impl(MutableColumnPtr& column, T value) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        auto& nested = nullable_column->get_nested_column();
        assert_cast<ColumnType&>(nested).insert_value(value);
        nullable_column->push_false_to_nullmap(1);
    } else {
        assert_cast<ColumnType&>(*column).insert_value(value);
    }
}

} // namespace

std::string join_path(const std::vector<std::string>& items) {
    return join(items, ".");
}

void insert_int32(MutableColumnPtr& column, Int32 value) {
    insert_numeric_impl<ColumnInt32>(column, value);
}

void insert_int64(MutableColumnPtr& column, Int64 value) {
    insert_numeric_impl<ColumnInt64>(column, value);
}

void insert_bool(MutableColumnPtr& column, bool value) {
    insert_numeric_impl<ColumnUInt8>(column, static_cast<UInt8>(value));
}

void insert_string(MutableColumnPtr& column, const std::string& value) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        nullable->get_null_map_data().push_back(0);
        auto& nested = nullable->get_nested_column();
        assert_cast<ColumnString&>(nested).insert_data(value.c_str(), value.size());
    } else {
        assert_cast<ColumnString&>(*column).insert_data(value.c_str(), value.size());
    }
}

void insert_null(MutableColumnPtr& column) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        nullable->get_null_map_data().push_back(1);
        nullable->get_nested_column().insert_default();
    } else {
        column->insert_default();
    }
}

std::string physical_type_to_string(tparquet::Type::type type) {
    switch (type) {
    case tparquet::Type::BOOLEAN:
        return "BOOLEAN";
    case tparquet::Type::INT32:
        return "INT32";
    case tparquet::Type::INT64:
        return "INT64";
    case tparquet::Type::INT96:
        return "INT96";
    case tparquet::Type::FLOAT:
        return "FLOAT";
    case tparquet::Type::DOUBLE:
        return "DOUBLE";
    case tparquet::Type::BYTE_ARRAY:
        return "BYTE_ARRAY";
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return "FIXED_LEN_BYTE_ARRAY";
    default:
        return "UNKNOWN";
    }
}

std::string compression_to_string(tparquet::CompressionCodec::type codec) {
    switch (codec) {
    case tparquet::CompressionCodec::UNCOMPRESSED:
        return "UNCOMPRESSED";
    case tparquet::CompressionCodec::SNAPPY:
        return "SNAPPY";
    case tparquet::CompressionCodec::GZIP:
        return "GZIP";
    case tparquet::CompressionCodec::LZO:
        return "LZO";
    case tparquet::CompressionCodec::BROTLI:
        return "BROTLI";
    case tparquet::CompressionCodec::LZ4:
        return "LZ4";
    case tparquet::CompressionCodec::ZSTD:
        return "ZSTD";
    case tparquet::CompressionCodec::LZ4_RAW:
        return "LZ4_RAW";
    default:
        return "UNKNOWN";
    }
}

std::string converted_type_to_string(tparquet::ConvertedType::type type) {
    switch (type) {
    case tparquet::ConvertedType::UTF8:
        return "UTF8";
    case tparquet::ConvertedType::MAP:
        return "MAP";
    case tparquet::ConvertedType::MAP_KEY_VALUE:
        return "MAP_KEY_VALUE";
    case tparquet::ConvertedType::LIST:
        return "LIST";
    case tparquet::ConvertedType::ENUM:
        return "ENUM";
    case tparquet::ConvertedType::DECIMAL:
        return "DECIMAL";
    case tparquet::ConvertedType::DATE:
        return "DATE";
    case tparquet::ConvertedType::TIME_MILLIS:
        return "TIME_MILLIS";
    case tparquet::ConvertedType::TIME_MICROS:
        return "TIME_MICROS";
    case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        return "TIMESTAMP_MILLIS";
    case tparquet::ConvertedType::TIMESTAMP_MICROS:
        return "TIMESTAMP_MICROS";
    case tparquet::ConvertedType::UINT_8:
        return "UINT_8";
    case tparquet::ConvertedType::UINT_16:
        return "UINT_16";
    case tparquet::ConvertedType::UINT_32:
        return "UINT_32";
    case tparquet::ConvertedType::UINT_64:
        return "UINT_64";
    case tparquet::ConvertedType::INT_8:
        return "INT_8";
    case tparquet::ConvertedType::INT_16:
        return "INT_16";
    case tparquet::ConvertedType::INT_32:
        return "INT_32";
    case tparquet::ConvertedType::INT_64:
        return "INT_64";
    case tparquet::ConvertedType::JSON:
        return "JSON";
    case tparquet::ConvertedType::BSON:
        return "BSON";
    case tparquet::ConvertedType::INTERVAL:
        return "INTERVAL";
    default:
        return "UNKNOWN";
    }
}

std::string logical_type_to_string(const tparquet::SchemaElement& element) {
    if (element.__isset.logicalType) {
        const auto& logical = element.logicalType;
        if (logical.__isset.STRING) {
            return "STRING";
        } else if (logical.__isset.MAP) {
            return "MAP";
        } else if (logical.__isset.LIST) {
            return "LIST";
        } else if (logical.__isset.ENUM) {
            return "ENUM";
        } else if (logical.__isset.DECIMAL) {
            return "DECIMAL";
        } else if (logical.__isset.DATE) {
            return "DATE";
        } else if (logical.__isset.TIME) {
            return "TIME";
        } else if (logical.__isset.TIMESTAMP) {
            return "TIMESTAMP";
        } else if (logical.__isset.INTEGER) {
            return "INTEGER";
        } else if (logical.__isset.UNKNOWN) {
            return "UNKNOWN";
        } else if (logical.__isset.JSON) {
            return "JSON";
        } else if (logical.__isset.BSON) {
            return "BSON";
        } else if (logical.__isset.UUID) {
            return "UUID";
        } else if (logical.__isset.FLOAT16) {
            return "FLOAT16";
        } else if (logical.__isset.VARIANT) {
            return "VARIANT";
        } else if (logical.__isset.GEOMETRY) {
            return "GEOMETRY";
        } else if (logical.__isset.GEOGRAPHY) {
            return "GEOGRAPHY";
        }
    }
    if (element.__isset.converted_type) {
        return converted_type_to_string(element.converted_type);
    }
    return "";
}

std::string encodings_to_string(const std::vector<tparquet::Encoding::type>& encodings) {
    std::vector<std::string> parts;
    parts.reserve(encodings.size());
    for (auto encoding : encodings) {
        switch (encoding) {
        case tparquet::Encoding::PLAIN:
            parts.emplace_back("PLAIN");
            break;
        case tparquet::Encoding::PLAIN_DICTIONARY:
            parts.emplace_back("PLAIN_DICTIONARY");
            break;
        case tparquet::Encoding::RLE:
            parts.emplace_back("RLE");
            break;
        case tparquet::Encoding::BIT_PACKED:
            parts.emplace_back("BIT_PACKED");
            break;
        case tparquet::Encoding::DELTA_BINARY_PACKED:
            parts.emplace_back("DELTA_BINARY_PACKED");
            break;
        case tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
            parts.emplace_back("DELTA_LENGTH_BYTE_ARRAY");
            break;
        case tparquet::Encoding::DELTA_BYTE_ARRAY:
            parts.emplace_back("DELTA_BYTE_ARRAY");
            break;
        case tparquet::Encoding::RLE_DICTIONARY:
            parts.emplace_back("RLE_DICTIONARY");
            break;
        default:
            parts.emplace_back("UNKNOWN");
            break;
        }
    }
    return fmt::format("{}", fmt::join(parts, ","));
}

bool try_get_statistics_encoded_value(const tparquet::Statistics& statistics, bool is_min,
                                      std::string* encoded_value) {
    if (is_min) {
        if (statistics.__isset.min_value) {
            *encoded_value = statistics.min_value;
            return true;
        }
        if (statistics.__isset.min) {
            *encoded_value = statistics.min;
            return true;
        }
    } else {
        if (statistics.__isset.max_value) {
            *encoded_value = statistics.max_value;
            return true;
        }
        if (statistics.__isset.max) {
            *encoded_value = statistics.max;
            return true;
        }
    }
    encoded_value->clear();
    return false;
}

std::string bytes_to_hex_string(const std::string& bytes) {
    static constexpr char kHexDigits[] = "0123456789ABCDEF";
    std::string hex;
    hex.resize(bytes.size() * 2);
    for (size_t i = 0; i < bytes.size(); ++i) {
        auto byte = static_cast<uint8_t>(bytes[i]);
        hex[i * 2] = kHexDigits[byte >> 4];
        hex[i * 2 + 1] = kHexDigits[byte & 0x0F];
    }
    return fmt::format("0x{}", hex);
}

std::string decode_statistics_value(const FieldSchema* schema_field,
                                    tparquet::Type::type physical_type,
                                    const std::string& encoded_value, const cctz::time_zone& ctz) {
    if (encoded_value.empty()) {
        return "";
    }
    if (schema_field == nullptr) {
        return bytes_to_hex_string(encoded_value);
    }

    auto logical_data_type = remove_nullable(schema_field->data_type);
    auto converter = parquet::PhysicalToLogicalConverter::get_converter(
            schema_field, logical_data_type, logical_data_type, &ctz);
    if (!converter || !converter->support()) {
        return bytes_to_hex_string(encoded_value);
    }

    ColumnPtr physical_column;
    switch (physical_type) {
    case tparquet::Type::type::BOOLEAN: {
        if (encoded_value.size() != sizeof(UInt8)) {
            return bytes_to_hex_string(encoded_value);
        }
        auto physical_col = ColumnUInt8::create();
        physical_col->insert_value(doris::unaligned_load<UInt8>(encoded_value.data()));
        physical_column = std::move(physical_col);
        break;
    }
    case tparquet::Type::type::INT32: {
        if (encoded_value.size() != sizeof(Int32)) {
            return bytes_to_hex_string(encoded_value);
        }
        auto physical_col = ColumnInt32::create();
        physical_col->insert_value(doris::unaligned_load<Int32>(encoded_value.data()));
        physical_column = std::move(physical_col);
        break;
    }
    case tparquet::Type::type::INT64: {
        if (encoded_value.size() != sizeof(Int64)) {
            return bytes_to_hex_string(encoded_value);
        }
        auto physical_col = ColumnInt64::create();
        physical_col->insert_value(doris::unaligned_load<Int64>(encoded_value.data()));
        physical_column = std::move(physical_col);
        break;
    }
    case tparquet::Type::type::FLOAT: {
        if (encoded_value.size() != sizeof(Float32)) {
            return bytes_to_hex_string(encoded_value);
        }
        auto physical_col = ColumnFloat32::create();
        physical_col->insert_value(doris::unaligned_load<Float32>(encoded_value.data()));
        physical_column = std::move(physical_col);
        break;
    }
    case tparquet::Type::type::DOUBLE: {
        if (encoded_value.size() != sizeof(Float64)) {
            return bytes_to_hex_string(encoded_value);
        }
        auto physical_col = ColumnFloat64::create();
        physical_col->insert_value(doris::unaligned_load<Float64>(encoded_value.data()));
        physical_column = std::move(physical_col);
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY: {
        auto physical_col = ColumnString::create();
        physical_col->insert_data(encoded_value.data(), encoded_value.size());
        physical_column = std::move(physical_col);
        break;
    }
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        int32_t type_length = schema_field->parquet_schema.__isset.type_length
                                      ? schema_field->parquet_schema.type_length
                                      : 0;
        if (type_length <= 0) {
            type_length = static_cast<int32_t>(encoded_value.size());
        }
        if (static_cast<size_t>(type_length) != encoded_value.size()) {
            return bytes_to_hex_string(encoded_value);
        }
        auto physical_col = ColumnUInt8::create();
        physical_col->resize(type_length);
        memcpy(physical_col->get_data().data(), encoded_value.data(), encoded_value.size());
        physical_column = std::move(physical_col);
        break;
    }
    case tparquet::Type::type::INT96: {
        constexpr size_t kInt96Size = 12;
        if (encoded_value.size() != kInt96Size) {
            return bytes_to_hex_string(encoded_value);
        }
        auto physical_col = ColumnInt8::create();
        physical_col->resize(kInt96Size);
        memcpy(physical_col->get_data().data(), encoded_value.data(), encoded_value.size());
        physical_column = std::move(physical_col);
        break;
    }
    default:
        return bytes_to_hex_string(encoded_value);
    }

    ColumnPtr logical_column;
    if (converter->is_consistent()) {
        logical_column = physical_column;
    } else {
        logical_column = logical_data_type->create_column();
        if (Status st = converter->physical_convert(physical_column, logical_column); !st.ok()) {
            return bytes_to_hex_string(encoded_value);
        }
    }

    if (logical_column->size() != 1) {
        return bytes_to_hex_string(encoded_value);
    }
    DataTypeSerDe::FormatOptions options;
    options.timezone = &ctz;
    return logical_data_type->to_string(*logical_column, 0, options);
}

void build_path_map(const FieldSchema& field, const std::string& prefix,
                    std::unordered_map<std::string, const FieldSchema*>* map) {
    std::string current = prefix.empty() ? field.name : fmt::format("{}.{}", prefix, field.name);
    if (field.children.empty()) {
        (*map)[current] = &field;
    } else {
        for (const auto& child : field.children) {
            build_path_map(child, current, map);
        }
    }
}

#define MERGE_STATS_CASE(ParquetType)                                                     \
    case ParquetType: {                                                                   \
        auto typed_left_stat = std::static_pointer_cast<                                  \
                ::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(left);  \
        auto typed_right_stat = std::static_pointer_cast<                                 \
                ::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(right); \
        typed_left_stat->Merge(*typed_right_stat);                                        \
        return;                                                                           \
    }

void merge_stats(const std::shared_ptr<::parquet::Statistics>& left,
                 const std::shared_ptr<::parquet::Statistics>& right) {
    if (left == nullptr || right == nullptr) {
        return;
    }
    DCHECK(left->physical_type() == right->physical_type());

    switch (left->physical_type()) {
        MERGE_STATS_CASE(::parquet::Type::BOOLEAN);
        MERGE_STATS_CASE(::parquet::Type::INT32);
        MERGE_STATS_CASE(::parquet::Type::INT64);
        MERGE_STATS_CASE(::parquet::Type::INT96);
        MERGE_STATS_CASE(::parquet::Type::FLOAT);
        MERGE_STATS_CASE(::parquet::Type::DOUBLE);
        MERGE_STATS_CASE(::parquet::Type::BYTE_ARRAY);
        MERGE_STATS_CASE(::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    default:
        LOG(WARNING) << "Unsupported parquet type for statistics merge: "
                     << static_cast<int>(left->physical_type());
        break;
    }
}

} // namespace doris::vectorized::parquet_utils
