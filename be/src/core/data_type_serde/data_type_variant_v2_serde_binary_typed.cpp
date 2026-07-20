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

#include <bit>
#include <climits>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

#include "core/binary_cast.hpp"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type_serde/data_type_variant_v2_serde_binary_internal.h"
#include "core/extended_types.h"

namespace doris::variant_v2_serde_binary_internal {
namespace {

using column_variant_v2_internal::dispatch_typed_column;

bool is_string_code(PhysicalTypeCode code) {
    return code == PhysicalTypeCode::CHAR || code == PhysicalTypeCode::VARCHAR ||
           code == PhysicalTypeCode::STRING;
}

size_t fixed_width(PhysicalTypeCode code) {
    switch (code) {
    case PhysicalTypeCode::BOOL:
    case PhysicalTypeCode::TINYINT:
        return 1;
    case PhysicalTypeCode::SMALLINT:
        return 2;
    case PhysicalTypeCode::INT:
    case PhysicalTypeCode::FLOAT:
    case PhysicalTypeCode::DECIMAL32:
    case PhysicalTypeCode::DATEV2:
    case PhysicalTypeCode::IPV4:
        return 4;
    case PhysicalTypeCode::BIGINT:
    case PhysicalTypeCode::DOUBLE:
    case PhysicalTypeCode::DECIMAL64:
    case PhysicalTypeCode::DATE:
    case PhysicalTypeCode::DATETIME:
    case PhysicalTypeCode::DATETIMEV2:
    case PhysicalTypeCode::TIMESTAMPTZ:
        return 8;
    case PhysicalTypeCode::LARGEINT:
    case PhysicalTypeCode::DECIMALV2:
    case PhysicalTypeCode::DECIMAL128I:
    case PhysicalTypeCode::IPV6:
        return 16;
    case PhysicalTypeCode::CHAR:
    case PhysicalTypeCode::VARCHAR:
    case PhysicalTypeCode::STRING:
        return 0;
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown Variant V2 exchange physical type code {}",
                    static_cast<uint16_t>(code));
}

uint32_t encode_length(const DataTypePtr& type) {
    const auto& string_type = assert_cast<const DataTypeString&>(*type);
    const int64_t length = string_type.len();
    if (type->get_primitive_type() == TYPE_STRING) {
        if (length != -1) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant V2 typed STRING must have length -1, found {}", length);
        }
        return 0;
    }
    if (length <= 0 || std::cmp_greater_equal(length, std::numeric_limits<uint32_t>::max())) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 typed CHAR/VARCHAR length {} is outside the wire domain",
                        length);
    }
    return static_cast<uint32_t>(length) + 1;
}

// NOLINTNEXTLINE(readability-function-size) -- exhaustive stable wire identity mapping.
TypeMeta encode_type_meta(const DataTypePtr& type) {
    TypeMeta result;
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        result.code = PhysicalTypeCode::BOOL;
        break;
    case TYPE_TINYINT:
        result.code = PhysicalTypeCode::TINYINT;
        break;
    case TYPE_SMALLINT:
        result.code = PhysicalTypeCode::SMALLINT;
        break;
    case TYPE_INT:
        result.code = PhysicalTypeCode::INT;
        break;
    case TYPE_BIGINT:
        result.code = PhysicalTypeCode::BIGINT;
        break;
    case TYPE_LARGEINT:
        result.code = PhysicalTypeCode::LARGEINT;
        break;
    case TYPE_FLOAT:
        result.code = PhysicalTypeCode::FLOAT;
        break;
    case TYPE_DOUBLE:
        result.code = PhysicalTypeCode::DOUBLE;
        break;
    case TYPE_DECIMALV2: {
        result.code = PhysicalTypeCode::DECIMALV2;
        const auto& decimal = assert_cast<const DataTypeDecimalV2&>(*type);
        result.precision = decimal.get_original_precision();
        result.scale = decimal.get_original_scale();
        break;
    }
    case TYPE_DECIMAL32:
        result.code = PhysicalTypeCode::DECIMAL32;
        result.precision = type->get_precision();
        result.scale = type->get_scale();
        break;
    case TYPE_DECIMAL64:
        result.code = PhysicalTypeCode::DECIMAL64;
        result.precision = type->get_precision();
        result.scale = type->get_scale();
        break;
    case TYPE_DECIMAL128I:
        result.code = PhysicalTypeCode::DECIMAL128I;
        result.precision = type->get_precision();
        result.scale = type->get_scale();
        break;
    case TYPE_DATE:
        result.code = PhysicalTypeCode::DATE;
        break;
    case TYPE_DATEV2:
        result.code = PhysicalTypeCode::DATEV2;
        break;
    case TYPE_DATETIME:
        result.code = PhysicalTypeCode::DATETIME;
        break;
    case TYPE_DATETIMEV2:
        result.code = PhysicalTypeCode::DATETIMEV2;
        result.scale = type->get_scale();
        break;
    case TYPE_TIMESTAMPTZ:
        result.code = PhysicalTypeCode::TIMESTAMPTZ;
        result.scale = type->get_scale();
        break;
    case TYPE_CHAR:
        result.code = PhysicalTypeCode::CHAR;
        result.length_code = encode_length(type);
        break;
    case TYPE_VARCHAR:
        result.code = PhysicalTypeCode::VARCHAR;
        result.length_code = encode_length(type);
        break;
    case TYPE_STRING:
        result.code = PhysicalTypeCode::STRING;
        result.length_code = encode_length(type);
        break;
    case TYPE_IPV4:
        result.code = PhysicalTypeCode::IPV4;
        break;
    case TYPE_IPV6:
        result.code = PhysicalTypeCode::IPV6;
        break;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Unsupported Variant V2 typed exchange identity {}", type->get_name());
    }
    return result;
}

void require_no_parameters(const TypeMeta& meta) {
    if (meta.precision != 0 || meta.scale != 0 || meta.length_code != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange type code {} has irrelevant parameters ({}, {}, {})",
                        static_cast<uint16_t>(meta.code), meta.precision, meta.scale,
                        meta.length_code);
    }
}

template <PrimitiveType Type>
DataTypePtr decode_decimal_type(const TypeMeta& meta) {
    if (meta.length_code != 0 || meta.precision == 0 ||
        meta.precision > max_decimal_precision<Type>() || meta.scale > meta.precision) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange decimal type code {} has invalid precision {}, scale "
                        "{}, or length {}",
                        static_cast<uint16_t>(meta.code), meta.precision, meta.scale,
                        meta.length_code);
    }
    if constexpr (Type == TYPE_DECIMALV2) {
        return std::make_shared<DataTypeDecimalV2>(DecimalV2Value::PRECISION, DecimalV2Value::SCALE,
                                                   meta.precision, meta.scale);
    } else {
        return std::make_shared<DataTypeDecimal<Type>>(meta.precision, meta.scale);
    }
}

DataTypePtr decode_string_type(const TypeMeta& meta, PrimitiveType primitive) {
    if (meta.precision != 0 || meta.scale != 0 || meta.length_code <= 1) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange CHAR/VARCHAR parameters are invalid");
    }
    const uint32_t length = meta.length_code - 1;
    if (length > INT_MAX) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange string length {} exceeds INT_MAX", length);
    }
    return std::make_shared<DataTypeString>(static_cast<int>(length), primitive);
}

// NOLINTNEXTLINE(readability-function-size) -- exhaustive stable wire identity mapping.
DataTypePtr decode_type(const TypeMeta& meta) {
    switch (meta.code) {
    case PhysicalTypeCode::BOOL:
        require_no_parameters(meta);
        return std::make_shared<DataTypeBool>();
    case PhysicalTypeCode::TINYINT:
        require_no_parameters(meta);
        return std::make_shared<DataTypeInt8>();
    case PhysicalTypeCode::SMALLINT:
        require_no_parameters(meta);
        return std::make_shared<DataTypeInt16>();
    case PhysicalTypeCode::INT:
        require_no_parameters(meta);
        return std::make_shared<DataTypeInt32>();
    case PhysicalTypeCode::BIGINT:
        require_no_parameters(meta);
        return std::make_shared<DataTypeInt64>();
    case PhysicalTypeCode::LARGEINT:
        require_no_parameters(meta);
        return std::make_shared<DataTypeInt128>();
    case PhysicalTypeCode::FLOAT:
        require_no_parameters(meta);
        return std::make_shared<DataTypeFloat32>();
    case PhysicalTypeCode::DOUBLE:
        require_no_parameters(meta);
        return std::make_shared<DataTypeFloat64>();
    case PhysicalTypeCode::DECIMALV2:
        return decode_decimal_type<TYPE_DECIMALV2>(meta);
    case PhysicalTypeCode::DECIMAL32:
        return decode_decimal_type<TYPE_DECIMAL32>(meta);
    case PhysicalTypeCode::DECIMAL64:
        return decode_decimal_type<TYPE_DECIMAL64>(meta);
    case PhysicalTypeCode::DECIMAL128I:
        return decode_decimal_type<TYPE_DECIMAL128I>(meta);
    case PhysicalTypeCode::DATE:
        require_no_parameters(meta);
        return std::make_shared<DataTypeDate>();
    case PhysicalTypeCode::DATEV2:
        require_no_parameters(meta);
        return std::make_shared<DataTypeDateV2>();
    case PhysicalTypeCode::DATETIME:
        require_no_parameters(meta);
        return std::make_shared<DataTypeDateTime>();
    case PhysicalTypeCode::DATETIMEV2:
        if (meta.precision != 0 || meta.scale > 6 || meta.length_code != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 exchange DATETIMEV2 parameters are invalid");
        }
        return std::make_shared<DataTypeDateTimeV2>(meta.scale);
    case PhysicalTypeCode::TIMESTAMPTZ:
        if (meta.precision != 0 || meta.scale > 6 || meta.length_code != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 exchange TIMESTAMPTZ parameters are invalid");
        }
        return std::make_shared<DataTypeTimeStampTz>(meta.scale);
    case PhysicalTypeCode::CHAR:
        return decode_string_type(meta, TYPE_CHAR);
    case PhysicalTypeCode::VARCHAR:
        return decode_string_type(meta, TYPE_VARCHAR);
    case PhysicalTypeCode::STRING:
        if (meta.precision != 0 || meta.scale != 0 || meta.length_code != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 exchange STRING parameters are invalid");
        }
        return std::make_shared<DataTypeString>();
    case PhysicalTypeCode::IPV4:
        require_no_parameters(meta);
        return std::make_shared<DataTypeIPv4>();
    case PhysicalTypeCode::IPV6:
        require_no_parameters(meta);
        return std::make_shared<DataTypeIPv6>();
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown Variant V2 exchange physical type code {}",
                    static_cast<uint16_t>(meta.code));
}

std::pair<TypeMeta, DataTypePtr> read_type_meta(std::span<const uint8_t> bytes) {
    if (bytes.size() != TYPE_META_BYTES) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange type metadata has {} bytes, expected {}", bytes.size(),
                        TYPE_META_BYTES);
    }
    Reader reader(bytes);
    const uint16_t raw_code = reader.u16("physical type code");
    if (raw_code < static_cast<uint16_t>(PhysicalTypeCode::BOOL) ||
        raw_code > static_cast<uint16_t>(PhysicalTypeCode::IPV6)) {
        throw Exception(ErrorCode::CORRUPTION, "Unknown Variant V2 exchange physical type code {}",
                        raw_code);
    }
    if (reader.u16("type metadata flags") != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange type metadata flags must be zero");
    }
    TypeMeta meta {.code = static_cast<PhysicalTypeCode>(raw_code),
                   .precision = reader.u32("type precision"),
                   .scale = reader.u32("type scale"),
                   .length_code = reader.u32("type length")};
    return {meta, decode_type(meta)};
}

template <typename T>
void write_integer(Writer& writer, T value) {
    using Unsigned = MakeUnsignedT<T>;
    auto bits = std::bit_cast<Unsigned>(value);
    for (size_t index = 0; index < sizeof(T); ++index) {
        writer.u8(static_cast<uint8_t>(static_cast<uint64_t>(bits)));
        if constexpr (sizeof(T) > 1) {
            bits >>= 8;
        }
    }
}

template <typename T>
T read_integer(Reader& reader, std::string_view description) {
    using Unsigned = MakeUnsignedT<T>;
    Unsigned bits = 0;
    for (size_t index = 0; index < sizeof(T); ++index) {
        bits |= static_cast<Unsigned>(reader.u8(description)) << (index * 8);
    }
    return std::bit_cast<T>(bits);
}

template <PrimitiveType Type, typename Column>
void write_fixed_value(const Column& column, size_t row, Writer& writer) {
    const auto& value = column.get_data()[row];
    if constexpr (Type == TYPE_FLOAT) {
        writer.u32(std::bit_cast<uint32_t>(value));
    } else if constexpr (Type == TYPE_DOUBLE) {
        writer.u64(std::bit_cast<uint64_t>(value));
    } else if constexpr (Type == TYPE_DECIMALV2) {
        write_integer(writer, value.value());
    } else if constexpr (Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                         Type == TYPE_DECIMAL128I) {
        write_integer(writer, value.value);
    } else if constexpr (Type == TYPE_DATE || Type == TYPE_DATETIME) {
        write_integer(writer, binary_cast<VecDateTimeValue, Int64>(value));
    } else if constexpr (Type == TYPE_DATEV2 || Type == TYPE_DATETIMEV2 ||
                         Type == TYPE_TIMESTAMPTZ) {
        write_integer(writer, value.to_date_int_val());
    } else {
        write_integer(writer, value);
    }
}

template <typename Column, typename Decoder>
void fill_fixed_column(IColumn& nested, size_t row_count, Reader& reader, Decoder&& decoder) {
    auto& column = assert_cast<Column&>(nested);
    auto& data = column.get_data();
    data.resize(row_count);
    for (size_t row = 0; row < row_count; ++row) {
        data[row] = decoder(reader);
    }
}

// NOLINTNEXTLINE(readability-function-size) -- exhaustive wire-to-column scalar mapping.
void decode_fixed_values(PhysicalTypeCode code, size_t row_count, std::span<const uint8_t> values,
                         IColumn& nested) {
    Reader reader(values);
    switch (code) {
    case PhysicalTypeCode::BOOL:
        fill_fixed_column<ColumnUInt8>(nested, row_count, reader, [](Reader& input) {
            return read_integer<UInt8>(input, "BOOL value");
        });
        return;
    case PhysicalTypeCode::TINYINT:
        fill_fixed_column<ColumnInt8>(nested, row_count, reader, [](Reader& input) {
            return read_integer<Int8>(input, "TINYINT value");
        });
        return;
    case PhysicalTypeCode::SMALLINT:
        fill_fixed_column<ColumnInt16>(nested, row_count, reader, [](Reader& input) {
            return read_integer<Int16>(input, "SMALLINT value");
        });
        return;
    case PhysicalTypeCode::INT:
        fill_fixed_column<ColumnInt32>(nested, row_count, reader, [](Reader& input) {
            return read_integer<Int32>(input, "INT value");
        });
        return;
    case PhysicalTypeCode::BIGINT:
        fill_fixed_column<ColumnInt64>(nested, row_count, reader, [](Reader& input) {
            return read_integer<Int64>(input, "BIGINT value");
        });
        return;
    case PhysicalTypeCode::LARGEINT:
        fill_fixed_column<ColumnInt128>(nested, row_count, reader, [](Reader& input) {
            return read_integer<Int128>(input, "LARGEINT value");
        });
        return;
    case PhysicalTypeCode::FLOAT:
        fill_fixed_column<ColumnFloat32>(nested, row_count, reader, [](Reader& input) {
            return std::bit_cast<Float32>(input.u32("FLOAT value"));
        });
        return;
    case PhysicalTypeCode::DOUBLE:
        fill_fixed_column<ColumnFloat64>(nested, row_count, reader, [](Reader& input) {
            return std::bit_cast<Float64>(input.u64("DOUBLE value"));
        });
        return;
    case PhysicalTypeCode::DECIMALV2:
        fill_fixed_column<ColumnDecimal128V2>(nested, row_count, reader, [](Reader& input) {
            return DecimalV2Value(read_integer<Int128>(input, "DECIMALV2 value"));
        });
        return;
    case PhysicalTypeCode::DECIMAL32:
        fill_fixed_column<ColumnDecimal32>(nested, row_count, reader, [](Reader& input) {
            return Decimal32(read_integer<Int32>(input, "DECIMAL32 value"));
        });
        return;
    case PhysicalTypeCode::DECIMAL64:
        fill_fixed_column<ColumnDecimal64>(nested, row_count, reader, [](Reader& input) {
            return Decimal64(read_integer<Int64>(input, "DECIMAL64 value"));
        });
        return;
    case PhysicalTypeCode::DECIMAL128I:
        fill_fixed_column<ColumnDecimal128V3>(nested, row_count, reader, [](Reader& input) {
            return Decimal128V3(read_integer<Int128>(input, "DECIMAL128I value"));
        });
        return;
    case PhysicalTypeCode::DATE:
        fill_fixed_column<ColumnDate>(nested, row_count, reader, [](Reader& input) {
            return binary_cast<Int64, VecDateTimeValue>(read_integer<Int64>(input, "DATE value"));
        });
        return;
    case PhysicalTypeCode::DATEV2:
        fill_fixed_column<ColumnDateV2>(nested, row_count, reader, [](Reader& input) {
            return DateV2Value<DateV2ValueType>(input.u32("DATEV2 value"));
        });
        return;
    case PhysicalTypeCode::DATETIME:
        fill_fixed_column<ColumnDateTime>(nested, row_count, reader, [](Reader& input) {
            return binary_cast<Int64, VecDateTimeValue>(
                    read_integer<Int64>(input, "DATETIME value"));
        });
        return;
    case PhysicalTypeCode::DATETIMEV2:
        fill_fixed_column<ColumnDateTimeV2>(nested, row_count, reader, [](Reader& input) {
            return DateV2Value<DateTimeV2ValueType>(input.u64("DATETIMEV2 value"));
        });
        return;
    case PhysicalTypeCode::TIMESTAMPTZ:
        fill_fixed_column<ColumnTimeStampTz>(nested, row_count, reader, [](Reader& input) {
            return TimestampTzValue(input.u64("TIMESTAMPTZ value"));
        });
        return;
    case PhysicalTypeCode::IPV4:
        fill_fixed_column<ColumnIPv4>(nested, row_count, reader,
                                      [](Reader& input) { return input.u32("IPv4 value"); });
        return;
    case PhysicalTypeCode::IPV6:
        fill_fixed_column<ColumnIPv6>(nested, row_count, reader, [](Reader& input) {
            return read_integer<IPv6>(input, "IPv6 value");
        });
        return;
    case PhysicalTypeCode::CHAR:
    case PhysicalTypeCode::VARCHAR:
    case PhysicalTypeCode::STRING:
        break;
    }
    throw Exception(ErrorCode::CORRUPTION, "Typed string entered fixed-width decode");
}

template <typename Integer>
uint32_t decimal_digits(Integer value) {
    using Unsigned = MakeUnsignedT<Integer>;
    auto remaining = static_cast<Unsigned>(value);
    if (value < 0) {
        remaining = ~remaining + Unsigned(1);
    }
    uint32_t digits = 0;
    do {
        remaining /= 10;
        ++digits;
    } while (remaining != 0);
    return digits;
}

void validate_decimal_values(const TypeMeta& meta, const DataTypePtr& type,
                             const ColumnNullable& nullable, int error_code) {
    switch (meta.code) {
    case PhysicalTypeCode::DECIMALV2:
    case PhysicalTypeCode::DECIMAL32:
    case PhysicalTypeCode::DECIMAL64:
    case PhysicalTypeCode::DECIMAL128I:
        break;
    default:
        return;
    }

    const auto& nullmap = nullable.get_null_map_data();
    dispatch_typed_column(
            nullable, type->get_primitive_type(), [&]<PrimitiveType Type>(const auto& column) {
                if constexpr (Type == TYPE_DECIMALV2 || Type == TYPE_DECIMAL32 ||
                              Type == TYPE_DECIMAL64 || Type == TYPE_DECIMAL128I) {
                    for (size_t row = 0; row < column.size(); ++row) {
                        if (nullmap[row] != 0) {
                            continue;
                        }
                        const auto value = [&]() {
                            if constexpr (Type == TYPE_DECIMALV2) {
                                return column.get_data()[row].value();
                            } else {
                                return column.get_data()[row].value;
                            }
                        }();
                        const uint32_t digits = decimal_digits(value);
                        if (digits > meta.precision) {
                            throw Exception(
                                    error_code,
                                    "Variant V2 typed decimal value at row {} has {} digits, "
                                    "exceeding declared precision {}",
                                    row, digits, meta.precision);
                        }
                    }
                }
            });
}

DorisVector<uint32_t> read_string_offsets(std::span<const uint8_t> bytes, size_t row_count,
                                          size_t values_size) {
    const size_t count = checked_add(row_count, 1, "typed string offset count");
    const size_t expected_bytes = checked_multiply(count, sizeof(uint32_t), "typed string offsets");
    if (bytes.size() != expected_bytes) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 typed string offsets have {} bytes, expected {}", bytes.size(),
                        expected_bytes);
    }
    Reader reader(bytes);
    DorisVector<uint32_t> offsets;
    offsets.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        offsets.push_back(reader.u32("typed string offset"));
    }
    if (offsets.front() != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 typed string offsets must start at zero");
    }
    for (size_t index = 1; index < offsets.size(); ++index) {
        if (offsets[index] < offsets[index - 1]) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 typed string offsets decrease at index {}", index);
        }
    }
    if (offsets.back() != values_size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 typed string end {} does not match value bytes {}",
                        offsets.back(), values_size);
    }
    return offsets;
}

} // namespace

TypedPlan plan_typed(const ResolvedColumn& source) {
    const auto& nullable = assert_cast<const ColumnNullable&>(source.view.typed_column());
    TypedPlan plan;
    plan.type_meta = encode_type_meta(source.view.typed_type());
    validate_decimal_values(plan.type_meta, source.view.typed_type(), nullable,
                            ErrorCode::INVALID_ARGUMENT);
    plan.nullmap.reserve(source.logical_rows);
    const auto& source_nullmap = nullable.get_null_map_data();
    for (size_t row = 0; row < source.logical_rows; ++row) {
        const uint8_t is_null = source_nullmap[source.physical_row(row)];
        if (is_null > 1) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant V2 typed null map contains {} at row {}", is_null, row);
        }
        plan.nullmap.push_back(is_null);
    }
    plan.nullmap_bytes = checked_u32(plan.nullmap.size(), "typed null map");

    if (is_string_code(plan.type_meta.code)) {
        const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
        plan.value_offsets.reserve(checked_add(source.logical_rows, 1, "string offset count"));
        plan.string_values.reserve(source.logical_rows);
        plan.value_offsets.push_back(0);
        size_t value_bytes = 0;
        for (size_t row = 0; row < source.logical_rows; ++row) {
            const StringRef value = strings.get_data_at(source.physical_row(row));
            plan.string_values.push_back(value);
            value_bytes = checked_add(value_bytes, value.size, "typed string bytes");
            plan.value_offsets.push_back(checked_u32(value_bytes, "typed string bytes"));
        }
        plan.value_offsets_bytes =
                checked_u32(checked_multiply(plan.value_offsets.size(), sizeof(uint32_t),
                                             "typed string offsets"),
                            "typed string offsets");
        plan.values_bytes = checked_u32(value_bytes, "typed string bytes");
    } else {
        const size_t value_bytes = checked_multiply(
                source.logical_rows, fixed_width(plan.type_meta.code), "typed fixed values");
        plan.values_bytes = checked_u32(value_bytes, "typed fixed values");
    }

    size_t frame_bytes = HEADER_BYTES + TYPED_DESCRIPTOR_BYTES;
    frame_bytes = checked_add(frame_bytes, TYPE_META_BYTES, "typed frame");
    frame_bytes = checked_add(frame_bytes, plan.nullmap_bytes, "typed frame");
    frame_bytes = checked_add(frame_bytes, plan.value_offsets_bytes, "typed frame");
    plan.frame_bytes = checked_add(frame_bytes, plan.values_bytes, "typed frame");
    return plan;
}

void write_typed(const ResolvedColumn& source, const TypedPlan& plan, Writer& writer) {
    writer.u32(TYPE_META_BYTES);
    writer.u32(0);
    writer.u32(plan.nullmap_bytes);
    writer.u32(plan.value_offsets_bytes);
    writer.u32(plan.values_bytes);
    writer.u16(static_cast<uint16_t>(plan.type_meta.code));
    writer.u16(0);
    writer.u32(plan.type_meta.precision);
    writer.u32(plan.type_meta.scale);
    writer.u32(plan.type_meta.length_code);
    writer.raw(plan.nullmap);
    for (const uint32_t offset : plan.value_offsets) {
        writer.u32(offset);
    }
    if (is_string_code(plan.type_meta.code)) {
        for (const StringRef value : plan.string_values) {
            writer.raw(value);
        }
        return;
    }

    const auto& nullable = assert_cast<const ColumnNullable&>(source.view.typed_column());
    dispatch_typed_column(
            nullable, source.view.typed_type()->get_primitive_type(),
            [&]<PrimitiveType Type>(const auto& column) {
                if constexpr (Type != TYPE_CHAR && Type != TYPE_VARCHAR && Type != TYPE_STRING) {
                    for (size_t row = 0; row < source.logical_rows; ++row) {
                        write_fixed_value<Type>(column, source.physical_row(row), writer);
                    }
                }
            });
}

ColumnVariantV2::MutablePtr decode_typed(uint64_t row_count_u64,
                                         std::span<const uint8_t> type_meta_bytes,
                                         std::span<const uint8_t> extension,
                                         std::span<const uint8_t> nullmap,
                                         std::span<const uint8_t> value_offsets,
                                         std::span<const uint8_t> values) {
    if (!extension.empty()) {
        throw Exception(ErrorCode::CORRUPTION, "Variant V2 typed extension size must be zero");
    }
    const size_t row_count = checked_size(row_count_u64, "typed row count");
    auto [meta, type] = read_type_meta(type_meta_bytes);
    if (nullmap.size() != row_count) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 typed null map has {} bytes, expected {}", nullmap.size(),
                        row_count);
    }
    for (size_t row = 0; row < row_count; ++row) {
        if (nullmap[row] > 1) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 typed null map contains {} at row {}", nullmap[row], row);
        }
    }

    MutableColumnPtr nested = type->create_column();
    if (is_string_code(meta.code)) {
        const DorisVector<uint32_t> offsets =
                read_string_offsets(value_offsets, row_count, values.size());
        auto& strings = assert_cast<ColumnString&>(*nested);
        ColumnString::check_chars_length(values.size(), row_count);
        auto& chars = strings.get_chars();
        auto& destination_offsets = strings.get_offsets();
        chars.resize(values.size());
        if (!values.empty()) {
            std::memcpy(chars.data(), values.data(), values.size());
        }
        destination_offsets.resize(row_count);
        for (size_t row = 0; row < row_count; ++row) {
            destination_offsets[row] = offsets[row + 1];
        }
    } else {
        if (!value_offsets.empty()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 fixed-width typed values must not have offsets");
        }
        const size_t expected_values =
                checked_multiply(row_count, fixed_width(meta.code), "typed fixed values");
        if (values.size() != expected_values) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 fixed values have {} bytes, expected {}", values.size(),
                            expected_values);
        }
        decode_fixed_values(meta.code, row_count, values, *nested);
    }

    auto null_column = ColumnUInt8::create();
    null_column->get_data().insert(nullmap.begin(), nullmap.end());
    auto nullable = ColumnNullable::create(std::move(nested), std::move(null_column));
    validate_decimal_values(meta, type, *nullable, ErrorCode::CORRUPTION);
    return ColumnVariantV2::create_typed(std::move(nullable), std::move(type));
}

} // namespace doris::variant_v2_serde_binary_internal
