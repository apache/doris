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

#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "exec/common/format_ip.h"
#include "util/variant/variant_canonical.h"
#include "util/variant/variant_scalar_encoding.h"

namespace doris::column_variant_v2_internal {

// Records the approved physical fallback chosen while adapting one Doris typed scalar to Variant.
// The visitor below is synchronous: any borrowed string produced for a callback is valid only until
// that callback returns.
enum class TypedFallbackKind : uint8_t { NONE, LARGEINT, DECIMAL256, IP };

bool is_supported_typed_identity(PrimitiveType type);
bool exact_typed_identity(const DataTypePtr& left, const DataTypePtr& right);
void validate_typed_decimal_scale(const IColumn& nested, PrimitiveType type, uint32_t scale);

namespace detail {

constexpr unsigned __int128 max_decimal38() {
    unsigned __int128 value = 1;
    for (uint8_t digit = 0; digit < 38; ++digit) {
        value *= 10;
    }
    return value - 1;
}

constexpr unsigned __int128 MAX_DECIMAL38 = max_decimal38();
constexpr int64_t MICROS_PER_SECOND = 1'000'000;
constexpr int64_t SECONDS_PER_DAY = 86'400;

inline unsigned __int128 unsigned_magnitude(__int128 value) noexcept {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    return value < 0 ? ~unsigned_value + 1 : unsigned_value;
}

size_t format_int128(__int128 value, char* output) noexcept;
size_t format_decimal256(wide::Int256 value, uint32_t scale, char* output) noexcept;

template <typename DateValue>
int32_t days_since_epoch(const DateValue& value, size_t row, std::string_view description) {
    if (!value.is_valid_date()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot encode invalid {} value at row {} as Variant", description, row);
    }
    const int64_t days = value.daynr() - static_cast<int64_t>(calc_daynr(1970, 1, 1));
    DCHECK_GE(days, std::numeric_limits<int32_t>::min());
    DCHECK_LE(days, std::numeric_limits<int32_t>::max());
    return static_cast<int32_t>(days);
}

template <typename DateTimeValue>
int64_t timestamp_micros(const DateTimeValue& value, size_t row, std::string_view description) {
    const int64_t days = days_since_epoch(value, row, description);
    const int64_t seconds =
            days * SECONDS_PER_DAY + value.hour() * 3600 + value.minute() * 60 + value.second();
    return seconds * MICROS_PER_SECOND + value.microsecond();
}

} // namespace detail

template <PrimitiveType Type, typename Column, typename Callback>
// NOLINTNEXTLINE(readability-function-size) -- centralized compile-time scalar mapping matrix.
void with_typed_scalar(const Column& column, size_t row, uint8_t scale, Callback&& callback) {
    if constexpr (Type == TYPE_BOOLEAN) {
        const bool value = column.get_data()[row] != 0;
        callback([value] { return VariantScalarEncodingPlan::boolean(value); },
                 [value] { return VariantCanonicalScalarRef::boolean(value); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT ||
                         Type == TYPE_BIGINT) {
        const auto value = column.get_data()[row];
        callback([value] { return VariantScalarEncodingPlan::integer(value); },
                 [value] { return VariantCanonicalScalarRef::exact_integer(value); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_LARGEINT) {
        const __int128 value = column.get_data()[row];
        if (detail::unsigned_magnitude(value) <= detail::MAX_DECIMAL38) {
            callback([value] { return VariantScalarEncodingPlan::largeint(value); },
                     [value] { return VariantCanonicalScalarRef::exact_integer(value); },
                     TypedFallbackKind::NONE);
        } else {
            std::array<char, 40> buffer {};
            const size_t size = detail::format_int128(value, buffer.data());
            const StringRef text(buffer.data(), size);
            callback([text] { return VariantScalarEncodingPlan::string(text); },
                     [text] { return VariantCanonicalScalarRef::string(text); },
                     TypedFallbackKind::LARGEINT);
        }
    } else if constexpr (Type == TYPE_FLOAT) {
        const float value = column.get_data()[row];
        callback([value] { return VariantScalarEncodingPlan::float32(value); },
                 [value] { return VariantCanonicalScalarRef::float32(value); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DOUBLE) {
        const double value = column.get_data()[row];
        callback([value] { return VariantScalarEncodingPlan::float64(value); },
                 [value] { return VariantCanonicalScalarRef::float64(value); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DECIMALV2) {
        const __int128 value = column.get_data()[row].value();
        callback([value, scale] { return VariantScalarEncodingPlan::decimal(value, scale, 16); },
                 [value, scale] { return VariantCanonicalScalarRef::decimal(value, scale); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DECIMAL32) {
        const int32_t value = column.get_data()[row].value;
        callback([value, scale] { return VariantScalarEncodingPlan::decimal(value, scale, 4); },
                 [value, scale] { return VariantCanonicalScalarRef::decimal(value, scale); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DECIMAL64) {
        const int64_t value = column.get_data()[row].value;
        callback([value, scale] { return VariantScalarEncodingPlan::decimal(value, scale, 8); },
                 [value, scale] { return VariantCanonicalScalarRef::decimal(value, scale); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DECIMAL128I) {
        const __int128 value = column.get_data()[row].value;
        callback([value, scale] { return VariantScalarEncodingPlan::decimal(value, scale, 16); },
                 [value, scale] { return VariantCanonicalScalarRef::decimal(value, scale); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DECIMAL256) {
        std::array<char, Decimal256::max_string_length()> buffer {};
        const size_t size =
                detail::format_decimal256(column.get_data()[row].value, scale, buffer.data());
        const StringRef text(buffer.data(), size);
        callback([text] { return VariantScalarEncodingPlan::string(text); },
                 [text] { return VariantCanonicalScalarRef::string(text); },
                 TypedFallbackKind::DECIMAL256);
    } else if constexpr (Type == TYPE_DATE) {
        const int32_t value = detail::days_since_epoch(column.get_data()[row], row, "DATE");
        callback([value] { return VariantScalarEncodingPlan::date(value); },
                 [value] { return VariantCanonicalScalarRef::date(value); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DATEV2) {
        const int32_t value = detail::days_since_epoch(column.get_data()[row], row, "DATEV2");
        callback([value] { return VariantScalarEncodingPlan::date(value); },
                 [value] { return VariantCanonicalScalarRef::date(value); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DATETIME) {
        const int64_t value = detail::timestamp_micros(column.get_data()[row], row, "DATETIME");
        callback([value] { return VariantScalarEncodingPlan::timestamp_micros(value, false); },
                 [value] { return VariantCanonicalScalarRef::timestamp_micros(value, false); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_DATETIMEV2) {
        const int64_t value = detail::timestamp_micros(column.get_data()[row], row, "DATETIMEV2");
        callback([value] { return VariantScalarEncodingPlan::timestamp_micros(value, false); },
                 [value] { return VariantCanonicalScalarRef::timestamp_micros(value, false); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_TIMESTAMPTZ) {
        const int64_t value = detail::timestamp_micros(column.get_data()[row], row, "TIMESTAMPTZ");
        callback([value] { return VariantScalarEncodingPlan::timestamp_micros(value, true); },
                 [value] { return VariantCanonicalScalarRef::timestamp_micros(value, true); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_STRING) {
        const StringRef value = column.get_data_at(row);
        callback([value] { return VariantScalarEncodingPlan::string(value); },
                 [value] { return VariantCanonicalScalarRef::string(value); },
                 TypedFallbackKind::NONE);
    } else if constexpr (Type == TYPE_IPV4) {
        std::array<char, IPV4_MAX_TEXT_LENGTH + 1> buffer {};
        char* end = buffer.data();
        const auto* address = reinterpret_cast<const unsigned char*>(&column.get_data()[row]);
        format_ipv4(address, end);
        const StringRef text(buffer.data(), end - buffer.data());
        callback([text] { return VariantScalarEncodingPlan::string(text); },
                 [text] { return VariantCanonicalScalarRef::string(text); }, TypedFallbackKind::IP);
    } else if constexpr (Type == TYPE_IPV6) {
        std::array<char, IPV6_MAX_TEXT_LENGTH + 1> buffer {};
        IPv6 address = column.get_data()[row];
        char* end = buffer.data();
        format_ipv6(reinterpret_cast<unsigned char*>(&address), end);
        const StringRef text(buffer.data(), end - buffer.data());
        callback([text] { return VariantScalarEncodingPlan::string(text); },
                 [text] { return VariantCanonicalScalarRef::string(text); }, TypedFallbackKind::IP);
    }
}

template <PrimitiveType Type, typename Column, typename Callback>
void visit_typed_rows(const ColumnNullable& nullable, const Column& column, uint32_t scale,
                      size_t start, size_t end, Callback&& callback) {
    DCHECK_LE(start, end);
    DCHECK_LE(end, nullable.size());
    uint8_t variant_scale = 0;
    if constexpr (Type == TYPE_DECIMALV2 || Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                  Type == TYPE_DECIMAL128I || Type == TYPE_DECIMAL256) {
        DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()))
                << "typed decimal scale exceeds the Variant scale domain";
        variant_scale = static_cast<uint8_t>(scale);
    }
    const auto& null_map = nullable.get_null_map_data();
    for (size_t row = start; row < end; ++row) {
        if (null_map[row] != 0) {
            callback(
                    row, [] { return VariantScalarEncodingPlan::null_value(); },
                    [] { return VariantCanonicalScalarRef::null_value(); },
                    TypedFallbackKind::NONE);
            continue;
        }
        with_typed_scalar<Type>(
                column, row, variant_scale,
                [&](auto&& physical_factory, auto&& canonical_factory, TypedFallbackKind fallback) {
                    callback(row, std::forward<decltype(physical_factory)>(physical_factory),
                             std::forward<decltype(canonical_factory)>(canonical_factory),
                             fallback);
                });
    }
}

template <PrimitiveType Type, typename Column, typename Callback>
void visit_typed_canonical_rows(const ColumnNullable& nullable, const Column& column,
                                uint32_t scale, size_t start, size_t end, Callback&& callback) {
    visit_typed_rows<Type>(
            nullable, column, scale, start, end,
            [&](size_t row, auto&&, auto&& canonical_factory, TypedFallbackKind) {
                callback(row, std::forward<decltype(canonical_factory)>(canonical_factory));
            });
}

// Dispatches once per batch; row callbacks remain statically bound and allocate no per-row object.
template <typename Callback>
void dispatch_typed_column(const ColumnNullable& nullable, PrimitiveType type,
                           Callback&& callback) {
    const IColumn& nested = nullable.get_nested_column();
    switch (type) {
    case TYPE_BOOLEAN:
        callback.template operator()<TYPE_BOOLEAN>(assert_cast<const ColumnUInt8&>(nested));
        return;
    case TYPE_TINYINT:
        callback.template operator()<TYPE_TINYINT>(assert_cast<const ColumnInt8&>(nested));
        return;
    case TYPE_SMALLINT:
        callback.template operator()<TYPE_SMALLINT>(assert_cast<const ColumnInt16&>(nested));
        return;
    case TYPE_INT:
        callback.template operator()<TYPE_INT>(assert_cast<const ColumnInt32&>(nested));
        return;
    case TYPE_BIGINT:
        callback.template operator()<TYPE_BIGINT>(assert_cast<const ColumnInt64&>(nested));
        return;
    case TYPE_LARGEINT:
        callback.template operator()<TYPE_LARGEINT>(assert_cast<const ColumnInt128&>(nested));
        return;
    case TYPE_FLOAT:
        callback.template operator()<TYPE_FLOAT>(assert_cast<const ColumnFloat32&>(nested));
        return;
    case TYPE_DOUBLE:
        callback.template operator()<TYPE_DOUBLE>(assert_cast<const ColumnFloat64&>(nested));
        return;
    case TYPE_DECIMALV2:
        callback.template operator()<TYPE_DECIMALV2>(
                assert_cast<const ColumnDecimal128V2&>(nested));
        return;
    case TYPE_DECIMAL32:
        callback.template operator()<TYPE_DECIMAL32>(assert_cast<const ColumnDecimal32&>(nested));
        return;
    case TYPE_DECIMAL64:
        callback.template operator()<TYPE_DECIMAL64>(assert_cast<const ColumnDecimal64&>(nested));
        return;
    case TYPE_DECIMAL128I:
        callback.template operator()<TYPE_DECIMAL128I>(
                assert_cast<const ColumnDecimal128V3&>(nested));
        return;
    case TYPE_DECIMAL256:
        callback.template operator()<TYPE_DECIMAL256>(assert_cast<const ColumnDecimal256&>(nested));
        return;
    case TYPE_DATE:
        callback.template operator()<TYPE_DATE>(assert_cast<const ColumnDate&>(nested));
        return;
    case TYPE_DATEV2:
        callback.template operator()<TYPE_DATEV2>(assert_cast<const ColumnDateV2&>(nested));
        return;
    case TYPE_DATETIME:
        callback.template operator()<TYPE_DATETIME>(assert_cast<const ColumnDateTime&>(nested));
        return;
    case TYPE_DATETIMEV2:
        callback.template operator()<TYPE_DATETIMEV2>(assert_cast<const ColumnDateTimeV2&>(nested));
        return;
    case TYPE_TIMESTAMPTZ:
        callback.template operator()<TYPE_TIMESTAMPTZ>(
                assert_cast<const ColumnTimeStampTz&>(nested));
        return;
    case TYPE_CHAR:
        callback.template operator()<TYPE_CHAR>(assert_cast<const ColumnString&>(nested));
        return;
    case TYPE_VARCHAR:
        callback.template operator()<TYPE_VARCHAR>(assert_cast<const ColumnString&>(nested));
        return;
    case TYPE_STRING:
        callback.template operator()<TYPE_STRING>(assert_cast<const ColumnString&>(nested));
        return;
    case TYPE_IPV4:
        callback.template operator()<TYPE_IPV4>(assert_cast<const ColumnIPv4&>(nested));
        return;
    case TYPE_IPV6:
        callback.template operator()<TYPE_IPV6>(assert_cast<const ColumnIPv6&>(nested));
        return;
    default:
        DORIS_CHECK(false) << "unsupported ColumnVariantV2 typed identity " << type;
    }
}

} // namespace doris::column_variant_v2_internal
