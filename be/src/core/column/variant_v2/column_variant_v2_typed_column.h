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

#include <array>
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/call_on_type_index.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/value/large_int_value.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_scalar.h"
#include "exec/common/format_ip.h"

namespace doris {

bool is_supported_variant_typed_identity(PrimitiveType type);

template <typename DateValue>
int32_t variant_days_since_epoch(const DateValue& value, size_t row, std::string_view description) {
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
int64_t variant_timestamp_micros(const DateTimeValue& value, size_t row,
                                 std::string_view description) {
    constexpr int64_t MICROS_PER_SECOND = 1'000'000;
    constexpr int64_t SECONDS_PER_DAY = 86'400;
    const int64_t days = variant_days_since_epoch(value, row, description);
    const int64_t seconds =
            days * SECONDS_PER_DAY + value.hour() * 3600 + value.minute() * 60 + value.second();
    return seconds * MICROS_PER_SECOND + value.microsecond();
}

template <PrimitiveType Type, typename Column, typename Callback>
// NOLINTNEXTLINE(readability-function-size) -- centralized compile-time scalar mapping matrix.
void with_variant_typed_scalar(const Column& column, size_t row, uint8_t scale,
                               Callback&& callback) {
    // The callback must consume the scalar synchronously: string refs for LARGEINT/IP textual
    // adapters borrow stack storage owned by this function.
    if constexpr (Type == TYPE_BOOLEAN) {
        const bool value = column.get_data()[row] != 0;
        callback(VariantScalarRef::boolean(value));
    } else if constexpr (Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT ||
                         Type == TYPE_BIGINT) {
        const auto value = column.get_data()[row];
        callback(VariantScalarRef::integer(value));
    } else if constexpr (Type == TYPE_LARGEINT) {
        const __int128 value = column.get_data()[row];
        if (variant_unsigned_magnitude(value) <= VARIANT_DECIMAL16_MAX) {
            callback(VariantScalarRef::decimal(value, 0, 16));
        } else {
            std::array<char, 40> buffer {};
            const size_t size = static_cast<size_t>(LargeIntValue::to_buffer(value, buffer.data()));
            const StringRef text(buffer.data(), size);
            callback(VariantScalarRef::string(text));
        }
    } else if constexpr (Type == TYPE_FLOAT) {
        const float value = column.get_data()[row];
        callback(VariantScalarRef::float32(value));
    } else if constexpr (Type == TYPE_DOUBLE) {
        const double value = column.get_data()[row];
        callback(VariantScalarRef::float64(value));
    } else if constexpr (Type == TYPE_DECIMALV2) {
        const __int128 value = column.get_data()[row].value();
        callback(VariantScalarRef::decimal(value, scale, 16));
    } else if constexpr (Type == TYPE_DECIMAL32) {
        const int32_t value = column.get_data()[row].value;
        callback(VariantScalarRef::decimal(value, scale, 4));
    } else if constexpr (Type == TYPE_DECIMAL64) {
        const int64_t value = column.get_data()[row].value;
        callback(VariantScalarRef::decimal(value, scale, 8));
    } else if constexpr (Type == TYPE_DECIMAL128I) {
        const __int128 value = column.get_data()[row].value;
        callback(VariantScalarRef::decimal(value, scale, 16));
    } else if constexpr (Type == TYPE_DATE) {
        const int32_t value = variant_days_since_epoch(column.get_data()[row], row, "DATE");
        callback(VariantScalarRef::date(value));
    } else if constexpr (Type == TYPE_DATEV2) {
        const int32_t value = variant_days_since_epoch(column.get_data()[row], row, "DATEV2");
        callback(VariantScalarRef::date(value));
    } else if constexpr (Type == TYPE_DATETIME) {
        const int64_t value = variant_timestamp_micros(column.get_data()[row], row, "DATETIME");
        callback(VariantScalarRef::timestamp_micros(value, false));
    } else if constexpr (Type == TYPE_DATETIMEV2) {
        const int64_t value = variant_timestamp_micros(column.get_data()[row], row, "DATETIMEV2");
        callback(VariantScalarRef::timestamp_micros(value, false));
    } else if constexpr (Type == TYPE_TIMESTAMP_NS) {
        callback(VariantScalarRef::timestamp_nanos(column.get_data()[row].epoch_nanos(), false));
    } else if constexpr (Type == TYPE_TIMESTAMPTZ) {
        const int64_t value = variant_timestamp_micros(column.get_data()[row], row, "TIMESTAMPTZ");
        callback(VariantScalarRef::timestamp_micros(value, true));
    } else if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_STRING) {
        const StringRef value = column.get_data_at(row);
        callback(VariantScalarRef::string(value));
    } else if constexpr (Type == TYPE_IPV4) {
        std::array<char, IPV4_MAX_TEXT_LENGTH + 1> buffer {};
        char* end = buffer.data();
        const auto* address = reinterpret_cast<const unsigned char*>(&column.get_data()[row]);
        format_ipv4(address, end);
        const StringRef text(buffer.data(), end - buffer.data());
        callback(VariantScalarRef::string(text));
    } else if constexpr (Type == TYPE_IPV6) {
        std::array<char, IPV6_MAX_TEXT_LENGTH + 1> buffer {};
        IPv6 address = column.get_data()[row];
        char* end = buffer.data();
        format_ipv6(reinterpret_cast<unsigned char*>(&address), end);
        const StringRef text(buffer.data(), end - buffer.data());
        callback(VariantScalarRef::string(text));
    }
}

template <typename Callback>
void dispatch_variant_typed_column(const IColumn& nested, PrimitiveType type, Callback&& callback) {
    DORIS_CHECK(is_supported_variant_typed_identity(type))
            << "unsupported ColumnVariantV2 typed identity " << type;
    const bool dispatched = dispatch_switch_all(type, [&](auto type_tag) {
        using TypeTag = decltype(type_tag);
        callback.template operator()<TypeTag::PType>(
                assert_cast<const typename TypeTag::ColumnType&>(nested));
        return true;
    });
    DORIS_CHECK(dispatched) << "unsupported ColumnVariantV2 typed identity " << type;
}

} // namespace doris
