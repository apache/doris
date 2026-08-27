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

#include <fmt/format.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "common/status.h"
#include "core/binary_cast.hpp"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/types.h"
#include "core/value/time_value.h"
#include "core/value/timestamptz_value.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "runtime/runtime_state.h"

namespace doris {

struct CastToTimestampNs {
private:
    struct FloatDecimalParts {
        int64_t integer;
        int64_t fraction;
        uint32_t scale;
    };

    template <typename T>
        requires std::is_floating_point_v<T>
    static FloatDecimalParts split_float_to_decimal(T value) {
        // fmt 7.x keeps Dragonbox in detail, so isolate the dependency in this helper. It is the
        // same shortest-decimal conversion used by fmt's default floating-point formatter.
        const auto decimal = fmt::detail::dragonbox::to_decimal(value);
        const auto significand = decimal.significand;
        const auto decimal_exponent = decimal.exponent;
        if (decimal_exponent >= 0) {
            const auto integer =
                    significand * common::exp10_i64(static_cast<uint32_t>(decimal_exponent));
            return {static_cast<int64_t>(integer), 0, 0};
        }

        const auto scale = static_cast<uint32_t>(-decimal_exponent);
        if (scale >= 19) {
            return {0, 0, 0};
        }
        const auto scale_multiplier = common::exp10_i64(scale);
        return {static_cast<int64_t>(significand / scale_multiplier),
                static_cast<int64_t>(significand % scale_multiplier), scale};
    }

public:
    // Floating-point input has no fixed decimal scale. Use its shortest round-trippable decimal
    // representation so digits 7-9 are not polluted by binary floating-point subtraction and the
    // result preserves the existing string-formatting cast semantics.
    template <typename T>
        requires std::is_floating_point_v<T>
    static bool from_float(T float_value, TimeStampNsValue& result, CastParameters& params) {
        if (params.is_strict) {
            return from_float<DatelikeParseMode::STRICT>(float_value, result, params);
        }
        return from_float<DatelikeParseMode::NON_STRICT>(float_value, result, params);
    }

    template <DatelikeParseMode ParseMode, typename T>
        requires std::is_floating_point_v<T>
    static bool from_float(T float_value, TimeStampNsValue& result, CastParameters& params) {
        constexpr bool IsStrict = is_datelike_parse_strict(ParseMode);
        DCHECK(IsStrict == params.is_strict);
        SET_PARAMS_RET_FALSE_IFN(
                float_value > 0 && std::isfinite(float_value) &&
                        float_value < static_cast<double>(std::numeric_limits<int64_t>::max()),
                "invalid float value for timestamp_ns: {}", float_value);

        const auto [integer, fraction, scale] = split_float_to_decimal(float_value);
        return from_decimal<ParseMode>(integer, fraction, scale, result, params);
    }

    template <typename T>
    static bool from_decimal(const T& int_part, const T& frac_part, int64_t decimal_scale,
                             TimeStampNsValue& result, CastParameters& params) {
        if (params.is_strict) {
            return from_decimal<DatelikeParseMode::STRICT>(int_part, frac_part, decimal_scale,
                                                           result, params);
        }
        return from_decimal<DatelikeParseMode::NON_STRICT>(int_part, frac_part, decimal_scale,
                                                           result, params);
    }

    template <DatelikeParseMode ParseMode, typename T>
    static bool from_decimal(const T& int_part, const T& frac_part, int64_t decimal_scale,
                             TimeStampNsValue& result, CastParameters& params) {
        constexpr bool IsStrict = is_datelike_parse_strict(ParseMode);
        constexpr int64_t target_scale = TimeStampNsValue::FRACTIONAL_DIGITS;
        DCHECK(IsStrict == params.is_strict);
        SET_PARAMS_RET_FALSE_IFN(int_part <= std::numeric_limits<int64_t>::max() && int_part >= 1,
                                 "invalid decimal value for timestamp_ns: {}.{}", int_part,
                                 frac_part);

        DateV2Value<DateTimeV2ValueType> datetime;
        if (!CastToDatetimeV2::from_integer<ParseMode>(int_part, datetime, params)) {
            return false;
        }

        T scaled_fraction = frac_part;
        if (decimal_scale > target_scale) {
            const auto divisor = decimal_scale_multiplier<T>(
                    static_cast<uint32_t>(decimal_scale - target_scale));
            const auto remainder = scaled_fraction % divisor;
            scaled_fraction /= divisor;
            if (remainder >= divisor / 2) {
                ++scaled_fraction;
            }
        } else if (decimal_scale < target_scale) {
            scaled_fraction *= decimal_scale_multiplier<T>(
                    static_cast<uint32_t>(target_scale - decimal_scale));
        }

        const auto fraction_limit =
                decimal_scale_multiplier<T>(static_cast<uint32_t>(target_scale));
        if (scaled_fraction == fraction_limit) {
            SET_PARAMS_RET_FALSE_IFN(datetime.template date_add_interval<TimeUnit::SECOND>(
                                             TimeInterval {TimeUnit::SECOND, 1, false}),
                                     "timestamp_ns overflow when rounding up to next second");
            scaled_fraction = 0;
        }

        const auto nanoseconds = static_cast<uint32_t>(scaled_fraction);
        datetime.set_microsecond(nanoseconds / TimeStampNsValue::NANOS_PER_MICROSECOND);
        SET_PARAMS_RET_FALSE_IFN(
                result.from_datetime(datetime,
                                     nanoseconds % TimeStampNsValue::NANOS_PER_MICROSECOND),
                "timestamp_ns value is outside the signed epoch-nanosecond range");
        return true;
    }

    template <DatelikeParseMode ParseMode>
    static inline bool from_string_strict_mode(const StringRef& str, TimeStampNsValue& res,
                                               const cctz::time_zone* local_time_zone,
                                               uint32_t to_scale, CastParameters& params) {
        return CastToDatetimeV2::from_string_strict_mode<ParseMode>(str, res, local_time_zone,
                                                                    to_scale, params);
    }

    static inline bool from_string_non_strict_mode(const StringRef& str, TimeStampNsValue& res,
                                                   const cctz::time_zone* local_time_zone,
                                                   uint32_t to_scale, CastParameters& params) {
        return CastToDatetimeV2::from_string_non_strict_mode(str, res, local_time_zone, to_scale,
                                                             params);
    }
};

template <CastModeType CastMode, typename FromDataType>
    requires IsStringType<FromDataType>
class CastToImpl<CastMode, FromDataType, DataTypeTimeStampNs> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t /*input_rows_count*/,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = assert_cast<const DataTypeString::ColumnType*>(
                block.get_by_position(arguments[0]).column.get());

        auto nested_to_type = remove_nullable(block.get_by_position(result).type);
        auto serde = nested_to_type->get_serde();

        DataTypeSerDe::FormatOptions options;
        options.timezone = &context->state()->timezone_obj();

        if constexpr (CastMode == CastModeType::StrictMode) {
            MutableColumnPtr column_to = nested_to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(*col_from, *column_to, options, null_map));
            block.get_by_position(result).column = std::move(column_to);
        } else {
            auto nullable_col_to = create_empty_nullable_column(nested_to_type);
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, *nullable_col_to, options));
            block.get_by_position(result).column = std::move(nullable_col_to);
        }

        return Status::OK();
    }
};

template <CastModeType CastMode, typename FromDataType>
    requires CastUtil::IsPureDigitType<FromDataType>
class CastToImpl<CastMode, FromDataType, DataTypeTimeStampNs> : public CastToBase {
public:
    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto& col_from = assert_cast<const typename FromDataType::ColumnType&>(
                *block.get_by_position(arguments[0]).column);
        auto col_to = ColumnTimeStampNs::create(input_rows_count);
        auto col_nullmap = ColumnUInt8::create(input_rows_count, 0);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map && null_map[i]) {
                continue;
            }

            Status status = Status::OK();
            TimeStampNsValue value;
            CastParameters params {.status = Status::OK(),
                                   .is_strict = CastMode == CastModeType::StrictMode};
            bool parsed = false;
            if constexpr (IsDataTypeDecimal<FromDataType>) {
                parsed = CastToTimestampNs::from_decimal(col_from.get_intergral_part(i),
                                                         col_from.get_fractional_part(i),
                                                         col_from.get_scale(), value, params);
            } else if constexpr (IsDataTypeInt<FromDataType>) {
                DateV2Value<DateTimeV2ValueType> datetime;
                parsed =
                        CastToDatetimeV2::from_integer(col_from.get_element(i), datetime, params) &&
                        value.from_datetime(datetime);
                if (!parsed && params.status.ok()) {
                    status = Status::InvalidArgument(
                            "timestamp_ns value is outside the signed epoch-nanosecond range");
                }
            } else {
                static_assert(IsDataTypeFloat<FromDataType>);
                parsed = CastToTimestampNs::from_float(col_from.get_element(i), value, params);
            }
            if (!parsed && status.ok()) {
                status = params.status.ok()
                                 ? Status::InvalidArgument("Invalid numeric datetime value")
                                 : params.status;
            }

            if (!status.ok()) {
                if constexpr (CastMode == CastModeType::StrictMode) {
                    status.prepend(
                            fmt::format("Cannot cast row {} from {} to TIMESTAMP_NS: ", i,
                                        block.get_by_position(arguments[0]).type->get_name()));
                    return status;
                }
                col_nullmap->get_data()[i] = true;
            } else {
                col_to->get_data()[i] = value;
            }
        }

        if constexpr (CastMode == CastModeType::StrictMode) {
            block.get_by_position(result).column = std::move(col_to);
        } else {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_nullmap));
        }
        return Status::OK();
    }
};

template <CastModeType CastMode, typename FromDataType>
    requires IsDatelikeTypes<FromDataType>
class CastToImpl<CastMode, FromDataType, DataTypeTimeStampNs> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto& col_from = assert_cast<const typename FromDataType::ColumnType&>(
                *block.get_by_position(arguments[0]).column);
        auto col_to = ColumnTimeStampNs::create(input_rows_count);
        auto col_nullmap = ColumnUInt8::create(input_rows_count, 0);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map && null_map[i]) {
                continue;
            }

            DateV2Value<DateTimeV2ValueType> datetime;
            if constexpr (IsDateType<FromDataType>) {
                const auto source = col_from.get_data()[i];
                datetime = binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(
                        source.to_datetime_v2());
            } else if constexpr (IsDateV2Type<FromDataType>) {
                DataTypeDateV2::cast_to_date_time_v2(col_from.get_data()[i], datetime);
            } else if constexpr (IsDateTimeType<FromDataType>) {
                const auto source = col_from.get_data()[i];
                datetime = binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(
                        source.to_datetime_v2());
            } else if constexpr (IsDateTimeV2Type<FromDataType>) {
                datetime = col_from.get_data()[i];
            } else {
                static_assert(IsTimeV2Type<FromDataType>);
                const auto scale = block.get_by_position(arguments[0]).type->get_scale();
                datetime.from_unixtime(context->state()->timestamp_ms() / 1000,
                                       context->state()->nano_seconds(),
                                       context->state()->timezone_obj(), scale);
                datetime.reset_time_part();

                const auto time_value = col_from.get_data()[i];
                const bool negative = TimeValue::sign(time_value) < 0;
                datetime.template date_add_interval<TimeUnit::HOUR, false>(
                        TimeInterval(HOUR, TimeValue::hour(time_value), negative));
                datetime.template date_add_interval<TimeUnit::MINUTE, false>(
                        TimeInterval(MINUTE, TimeValue::minute(time_value), negative));
                datetime.template date_add_interval<TimeUnit::SECOND, false>(
                        TimeInterval(SECOND, TimeValue::second(time_value), negative));
                datetime.template date_add_interval<TimeUnit::MICROSECOND, false>(
                        TimeInterval(MICROSECOND, TimeValue::microsecond(time_value), negative));
            }

            if (!col_to->get_data()[i].from_datetime(datetime)) {
                if constexpr (CastMode == CastModeType::StrictMode) {
                    return Status::InvalidArgument(
                            "TIMESTAMP_NS overflow when casting row {} from {} to TIMESTAMP_NS", i,
                            block.get_by_position(arguments[0]).type->get_name());
                }
                col_nullmap->get_data()[i] = true;
            }
        }

        if constexpr (CastMode == CastModeType::StrictMode) {
            block.get_by_position(result).column = std::move(col_to);
        } else {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_nullmap));
        }
        return Status::OK();
    }
};

template <CastModeType CastMode>
class CastToImpl<CastMode, DataTypeTimeStampTz, DataTypeTimeStampNs> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto& col_from =
                assert_cast<const ColumnTimeStampTz&>(*block.get_by_position(arguments[0]).column)
                        .get_data();
        auto col_to = ColumnTimeStampNs::create(input_rows_count);
        auto col_null = ColumnBool::create(input_rows_count, 0);
        const auto& local_time_zone = context->state()->timezone_obj();
        const auto source_scale = block.get_by_position(arguments[0]).type->get_scale();

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map && null_map[i]) {
                continue;
            }
            TimestampTzValue source {col_from[i]};
            DateV2Value<DateTimeV2ValueType> datetime;
            const bool converted =
                    source.to_datetime(datetime, local_time_zone, source_scale, source_scale) &&
                    col_to->get_data()[i].from_datetime(datetime);
            if (!converted) {
                if constexpr (CastMode == CastModeType::StrictMode) {
                    return Status::InvalidArgument(
                            "can not cast timestamptz {} to TIMESTAMP_NS in timezone {}",
                            source.to_string(local_time_zone), context->state()->timezone());
                }
                col_null->get_data()[i] = true;
            }
        }

        if constexpr (CastMode == CastModeType::StrictMode) {
            block.get_by_position(result).column = std::move(col_to);
        } else {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null));
        }
        return Status::OK();
    }
};

} // namespace doris
