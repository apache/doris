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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/DateTimeTransforms.h
// and modified by Doris

#pragma once

#include <libdivide.h>

#include <cmath>
#include <cstdint>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/int_exp.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/function_context.h"
#include "vec/functions/date_format_type.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

// FIXME: This file contains widespread UB due to unsafe type-punning casts.
//        These must be properly refactored to eliminate reliance on reinterpret-style behavior.
//
// Temporarily suppress GCC 15+ warnings on user-defined type casts to allow build to proceed.
#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-user-defined"
#endif

namespace doris::vectorized {
#include "common/compile_check_begin.h"

#define TIME_FUNCTION_IMPL(CLASS, UNIT, FUNCTION)                                       \
    template <PrimitiveType PType>                                                      \
    struct CLASS {                                                                      \
        static constexpr PrimitiveType OpArgType = PType;                               \
        using CppType = typename PrimitiveTypeTraits<PType>::CppType;                   \
        static constexpr auto name = #UNIT;                                             \
                                                                                        \
        static inline auto execute(const CppType& date_time_value) {                    \
            return date_time_value.FUNCTION;                                            \
        }                                                                               \
                                                                                        \
        static DataTypes get_variadic_argument_types() {                                \
            return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()}; \
        }                                                                               \
    }

#define TO_TIME_FUNCTION(CLASS, UNIT) TIME_FUNCTION_IMPL(CLASS, UNIT, UNIT())

TO_TIME_FUNCTION(ToYearImpl, year);
template <PrimitiveType PType>
struct ToCenturyImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using CppType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr auto name = "century";

    static inline auto execute(const CppType& t) {
        const auto& date_time_value = t;
        int year = date_time_value.year();
        return (year - 1) / 100 + 1;
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};
TO_TIME_FUNCTION(ToYearOfWeekImpl, year_of_week);
TO_TIME_FUNCTION(ToQuarterImpl, quarter);
TO_TIME_FUNCTION(ToMonthImpl, month);
TO_TIME_FUNCTION(ToDayImpl, day);
TO_TIME_FUNCTION(ToHourImpl, hour);
TO_TIME_FUNCTION(ToMinuteImpl, minute);
TO_TIME_FUNCTION(ToSecondImpl, second);
TO_TIME_FUNCTION(ToMicroSecondImpl, microsecond);

TIME_FUNCTION_IMPL(WeekOfYearImpl, weekofyear, week(mysql_week_mode(3)));
TIME_FUNCTION_IMPL(DayOfYearImpl, dayofyear, day_of_year());
TIME_FUNCTION_IMPL(DayOfMonthImpl, dayofmonth, day());
TIME_FUNCTION_IMPL(DayOfWeekImpl, dayofweek, day_of_week());
TIME_FUNCTION_IMPL(WeekDayImpl, weekday, weekday());
// TODO: the method should be always not nullable
TIME_FUNCTION_IMPL(ToDaysImpl, to_days, daynr());
TIME_FUNCTION_IMPL(ToSecondsImpl, to_seconds,
                   daynr() * 86400L + date_time_value.time_part_to_seconds());

#define TIME_FUNCTION_ONE_ARG_IMPL(CLASS, UNIT, FUNCTION)                                     \
    template <PrimitiveType PType>                                                            \
    struct CLASS {                                                                            \
        static constexpr PrimitiveType OpArgType = PType;                                     \
        using ArgType = typename PrimitiveTypeTraits<PType>::CppType;                         \
        static constexpr auto name = #UNIT;                                                   \
                                                                                              \
        static inline auto execute(const ArgType& t) {                                        \
            const auto& date_time_value = (typename PrimitiveTypeTraits<PType>::CppType&)(t); \
            return date_time_value.FUNCTION;                                                  \
        }                                                                                     \
                                                                                              \
        static DataTypes get_variadic_argument_types() {                                      \
            return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};       \
        }                                                                                     \
    }

TIME_FUNCTION_ONE_ARG_IMPL(ToWeekOneArgImpl, week, week(mysql_week_mode(0)));
TIME_FUNCTION_ONE_ARG_IMPL(ToYearWeekOneArgImpl, yearweek, year_week(mysql_week_mode(0)));

template <PrimitiveType PType>
struct ToDateImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using DateType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr auto name = "to_date";

    static auto execute(const DateType& t) {
        if constexpr (std::is_same_v<DateType, DateV2Value<DateV2ValueType>>) {
            return t;
        } else if constexpr (std::is_same_v<DateType, VecDateTimeValue>) {
            t.cast_to_date();
            return t;
        } else {
            return binary_cast<UInt32, DateV2Value<DateV2ValueType>>(
                    (UInt32)(t.to_date_int_val() >> TIME_PART_LENGTH));
        }
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};

template <PrimitiveType ArgType>
struct DateImpl : public ToDateImpl<ArgType> {
    static constexpr auto name = "date";
};

// TODO: This function look like no need do indeed copy here, we should optimize this function
template <PrimitiveType PType>
struct TimeStampImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr auto name = "timestamp";

    static auto execute(const ArgType& t) { return t; }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};

template <PrimitiveType PType>
struct DayNameImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr auto name = "dayname";
    static constexpr auto max_size = MAX_DAY_NAME_LEN;

    static auto execute(const typename PrimitiveTypeTraits<PType>::CppType& dt,
                        ColumnString::Chars& res_data, size_t& offset, const char* const* day_names,
                        FunctionContext* /*context*/) {
        DCHECK(day_names != nullptr);
        const auto* day_name = dt.day_name_with_locale(day_names);
        if (day_name != nullptr) {
            auto len = strlen(day_name);
            memcpy(&res_data[offset], day_name, len);
            offset += len;
        }
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};

template <PrimitiveType PType>
struct ToIso8601Impl {
    static constexpr PrimitiveType OpArgType = PType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr auto name = "to_iso8601";
    static constexpr auto max_size = PType == TYPE_DATEV2 ? 10 : 26;

    static auto execute(const typename PrimitiveTypeTraits<PType>::CppType& dt,
                        ColumnString::Chars& res_data, size_t& offset,
                        const char* const* /*names_ptr*/, FunctionContext* /*context*/) {
        auto length = dt.to_buffer((char*)res_data.data() + offset,
                                   std::is_same_v<ArgType, UInt32> ? -1 : 6);
        if (PType == TYPE_DATETIMEV2 || PType == TYPE_TIMESTAMPTZ) {
            res_data[offset + 10] = 'T';
        }

        offset += length;
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};

// Specialization for TIMESTAMPTZ type
template <>
struct ToIso8601Impl<TYPE_TIMESTAMPTZ> {
    static constexpr PrimitiveType OpArgType = TYPE_TIMESTAMPTZ;
    using ArgType = typename PrimitiveTypeTraits<TYPE_TIMESTAMPTZ>::CppType;
    static constexpr auto name = "to_iso8601";
    // Format: YYYY-MM-DDTHH:MM:SS.SSSSSS+HH:MM
    static constexpr auto max_size = 32;

    static auto execute(const TimestampTzValue& tz_value, ColumnString::Chars& res_data,
                        size_t& offset, const char* const* /*names_ptr*/,
                        FunctionContext* context) {
        // Get timezone
        const auto& local_time_zone = context->state()->timezone_obj();

        // Convert UTC time to local time
        cctz::civil_second utc_sec(tz_value.year(), tz_value.month(), tz_value.day(),
                                   tz_value.hour(), tz_value.minute(), tz_value.second());
        cctz::time_point<cctz::seconds> local_time = cctz::convert(utc_sec, cctz::utc_time_zone());

        auto lookup_result = local_time_zone.lookup(local_time);
        cctz::civil_second civ = lookup_result.cs;
        auto time_offset = lookup_result.offset;

        int offset_hours = time_offset / 3600;
        int offset_mins = (std::abs(time_offset) % 3600) / 60;

        // Create local datetime value
        DateV2Value<DateTimeV2ValueType> local_dt;
        local_dt.unchecked_set_time((uint16_t)civ.year(), (uint8_t)civ.month(), (uint8_t)civ.day(),
                                    (uint8_t)civ.hour(), (uint8_t)civ.minute(),
                                    (uint8_t)civ.second(), tz_value.microsecond());

        // YYYY-MM-DDTHH:MM:SS.SSSSSS+HH:MM
        auto length = local_dt.to_buffer((char*)res_data.data() + offset, 6);
        res_data[offset + 10] = 'T';
        res_data[offset + length] = (offset_hours >= 0 ? '+' : '-');
        res_data[offset + length + 1] = static_cast<char>('0' + std::abs(offset_hours) / 10);
        res_data[offset + length + 2] = '0' + std::abs(offset_hours) % 10;
        res_data[offset + length + 3] = ':';
        res_data[offset + length + 4] = static_cast<char>('0' + offset_mins / 10);
        res_data[offset + length + 5] = '0' + offset_mins % 10;

        offset += length + 6;
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<TYPE_TIMESTAMPTZ>::DataType>()};
    }
};

template <PrimitiveType PType>
struct MonthNameImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr auto name = "monthname";
    static constexpr auto max_size = MAX_MONTH_NAME_LEN;

    static auto execute(const typename PrimitiveTypeTraits<PType>::CppType& dt,
                        ColumnString::Chars& res_data, size_t& offset,
                        const char* const* month_names, FunctionContext* /*context*/) {
        DCHECK(month_names != nullptr);
        const auto* month_name = dt.month_name_with_locale(month_names);
        if (month_name != nullptr) {
            auto len = strlen(month_name);
            memcpy(&res_data[offset], month_name, len);
            offset += len;
        }
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};

template <typename FormatImpl, const char* FuncName>
struct DateTimeV2FormatImpl {
    static constexpr PrimitiveType OpArgType = TYPE_DATETIMEV2;
    using ArgType = typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType;
    static constexpr auto name = FuncName;
    static constexpr auto max_size = FormatImpl::row_size;

    static auto execute(const ArgType& dt, ColumnString::Chars& res_data, size_t& offset,
                        const char* const* /*names_ptr*/, FunctionContext* /*context*/) {
        auto* buf = reinterpret_cast<char*>(&res_data[offset]);
        offset += FormatImpl::date_to_str(dt, buf);
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::DataType>()};
    }
};

inline constexpr char kYearMonthName[] = "year_month";
inline constexpr char kDayHourName[] = "day_hour";
inline constexpr char kDayMinuteName[] = "day_minute";
inline constexpr char kDaySecondName[] = "day_second";
inline constexpr char kDayMicrosecondName[] = "day_microsecond";
inline constexpr char kHourMinuteName[] = "hour_minute";
inline constexpr char kHourSecondName[] = "hour_second";
inline constexpr char kHourMicrosecondName[] = "hour_microsecond";
inline constexpr char kMinuteSecondName[] = "minute_second";
inline constexpr char kMinuteMicrosecondName[] = "minute_microsecond";
inline constexpr char kSecondMicrosecondName[] = "second_microsecond";

using YearMonthImpl = DateTimeV2FormatImpl<time_format_type::yyyy_MMImpl, kYearMonthName>;
using DayHourImpl = DateTimeV2FormatImpl<time_format_type::dd_HHImpl, kDayHourName>;
using DayMinuteImpl = DateTimeV2FormatImpl<time_format_type::dd_HH_mmImpl, kDayMinuteName>;
using DaySecondImpl = DateTimeV2FormatImpl<time_format_type::dd_HH_mm_ssImpl, kDaySecondName>;
using DayMicrosecondImpl =
        DateTimeV2FormatImpl<time_format_type::dd_HH_mm_ss_SSSSSSImpl, kDayMicrosecondName>;
using HourMinuteImpl = DateTimeV2FormatImpl<time_format_type::HH_mmImpl, kHourMinuteName>;
using HourSecondImpl = DateTimeV2FormatImpl<time_format_type::HH_mm_ssImpl, kHourSecondName>;
using HourMicrosecondImpl =
        DateTimeV2FormatImpl<time_format_type::HH_mm_ss_SSSSSSImpl, kHourMicrosecondName>;
using MinuteSecondImpl = DateTimeV2FormatImpl<time_format_type::mm_ssImpl, kMinuteSecondName>;
using MinuteMicrosecondImpl =
        DateTimeV2FormatImpl<time_format_type::mm_ss_SSSSSSImpl, kMinuteMicrosecondName>;
using SecondMicrosecondImpl =
        DateTimeV2FormatImpl<time_format_type::ss_SSSSSSImpl, kSecondMicrosecondName>;

template <PrimitiveType PType>
struct DateFormatImpl {
    using DateType = typename PrimitiveTypeTraits<PType>::CppType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr PrimitiveType FromPType = PType;

    static constexpr auto name = "date_format";

    template <typename Impl>
    static bool execute(const DateType& dt, StringRef format, ColumnString::Chars& res_data,
                        size_t& offset, const cctz::time_zone& time_zone) {
        if constexpr (std::is_same_v<Impl, time_format_type::UserDefinedImpl>) {
            // Handle non-special formats.
            char buf[100 + SAFE_FORMAT_STRING_MARGIN];
            if (!dt.to_format_string_conservative(format.data, format.size, buf,
                                                  100 + SAFE_FORMAT_STRING_MARGIN)) {
                return true;
            }

            auto len = strlen(buf);
            res_data.insert(buf, buf + len);
            offset += len;
            return false;
        } else {
            if (!dt.is_valid_date()) {
                return true;
            }

            // No buffer is needed here because these specially optimized formats have fixed lengths,
            // and sufficient memory has already been reserved.
            auto len = Impl::date_to_str(dt, (char*)res_data.data() + offset);
            offset += len;

            return false;
        }
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>(),
                std::make_shared<vectorized::DataTypeString>()};
    }
};

template <bool WithStringArg, bool NewVersion = false>
struct FromUnixTimeImpl {
    using ArgType = Int64;
    static constexpr PrimitiveType FromPType = TYPE_BIGINT;

    static DataTypes get_variadic_argument_types() {
        if constexpr (WithStringArg) {
            return {std::make_shared<DataTypeInt64>(),
                    std::make_shared<vectorized::DataTypeString>()};
        } else {
            return {std::make_shared<DataTypeInt64>()};
        }
    }
    static const int64_t TIMESTAMP_VALID_MAX = 32536771199;
    static constexpr auto name = NewVersion ? "from_unixtime_new" : "from_unixtime";

    [[nodiscard]] static bool check_valid(const ArgType& val) {
        if constexpr (NewVersion) {
            if (val < 0) [[unlikely]] {
                return false;
            }
        } else {
            if (val < 0 || val > TIMESTAMP_VALID_MAX) [[unlikely]] {
                return false;
            }
        }
        return true;
    }

    static DateV2Value<DateTimeV2ValueType> get_datetime_value(const ArgType& val,
                                                               const cctz::time_zone& time_zone) {
        DateV2Value<DateTimeV2ValueType> dt;
        dt.from_unixtime(val, time_zone);
        return dt;
    }

    // return true if null(result is invalid)
    template <typename Impl>
    static bool execute(const ArgType& val, StringRef format, ColumnString::Chars& res_data,
                        size_t& offset, const cctz::time_zone& time_zone) {
        if (!check_valid(val)) {
            return true;
        }
        DateV2Value<DateTimeV2ValueType> dt = get_datetime_value(val, time_zone);
        if (!dt.is_valid_date()) [[unlikely]] {
            return true;
        }
        if constexpr (std::is_same_v<Impl, time_format_type::UserDefinedImpl>) {
            char buf[100 + SAFE_FORMAT_STRING_MARGIN];
            if (!dt.to_format_string_conservative(format.data, format.size, buf,
                                                  100 + SAFE_FORMAT_STRING_MARGIN)) {
                return true;
            }

            auto len = strlen(buf);
            res_data.insert(buf, buf + len);
            offset += len;
        } else {
            // No buffer is needed here because these specially optimized formats have fixed lengths,
            // and sufficient memory has already been reserved.
            auto len = Impl::date_to_str(dt, (char*)res_data.data() + offset);
            offset += len;
        }
        return false;
    }
};

// only new verison
template <bool WithStringArg>
struct FromUnixTimeDecimalImpl {
    using ArgType = Int64;
    static constexpr PrimitiveType FromPType = TYPE_DECIMAL64;
    constexpr static short Scale = 6; // same with argument's scale in FE's signature

    static DataTypes get_variadic_argument_types() {
        if constexpr (WithStringArg) {
            return {std::make_shared<DataTypeDecimal64>(),
                    std::make_shared<vectorized::DataTypeString>()};
        } else {
            return {std::make_shared<DataTypeDecimal64>()};
        }
    }
    static constexpr auto name = "from_unixtime_new";

    [[nodiscard]] static bool check_valid(const ArgType& val) {
        if (val < 0) [[unlikely]] {
            return false;
        }
        return true;
    }

    static DateV2Value<DateTimeV2ValueType> get_datetime_value(const ArgType& interger,
                                                               const ArgType& fraction,
                                                               const cctz::time_zone& time_zone) {
        DateV2Value<DateTimeV2ValueType> dt;
        // 9 is nanoseconds, our input's scale is 6
        dt.from_unixtime(interger, (int32_t)fraction * common::exp10_i32(9 - Scale), time_zone, 6);
        return dt;
    }

    // return true if null(result is invalid)
    template <typename Impl>
    static bool execute_decimal(const ArgType& interger, const ArgType& fraction, StringRef format,
                                ColumnString::Chars& res_data, size_t& offset,
                                const cctz::time_zone& time_zone) {
        if (!check_valid(interger + (fraction > 0 ? 1 : ((fraction < 0) ? -1 : 0)))) [[unlikely]] {
            return true;
        }
        DateV2Value<DateTimeV2ValueType> dt = get_datetime_value(interger, fraction, time_zone);
        if (!dt.is_valid_date()) [[unlikely]] {
            return true;
        }
        if constexpr (std::is_same_v<Impl, time_format_type::UserDefinedImpl>) {
            char buf[100 + SAFE_FORMAT_STRING_MARGIN];
            if (!dt.to_format_string_conservative(format.data, format.size, buf,
                                                  100 + SAFE_FORMAT_STRING_MARGIN)) {
                return true;
            }

            auto len = strlen(buf);
            res_data.insert(buf, buf + len);
            offset += len;
        } else {
            // No buffer is needed here because these specially optimized formats have fixed lengths,
            // and sufficient memory has already been reserved.
            auto len = time_format_type::yyyy_MM_dd_HH_mm_ss_SSSSSSImpl::date_to_str(
                    dt, (char*)res_data.data() + offset);
            offset += len;
        }
        return false;
    }
};

// Base template for optimized time field(HOUR, MINUTE, SECOND, MS) extraction from Unix timestamp
// Uses lookup_offset to avoid expensive civil_second construction
template <typename Impl>
class FunctionTimeFieldFromUnixtime : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionTimeFieldFromUnixtime<Impl>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        // microsecond_from_unixtime returns Int32, others (hour/minute/second) return Int8
        if constexpr (Impl::ArgType == PrimitiveType::TYPE_DECIMAL64) {
            return make_nullable(std::make_shared<DataTypeInt32>());
        } else {
            return make_nullable(std::make_shared<DataTypeInt8>());
        }
    }

    // (UTC 9999-12-31 23:59:59) - 24 * 3600
    static const int64_t TIMESTAMP_VALID_MAX = 253402243199L;

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        using ArgColType = typename PrimitiveTypeTraits<Impl::ArgType>::ColumnType;
        using ResColType = std::conditional_t<Impl::ArgType == PrimitiveType::TYPE_DECIMAL64,
                                              ColumnInt32, ColumnInt8>;
        using ResItemType = typename ResColType::value_type;
        auto res = ResColType::create();

        const auto* ts_col =
                assert_cast<const ArgColType*>(block.get_by_position(arguments[0]).column.get());
        if constexpr (Impl::ArgType == PrimitiveType::TYPE_DECIMAL64) {
            // microsecond_from_unixtime only
            const auto scale = static_cast<int32_t>(ts_col->get_scale());

            for (int i = 0; i < input_rows_count; ++i) {
                const auto seconds = ts_col->get_intergral_part(i);
                const auto fraction = ts_col->get_fractional_part(i);

                if (seconds < 0 || seconds > TIMESTAMP_VALID_MAX) {
                    return Status::InvalidArgument(
                            "The input value of TimeFiled(from_unixtime()) must between 0 and "
                            "253402243199L");
                }

                ResItemType value = Impl::extract_field(fraction, scale);
                res->insert_value(value);
            }
        } else {
            auto ctz = context->state()->timezone_obj();
            for (int i = 0; i < input_rows_count; ++i) {
                auto date = ts_col->get_element(i);

                if (date < 0 || date > TIMESTAMP_VALID_MAX) {
                    return Status::InvalidArgument(
                            "The input value of TimeFiled(from_unixtime()) must between 0 and "
                            "253402243199L");
                }

                ResItemType value = Impl::extract_field(date, ctz);
                res->insert_value(value);
            }
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

struct HourFromUnixtimeImpl {
    static constexpr PrimitiveType ArgType = PrimitiveType::TYPE_BIGINT;
    static constexpr auto name = "hour_from_unixtime";

    static int8_t extract_field(int64_t local_time, const cctz::time_zone& ctz) {
        static const auto epoch = std::chrono::time_point_cast<cctz::sys_seconds>(
                std::chrono::system_clock::from_time_t(0));
        cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(local_time);
        int offset = ctz.lookup_offset(t).offset;
        local_time += offset;

        static const libdivide::divider<int64_t> fast_div_3600(3600);
        static const libdivide::divider<int64_t> fast_div_86400(86400);

        int64_t remainder;
        if (LIKELY(local_time >= 0)) {
            remainder = local_time - local_time / fast_div_86400 * 86400;
        } else {
            remainder = local_time % 86400;
            if (remainder < 0) {
                remainder += 86400;
            }
        }
        return static_cast<int8_t>(remainder / fast_div_3600);
    }
};

struct MinuteFromUnixtimeImpl {
    static constexpr PrimitiveType ArgType = PrimitiveType::TYPE_BIGINT;
    static constexpr auto name = "minute_from_unixtime";

    static int8_t extract_field(int64_t local_time, const cctz::time_zone& /*ctz*/) {
        static const libdivide::divider<int64_t> fast_div_60(60);
        static const libdivide::divider<int64_t> fast_div_3600(3600);

        local_time = local_time - local_time / fast_div_3600 * 3600;

        return static_cast<int8_t>(local_time / fast_div_60);
    }
};

struct SecondFromUnixtimeImpl {
    static constexpr PrimitiveType ArgType = PrimitiveType::TYPE_BIGINT;
    static constexpr auto name = "second_from_unixtime";

    static int8_t extract_field(int64_t local_time, const cctz::time_zone& /*ctz*/) {
        return static_cast<int8_t>(local_time % 60);
    }
};

struct MicrosecondFromUnixtimeImpl {
    static constexpr PrimitiveType ArgType = PrimitiveType::TYPE_DECIMAL64;
    static constexpr auto name = "microsecond_from_unixtime";

    static int32_t extract_field(int64_t fraction, int scale) {
        if (scale < 6) {
            fraction *= common::exp10_i64(6 - scale);
        }
        return static_cast<int32_t>(fraction);
    }
};

template <PrimitiveType ArgPType>
class FunctionTimeFormat : public IFunction {
public:
    using ArgColType = typename PrimitiveTypeTraits<ArgPType>::ColumnType;
    using ArgCppType = typename PrimitiveTypeTraits<ArgPType>::CppType;

    static constexpr auto name = "time_format";
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionTimeFormat>(); }
    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<typename PrimitiveTypeTraits<ArgPType>::DataType>(),
                std::make_shared<vectorized::DataTypeString>()};
    }
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    size_t get_number_of_arguments() const override { return 2; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_col = ColumnString::create();
        ColumnString::Chars& res_chars = res_col->get_chars();
        ColumnString::Offsets& res_offsets = res_col->get_offsets();

        auto null_map = ColumnUInt8::create();
        auto& null_map_data = null_map->get_data();
        null_map_data.resize_fill(input_rows_count, 0);

        res_offsets.reserve(input_rows_count);

        ColumnPtr arg_col[2];
        bool is_const[2];
        for (size_t i = 0; i < 2; ++i) {
            const ColumnPtr& col = block.get_by_position(arguments[i]).column;
            std::tie(arg_col[i], is_const[i]) = unpack_if_const(col);
        }

        const auto* datetime_col = assert_cast<const ArgColType*>(arg_col[0].get());
        const auto* format_col = assert_cast<const ColumnString*>(arg_col[1].get());
        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto& datetime_val = datetime_col->get_element(index_check_const(i, is_const[0]));
            StringRef format = format_col->get_data_at(index_check_const(i, is_const[1]));
            TimeValue::TimeType time = get_time_value(datetime_val);

            char buf[100 + SAFE_FORMAT_STRING_MARGIN];
            if (!TimeValue::to_format_string_conservative(format.data, format.size, buf,
                                                          100 + SAFE_FORMAT_STRING_MARGIN, time)) {
                null_map_data[i] = 1;
                res_offsets.push_back(res_chars.size());
                continue;
            }
            res_chars.insert(buf, buf + strlen(buf));
            res_offsets.push_back(res_chars.size());
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res_col), std::move(null_map)));
        return Status::OK();
    }

private:
    TimeValue::TimeType get_time_value(const ArgCppType& datetime_val) const {
        if constexpr (ArgPType == PrimitiveType::TYPE_TIMEV2) {
            return static_cast<TimeValue::TimeType>(datetime_val);
        } else {
            return TimeValue::make_time(datetime_val.hour(), datetime_val.minute(),
                                        datetime_val.second(), datetime_val.microsecond());
        }
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized

#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic pop
#endif
