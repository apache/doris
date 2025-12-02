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

#include <cmath>
#include <cstdint>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "util/binary_cast.hpp"
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
#include "vec/functions/date_format_type.h"
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

#define TIME_FUNCTION_IMPL(CLASS, UNIT, FUNCTION)                                             \
    template <PrimitiveType PType>                                                            \
    struct CLASS {                                                                            \
        static constexpr PrimitiveType OpArgType = PType;                                     \
        using NativeType = typename PrimitiveTypeTraits<PType>::CppNativeType;                \
        static constexpr auto name = #UNIT;                                                   \
                                                                                              \
        static inline auto execute(const NativeType& t) {                                     \
            const auto& date_time_value = (typename PrimitiveTypeTraits<PType>::CppType&)(t); \
            return date_time_value.FUNCTION;                                                  \
        }                                                                                     \
                                                                                              \
        static DataTypes get_variadic_argument_types() {                                      \
            return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};       \
        }                                                                                     \
    }

#define TO_TIME_FUNCTION(CLASS, UNIT) TIME_FUNCTION_IMPL(CLASS, UNIT, UNIT())

TO_TIME_FUNCTION(ToYearImpl, year);
template <PrimitiveType PType>
struct ToCenturyImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using NativeType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    static constexpr auto name = "century";

    static inline auto execute(const NativeType& t) {
        const auto& date_time_value = (typename PrimitiveTypeTraits<PType>::CppType&)(t);
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

#define TIME_FUNCTION_ONE_ARG_IMPL(CLASS, UNIT, FUNCTION)                                     \
    template <PrimitiveType PType>                                                            \
    struct CLASS {                                                                            \
        static constexpr PrimitiveType OpArgType = PType;                                     \
        using ArgType = typename PrimitiveTypeTraits<PType>::CppNativeType;                   \
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
    using NativeType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    using DateType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr auto name = "to_date";

    static auto execute(const NativeType& t) {
        auto dt = binary_cast<NativeType, DateType>(t);
        if constexpr (std::is_same_v<DateType, DateV2Value<DateV2ValueType>>) {
            return binary_cast<DateType, NativeType>(dt);
        } else if constexpr (std::is_same_v<DateType, VecDateTimeValue>) {
            dt.cast_to_date();
            return binary_cast<DateType, NativeType>(dt);
        } else {
            return (PrimitiveTypeTraits<TYPE_DATEV2>::CppNativeType)(
                    binary_cast<DateType, NativeType>(dt) >> TIME_PART_LENGTH);
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
    using ArgType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    static constexpr auto name = "timestamp";

    static auto execute(const ArgType& t) { return t; }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};

template <PrimitiveType PType>
struct DayNameImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    static constexpr auto name = "dayname";
    static constexpr auto max_size = MAX_DAY_NAME_LEN;

    static auto execute(const typename PrimitiveTypeTraits<PType>::CppType& dt,
                        ColumnString::Chars& res_data, size_t& offset,
                        const char* const* day_names) {
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
    using ArgType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    static constexpr auto name = "to_iso8601";
    static constexpr auto max_size = std::is_same_v<ArgType, UInt32> ? 10 : 26;

    static auto execute(const typename PrimitiveTypeTraits<PType>::CppType& dt,
                        ColumnString::Chars& res_data, size_t& offset,
                        const char* const* /*names_ptr*/) {
        auto length = dt.to_buffer((char*)res_data.data() + offset,
                                   std::is_same_v<ArgType, UInt32> ? -1 : 6);
        if (std::is_same_v<ArgType, UInt64>) {
            res_data[offset + 10] = 'T';
        }

        offset += length;
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>()};
    }
};

template <PrimitiveType PType>
struct MonthNameImpl {
    static constexpr PrimitiveType OpArgType = PType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    static constexpr auto name = "monthname";
    static constexpr auto max_size = MAX_MONTH_NAME_LEN;

    static auto execute(const typename PrimitiveTypeTraits<PType>::CppType& dt,
                        ColumnString::Chars& res_data, size_t& offset,
                        const char* const* month_names) {
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

template <PrimitiveType PType>
struct DateFormatImpl {
    using DateType = typename PrimitiveTypeTraits<PType>::CppType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    static constexpr PrimitiveType FromPType = PType;

    static constexpr auto name = "date_format";

    template <typename Impl>
    static bool execute(const ArgType& t, StringRef format, ColumnString::Chars& res_data,
                        size_t& offset, const cctz::time_zone& time_zone) {
        if constexpr (std::is_same_v<Impl, time_format_type::UserDefinedImpl>) {
            // Handle non-special formats.
            const auto& dt = (DateType&)t;
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
            const auto& dt = (DateType&)t;

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

#include "common/compile_check_end.h"
} // namespace doris::vectorized

#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic pop
#endif
