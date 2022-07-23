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

#include "common/status.h"
#include "runtime/datetime_value.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/exception.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/functions/function_helpers.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

#define TIME_FUNCTION_IMPL(CLASS, UNIT, FUNCTION)                                               \
    template <typename DateValueType, typename ArgType>                                         \
    struct CLASS {                                                                              \
        using ARG_TYPE = ArgType;                                                               \
        static constexpr auto name = #UNIT;                                                     \
                                                                                                \
        static inline auto execute(const ARG_TYPE& t, bool& is_null) {                          \
            const auto& date_time_value = (DateValueType&)(t);                                  \
            is_null = !date_time_value.is_valid_date();                                         \
            return date_time_value.FUNCTION;                                                    \
        }                                                                                       \
                                                                                                \
        static DataTypes get_variadic_argument_types() {                                        \
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {                    \
                return {std::make_shared<DataTypeDateTime>()};                                  \
            } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) { \
                return {std::make_shared<DataTypeDateV2>()};                                    \
            } else {                                                                            \
                return {std::make_shared<DataTypeDateTimeV2>()};                                \
            }                                                                                   \
        }                                                                                       \
    }

#define TO_TIME_FUNCTION(CLASS, UNIT) TIME_FUNCTION_IMPL(CLASS, UNIT, UNIT())

TO_TIME_FUNCTION(ToYearImpl, year);
TO_TIME_FUNCTION(ToQuarterImpl, quarter);
TO_TIME_FUNCTION(ToMonthImpl, month);
TO_TIME_FUNCTION(ToDayImpl, day);
TO_TIME_FUNCTION(ToHourImpl, hour);
TO_TIME_FUNCTION(ToMinuteImpl, minute);
TO_TIME_FUNCTION(ToSecondImpl, second);

TIME_FUNCTION_IMPL(WeekOfYearImpl, weekofyear, week(mysql_week_mode(3)));
TIME_FUNCTION_IMPL(DayOfYearImpl, dayofyear, day_of_year());
TIME_FUNCTION_IMPL(DayOfMonthImpl, dayofmonth, day());
TIME_FUNCTION_IMPL(DayOfWeekImpl, dayofweek, day_of_week());
TIME_FUNCTION_IMPL(WeekDayImpl, weekday, weekday());
// TODO: the method should be always not nullable
TIME_FUNCTION_IMPL(ToDaysImpl, to_days, daynr());

#define TIME_FUNCTION_ONE_ARG_IMPL(CLASS, UNIT, FUNCTION)                                       \
    template <typename DateValueType, typename ArgType>                                         \
    struct CLASS {                                                                              \
        using ARG_TYPE = ArgType;                                                               \
        static constexpr auto name = #UNIT;                                                     \
                                                                                                \
        static inline auto execute(const ARG_TYPE& t, bool& is_null) {                          \
            const auto& date_time_value = (DateValueType&)(t);                                  \
            is_null = !date_time_value.is_valid_date();                                         \
            return date_time_value.FUNCTION;                                                    \
        }                                                                                       \
                                                                                                \
        static DataTypes get_variadic_argument_types() {                                        \
            if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {                    \
                return {std::make_shared<DataTypeDateTime>()};                                  \
            } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) { \
                return {std::make_shared<DataTypeDateV2>()};                                    \
            } else {                                                                            \
                return {std::make_shared<DataTypeDateTimeV2>()};                                \
            }                                                                                   \
        }                                                                                       \
    }

TIME_FUNCTION_ONE_ARG_IMPL(ToWeekOneArgImpl, week, week(mysql_week_mode(0)));
TIME_FUNCTION_ONE_ARG_IMPL(ToYearWeekOneArgImpl, yearweek, year_week(mysql_week_mode(0)));

template <typename DateValueType, typename ArgType>
struct ToDateImpl {
    using ARG_TYPE = ArgType;
    static constexpr auto name = "to_date";

    static inline auto execute(const ArgType& t, bool& is_null) {
        auto dt = binary_cast<ArgType, DateValueType>(t);
        is_null = !dt.is_valid_date();
        if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
            return binary_cast<DateValueType, ArgType>(dt);
        } else if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            dt.cast_to_date();
            return binary_cast<DateValueType, ArgType>(dt);
        } else {
            return (UInt32)(binary_cast<DateValueType, ArgType>(dt) >> TIME_PART_LENGTH);
        }
    }

    static DataTypes get_variadic_argument_types() {
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            return {std::make_shared<DataTypeDateTime>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
            return {std::make_shared<DataTypeDateV2>()};
        } else {
            return {std::make_shared<DataTypeDateTimeV2>()};
        }
    }
};

template <typename DateValue, typename ArgType>
struct DateImpl : public ToDateImpl<DateValue, ArgType> {
    static constexpr auto name = "date";
};

// TODO: This function look like no need do indeed copy here, we should optimize
// this function
template <typename ArgType>
struct TimeStampImpl {
    using ARG_TYPE = ArgType;
    static constexpr auto name = "timestamp";

    static inline auto execute(const ARG_TYPE& t, bool& is_null) { return t; }
};

template <typename DateValueType, typename ArgType>
struct DayNameImpl {
    using ARG_TYPE = ArgType;
    static constexpr auto name = "dayname";
    static constexpr auto max_size = MAX_DAY_NAME_LEN;

    static inline auto execute(const DateValueType& dt, ColumnString::Chars& res_data,
                               size_t& offset, bool& is_null) {
        const auto* day_name = dt.day_name();
        is_null = !dt.is_valid_date();
        if (day_name == nullptr || is_null) {
            offset += 1;
            res_data[offset - 1] = 0;
        } else {
            auto len = strlen(day_name);
            memcpy(&res_data[offset], day_name, len);
            offset += len + 1;
            res_data[offset - 1] = 0;
        }
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            return {std::make_shared<DataTypeDateTime>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
            return {std::make_shared<DataTypeDateV2>()};
        } else {
            return {std::make_shared<DataTypeDateTimeV2>()};
        }
    }
};

template <typename DateValueType, typename ArgType>
struct MonthNameImpl {
    using ARG_TYPE = ArgType;
    static constexpr auto name = "monthname";
    static constexpr auto max_size = MAX_MONTH_NAME_LEN;

    static inline auto execute(const DateValueType& dt, ColumnString::Chars& res_data,
                               size_t& offset, bool& is_null) {
        const auto* month_name = dt.month_name();
        is_null = !dt.is_valid_date();
        if (month_name == nullptr || is_null) {
            offset += 1;
            res_data[offset - 1] = 0;
        } else {
            auto len = strlen(month_name);
            memcpy(&res_data[offset], month_name, len);
            offset += (len + 1);
            res_data[offset - 1] = 0;
        }
        return offset;
    }

    static DataTypes get_variadic_argument_types() {
        if constexpr (std::is_same_v<DateValueType, VecDateTimeValue>) {
            return {std::make_shared<DataTypeDateTime>()};
        } else if constexpr (std::is_same_v<DateValueType, DateV2Value<DateV2ValueType>>) {
            return {std::make_shared<DataTypeDateV2>()};
        } else {
            return {std::make_shared<DataTypeDateTimeV2>()};
        }
    }
};

template <typename DateType, typename ArgType>
struct DateFormatImpl {
    using FromType = ArgType;

    static constexpr auto name = "date_format";

    static inline auto execute(const FromType& t, StringRef format, ColumnString::Chars& res_data,
                               size_t& offset) {
        const auto& dt = (DateType&)t;
        if (format.size > 128) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair {offset, true};
        }
        char buf[128];
        if (!dt.to_format_string(format.data, format.size, buf)) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair {offset, true};
        }

        auto len = strlen(buf) + 1;
        res_data.insert(buf, buf + len);
        offset += len;
        return std::pair {offset, false};
    }

    static DataTypes get_variadic_argument_types() {
        if constexpr (std::is_same_v<DateType, VecDateTimeValue>) {
            return std::vector<DataTypePtr> {
                    std::dynamic_pointer_cast<const IDataType>(
                            std::make_shared<vectorized::DataTypeDateTime>()),
                    std::dynamic_pointer_cast<const IDataType>(
                            std::make_shared<vectorized::DataTypeString>())};
        } else if constexpr (std::is_same_v<DateType, DateV2Value<DateV2ValueType>>) {
            return std::vector<DataTypePtr> {
                    std::dynamic_pointer_cast<const IDataType>(
                            std::make_shared<vectorized::DataTypeDateV2>()),
                    std::dynamic_pointer_cast<const IDataType>(
                            std::make_shared<vectorized::DataTypeString>())};
        } else {
            return std::vector<DataTypePtr> {
                    std::dynamic_pointer_cast<const IDataType>(
                            std::make_shared<vectorized::DataTypeDateTimeV2>()),
                    std::dynamic_pointer_cast<const IDataType>(
                            std::make_shared<vectorized::DataTypeString>())};
        }
    }
};

// TODO: This function should be depend on argments not always nullable
template <typename DateType>
struct FromUnixTimeImpl {
    using FromType = Int32;

    static constexpr auto name = "from_unixtime";

    static inline auto execute(FromType val, StringRef format, ColumnString::Chars& res_data,
                               size_t& offset) {
        // TODO: use default time zone, slowly and incorrect, just for test use
        static cctz::time_zone time_zone = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));

        DateType dt;
        if (format.size > 128 || val < 0 || val > INT_MAX || !dt.from_unixtime(val, time_zone)) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair {offset, true};
        }

        char buf[128];
        if (!dt.to_format_string(format.data, format.size, buf)) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair {offset, true};
        }

        auto len = strlen(buf) + 1;
        res_data.insert(buf, buf + len);
        offset += len;
        return std::pair {offset, false};
    }
};

template <typename Transform>
struct TransformerToStringOneArgument {
    static void vector(const PaddedPODArray<Int64>& ts, ColumnString::Chars& res_data,
                       ColumnString::Offsets& res_offsets, NullMap& null_map) {
        const auto len = ts.size();
        res_data.resize(len * Transform::max_size);
        res_offsets.resize(len);
        null_map.resize_fill(len, false);

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            const auto& t = ts[i];
            const auto& date_time_value = reinterpret_cast<const VecDateTimeValue&>(t);
            res_offsets[i] = Transform::execute(date_time_value, res_data, offset,
                                                reinterpret_cast<bool&>(null_map[i]));
        }
    }

    static void vector(const PaddedPODArray<UInt32>& ts, ColumnString::Chars& res_data,
                       ColumnString::Offsets& res_offsets, NullMap& null_map) {
        const auto len = ts.size();
        res_data.resize(len * Transform::max_size);
        res_offsets.resize(len);
        null_map.resize_fill(len, false);

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            const auto& t = ts[i];
            const auto& date_time_value = reinterpret_cast<const DateV2Value<DateV2ValueType>&>(t);
            res_offsets[i] = Transform::execute(date_time_value, res_data, offset,
                                                reinterpret_cast<bool&>(null_map[i]));
        }
    }

    static void vector(const PaddedPODArray<UInt64>& ts, ColumnString::Chars& res_data,
                       ColumnString::Offsets& res_offsets, NullMap& null_map) {
        const auto len = ts.size();
        res_data.resize(len * Transform::max_size);
        res_offsets.resize(len);
        null_map.resize_fill(len, false);

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            const auto& t = ts[i];
            const auto& date_time_value =
                    reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(t);
            res_offsets[i] = Transform::execute(date_time_value, res_data, offset,
                                                reinterpret_cast<bool&>(null_map[i]));
        }
    }
};

template <typename Transform>
struct TransformerToStringTwoArgument {
    static void vector_constant(const PaddedPODArray<typename Transform::FromType>& ts,
                                const std::string& format, ColumnString::Chars& res_data,
                                ColumnString::Offsets& res_offsets,
                                PaddedPODArray<UInt8>& null_map) {
        auto len = ts.size();
        res_offsets.resize(len);
        null_map.resize_fill(len, false);

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            const auto& t = ts[i];
            const auto [new_offset, is_null] = Transform::execute(
                    t, StringRef(format.c_str(), format.size()), res_data, offset);

            res_offsets[i] = new_offset;
            null_map[i] = is_null;
        }
    }
};

template <typename FromType, typename ToType, typename Transform>
struct Transformer {
    static void vector(const PaddedPODArray<FromType>& vec_from, PaddedPODArray<ToType>& vec_to,
                       NullMap& null_map) {
        size_t size = vec_from.size();
        vec_to.resize(size);
        null_map.resize_fill(size, false);

        for (size_t i = 0; i < size; ++i) {
            vec_to[i] = Transform::execute(vec_from[i], reinterpret_cast<bool&>(null_map[i]));
        }
    }
};

template <typename FromType, typename ToType, typename Transform>
struct DateTimeTransformImpl {
    static Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                          size_t /*input_rows_count*/) {
        using Op = Transformer<FromType, ToType, Transform>;

        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        if (const auto* sources = check_and_get_column<ColumnVector<FromType>>(source_col.get())) {
            auto col_to = ColumnVector<ToType>::create();
            auto null_map = ColumnVector<UInt8>::create();
            Op::vector(sources->get_data(), col_to->get_data(), null_map->get_data());
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_to), std::move(null_map)));
        } else {
            return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        Transform::name);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized