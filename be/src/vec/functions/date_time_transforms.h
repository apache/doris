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

#include "common/status.h"
#include "runtime/datetime_value.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/exception.h"
#include "vec/core/types.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

#define TIME_FUNCTION_IMPL(CLASS, UNIT, FUNCTION)                     \
    struct CLASS {                                                    \
        static constexpr auto name = #UNIT;                           \
        static inline auto execute(const Int128& t) {                 \
            const auto& date_time_value = (doris::DateTimeValue&)(t); \
            return date_time_value.FUNCTION;                          \
        }                                                             \
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
TIME_FUNCTION_IMPL(ToDaysImpl, to_days, daynr());

struct ToDateImpl {
    static constexpr auto name = "to_date";

    static inline auto execute(const Int128& t) {
        auto dt = binary_cast<Int128, doris::DateTimeValue>(t);
        dt.cast_to_date();
        return binary_cast<doris::DateTimeValue, Int128>(dt);
    }
};
struct DateImpl : public ToDateImpl {
    static constexpr auto name = "date";
};

// TODO: This function look like no need do indeed copy here, we should optimize
// this function
struct TimeStampImpl {
    static constexpr auto name = "timestamp";
    static inline auto execute(const Int128& t) { return t; }
};

struct UnixTimeStampImpl {
    static constexpr auto name = "unix_timestamp";
    static inline int execute(const Int128& t) {
        // TODO: use default time zone, slowly and incorrect, just for test use
        static cctz::time_zone time_zone = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));

        const auto& dt = (doris::DateTimeValue&)(t);
        int64_t timestamp = 0;
        dt.unix_timestamp(&timestamp, time_zone);

        return (timestamp < 0 || timestamp > INT_MAX) ? 0 : timestamp;
    }
};

struct DayNameImpl {
    static constexpr auto name = "dayname";
    static constexpr auto max_size = MAX_DAY_NAME_LEN;

    static inline auto execute(const DateTimeValue& dt, ColumnString::Chars& res_data,
                               size_t& offset) {
        const auto* day_name = dt.day_name();
        if (day_name == nullptr) {
            offset += 1;
            res_data[offset - 1] = 0;
        } else {
            auto len = strlen(day_name);
            memcpy_small_allow_read_write_overflow15(&res_data[offset], day_name, len);
            offset += len + 1;
            res_data[offset - 1] = 0;
        }
        return offset;
    }
};

struct MonthNameImpl {
    static constexpr auto name = "monthname";
    static constexpr auto max_size = MAX_MONTH_NAME_LEN;

    static inline auto execute(const DateTimeValue& dt, ColumnString::Chars& res_data,
                               size_t& offset) {
        const auto* month_name = dt.month_name();
        if (month_name == nullptr) {
            offset += 1;
            res_data[offset - 1] = 0;
        } else {
            auto len = strlen(month_name);
            memcpy_small_allow_read_write_overflow15(&res_data[offset], month_name, len);
            offset += len + 1;
            res_data[offset - 1] = 0;
        }
        return offset;
    }
};

struct DateFormatImpl {
    using FromType = Int128;

    static constexpr auto name = "date_format";

    static inline auto execute(const Int128& t, StringRef format, ColumnString::Chars& res_data,
                               size_t& offset) {
        const auto& dt = (DateTimeValue&)t;
        if (format.size > 128) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair{offset, true};
        }
        char buf[128];
        if (!dt.to_format_string(format.data, format.size, buf)) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair{offset, true};
        }

        auto len = strlen(buf) + 1;
        res_data.insert(buf, buf + len);
        offset += len;
        return std::pair{offset, false};
    }
};

struct FromUnixTimeImpl {
    using FromType = Int32;

    static constexpr auto name = "from_unixtime";

    static inline auto execute(FromType val, StringRef format, ColumnString::Chars& res_data,
                               size_t& offset) {
        // TODO: use default time zone, slowly and incorrect, just for test use
        static cctz::time_zone time_zone = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));

        DateTimeValue dt;
        if (format.size > 128 || val < 0 || val > INT_MAX || !dt.from_unixtime(val, time_zone)) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair{offset, true};
        }

        char buf[128];
        if (!dt.to_format_string(format.data, format.size, buf)) {
            offset += 1;
            res_data.emplace_back(0);
            return std::pair{offset, true};
        }

        auto len = strlen(buf) + 1;
        res_data.insert(buf, buf + len);
        offset += len;
        return std::pair{offset, false};
    }
};

template <typename Transform>
struct TransformerToStringOneArgument {
    static void vector(const PaddedPODArray<Int128>& ts, ColumnString::Chars& res_data,
                       ColumnString::Offsets& res_offsets) {
        auto len = ts.size();
        res_data.resize(len * Transform::max_size);
        res_offsets.resize(len);

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            const auto& t = ts[i];
            const auto& date_time_value = reinterpret_cast<const DateTimeValue&>(t);
            res_offsets[i] = Transform::execute(date_time_value, res_data, offset);
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
    //    static void vector(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, const DateLUTImpl & time_zone)
    static void vector(const PaddedPODArray<FromType>& vec_from, PaddedPODArray<ToType>& vec_to) {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i) vec_to[i] = Transform::execute(vec_from[i]);
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
            Op::vector(sources->get_data(), col_to->get_data());
            block.replace_by_position(result, std::move(col_to));
        } else {
            return Status::RuntimeError(fmt::format(
                    "Illegal column {} of first argument of function {}",
                    block.get_by_position(arguments[0]).column->get_name(), Transform::name));
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized