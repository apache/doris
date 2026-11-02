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

#include "exprs/function/function_date_or_datetime_computation.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

namespace {

constexpr size_t weekday_hash(std::string_view weekday) {
    const auto first = static_cast<unsigned char>(weekday[0]) | 0x20U;
    const auto second = static_cast<unsigned char>(weekday[1]) | 0x20U;
    return (first ^ (second * 26U) ^ (weekday.size() * 30U)) & 31U;
}

template <typename T>
bool weekday_chunk_equal(const char* input, const char* expected) {
    T actual;
    T target;
    std::memcpy(&actual, input, sizeof(T));
    std::memcpy(&target, expected, sizeof(T));
    return (actual | static_cast<T>(0x2020202020202020ULL)) == target;
}

// Return ISO weekday 1..7, or 0 for anything except a supported English alias.
inline int parse_weekday(std::string_view weekday) {
    struct Entry {
        std::string_view name;
        int day = 0;
    };
    static constexpr std::array<Entry, 21> names = {{
            {.name = "mo", .day = 1}, {.name = "mon", .day = 1}, {.name = "monday", .day = 1},
            {.name = "tu", .day = 2}, {.name = "tue", .day = 2}, {.name = "tuesday", .day = 2},
            {.name = "we", .day = 3}, {.name = "wed", .day = 3}, {.name = "wednesday", .day = 3},
            {.name = "th", .day = 4}, {.name = "thu", .day = 4}, {.name = "thursday", .day = 4},
            {.name = "fr", .day = 5}, {.name = "fri", .day = 5}, {.name = "friday", .day = 5},
            {.name = "sa", .day = 6}, {.name = "sat", .day = 6}, {.name = "saturday", .day = 6},
            {.name = "su", .day = 7}, {.name = "sun", .day = 7}, {.name = "sunday", .day = 7},
    }};
    // The hash constants distinguish all 21 aliases in 32 slots: "mon" hashes to 17,
    // "monday" to 31. Verify every placement at compile time instead of handling collisions.
    static constexpr auto table = [] {
        std::array<Entry, 32> result {};
        for (const auto& entry : names) {
            result[weekday_hash(entry.name)] = entry;
        }
        return result;
    }();
    static_assert(std::ranges::all_of(names, [](const auto& entry) {
        return table[weekday_hash(entry.name)].name == entry.name;
    }));

    // Empty/single-byte input cannot supply the hash characters; no alias exceeds 9 bytes.
    if (weekday.size() < 2 || weekday.size() > 9) {
        return 0;
    }
    const auto& entry = table[weekday_hash(weekday)];
    if (entry.name.size() != weekday.size()) {
        return 0;
    }
    // A perfect hash is collision-free only for stored keys. For example, "monddy" must
    // still be rejected even though its first two characters and length match "monday".
    const char* input = weekday.data();
    const char* expected = entry.name.data();
    const auto length = weekday.size();
    // Fixed-size loads fold ASCII case a word at a time without reading outside the view.
    // The two 4-byte loads overlap for 6/7-byte names; a 9-byte name needs 8 bytes plus its tail.
    bool matches;
    if (length <= 3) {
        matches = weekday_chunk_equal<uint16_t>(input, expected) &&
                  (static_cast<unsigned char>(input[length - 1]) | 0x20U) == expected[length - 1];
    } else if (length <= 8) {
        matches = weekday_chunk_equal<uint32_t>(input, expected) &&
                  weekday_chunk_equal<uint32_t>(input + length - 4, expected + length - 4);
    } else {
        matches = weekday_chunk_equal<uint64_t>(input, expected) &&
                  (static_cast<unsigned char>(input[8]) | 0x20U) == expected[8];
    }
    return matches ? entry.day : 0;
}

template <bool IsPrevious>
class FunctionRelativeDay : public IFunction {
public:
    static constexpr auto name = IsPrevious ? "previous_day" : "next_day";
    static FunctionPtr create() { return std::make_shared<FunctionRelativeDay<IsPrevious>>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        auto result = std::make_shared<DataTypeDateV2>();
        if (arguments[0].type->is_nullable() || arguments[1].type->is_nullable()) {
            return make_nullable(std::move(result));
        }
        return result;
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        CHECK_EQ(arguments.size(), 2);
        auto res = ColumnDateV2::create();
        res->reserve(input_rows_count);
        const auto& date_column = block.get_by_position(arguments[0]).column;
        const auto& week_column = block.get_by_position(arguments[1]).column;
        const auto result_nullable = block.get_by_position(result).type->is_nullable();
        ColumnUInt8::MutablePtr null_map;
        if (result_nullable) {
            null_map = ColumnUInt8::create(input_rows_count, 0);
        }
        Status status;
        auto* null_map_data = null_map ? &null_map->get_data() : nullptr;
        auto date_type = block.get_by_position(arguments[0]).type->get_primitive_type();
        if (date_type == TYPE_TIMESTAMP_NS) {
            status = execute_vector<TYPE_TIMESTAMP_NS>(input_rows_count, date_column, week_column,
                                                       *res, null_map_data);
        } else {
            DORIS_CHECK_EQ(date_type, TYPE_DATEV2);
            status = execute_vector<TYPE_DATEV2>(input_rows_count, date_column, week_column, *res,
                                                 null_map_data);
        }
        if (!status.ok()) {
            return status;
        }
        if (result_nullable) {
            block.replace_by_position(result,
                                      ColumnNullable::create(std::move(res), std::move(null_map)));
        } else {
            block.replace_by_position(result, std::move(res));
        }
        return Status::OK();
    }

private:
    static Status compute_relative_day(DateV2Value<DateV2ValueType>& dtv, const int week_day) {
        auto delta_days = IsPrevious ? ((dtv.weekday() + 1) - week_day + 7) % 7
                                     : (week_day - (dtv.weekday() + 1) + 7) % 7;
        delta_days = delta_days == 0 ? 7 : delta_days;
        dtv.date_add_interval<TimeUnit::DAY>(TimeInterval(TimeUnit::DAY, delta_days, IsPrevious));
        return Status::OK();
    }

    template <PrimitiveType DateType>
    static Status execute_vector(size_t input_rows_count, const ColumnPtr& date_column,
                                 const ColumnPtr& week_column, ColumnDateV2& res_col,
                                 NullMap* null_map) {
        const auto date_view = ColumnView<DateType>::create(date_column);
        const auto week_view = ColumnView<TYPE_STRING>::create(week_column);
        int const_week_day = 0;
        if (week_view.is_const && input_rows_count > 0 && !week_view.is_null_at(0)) {
            const_week_day = parse_weekday(week_view.value_at(0));
        }
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (date_view.is_null_at(i) || week_view.is_null_at(i)) {
                DORIS_CHECK(null_map != nullptr);
                (*null_map)[i] = 1;
                res_col.insert_default();
                continue;
            }
            auto dtv = date_v2_from_date_like(date_view.value_at(i));
            auto week = week_view.value_at(i);
            const auto week_day = week_view.is_const ? const_week_day : parse_weekday(week);
            // Validate after NULL propagation: next_day(NULL, 'MONDDY') must remain NULL,
            // even though the constant weekday is parsed before entering the row loop.
            if (week_day == 0) {
                return Status::InvalidArgument("Function {} failed to parse weekday: {}", name,
                                               week);
            }
            RETURN_IF_ERROR(compute_relative_day(dtv, week_day));
            res_col.insert_value(dtv);
        }
        return Status::OK();
    }
};

using FunctionNextDay = FunctionRelativeDay<false>;
using FunctionPreviousDay = FunctionRelativeDay<true>;

} // namespace

struct NowFunctionName {
    static constexpr auto name = "now";
};

//TODO: remove the inter-layer CurrentDateTimeImpl
using FunctionNow = FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, false>>;

using FunctionNowWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, true>>;

struct CurDateFunctionName {
    static constexpr auto name = "curdate";
};

FunctionBuilderPtr createCurDateFunctionBuilderFunction() {
    return std::make_shared<CurrentDateFunctionBuilder<CurDateFunctionName>>();
}

struct CurTimeFunctionName {
    static constexpr auto name = "curtime";
};

using FunctionCurTime = FunctionCurrentDateOrDateTime<CurrentTimeImpl<CurTimeFunctionName>>;
using FunctionUtcTimeStamp = FunctionCurrentDateOrDateTime<UtcImpl<PrimitiveType::TYPE_DATETIMEV2>>;
using FunctionUtcDate = FunctionCurrentDateOrDateTime<UtcImpl<PrimitiveType::TYPE_DATEV2>>;
using FunctionUtcTime = FunctionCurrentDateOrDateTime<UtcImpl<PrimitiveType::TYPE_TIMEV2>>;
using FunctionTimeToSec = FunctionCurrentDateOrDateTime<TimeToSecImpl>;
using FunctionSecToTime = FunctionCurrentDateOrDateTime<SecToTimeImpl>;
using FunctionMicroSecToDateTime = TimestampToDateTime<MicroSec>;
using FunctionMilliSecToDateTime = TimestampToDateTime<MilliSec>;
using FunctionSecToDateTime = TimestampToDateTime<Sec>;
using FunctionPeriodAdd = FunctionNeedsToHandleNull<PeriodAddImpl, PrimitiveType::TYPE_BIGINT>;
using FunctionPeriodDiff = FunctionNeedsToHandleNull<PeriodDiffImpl, PrimitiveType::TYPE_BIGINT>;

// V2 function type aliases for DATEV2 and DATETIMEV2
using FunctionAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<TYPE_DATEV2>>;
using FunctionAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl<TYPE_DATEV2>>;
using FunctionAddMonths = FunctionDateOrDateTimeComputation<AddMonthsImpl<TYPE_DATEV2>>;
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_DATEV2>>;
using FunctionAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl<TYPE_DATEV2>>;

using FunctionSubDays = FunctionDateOrDateTimeComputation<SubtractDaysImpl<TYPE_DATEV2>>;
using FunctionSubWeeks = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<TYPE_DATEV2>>;
using FunctionSubMonths = FunctionDateOrDateTimeComputation<SubtractMonthsImpl<TYPE_DATEV2>>;
using FunctionSubQuarters = FunctionDateOrDateTimeComputation<SubtractQuartersImpl<TYPE_DATEV2>>;
using FunctionSubYears = FunctionDateOrDateTimeComputation<SubtractYearsImpl<TYPE_DATEV2>>;

using FunctionToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<TYPE_DATEV2>>;
using FunctionToWeekTwoArgs = FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<TYPE_DATEV2>>;

using FunctionDatetimeAddMicroseconds =
        FunctionDateOrDateTimeComputation<AddMicrosecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddMilliseconds =
        FunctionDateOrDateTimeComputation<AddMillisecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddSeconds =
        FunctionDateOrDateTimeComputation<AddSecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddMinutes =
        FunctionDateOrDateTimeComputation<AddMinutesImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddHours = FunctionDateOrDateTimeComputation<AddHoursImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddMonths = FunctionDateOrDateTimeComputation<AddMonthsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl<TYPE_DATETIMEV2>>;

#define TIMESTAMP_NS_COMPUTATION_ALIAS(NAME, IMPL) \
    using FunctionTimeStampNs##NAME = FunctionDateOrDateTimeComputation<IMPL<TYPE_TIMESTAMP_NS>>

TIMESTAMP_NS_COMPUTATION_ALIAS(AddMicroseconds, AddMicrosecondsImpl);
using FunctionTimeStampNsAddNanoseconds = FunctionDateOrDateTimeComputation<AddNanosecondsImpl>;
TIMESTAMP_NS_COMPUTATION_ALIAS(AddMilliseconds, AddMillisecondsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddSeconds, AddSecondsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddMinutes, AddMinutesImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddHours, AddHoursImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddDays, AddDaysImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddWeeks, AddWeeksImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddMonths, AddMonthsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddYears, AddYearsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(AddQuarters, AddQuartersImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubMicroseconds, SubtractMicrosecondsImpl);
using FunctionTimeStampNsSubNanoseconds =
        FunctionDateOrDateTimeComputation<SubtractNanosecondsImpl>;
TIMESTAMP_NS_COMPUTATION_ALIAS(SubMilliseconds, SubtractMillisecondsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubSeconds, SubtractSecondsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubMinutes, SubtractMinutesImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubHours, SubtractHoursImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubDays, SubtractDaysImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubWeeks, SubtractWeeksImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubMonths, SubtractMonthsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubQuarters, SubtractQuartersImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(SubYears, SubtractYearsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(ToYearWeekTwoArgs, ToYearWeekTwoArgsImpl);
TIMESTAMP_NS_COMPUTATION_ALIAS(ToWeekTwoArgs, ToWeekTwoArgsImpl);

using FunctionTimestamptzAddMicroseconds =
        FunctionDateOrDateTimeComputation<AddMicrosecondsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddMilliseconds =
        FunctionDateOrDateTimeComputation<AddMillisecondsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddSeconds =
        FunctionDateOrDateTimeComputation<AddSecondsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddMinutes =
        FunctionDateOrDateTimeComputation<AddMinutesImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddHours =
        FunctionDateOrDateTimeComputation<AddHoursImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddWeeks =
        FunctionDateOrDateTimeComputation<AddWeeksImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddMonths =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzAddYears =
        FunctionDateOrDateTimeComputation<AddYearsImpl<TYPE_TIMESTAMPTZ>>;
#define FUNCTION_TIME_UNION_CAL(TYPE)                                                   \
    using FunctionDatetimeAdd##TYPE =                                                   \
            FunctionDateOrDateTimeComputation<Add##TYPE##Impl<TYPE_DATETIMEV2>>;        \
    using FunctionDatetimeSub##TYPE =                                                   \
            FunctionDateOrDateTimeComputation<Subtract##TYPE##Impl<TYPE_DATETIMEV2>>;   \
    using FunctionTimeStampNsAdd##TYPE =                                                \
            FunctionDateOrDateTimeComputation<Add##TYPE##Impl<TYPE_TIMESTAMP_NS>>;      \
    using FunctionTimeStampNsSub##TYPE =                                                \
            FunctionDateOrDateTimeComputation<Subtract##TYPE##Impl<TYPE_TIMESTAMP_NS>>; \
    using FunctionTimestamptzAdd##TYPE =                                                \
            FunctionDateOrDateTimeComputation<Add##TYPE##Impl<TYPE_TIMESTAMPTZ>>;       \
    using FunctionTimestamptzSub##TYPE =                                                \
            FunctionDateOrDateTimeComputation<Subtract##TYPE##Impl<TYPE_TIMESTAMPTZ>>;

FUNCTION_TIME_UNION_CAL(SecondMicrosecond);
FUNCTION_TIME_UNION_CAL(MinuteMicrosecond);
FUNCTION_TIME_UNION_CAL(MinuteSecond);
FUNCTION_TIME_UNION_CAL(HourMicrosecond);
FUNCTION_TIME_UNION_CAL(HourSecond);
FUNCTION_TIME_UNION_CAL(HourMinute);
FUNCTION_TIME_UNION_CAL(DayMicrosecond);
FUNCTION_TIME_UNION_CAL(DaySecond);
FUNCTION_TIME_UNION_CAL(DayMinute);
FUNCTION_TIME_UNION_CAL(DayHour);
FUNCTION_TIME_UNION_CAL(YearMonth);

using FunctionDatetimeAddQuarters =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubMicroseconds =
        FunctionDateOrDateTimeComputation<SubtractMicrosecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubMilliseconds =
        FunctionDateOrDateTimeComputation<SubtractMillisecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubSeconds =
        FunctionDateOrDateTimeComputation<SubtractSecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubMinutes =
        FunctionDateOrDateTimeComputation<SubtractMinutesImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubHours =
        FunctionDateOrDateTimeComputation<SubtractHoursImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubDays =
        FunctionDateOrDateTimeComputation<SubtractDaysImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubWeeks =
        FunctionDateOrDateTimeComputation<SubtractWeeksImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubMonths =
        FunctionDateOrDateTimeComputation<SubtractMonthsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubQuarters =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeSubYears =
        FunctionDateOrDateTimeComputation<SubtractYearsImpl<TYPE_DATETIMEV2>>;

using FunctionTimestamptzAddQuarters =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubMicroseconds =
        FunctionDateOrDateTimeComputation<SubtractMicrosecondsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubMilliseconds =
        FunctionDateOrDateTimeComputation<SubtractMillisecondsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubSeconds =
        FunctionDateOrDateTimeComputation<SubtractSecondsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubMinutes =
        FunctionDateOrDateTimeComputation<SubtractMinutesImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubHours =
        FunctionDateOrDateTimeComputation<SubtractHoursImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubDays =
        FunctionDateOrDateTimeComputation<SubtractDaysImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubWeeks =
        FunctionDateOrDateTimeComputation<SubtractWeeksImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubMonths =
        FunctionDateOrDateTimeComputation<SubtractMonthsImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubQuarters =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<TYPE_TIMESTAMPTZ>>;
using FunctionTimestamptzSubYears =
        FunctionDateOrDateTimeComputation<SubtractYearsImpl<TYPE_TIMESTAMPTZ>>;

using FunctionAddTimeDatetime = FunctionNeedsToHandleNull<AddTimeDatetimeImpl, TYPE_DATETIMEV2>;
using FunctionAddTimeTimeStampNs =
        FunctionNeedsToHandleNull<AddTimeTimeStampNsImpl, TYPE_TIMESTAMP_NS>;
using FunctionAddTimeTime = FunctionNeedsToHandleNull<AddTimeTimeImpl, TYPE_TIMEV2>;
using FunctionAddTimeTimestampTz =
        FunctionNeedsToHandleNull<AddTimeTimestamptzImpl, TYPE_TIMESTAMPTZ>;
using FunctionSubTimeDatetime = FunctionNeedsToHandleNull<SubTimeDatetimeImpl, TYPE_DATETIMEV2>;
using FunctionSubTimeTimeStampNs =
        FunctionNeedsToHandleNull<SubTimeTimeStampNsImpl, TYPE_TIMESTAMP_NS>;
using FunctionSubTimeTime = FunctionNeedsToHandleNull<SubTimeTimeImpl, TYPE_TIMEV2>;
using FunctionSubTimeTimestampTz =
        FunctionNeedsToHandleNull<SubTimeTimestamptzImpl, TYPE_TIMESTAMPTZ>;

#define FUNCTION_TIME_DIFF(NAME, IMPL, TYPE) using NAME##_##TYPE = FunctionTimeDiff<IMPL<TYPE>>;

#define ALL_FUNCTION_TIME_DIFF(NAME, IMPL)            \
    FUNCTION_TIME_DIFF(NAME, IMPL, TYPE_DATETIMEV2)   \
    FUNCTION_TIME_DIFF(NAME, IMPL, TYPE_TIMESTAMP_NS) \
    FUNCTION_TIME_DIFF(NAME, IMPL, TYPE_DATEV2)       \
    FUNCTION_TIME_DIFF(NAME, IMPL, TYPE_TIMESTAMPTZ)
// these diff functions accept all v2 types. but for v1 only datetime.
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeDateDiff, DateDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeTimeDiff, TimeDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeYearsDiff, YearsDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeQuartersDiff, QuartersDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeMonthsDiff, MonthsDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeWeeksDiff, WeeksDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeHoursDiff, HoursDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeMinutesDiff, MintuesDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeSecondsDiff, SecondsDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeDaysDiff, DaysDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeMilliSecondsDiff, MilliSecondsDiffImpl)
ALL_FUNCTION_TIME_DIFF(FunctionDatetimeMicroSecondsDiff, MicroSecondsDiffImpl)
using FunctionTimeStampNsNanoSecondsDiff = FunctionTimeDiff<NanoSecondsDiffImpl>;

using FunctionDatetimeToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeToWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<TYPE_DATETIMEV2>>;

// Date/time functions are deliberately registered together to keep the factory surface auditable.
// NOLINTNEXTLINE(readability-function-size)
void register_function_date_time_computation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionNextDay>();
    factory.register_function<FunctionPreviousDay>();
    factory.register_function<FunctionNow>();
    factory.register_function<FunctionNowWithPrecision>();
    factory.register_function(CurDateFunctionName::name, &createCurDateFunctionBuilderFunction);
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionUtcTimeStamp>();
    factory.register_function<FunctionUtcDate>();
    factory.register_function<FunctionUtcTime>();
    factory.register_function<FunctionTimeToSec>();
    factory.register_function<FunctionSecToTime>();
    factory.register_function<FunctionMicroSecToDateTime>();
    factory.register_function<FunctionMilliSecToDateTime>();
    factory.register_function<FunctionSecToDateTime>();
    factory.register_function<FunctionMonthsBetween>();
    factory.register_function<FunctionTime>();
    factory.register_function<FunctionGetFormat>();
    factory.register_function<FunctionPeriodAdd>();
    factory.register_function<FunctionPeriodDiff>();

    // alias
    factory.register_alias("days_add", "date_add");
    factory.register_alias("days_add", "adddate");
    factory.register_alias("months_add", "add_months");
    factory.register_alias("days_sub", "date_sub");
    factory.register_alias("days_sub", "subdate");
    factory.register_alias("now", "current_timestamp");
    factory.register_alias("now", "localtime");
    factory.register_alias("now", "localtimestamp");
    factory.register_alias("curdate", "current_date");
    factory.register_alias("curtime", "current_time");

    // Register V2 functions (DATEV2 and DATETIMEV2)
    factory.register_function<FunctionAddDays>();
    factory.register_function<FunctionAddWeeks>();
    factory.register_function<FunctionAddMonths>();
    factory.register_function<FunctionAddYears>();
    factory.register_function<FunctionAddQuarters>();

    factory.register_function<FunctionDatetimeAddMicroseconds>();
    factory.register_function<FunctionDatetimeAddMilliseconds>();
    factory.register_function<FunctionDatetimeAddSeconds>();
    factory.register_function<FunctionDatetimeAddMinutes>();
    factory.register_function<FunctionDatetimeAddHours>();
    factory.register_function<FunctionDatetimeAddDays>();
    factory.register_function<FunctionDatetimeAddWeeks>();
    factory.register_function<FunctionDatetimeAddMonths>();
    factory.register_function<FunctionDatetimeAddYears>();
    factory.register_function<FunctionDatetimeAddQuarters>();
    factory.register_function<FunctionTimeStampNsAddMicroseconds>();
    factory.register_function<FunctionTimeStampNsAddNanoseconds>();
    factory.register_function<FunctionTimeStampNsAddMilliseconds>();
    factory.register_function<FunctionTimeStampNsAddSeconds>();
    factory.register_function<FunctionTimeStampNsAddMinutes>();
    factory.register_function<FunctionTimeStampNsAddHours>();
    factory.register_function<FunctionTimeStampNsAddDays>();
    factory.register_function<FunctionTimeStampNsAddWeeks>();
    factory.register_function<FunctionTimeStampNsAddMonths>();
    factory.register_function<FunctionTimeStampNsAddYears>();
    factory.register_function<FunctionTimeStampNsAddQuarters>();
    factory.register_function<FunctionTimestamptzAddMicroseconds>();
    factory.register_function<FunctionTimestamptzAddMilliseconds>();
    factory.register_function<FunctionTimestamptzAddSeconds>();
    factory.register_function<FunctionTimestamptzAddMinutes>();
    factory.register_function<FunctionTimestamptzAddHours>();
    factory.register_function<FunctionTimestamptzAddDays>();
    factory.register_function<FunctionTimestamptzAddWeeks>();
    factory.register_function<FunctionTimestamptzAddMonths>();
    factory.register_function<FunctionTimestamptzAddYears>();
    factory.register_function<FunctionTimestamptzAddQuarters>();

#define REGISTER_TIME_UNION_CAL(TYPE)                          \
    factory.register_function<FunctionDatetimeAdd##TYPE>();    \
    factory.register_function<FunctionDatetimeSub##TYPE>();    \
    factory.register_function<FunctionTimeStampNsAdd##TYPE>(); \
    factory.register_function<FunctionTimeStampNsSub##TYPE>(); \
    factory.register_function<FunctionTimestamptzAdd##TYPE>(); \
    factory.register_function<FunctionTimestamptzSub##TYPE>();

    REGISTER_TIME_UNION_CAL(SecondMicrosecond);
    REGISTER_TIME_UNION_CAL(MinuteMicrosecond);
    REGISTER_TIME_UNION_CAL(MinuteSecond);
    REGISTER_TIME_UNION_CAL(HourMicrosecond);
    REGISTER_TIME_UNION_CAL(HourSecond);
    REGISTER_TIME_UNION_CAL(HourMinute);
    REGISTER_TIME_UNION_CAL(DayMicrosecond);
    REGISTER_TIME_UNION_CAL(DaySecond);
    REGISTER_TIME_UNION_CAL(DayMinute);
    REGISTER_TIME_UNION_CAL(DayHour);
    REGISTER_TIME_UNION_CAL(YearMonth);

    factory.register_function<FunctionSubDays>();
    factory.register_function<FunctionSubMonths>();
    factory.register_function<FunctionSubYears>();
    factory.register_function<FunctionSubQuarters>();
    factory.register_function<FunctionSubWeeks>();

    factory.register_function<FunctionDatetimeSubMicroseconds>();
    factory.register_function<FunctionDatetimeSubMilliseconds>();
    factory.register_function<FunctionDatetimeSubSeconds>();
    factory.register_function<FunctionDatetimeSubMinutes>();
    factory.register_function<FunctionDatetimeSubHours>();
    factory.register_function<FunctionDatetimeSubDays>();
    factory.register_function<FunctionDatetimeSubMonths>();
    factory.register_function<FunctionDatetimeSubYears>();
    factory.register_function<FunctionDatetimeSubQuarters>();
    factory.register_function<FunctionDatetimeSubWeeks>();
    factory.register_function<FunctionTimeStampNsSubMicroseconds>();
    factory.register_function<FunctionTimeStampNsSubNanoseconds>();
    factory.register_function<FunctionTimeStampNsSubMilliseconds>();
    factory.register_function<FunctionTimeStampNsSubSeconds>();
    factory.register_function<FunctionTimeStampNsSubMinutes>();
    factory.register_function<FunctionTimeStampNsSubHours>();
    factory.register_function<FunctionTimeStampNsSubDays>();
    factory.register_function<FunctionTimeStampNsSubMonths>();
    factory.register_function<FunctionTimeStampNsSubYears>();
    factory.register_function<FunctionTimeStampNsSubQuarters>();
    factory.register_function<FunctionTimeStampNsSubWeeks>();

    factory.register_function<FunctionTimestamptzSubMicroseconds>();
    factory.register_function<FunctionTimestamptzSubMilliseconds>();
    factory.register_function<FunctionTimestamptzSubSeconds>();
    factory.register_function<FunctionTimestamptzSubMinutes>();
    factory.register_function<FunctionTimestamptzSubHours>();
    factory.register_function<FunctionTimestamptzSubDays>();
    factory.register_function<FunctionTimestamptzSubMonths>();
    factory.register_function<FunctionTimestamptzSubYears>();
    factory.register_function<FunctionTimestamptzSubQuarters>();
    factory.register_function<FunctionTimestamptzSubWeeks>();

    factory.register_function<FunctionAddTimeDatetime>();
    factory.register_function<FunctionAddTimeTimeStampNs>();
    factory.register_function<FunctionAddTimeTime>();
    factory.register_function<FunctionAddTimeTimestampTz>();
    factory.register_function<FunctionSubTimeDatetime>();
    factory.register_function<FunctionSubTimeTimeStampNs>();
    factory.register_function<FunctionSubTimeTime>();
    factory.register_function<FunctionSubTimeTimestampTz>();

#define REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE) factory.register_function<NAME##_##TYPE>();

#define REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(NAME)            \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_DATETIMEV2)   \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_TIMESTAMP_NS) \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_DATEV2)       \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_TIMESTAMPTZ)

    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeDateDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeTimeDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeYearsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeQuartersDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeMonthsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeWeeksDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeHoursDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeMinutesDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeSecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeDaysDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeMilliSecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeMicroSecondsDiff)
    factory.register_function<FunctionTimeStampNsNanoSecondsDiff>();

    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();
    factory.register_function<FunctionDatetimeToYearWeekTwoArgs>();
    factory.register_function<FunctionDatetimeToWeekTwoArgs>();
    factory.register_function<FunctionTimeStampNsToYearWeekTwoArgs>();
    factory.register_function<FunctionTimeStampNsToWeekTwoArgs>();
}

} // namespace doris
