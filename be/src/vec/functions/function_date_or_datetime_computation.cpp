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

#include "vec/functions/function_date_or_datetime_computation.h"

#include "runtime/define_primitive_type.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

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
#define FUNCTION_TIME_UNION_CAL(TYPE)                                                 \
    using FunctionDatetimeAdd##TYPE =                                                 \
            FunctionDateOrDateTimeComputation<Add##TYPE##Impl<TYPE_DATETIMEV2>>;      \
    using FunctionDatetimeSub##TYPE =                                                 \
            FunctionDateOrDateTimeComputation<Subtract##TYPE##Impl<TYPE_DATETIMEV2>>; \
    using FunctionTimestamptzAdd##TYPE =                                              \
            FunctionDateOrDateTimeComputation<Add##TYPE##Impl<TYPE_TIMESTAMPTZ>>;     \
    using FunctionTimestamptzSub##TYPE =                                              \
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
using FunctionAddTimeTime = FunctionNeedsToHandleNull<AddTimeTimeImpl, TYPE_TIMEV2>;
using FunctionAddTimeTimestampTz =
        FunctionNeedsToHandleNull<AddTimeTimestamptzImpl, TYPE_TIMESTAMPTZ>;
using FunctionSubTimeDatetime = FunctionNeedsToHandleNull<SubTimeDatetimeImpl, TYPE_DATETIMEV2>;
using FunctionSubTimeTime = FunctionNeedsToHandleNull<SubTimeTimeImpl, TYPE_TIMEV2>;
using FunctionSubTimeTimestampTz =
        FunctionNeedsToHandleNull<SubTimeTimestamptzImpl, TYPE_TIMESTAMPTZ>;

#define FUNCTION_TIME_DIFF(NAME, IMPL, TYPE) using NAME##_##TYPE = FunctionTimeDiff<IMPL<TYPE>>;

#define ALL_FUNCTION_TIME_DIFF(NAME, IMPL)          \
    FUNCTION_TIME_DIFF(NAME, IMPL, TYPE_DATETIMEV2) \
    FUNCTION_TIME_DIFF(NAME, IMPL, TYPE_DATEV2)
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

using FunctionDatetimeToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeToWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<TYPE_DATETIMEV2>>;

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
    factory.register_function<FunctionAddTimeTime>();
    factory.register_function<FunctionAddTimeTimestampTz>();
    factory.register_function<FunctionSubTimeDatetime>();
    factory.register_function<FunctionSubTimeTime>();
    factory.register_function<FunctionSubTimeTimestampTz>();

#define REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE) factory.register_function<NAME##_##TYPE>();

#define REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(NAME)          \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_DATETIMEV2) \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_DATEV2)

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

    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();
    factory.register_function<FunctionDatetimeToYearWeekTwoArgs>();
    factory.register_function<FunctionDatetimeToWeekTwoArgs>();
}

} // namespace doris::vectorized