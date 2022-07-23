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

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionAddSeconds = FunctionDateOrDateTimeComputation<
        AddSecondsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddSecondsV2 =
        FunctionDateOrDateTimeComputation<AddSecondsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddMinutes = FunctionDateOrDateTimeComputation<
        AddMinutesImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddHours =
        FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddDays =
        FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddWeeks =
        FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddMonths =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddMinutesV2 =
        FunctionDateOrDateTimeComputation<AddMinutesImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddHoursV2 =
        FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddDaysV2 =
        FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddWeeksV2 =
        FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddMonthsV2 =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<
        AddQuartersImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddQuartersV2 =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddYears =
        FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddYearsV2 =
        FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;

using FunctionSubSeconds = FunctionDateOrDateTimeComputation<
        SubtractSecondsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubMinutes = FunctionDateOrDateTimeComputation<
        SubtractMinutesImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubHours = FunctionDateOrDateTimeComputation<
        SubtractHoursImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubDays = FunctionDateOrDateTimeComputation<
        SubtractDaysImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubWeeks = FunctionDateOrDateTimeComputation<
        SubtractWeeksImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubMonths = FunctionDateOrDateTimeComputation<
        SubtractMonthsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubQuarters = FunctionDateOrDateTimeComputation<
        SubtractQuartersImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubYears = FunctionDateOrDateTimeComputation<
        SubtractYearsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionSubSecondsV2 = FunctionDateOrDateTimeComputation<
        SubtractSecondsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubMinutesV2 = FunctionDateOrDateTimeComputation<
        SubtractMinutesImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubHoursV2 = FunctionDateOrDateTimeComputation<
        SubtractHoursImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubDaysV2 =
        FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubWeeksV2 = FunctionDateOrDateTimeComputation<
        SubtractWeeksImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubMonthsV2 = FunctionDateOrDateTimeComputation<
        SubtractMonthsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubQuartersV2 = FunctionDateOrDateTimeComputation<
        SubtractQuartersImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubYearsV2 = FunctionDateOrDateTimeComputation<
        SubtractYearsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;

using FunctionDateDiff = FunctionDateOrDateTimeComputation<DateDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionTimeDiff = FunctionDateOrDateTimeComputation<TimeDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionYearsDiff = FunctionDateOrDateTimeComputation<YearsDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionMonthsDiff = FunctionDateOrDateTimeComputation<MonthsDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionDaysDiff = FunctionDateOrDateTimeComputation<DaysDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionWeeksDiff = FunctionDateOrDateTimeComputation<WeeksDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionHoursDiff = FunctionDateOrDateTimeComputation<HoursDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionMinutesDiff = FunctionDateOrDateTimeComputation<MintueSDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;
using FunctionSecondsDiff = FunctionDateOrDateTimeComputation<SecondsDiffImpl<
        VecDateTimeValue, VecDateTimeValue, DataTypeDateTime, DataTypeDateTime, Int64, Int64>>;

using FunctionToYearWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<VecDateTimeValue, DataTypeDateTime, Int64>>;
using FunctionToYearWeekTwoArgsV2 = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<DateV2Value<DateV2ValueType>, DataTypeDateV2, UInt32>>;
using FunctionToWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<VecDateTimeValue, DataTypeDateTime, Int64>>;
using FunctionToWeekTwoArgsV2 = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<DateV2Value<DateV2ValueType>, DataTypeDateV2, UInt32>>;

using FunctionDatetimeV2AddSeconds = FunctionDateOrDateTimeComputation<
        AddSecondsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddMinutes = FunctionDateOrDateTimeComputation<
        AddMinutesImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddHours = FunctionDateOrDateTimeComputation<
        AddHoursImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddDays = FunctionDateOrDateTimeComputation<
        AddDaysImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddWeeks = FunctionDateOrDateTimeComputation<
        AddWeeksImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddMonths = FunctionDateOrDateTimeComputation<
        AddMonthsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;

using FunctionDatetimeV2AddQuarters = FunctionDateOrDateTimeComputation<
        AddQuartersImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddYears = FunctionDateOrDateTimeComputation<
        AddYearsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;

using FunctionDatetimeV2SubSeconds = FunctionDateOrDateTimeComputation<
        SubtractSecondsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubMinutes = FunctionDateOrDateTimeComputation<
        SubtractMinutesImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubHours = FunctionDateOrDateTimeComputation<
        SubtractHoursImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubDays = FunctionDateOrDateTimeComputation<
        SubtractDaysImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubWeeks = FunctionDateOrDateTimeComputation<
        SubtractWeeksImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubMonths = FunctionDateOrDateTimeComputation<
        SubtractMonthsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubQuarters = FunctionDateOrDateTimeComputation<
        SubtractQuartersImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubYears = FunctionDateOrDateTimeComputation<
        SubtractYearsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;

#define FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, TYPE1, TYPE2, ARG1, ARG2, DATE_VALUE1, \
                                      DATE_VALUE2)                                       \
    using NAME##_##TYPE1##_##TYPE2 = FunctionDateOrDateTimeComputation<                  \
            IMPL<DATE_VALUE1, DATE_VALUE2, TYPE1, TYPE2, ARG1, ARG2>>;

#define ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL)                                              \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateTimeV2, UInt64,      \
                                  UInt64, DateV2Value<DateTimeV2ValueType>,                        \
                                  DateV2Value<DateTimeV2ValueType>)                                \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateV2, UInt64, UInt32,  \
                                  DateV2Value<DateTimeV2ValueType>, DateV2Value<DateV2ValueType>)  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateTimeV2, UInt32, UInt64,  \
                                  DateV2Value<DateV2ValueType>, DateV2Value<DateTimeV2ValueType>)  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateTime, UInt64, Int64, \
                                  DateV2Value<DateTimeV2ValueType>, VecDateTimeValue)              \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTime, DataTypeDateTimeV2, Int64, UInt64, \
                                  VecDateTimeValue, DateV2Value<DateTimeV2ValueType>)              \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTime, DataTypeDateV2, Int64, UInt32,     \
                                  VecDateTimeValue, DateV2Value<DateV2ValueType>)                  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateTime, UInt32, Int64,     \
                                  DateV2Value<DateV2ValueType>, VecDateTimeValue)                  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32,      \
                                  DateV2Value<DateV2ValueType>, DateV2Value<DateV2ValueType>)

ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2DateDiff, DateDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2TimeDiff, TimeDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2YearsDiff, YearsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MonthsDiff, MonthsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2WeeksDiff, WeeksDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2HoursDiff, HoursDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MinutesDiff, MintueSDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2SecondsDiff, SecondsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2DaysDiff, DaysDiffImpl)

using FunctionDatetimeV2ToYearWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<DateV2Value<DateTimeV2ValueType>, DataTypeDateTimeV2, UInt64>>;
using FunctionDatetimeV2ToWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<DateV2Value<DateTimeV2ValueType>, DataTypeDateTimeV2, UInt64>>;

struct NowFunctionName {
    static constexpr auto name = "now";
};

struct CurrentTimestampFunctionName {
    static constexpr auto name = "current_timestamp";
};

struct LocalTimeFunctionName {
    static constexpr auto name = "localtime";
};

struct LocalTimestampFunctionName {
    static constexpr auto name = "localtimestamp";
};

using FunctionNow = FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName>>;
using FunctionCurrentTimestamp =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<CurrentTimestampFunctionName>>;
using FunctionLocalTime = FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimeFunctionName>>;
using FunctionLocalTimestamp =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimestampFunctionName>>;

struct CurDateFunctionName {
    static constexpr auto name = "curdate";
};

struct CurrentDateFunctionName {
    static constexpr auto name = "current_date";
};

FunctionBuilderPtr createCurrentDateFunctionBuilderFunction() {
    return std::make_shared<CurrentDateFunctionBuilder<CurrentDateFunctionName>>();
}
FunctionBuilderPtr createCurDateFunctionBuilderFunction() {
    return std::make_shared<CurrentDateFunctionBuilder<CurDateFunctionName>>();
}

struct CurTimeFunctionName {
    static constexpr auto name = "curtime";
};
struct CurrentTimeFunctionName {
    static constexpr auto name = "current_time";
};

using FunctionCurTime = FunctionCurrentDateOrDateTime<CurrentTimeImpl<CurTimeFunctionName>>;
using FunctionCurrentTime = FunctionCurrentDateOrDateTime<CurrentTimeImpl<CurrentTimeFunctionName>>;
using FunctionUtcTimeStamp = FunctionCurrentDateOrDateTime<UtcTimestampImpl>;

void register_function_date_time_computation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAddSeconds>();
    factory.register_function<FunctionAddMinutes>();
    factory.register_function<FunctionAddHours>();
    factory.register_function<FunctionAddDays>();
    factory.register_function<FunctionAddSecondsV2>();
    factory.register_function<FunctionAddMinutesV2>();
    factory.register_function<FunctionAddHoursV2>();
    factory.register_function<FunctionAddDaysV2>();
    factory.register_alias("days_add", "date_add");
    factory.register_alias("days_add", "adddate");
    factory.register_function<FunctionAddWeeks>();
    factory.register_function<FunctionAddMonths>();
    factory.register_function<FunctionAddYears>();
    factory.register_function<FunctionAddQuarters>();
    factory.register_function<FunctionAddWeeksV2>();
    factory.register_function<FunctionAddMonthsV2>();
    factory.register_function<FunctionAddYearsV2>();
    factory.register_function<FunctionAddQuartersV2>();

    factory.register_function<FunctionDatetimeV2AddSeconds>();
    factory.register_function<FunctionDatetimeV2AddMinutes>();
    factory.register_function<FunctionDatetimeV2AddHours>();
    factory.register_function<FunctionDatetimeV2AddDays>();
    factory.register_function<FunctionDatetimeV2AddWeeks>();
    factory.register_function<FunctionDatetimeV2AddMonths>();
    factory.register_function<FunctionDatetimeV2AddYears>();
    factory.register_function<FunctionDatetimeV2AddQuarters>();

    factory.register_function<FunctionSubSeconds>();
    factory.register_function<FunctionSubMinutes>();
    factory.register_function<FunctionSubHours>();
    factory.register_function<FunctionSubDays>();
    factory.register_function<FunctionSubSecondsV2>();
    factory.register_function<FunctionSubMinutesV2>();
    factory.register_function<FunctionSubHoursV2>();
    factory.register_function<FunctionSubDaysV2>();
    factory.register_alias("days_sub", "date_sub");
    factory.register_alias("days_sub", "subdate");
    factory.register_function<FunctionSubMonths>();
    factory.register_function<FunctionSubYears>();
    factory.register_function<FunctionSubQuarters>();
    factory.register_function<FunctionSubWeeks>();
    factory.register_function<FunctionSubMonthsV2>();
    factory.register_function<FunctionSubYearsV2>();
    factory.register_function<FunctionSubQuartersV2>();
    factory.register_function<FunctionSubWeeksV2>();

    factory.register_function<FunctionDatetimeV2SubSeconds>();
    factory.register_function<FunctionDatetimeV2SubMinutes>();
    factory.register_function<FunctionDatetimeV2SubHours>();
    factory.register_function<FunctionDatetimeV2SubDays>();
    factory.register_function<FunctionDatetimeV2SubMonths>();
    factory.register_function<FunctionDatetimeV2SubYears>();
    factory.register_function<FunctionDatetimeV2SubQuarters>();
    factory.register_function<FunctionDatetimeV2SubWeeks>();

    factory.register_function<FunctionDateDiff>();
    factory.register_function<FunctionTimeDiff>();
    factory.register_function<FunctionYearsDiff>();
    factory.register_function<FunctionMonthsDiff>();
    factory.register_function<FunctionWeeksDiff>();
    factory.register_function<FunctionDaysDiff>();
    factory.register_function<FunctionHoursDiff>();
    factory.register_function<FunctionMinutesDiff>();
    factory.register_function<FunctionSecondsDiff>();

#define REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, TYPE1, TYPE2) \
    factory.register_function<NAME##_##TYPE1##_##TYPE2>();

#define REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME)                                 \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateTimeV2) \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateV2)     \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateTimeV2)     \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateTime)   \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateTime)       \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateV2)         \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTime, DataTypeDateV2)       \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTime, DataTypeDateTimeV2)

    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2DateDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2TimeDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2YearsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MonthsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2WeeksDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2HoursDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MinutesDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2SecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2DaysDiff)

    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();
    factory.register_function<FunctionToYearWeekTwoArgsV2>();
    factory.register_function<FunctionToWeekTwoArgsV2>();
    factory.register_function<FunctionDatetimeV2ToYearWeekTwoArgs>();
    factory.register_function<FunctionDatetimeV2ToWeekTwoArgs>();

    factory.register_function<FunctionNow>();
    factory.register_function<FunctionCurrentTimestamp>();
    factory.register_function<FunctionLocalTime>();
    factory.register_function<FunctionLocalTimestamp>();
    factory.register_function(CurrentDateFunctionName::name,
                              &createCurrentDateFunctionBuilderFunction);
    factory.register_function(CurDateFunctionName::name, &createCurDateFunctionBuilderFunction);
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionCurrentTime>();
    factory.register_function<FunctionUtcTimeStamp>();
}

} // namespace doris::vectorized