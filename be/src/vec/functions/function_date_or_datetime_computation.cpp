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
using FunctionDateDiffV2 = FunctionDateOrDateTimeComputation<
        DateDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionTimeDiffV2 = FunctionDateOrDateTimeComputation<
        TimeDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionYearsDiffV2 = FunctionDateOrDateTimeComputation<
        YearsDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionMonthsDiffV2 = FunctionDateOrDateTimeComputation<
        MonthsDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionDaysDiffV2 = FunctionDateOrDateTimeComputation<
        DaysDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionWeeksDiffV2 = FunctionDateOrDateTimeComputation<
        WeeksDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionHoursDiffV2 = FunctionDateOrDateTimeComputation<
        HoursDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionMinutesDiffV2 = FunctionDateOrDateTimeComputation<
        MintueSDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;
using FunctionSecondsDiffV2 = FunctionDateOrDateTimeComputation<
        SecondsDiffImpl<DateV2Value, DateV2Value, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32>>;

using FunctionToYearWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<VecDateTimeValue, DataTypeDateTime, Int64>>;
using FunctionToYearWeekTwoArgsV2 = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<DateV2Value, DataTypeDateV2, UInt32>>;
using FunctionToWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<VecDateTimeValue, DataTypeDateTime, Int64>>;
using FunctionToWeekTwoArgsV2 =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<DateV2Value, DataTypeDateV2, UInt32>>;

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

using FunctionCurDate = FunctionCurrentDateOrDateTime<CurrentDateImpl<CurDateFunctionName>>;
using FunctionCurrentDate = FunctionCurrentDateOrDateTime<CurrentDateImpl<CurrentDateFunctionName>>;

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

    factory.register_function<FunctionDateDiff>();
    factory.register_function<FunctionTimeDiff>();
    factory.register_function<FunctionYearsDiff>();
    factory.register_function<FunctionMonthsDiff>();
    factory.register_function<FunctionWeeksDiff>();
    factory.register_function<FunctionDaysDiff>();
    factory.register_function<FunctionHoursDiff>();
    factory.register_function<FunctionMinutesDiff>();
    factory.register_function<FunctionSecondsDiff>();
    factory.register_function<FunctionDateDiffV2>();
    factory.register_function<FunctionTimeDiffV2>();
    factory.register_function<FunctionYearsDiffV2>();
    factory.register_function<FunctionMonthsDiffV2>();
    factory.register_function<FunctionWeeksDiffV2>();
    factory.register_function<FunctionDaysDiffV2>();
    factory.register_function<FunctionHoursDiffV2>();
    factory.register_function<FunctionMinutesDiffV2>();
    factory.register_function<FunctionSecondsDiffV2>();

    // Functions below make dateV2 be compatible with V1
    factory.register_function<FunctionDateOrDateTimeComputation<DateDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<TimeDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<YearsDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<MonthsDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<WeeksDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<DaysDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<HoursDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<MintueSDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<SecondsDiffImpl<
            VecDateTimeValue, DateV2Value, DataTypeDateTime, DataTypeDateV2, Int64, UInt32>>>();

    factory.register_function<FunctionDateOrDateTimeComputation<DateDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<TimeDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<YearsDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<MonthsDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<WeeksDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<DaysDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<HoursDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<MintueSDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();
    factory.register_function<FunctionDateOrDateTimeComputation<SecondsDiffImpl<
            DateV2Value, VecDateTimeValue, DataTypeDateV2, DataTypeDateTime, UInt32, Int64>>>();

    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();
    factory.register_function<FunctionToYearWeekTwoArgsV2>();
    factory.register_function<FunctionToWeekTwoArgsV2>();

    factory.register_function<FunctionNow>();
    factory.register_function<FunctionCurrentTimestamp>();
    factory.register_function<FunctionLocalTime>();
    factory.register_function<FunctionLocalTimestamp>();
    factory.register_function<FunctionCurDate>();
    factory.register_function<FunctionCurrentDate>();
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionCurrentTime>();
    factory.register_function<FunctionUtcTimeStamp>();
}

} // namespace doris::vectorized