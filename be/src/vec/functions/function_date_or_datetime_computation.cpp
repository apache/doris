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

using FunctionAddSeconds = FunctionDateOrDateTimeComputation<AddSecondsImpl>;
using FunctionAddMinutes = FunctionDateOrDateTimeComputation<AddMinutesImpl>;
using FunctionAddHours = FunctionDateOrDateTimeComputation<AddHoursImpl>;
using FunctionAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl>;
using FunctionAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl>;
using FunctionAddMonths = FunctionDateOrDateTimeComputation<AddMonthsImpl>;
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<AddQuartersImpl>;
using FunctionAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl>;

using FunctionSubSeconds = FunctionDateOrDateTimeComputation<SubtractSecondsImpl>;
using FunctionSubMinutes = FunctionDateOrDateTimeComputation<SubtractMinutesImpl>;
using FunctionSubHours = FunctionDateOrDateTimeComputation<SubtractHoursImpl>;
using FunctionSubDays = FunctionDateOrDateTimeComputation<SubtractDaysImpl>;
using FunctionSubWeeks = FunctionDateOrDateTimeComputation<SubtractWeeksImpl>;
using FunctionSubMonths = FunctionDateOrDateTimeComputation<SubtractMonthsImpl>;
using FunctionSubQuarters = FunctionDateOrDateTimeComputation<SubtractQuartersImpl>;
using FunctionSubYears = FunctionDateOrDateTimeComputation<SubtractYearsImpl>;

using FunctionDateDiff = FunctionDateOrDateTimeComputation<DateDiffImpl>;
using FunctionTimeDiff = FunctionDateOrDateTimeComputation<TimeDiffImpl>;
using FunctionYearsDiff = FunctionDateOrDateTimeComputation<YearsDiffImpl>;
using FunctionMonthsDiff = FunctionDateOrDateTimeComputation<MonthsDiffImpl>;
using FunctionDaysDiff = FunctionDateOrDateTimeComputation<DaysDiffImpl>;
using FunctionWeeksDiff = FunctionDateOrDateTimeComputation<WeeksDiffImpl>;
using FunctionHoursDiff = FunctionDateOrDateTimeComputation<HoursDiffImpl>;
using FunctionMinutesDiff = FunctionDateOrDateTimeComputation<MintueSDiffImpl>;
using FunctionSecondsDiff = FunctionDateOrDateTimeComputation<SecondsDiffImpl>;

using FunctionToYearWeekTwoArgs = FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl>;
using FunctionToWeekTwoArgs = FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl>;

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
    factory.register_alias("days_add", "date_add");
    factory.register_alias("days_add", "adddate");
    factory.register_function<FunctionAddWeeks>();
    factory.register_function<FunctionAddMonths>();
    factory.register_function<FunctionAddYears>();
    factory.register_function<FunctionAddQuarters>();

    factory.register_function<FunctionSubSeconds>();
    factory.register_function<FunctionSubMinutes>();
    factory.register_function<FunctionSubHours>();
    factory.register_function<FunctionSubDays>();
    factory.register_alias("days_sub", "date_sub");
    factory.register_alias("days_sub", "subdate");
    factory.register_function<FunctionSubMonths>();
    factory.register_function<FunctionSubYears>();
    factory.register_function<FunctionSubQuarters>();
    factory.register_function<FunctionSubWeeks>();

    factory.register_function<FunctionDateDiff>();
    factory.register_function<FunctionTimeDiff>();
    factory.register_function<FunctionYearsDiff>();
    factory.register_function<FunctionMonthsDiff>();
    factory.register_function<FunctionWeeksDiff>();
    factory.register_function<FunctionDaysDiff>();
    factory.register_function<FunctionHoursDiff>();
    factory.register_function<FunctionMinutesDiff>();
    factory.register_function<FunctionSecondsDiff>();

    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();

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