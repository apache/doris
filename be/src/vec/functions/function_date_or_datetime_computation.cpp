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

using FunctionAddSeconds = FunctionDateOrDateTimeComputation<AddSecondsImpl<TYPE_DATETIME>>;
using FunctionAddMinutes = FunctionDateOrDateTimeComputation<AddMinutesImpl<TYPE_DATETIME>>;
using FunctionAddHours = FunctionDateOrDateTimeComputation<AddHoursImpl<TYPE_DATETIME>>;
using FunctionAddSecondsDate = FunctionDateOrDateTimeComputation<AddSecondsImpl<TYPE_DATE>>;
using FunctionAddMinutesDate = FunctionDateOrDateTimeComputation<AddMinutesImpl<TYPE_DATE>>;
using FunctionAddHoursDate = FunctionDateOrDateTimeComputation<AddHoursImpl<TYPE_DATE>>;
using FunctionAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<TYPE_DATETIME>>;
using FunctionAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl<TYPE_DATETIME>>;
using FunctionAddMonths = FunctionDateOrDateTimeComputation<AddMonthsImpl<TYPE_DATETIME>>;
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_DATETIME>>;
using FunctionAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl<TYPE_DATETIME>>;
using FunctionAddDaysDate = FunctionDateOrDateTimeComputation<AddDaysImpl<TYPE_DATE>>;
using FunctionAddWeeksDate = FunctionDateOrDateTimeComputation<AddWeeksImpl<TYPE_DATE>>;
using FunctionAddMonthsDate = FunctionDateOrDateTimeComputation<AddMonthsImpl<TYPE_DATE>>;
using FunctionAddQuartersDate = FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_DATE>>;
using FunctionAddYearsDate = FunctionDateOrDateTimeComputation<AddYearsImpl<TYPE_DATE>>;

using FunctionSubSeconds = FunctionDateOrDateTimeComputation<SubtractSecondsImpl<TYPE_DATETIME>>;
using FunctionSubMinutes = FunctionDateOrDateTimeComputation<SubtractMinutesImpl<TYPE_DATETIME>>;
using FunctionSubHours = FunctionDateOrDateTimeComputation<SubtractHoursImpl<TYPE_DATETIME>>;
using FunctionSubSecondsDate = FunctionDateOrDateTimeComputation<SubtractSecondsImpl<TYPE_DATE>>;
using FunctionSubMinutesDate = FunctionDateOrDateTimeComputation<SubtractMinutesImpl<TYPE_DATE>>;
using FunctionSubHoursDate = FunctionDateOrDateTimeComputation<SubtractHoursImpl<TYPE_DATE>>;
using FunctionSubDays = FunctionDateOrDateTimeComputation<SubtractDaysImpl<TYPE_DATETIME>>;
using FunctionSubWeeks = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<TYPE_DATETIME>>;
using FunctionSubMonths = FunctionDateOrDateTimeComputation<SubtractMonthsImpl<TYPE_DATETIME>>;
using FunctionSubQuarters = FunctionDateOrDateTimeComputation<SubtractQuartersImpl<TYPE_DATETIME>>;
using FunctionSubYears = FunctionDateOrDateTimeComputation<SubtractYearsImpl<TYPE_DATETIME>>;
using FunctionSubDaysDate = FunctionDateOrDateTimeComputation<SubtractDaysImpl<TYPE_DATE>>;
using FunctionSubWeeksDate = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<TYPE_DATE>>;
using FunctionSubMonthsDate = FunctionDateOrDateTimeComputation<SubtractMonthsImpl<TYPE_DATE>>;
using FunctionSubQuartersDate = FunctionDateOrDateTimeComputation<SubtractQuartersImpl<TYPE_DATE>>;
using FunctionSubYearsDate = FunctionDateOrDateTimeComputation<SubtractYearsImpl<TYPE_DATE>>;

using FunctionDateDiff = FunctionTimeDiff<DateDiffImpl<TYPE_DATETIME>>;
using FunctionTimeDiffImpl = FunctionTimeDiff<TimeDiffImpl<TYPE_DATETIME>>;
using FunctionYearsDiff = FunctionTimeDiff<YearsDiffImpl<TYPE_DATETIME>>;
using FunctionMonthsDiff = FunctionTimeDiff<MonthsDiffImpl<TYPE_DATETIME>>;
using FunctionDaysDiff = FunctionTimeDiff<DaysDiffImpl<TYPE_DATETIME>>;
using FunctionWeeksDiff = FunctionTimeDiff<WeeksDiffImpl<TYPE_DATETIME>>;
using FunctionHoursDiff = FunctionTimeDiff<HoursDiffImpl<TYPE_DATETIME>>;
using FunctionMinutesDiff = FunctionTimeDiff<MintuesDiffImpl<TYPE_DATETIME>>;
using FunctionSecondsDiff = FunctionTimeDiff<SecondsDiffImpl<TYPE_DATETIME>>;

using FunctionToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<TYPE_DATETIME>>;
using FunctionToWeekTwoArgs = FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<TYPE_DATETIME>>;

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
using FunctionUtcTimeStamp = FunctionCurrentDateOrDateTime<UtcTimestampImpl>;
using FunctionTimeToSec = FunctionCurrentDateOrDateTime<TimeToSecImpl>;
using FunctionSecToTime = FunctionCurrentDateOrDateTime<SecToTimeImpl>;
using FunctionMicroSecToDateTime = TimestampToDateTime<MicroSec>;
using FunctionMilliSecToDateTime = TimestampToDateTime<MilliSec>;
using FunctionSecToDateTime = TimestampToDateTime<Sec>;

void register_function_date_time_computation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAddSeconds>();
    factory.register_function<FunctionAddMinutes>();
    factory.register_function<FunctionAddHours>();
    factory.register_function<FunctionAddDays>();
    factory.register_function<FunctionAddWeeks>();
    factory.register_function<FunctionAddMonths>();
    factory.register_function<FunctionAddYears>();
    factory.register_function<FunctionAddQuarters>();
    factory.register_function<FunctionAddDaysDate>();
    factory.register_function<FunctionAddWeeksDate>();
    factory.register_function<FunctionAddMonthsDate>();
    factory.register_function<FunctionAddYearsDate>();
    factory.register_function<FunctionAddQuartersDate>();
    factory.register_function<FunctionAddSecondsDate>();
    factory.register_function<FunctionAddMinutesDate>();
    factory.register_function<FunctionAddHoursDate>();

    factory.register_function<FunctionSubSeconds>();
    factory.register_function<FunctionSubMinutes>();
    factory.register_function<FunctionSubHours>();
    factory.register_function<FunctionSubDays>();
    factory.register_function<FunctionSubMonths>();
    factory.register_function<FunctionSubYears>();
    factory.register_function<FunctionSubQuarters>();
    factory.register_function<FunctionSubWeeks>();
    factory.register_function<FunctionSubDaysDate>();
    factory.register_function<FunctionSubMonthsDate>();
    factory.register_function<FunctionSubYearsDate>();
    factory.register_function<FunctionSubQuartersDate>();
    factory.register_function<FunctionSubWeeksDate>();
    factory.register_function<FunctionSubSecondsDate>();
    factory.register_function<FunctionSubMinutesDate>();
    factory.register_function<FunctionSubHoursDate>();

    factory.register_function<FunctionDateDiff>();
    factory.register_function<FunctionTimeDiffImpl>();
    factory.register_function<FunctionYearsDiff>();
    factory.register_function<FunctionMonthsDiff>();
    factory.register_function<FunctionWeeksDiff>();
    factory.register_function<FunctionDaysDiff>();
    factory.register_function<FunctionHoursDiff>();
    factory.register_function<FunctionMinutesDiff>();
    factory.register_function<FunctionSecondsDiff>();

    factory.register_function<FunctionNextDay>();

    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();

    factory.register_function<FunctionNow>();
    factory.register_function<FunctionNowWithPrecision>();
    factory.register_function(CurDateFunctionName::name, &createCurDateFunctionBuilderFunction);
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionUtcTimeStamp>();
    factory.register_function<FunctionTimeToSec>();
    factory.register_function<FunctionSecToTime>();
    factory.register_function<FunctionMicroSecToDateTime>();
    factory.register_function<FunctionMilliSecToDateTime>();
    factory.register_function<FunctionSecToDateTime>();
    factory.register_function<FunctionMonthsBetween>();
    factory.register_function<FunctionTime>();

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
}

} // namespace doris::vectorized
