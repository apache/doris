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

using FunctionDatetimeAddSeconds =
        FunctionDateOrDateTimeComputation<AddSecondsImpl<DataTypeDateTime>>;
using FunctionDatetimeAddMinutes =
        FunctionDateOrDateTimeComputation<AddMinutesImpl<DataTypeDateTime>>;
using FunctionDatetimeAddHours = FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateTime>>;
using FunctionDatetimeAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateTime>>;
using FunctionDatetimeAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateTime>>;
using FunctionDatetimeAddMonths =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateTime>>;
using FunctionDatetimeAddQuarters =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDateTime>>;
using FunctionDatetimeAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateTime>>;

using FunctionDateAddSeconds = FunctionDateOrDateTimeComputation<AddSecondsImpl<DataTypeDate>>;
using FunctionDateAddMinutes = FunctionDateOrDateTimeComputation<AddMinutesImpl<DataTypeDate>>;
using FunctionDateAddHours = FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDate>>;
using FunctionDateAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDate>>;
using FunctionDateAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDate>>;
using FunctionDateAddMonths = FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDate>>;
using FunctionDateAddQuarters = FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDate>>;
using FunctionDateAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDate>>;

using FunctionDatetimeSubSeconds =
        FunctionDateOrDateTimeComputation<SubtractSecondsImpl<DataTypeDateTime>>;
using FunctionDatetimeSubMinutes =
        FunctionDateOrDateTimeComputation<SubtractMinutesImpl<DataTypeDateTime>>;
using FunctionDatetimeSubHours =
        FunctionDateOrDateTimeComputation<SubtractHoursImpl<DataTypeDateTime>>;
using FunctionDatetimeSubDays =
        FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDateTime>>;
using FunctionDatetimeSubWeeks =
        FunctionDateOrDateTimeComputation<SubtractWeeksImpl<DataTypeDateTime>>;
using FunctionDatetimeSubMonths =
        FunctionDateOrDateTimeComputation<SubtractMonthsImpl<DataTypeDateTime>>;
using FunctionDatetimeSubQuarters =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<DataTypeDateTime>>;
using FunctionDatetimeSubYears =
        FunctionDateOrDateTimeComputation<SubtractYearsImpl<DataTypeDateTime>>;

using FunctionDateSubSeconds = FunctionDateOrDateTimeComputation<SubtractSecondsImpl<DataTypeDate>>;
using FunctionDateSubMinutes = FunctionDateOrDateTimeComputation<SubtractMinutesImpl<DataTypeDate>>;
using FunctionDateSubHours = FunctionDateOrDateTimeComputation<SubtractHoursImpl<DataTypeDate>>;
using FunctionDateSubDays = FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDate>>;
using FunctionDateSubWeeks = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<DataTypeDate>>;
using FunctionDateSubMonths = FunctionDateOrDateTimeComputation<SubtractMonthsImpl<DataTypeDate>>;
using FunctionDateSubQuarters =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<DataTypeDate>>;
using FunctionDateSubYears = FunctionDateOrDateTimeComputation<SubtractYearsImpl<DataTypeDate>>;

using FunctionDateDiff =
        FunctionDateOrDateTimeComputation<DateDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionTimeDiff =
        FunctionDateOrDateTimeComputation<TimeDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionYearsDiff =
        FunctionDateOrDateTimeComputation<YearsDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionMonthsDiff =
        FunctionDateOrDateTimeComputation<MonthsDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionDaysDiff =
        FunctionDateOrDateTimeComputation<DaysDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionWeeksDiff =
        FunctionDateOrDateTimeComputation<WeeksDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionHoursDiff =
        FunctionDateOrDateTimeComputation<HoursDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionMinutesDiff =
        FunctionDateOrDateTimeComputation<MintueSDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionSecondsDiff =
        FunctionDateOrDateTimeComputation<SecondsDiffImpl<DataTypeDateTime, DataTypeDateTime>>;

using FunctionToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<DataTypeDateTime>>;
using FunctionToWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<DataTypeDateTime>>;

struct NowFunctionName {
    static constexpr auto name = "now";
};

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
    factory.register_function<FunctionDatetimeAddSeconds>();
    factory.register_function<FunctionDatetimeAddMinutes>();
    factory.register_function<FunctionDatetimeAddHours>();
    factory.register_function<FunctionDatetimeAddDays>();
    factory.register_function<FunctionDatetimeAddWeeks>();
    factory.register_function<FunctionDatetimeAddMonths>();
    factory.register_function<FunctionDatetimeAddYears>();
    factory.register_function<FunctionDatetimeAddQuarters>();

    factory.register_function<FunctionDateAddSeconds>();
    factory.register_function<FunctionDateAddMinutes>();
    factory.register_function<FunctionDateAddHours>();
    factory.register_function<FunctionDateAddDays>();
    factory.register_function<FunctionDateAddWeeks>();
    factory.register_function<FunctionDateAddMonths>();
    factory.register_function<FunctionDateAddYears>();
    factory.register_function<FunctionDateAddQuarters>();

    factory.register_function<FunctionDatetimeSubSeconds>();
    factory.register_function<FunctionDatetimeSubMinutes>();
    factory.register_function<FunctionDatetimeSubHours>();
    factory.register_function<FunctionDatetimeSubDays>();
    factory.register_function<FunctionDatetimeSubMonths>();
    factory.register_function<FunctionDatetimeSubYears>();
    factory.register_function<FunctionDatetimeSubQuarters>();
    factory.register_function<FunctionDatetimeSubWeeks>();

    factory.register_function<FunctionDateSubSeconds>();
    factory.register_function<FunctionDateSubMinutes>();
    factory.register_function<FunctionDateSubHours>();
    factory.register_function<FunctionDateSubDays>();
    factory.register_function<FunctionDateSubMonths>();
    factory.register_function<FunctionDateSubYears>();
    factory.register_function<FunctionDateSubQuarters>();
    factory.register_function<FunctionDateSubWeeks>();

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
    factory.register_function<FunctionNowWithPrecision>();
    factory.register_function(CurDateFunctionName::name, &createCurDateFunctionBuilderFunction);
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionUtcTimeStamp>();
    factory.register_function<FunctionTimeToSec>();
    factory.register_function<FunctionSecToTime>();
    factory.register_function<FunctionMicroSecToDateTime>();
    factory.register_function<FunctionMilliSecToDateTime>();
    factory.register_function<FunctionSecToDateTime>();

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
