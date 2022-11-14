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
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<
        AddQuartersImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddYears =
        FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;

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
using FunctionToWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<VecDateTimeValue, DataTypeDateTime, Int64>>;

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

using FunctionNow = FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, false>>;
using FunctionCurrentTimestamp =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<CurrentTimestampFunctionName, false>>;
using FunctionLocalTime =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimeFunctionName, false>>;
using FunctionLocalTimestamp =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimestampFunctionName, false>>;

using FunctionNowWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, true>>;
using FunctionCurrentTimestampWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<CurrentTimestampFunctionName, true>>;
using FunctionLocalTimeWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimeFunctionName, true>>;
using FunctionLocalTimestampWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimestampFunctionName, true>>;

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
    factory.register_function<FunctionNowWithPrecision>();
    factory.register_function<FunctionCurrentTimestampWithPrecision>();
    factory.register_function<FunctionLocalTimeWithPrecision>();
    factory.register_function<FunctionLocalTimestampWithPrecision>();
    factory.register_function(CurrentDateFunctionName::name,
                              &createCurrentDateFunctionBuilderFunction);
    factory.register_function(CurDateFunctionName::name, &createCurDateFunctionBuilderFunction);
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionCurrentTime>();
    factory.register_function<FunctionUtcTimeStamp>();

    // alias
    factory.register_alias("days_add", "date_add");
    factory.register_alias("days_add", "adddate");
    factory.register_alias("months_add", "add_months");
}

} // namespace doris::vectorized
