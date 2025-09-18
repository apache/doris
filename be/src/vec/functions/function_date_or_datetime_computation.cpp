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
