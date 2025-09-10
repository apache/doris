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

#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/functions/function_date_or_datetime_computation.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionAddDaysV2 = FunctionDateOrDateTimeComputation<AddDaysImpl<TYPE_DATEV2>>;
using FunctionAddWeeksV2 = FunctionDateOrDateTimeComputation<AddWeeksImpl<TYPE_DATEV2>>;
using FunctionAddMonthsV2 = FunctionDateOrDateTimeComputation<AddMonthsImpl<TYPE_DATEV2>>;
using FunctionAddQuartersV2 = FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_DATEV2>>;
using FunctionAddYearsV2 = FunctionDateOrDateTimeComputation<AddYearsImpl<TYPE_DATEV2>>;

using FunctionSubDaysV2 = FunctionDateOrDateTimeComputation<SubtractDaysImpl<TYPE_DATEV2>>;
using FunctionSubWeeksV2 = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<TYPE_DATEV2>>;
using FunctionSubMonthsV2 = FunctionDateOrDateTimeComputation<SubtractMonthsImpl<TYPE_DATEV2>>;
using FunctionSubQuartersV2 = FunctionDateOrDateTimeComputation<SubtractQuartersImpl<TYPE_DATEV2>>;
using FunctionSubYearsV2 = FunctionDateOrDateTimeComputation<SubtractYearsImpl<TYPE_DATEV2>>;

using FunctionToYearWeekTwoArgsV2 =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<TYPE_DATEV2>>;
using FunctionToWeekTwoArgsV2 = FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<TYPE_DATEV2>>;

using FunctionDatetimeV2AddMicroseconds =
        FunctionDateOrDateTimeComputation<AddMicrosecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddMilliseconds =
        FunctionDateOrDateTimeComputation<AddMillisecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddSeconds =
        FunctionDateOrDateTimeComputation<AddSecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddMinutes =
        FunctionDateOrDateTimeComputation<AddMinutesImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddHours = FunctionDateOrDateTimeComputation<AddHoursImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddMonths =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddQuarters =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddYears = FunctionDateOrDateTimeComputation<AddYearsImpl<TYPE_DATETIMEV2>>;

using FunctionDatetimeV2SubMicroseconds =
        FunctionDateOrDateTimeComputation<SubtractMicrosecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubMilliseconds =
        FunctionDateOrDateTimeComputation<SubtractMillisecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubSeconds =
        FunctionDateOrDateTimeComputation<SubtractSecondsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubMinutes =
        FunctionDateOrDateTimeComputation<SubtractMinutesImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubHours =
        FunctionDateOrDateTimeComputation<SubtractHoursImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubDays =
        FunctionDateOrDateTimeComputation<SubtractDaysImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubWeeks =
        FunctionDateOrDateTimeComputation<SubtractWeeksImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubMonths =
        FunctionDateOrDateTimeComputation<SubtractMonthsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubQuarters =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2SubYears =
        FunctionDateOrDateTimeComputation<SubtractYearsImpl<TYPE_DATETIMEV2>>;

#define FUNCTION_TIME_DIFF_V2(NAME, IMPL, TYPE) using NAME##_##TYPE = FunctionTimeDiff<IMPL<TYPE>>;

#define ALL_FUNCTION_TIME_DIFF_V2(NAME, IMPL)          \
    FUNCTION_TIME_DIFF_V2(NAME, IMPL, TYPE_DATETIMEV2) \
    FUNCTION_TIME_DIFF_V2(NAME, IMPL, TYPE_DATEV2)
// these diff functions accept all v2 types. but for v1 only datetime.
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2DateDiff, DateDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2TimeDiff, TimeDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2YearsDiff, YearsDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2QuartersDiff, QuartersDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2MonthsDiff, MonthsDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2WeeksDiff, WeeksDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2HoursDiff, HoursDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2MinutesDiff, MintuesDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2SecondsDiff, SecondsDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2DaysDiff, DaysDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2MilliSecondsDiff, MilliSecondsDiffImpl)
ALL_FUNCTION_TIME_DIFF_V2(FunctionDatetimeV2MicroSecondsDiff, MicroSecondsDiffImpl)

using FunctionDatetimeV2ToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2ToWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<TYPE_DATETIMEV2>>;

void register_function_date_time_computation_v2(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAddDaysV2>();
    factory.register_function<FunctionAddWeeksV2>();
    factory.register_function<FunctionAddMonthsV2>();
    factory.register_function<FunctionAddYearsV2>();
    factory.register_function<FunctionAddQuartersV2>();

    factory.register_function<FunctionDatetimeV2AddMicroseconds>();
    factory.register_function<FunctionDatetimeV2AddMilliseconds>();
    factory.register_function<FunctionDatetimeV2AddSeconds>();
    factory.register_function<FunctionDatetimeV2AddMinutes>();
    factory.register_function<FunctionDatetimeV2AddHours>();
    factory.register_function<FunctionDatetimeV2AddDays>();
    factory.register_function<FunctionDatetimeV2AddWeeks>();
    factory.register_function<FunctionDatetimeV2AddMonths>();
    factory.register_function<FunctionDatetimeV2AddYears>();
    factory.register_function<FunctionDatetimeV2AddQuarters>();

    factory.register_function<FunctionSubDaysV2>();
    factory.register_function<FunctionSubMonthsV2>();
    factory.register_function<FunctionSubYearsV2>();
    factory.register_function<FunctionSubQuartersV2>();
    factory.register_function<FunctionSubWeeksV2>();

    factory.register_function<FunctionDatetimeV2SubMicroseconds>();
    factory.register_function<FunctionDatetimeV2SubMilliseconds>();
    factory.register_function<FunctionDatetimeV2SubSeconds>();
    factory.register_function<FunctionDatetimeV2SubMinutes>();
    factory.register_function<FunctionDatetimeV2SubHours>();
    factory.register_function<FunctionDatetimeV2SubDays>();
    factory.register_function<FunctionDatetimeV2SubMonths>();
    factory.register_function<FunctionDatetimeV2SubYears>();
    factory.register_function<FunctionDatetimeV2SubQuarters>();
    factory.register_function<FunctionDatetimeV2SubWeeks>();

#define REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE) factory.register_function<NAME##_##TYPE>();

#define REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(NAME)          \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_DATETIMEV2) \
    REGISTER_DATEV2_FUNCTIONS_DIFF(NAME, TYPE_DATEV2)

    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2DateDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2TimeDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2YearsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2QuartersDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2MonthsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2WeeksDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2HoursDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2MinutesDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2SecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2DaysDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2MilliSecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_DIFF(FunctionDatetimeV2MicroSecondsDiff)

    factory.register_function<FunctionToYearWeekTwoArgsV2>();
    factory.register_function<FunctionToWeekTwoArgsV2>();
    factory.register_function<FunctionDatetimeV2ToYearWeekTwoArgs>();
    factory.register_function<FunctionDatetimeV2ToWeekTwoArgs>();
}

} // namespace doris::vectorized
