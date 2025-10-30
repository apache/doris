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
using FunctionDatetimeAddQuarters =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<TYPE_DATETIMEV2>>;
using FunctionDatetimeV2AddDaySecond =
        FunctionDateOrDateTimeComputation<AddDaySecondImpl<TYPE_DATETIMEV2>>;
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

void register_function_date_time_computation_v2(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAddDays>();
    factory.register_function<FunctionAddWeeks>();
    factory.register_function<FunctionAddMonths>();
    factory.register_function<FunctionAddYears>();
    factory.register_function<FunctionAddQuarters>();

<<<<<<< HEAD
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
    factory.register_function<FunctionDatetimeV2AddDaySecond>();
=======
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
>>>>>>> 95fce2175b (fix_be)

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
