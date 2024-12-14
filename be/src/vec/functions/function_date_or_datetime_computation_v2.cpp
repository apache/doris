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

#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function_date_or_datetime_computation.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionDateAddSecondsV2 = FunctionDateOrDateTimeComputation<AddSecondsImpl<DataTypeDateV2>>;
using FunctionDateAddMinutesV2 = FunctionDateOrDateTimeComputation<AddMinutesImpl<DataTypeDateV2>>;
using FunctionDateAddHoursV2 = FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateV2>>;
using FunctionDateAddDaysV2 = FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateV2>>;
using FunctionDateAddWeeksV2 = FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateV2>>;
using FunctionDateAddMonthsV2 = FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateV2>>;
using FunctionDateAddQuartersV2 =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDateV2>>;
using FunctionDateAddYearsV2 = FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateV2>>;

using FunctionDateSubSecondsV2 =
        FunctionDateOrDateTimeComputation<SubtractSecondsImpl<DataTypeDateV2>>;
using FunctionDateSubMinutesV2 =
        FunctionDateOrDateTimeComputation<SubtractMinutesImpl<DataTypeDateV2>>;
using FunctionDateSubHoursV2 = FunctionDateOrDateTimeComputation<SubtractHoursImpl<DataTypeDateV2>>;
using FunctionDateSubDaysV2 = FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDateV2>>;
using FunctionDateSubWeeksV2 = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<DataTypeDateV2>>;
using FunctionDateSubMonthsV2 =
        FunctionDateOrDateTimeComputation<SubtractMonthsImpl<DataTypeDateV2>>;
using FunctionDateSubQuartersV2 =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<DataTypeDateV2>>;
using FunctionDateSubYearsV2 = FunctionDateOrDateTimeComputation<SubtractYearsImpl<DataTypeDateV2>>;

using FunctionDateToYearWeekTwoArgsV2 =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<DataTypeDateV2>>;
using FunctionDateToWeekTwoArgsV2 =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<DataTypeDateV2>>;

using FunctionDatetimeV2AddMicroseconds =
        FunctionDateOrDateTimeComputation<AddMicrosecondsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddMilliseconds =
        FunctionDateOrDateTimeComputation<AddMillisecondsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddSeconds =
        FunctionDateOrDateTimeComputation<AddSecondsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddMinutes =
        FunctionDateOrDateTimeComputation<AddMinutesImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddHours =
        FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddDays =
        FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddWeeks =
        FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddMonths =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddQuarters =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddYears =
        FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateTimeV2>>;

using FunctionDatetimeV2SubMicroseconds =
        FunctionDateOrDateTimeComputation<SubtractMicrosecondsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubMilliseconds =
        FunctionDateOrDateTimeComputation<SubtractMillisecondsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubSeconds =
        FunctionDateOrDateTimeComputation<SubtractSecondsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubMinutes =
        FunctionDateOrDateTimeComputation<SubtractMinutesImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubHours =
        FunctionDateOrDateTimeComputation<SubtractHoursImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubDays =
        FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubWeeks =
        FunctionDateOrDateTimeComputation<SubtractWeeksImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubMonths =
        FunctionDateOrDateTimeComputation<SubtractMonthsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubQuarters =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubYears =
        FunctionDateOrDateTimeComputation<SubtractYearsImpl<DataTypeDateTimeV2>>;

#define FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, TYPE1, TYPE2) \
    using NAME##_##TYPE1##_##TYPE2 = FunctionDateOrDateTimeComputation<IMPL<TYPE1, TYPE2>>;

#define ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL)                                 \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateTimeV2) \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateV2)     \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateTimeV2)     \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateV2)

ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2DateDiff, DateDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2TimeDiff, TimeDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2YearsDiff, YearsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MonthsDiff, MonthsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2WeeksDiff, WeeksDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2HoursDiff, HoursDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MinutesDiff, MintueSDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2SecondsDiff, SecondsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2DaysDiff, DaysDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MilliSecondsDiff, MilliSecondsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MicroSecondsDiff, MicroSecondsDiffImpl)

using FunctionDatetimeV2ToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<DataTypeDateTimeV2>>;
using FunctionDatetimeV2ToWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<DataTypeDateTimeV2>>;

void register_function_date_time_computation_v2(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDateAddSecondsV2>();
    factory.register_function<FunctionDateAddMinutesV2>();
    factory.register_function<FunctionDateAddHoursV2>();
    factory.register_function<FunctionDateAddDaysV2>();
    factory.register_function<FunctionDateAddWeeksV2>();
    factory.register_function<FunctionDateAddMonthsV2>();
    factory.register_function<FunctionDateAddYearsV2>();
    factory.register_function<FunctionDateAddQuartersV2>();

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

    factory.register_function<FunctionDateSubSecondsV2>();
    factory.register_function<FunctionDateSubMinutesV2>();
    factory.register_function<FunctionDateSubHoursV2>();
    factory.register_function<FunctionDateSubDaysV2>();
    factory.register_function<FunctionDateSubMonthsV2>();
    factory.register_function<FunctionDateSubYearsV2>();
    factory.register_function<FunctionDateSubQuartersV2>();
    factory.register_function<FunctionDateSubWeeksV2>();

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

#define REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, TYPE1, TYPE2) \
    factory.register_function<NAME##_##TYPE1##_##TYPE2>();

#define REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME)                                 \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateTimeV2) \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateV2)     \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateTimeV2)     \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateV2)

    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2DateDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2TimeDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2YearsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MonthsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2WeeksDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2HoursDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MinutesDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2SecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2DaysDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MilliSecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MicroSecondsDiff)

    factory.register_function<FunctionDateToYearWeekTwoArgsV2>();
    factory.register_function<FunctionDateToWeekTwoArgsV2>();
    factory.register_function<FunctionDatetimeV2ToYearWeekTwoArgs>();
    factory.register_function<FunctionDatetimeV2ToWeekTwoArgs>();
}

} // namespace doris::vectorized
