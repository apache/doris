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

using FunctionAddSecondsV2 = FunctionDateOrDateTimeComputation<AddSecondsImpl<DataTypeDateV2>>;
using FunctionAddMinutesV2 = FunctionDateOrDateTimeComputation<AddMinutesImpl<DataTypeDateV2>>;
using FunctionAddHoursV2 = FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateV2>>;
using FunctionAddDaysV2 = FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateV2>>;
using FunctionAddWeeksV2 = FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateV2>>;
using FunctionAddMonthsV2 = FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateV2>>;
using FunctionAddQuartersV2 = FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDateV2>>;
using FunctionAddYearsV2 = FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateV2>>;

using FunctionSubSecondsV2 = FunctionDateOrDateTimeComputation<SubtractSecondsImpl<DataTypeDateV2>>;
using FunctionSubMinutesV2 = FunctionDateOrDateTimeComputation<SubtractMinutesImpl<DataTypeDateV2>>;
using FunctionSubHoursV2 = FunctionDateOrDateTimeComputation<SubtractHoursImpl<DataTypeDateV2>>;
using FunctionSubDaysV2 = FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDateV2>>;
using FunctionSubWeeksV2 = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<DataTypeDateV2>>;
using FunctionSubMonthsV2 = FunctionDateOrDateTimeComputation<SubtractMonthsImpl<DataTypeDateV2>>;
using FunctionSubQuartersV2 =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<DataTypeDateV2>>;
using FunctionSubYearsV2 = FunctionDateOrDateTimeComputation<SubtractYearsImpl<DataTypeDateV2>>;

using FunctionToYearWeekTwoArgsV2 =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<DataTypeDateV2>>;
using FunctionToWeekTwoArgsV2 =
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
    factory.register_function<FunctionAddSecondsV2>();
    factory.register_function<FunctionAddMinutesV2>();
    factory.register_function<FunctionAddHoursV2>();
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

    factory.register_function<FunctionSubSecondsV2>();
    factory.register_function<FunctionSubMinutesV2>();
    factory.register_function<FunctionSubHoursV2>();
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

    factory.register_function<FunctionToYearWeekTwoArgsV2>();
    factory.register_function<FunctionToWeekTwoArgsV2>();
    factory.register_function<FunctionDatetimeV2ToYearWeekTwoArgs>();
    factory.register_function<FunctionDatetimeV2ToWeekTwoArgs>();
}

} // namespace doris::vectorized
