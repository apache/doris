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

void register_function_date_time_computation_diff(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDateDiff>();
    factory.register_function<FunctionTimeDiff>();
    factory.register_function<FunctionYearsDiff>();
    factory.register_function<FunctionMonthsDiff>();
    factory.register_function<FunctionWeeksDiff>();
    factory.register_function<FunctionDaysDiff>();
    factory.register_function<FunctionHoursDiff>();
    factory.register_function<FunctionMinutesDiff>();
    factory.register_function<FunctionSecondsDiff>();
}

}