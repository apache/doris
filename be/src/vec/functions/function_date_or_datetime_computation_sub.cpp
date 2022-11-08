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
using FunctionSubSecondsV2 = FunctionDateOrDateTimeComputation<
        SubtractSecondsImpl<DataTypeDateV2, UInt32, DataTypeDateTimeV2>>;
using FunctionSubMinutesV2 = FunctionDateOrDateTimeComputation<
        SubtractMinutesImpl<DataTypeDateV2, UInt32, DataTypeDateTimeV2>>;
using FunctionSubHoursV2 = FunctionDateOrDateTimeComputation<
        SubtractHoursImpl<DataTypeDateV2, UInt32, DataTypeDateTimeV2>>;
using FunctionSubDaysV2 =
        FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubWeeksV2 = FunctionDateOrDateTimeComputation<
        SubtractWeeksImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubMonthsV2 = FunctionDateOrDateTimeComputation<
        SubtractMonthsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubQuartersV2 = FunctionDateOrDateTimeComputation<
        SubtractQuartersImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionSubYearsV2 = FunctionDateOrDateTimeComputation<
        SubtractYearsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;

void register_function_date_time_computation_sub(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSubSeconds>();
    factory.register_function<FunctionSubMinutes>();
    factory.register_function<FunctionSubHours>();
    factory.register_function<FunctionSubDays>();
    factory.register_function<FunctionSubSecondsV2>();
    factory.register_function<FunctionSubMinutesV2>();
    factory.register_function<FunctionSubHoursV2>();
    factory.register_function<FunctionSubDaysV2>();
    factory.register_alias("days_sub", "date_sub");
    factory.register_alias("days_sub", "subdate");
    factory.register_function<FunctionSubMonths>();
    factory.register_function<FunctionSubYears>();
    factory.register_function<FunctionSubQuarters>();
    factory.register_function<FunctionSubWeeks>();
    factory.register_function<FunctionSubMonthsV2>();
    factory.register_function<FunctionSubYearsV2>();
    factory.register_function<FunctionSubQuartersV2>();
    factory.register_function<FunctionSubWeeksV2>();

}


}