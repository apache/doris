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
using FunctionDatetimeV2AddSeconds = FunctionDateOrDateTimeComputation<
        AddSecondsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddMinutes = FunctionDateOrDateTimeComputation<
        AddMinutesImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddHours = FunctionDateOrDateTimeComputation<
        AddHoursImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddDays = FunctionDateOrDateTimeComputation<
        AddDaysImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddWeeks = FunctionDateOrDateTimeComputation<
        AddWeeksImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddMonths = FunctionDateOrDateTimeComputation<
        AddMonthsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;

using FunctionDatetimeV2AddQuarters = FunctionDateOrDateTimeComputation<
        AddQuartersImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2AddYears = FunctionDateOrDateTimeComputation<
        AddYearsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;

void register_function_date_time_computation_v2_add(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDatetimeV2AddSeconds>();
    factory.register_function<FunctionDatetimeV2AddMinutes>();
    factory.register_function<FunctionDatetimeV2AddHours>();
    factory.register_function<FunctionDatetimeV2AddDays>();
    factory.register_function<FunctionDatetimeV2AddWeeks>();
    factory.register_function<FunctionDatetimeV2AddMonths>();
    factory.register_function<FunctionDatetimeV2AddYears>();
    factory.register_function<FunctionDatetimeV2AddQuarters>();
}


}