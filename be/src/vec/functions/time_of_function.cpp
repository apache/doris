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

#include "vec/data_types/data_type_number.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function_date_or_datetime_to_something.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionWeekOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, WeekOfYearImpl<VecDateTimeValue, Int64>>;
using FunctionWeekOfYearV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          WeekOfYearImpl<DateV2Value<DateV2ValueType>, UInt32>>;
using FunctionDayOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, DayOfYearImpl<VecDateTimeValue, Int64>>;
using FunctionDayOfYearV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          DayOfYearImpl<DateV2Value<DateV2ValueType>, UInt32>>;
using FunctionDayOfWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, DayOfWeekImpl<VecDateTimeValue, Int64>>;
using FunctionDayOfWeekV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          DayOfWeekImpl<DateV2Value<DateV2ValueType>, UInt32>>;
using FunctionDayOfMonth =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, DayOfMonthImpl<VecDateTimeValue, Int64>>;
using FunctionDayOfMonthV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          DayOfMonthImpl<DateV2Value<DateV2ValueType>, UInt32>>;
using FunctionYearWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          ToYearWeekOneArgImpl<VecDateTimeValue, Int64>>;
using FunctionYearWeekV2 = FunctionDateOrDateTimeToSomething<
        DataTypeInt32, ToYearWeekOneArgImpl<DateV2Value<DateV2ValueType>, UInt32>>;
using FunctionWeekDay =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, WeekDayImpl<VecDateTimeValue, Int64>>;
using FunctionWeekDayV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          WeekDayImpl<DateV2Value<DateV2ValueType>, UInt32>>;

using FunctionDateTimeV2WeekOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          WeekOfYearImpl<DateV2Value<DateTimeV2ValueType>, UInt64>>;
using FunctionDateTimeV2DayOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          DayOfYearImpl<DateV2Value<DateTimeV2ValueType>, UInt64>>;
using FunctionDateTimeV2DayOfWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          DayOfWeekImpl<DateV2Value<DateTimeV2ValueType>, UInt64>>;
using FunctionDateTimeV2DayOfMonth =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          DayOfMonthImpl<DateV2Value<DateTimeV2ValueType>, UInt64>>;
using FunctionDateTimeV2YearWeek = FunctionDateOrDateTimeToSomething<
        DataTypeInt32, ToYearWeekOneArgImpl<DateV2Value<DateTimeV2ValueType>, UInt64>>;
using FunctionDateTimeV2WeekDay =
        FunctionDateOrDateTimeToSomething<DataTypeInt32,
                                          WeekDayImpl<DateV2Value<DateTimeV2ValueType>, UInt64>>;

void register_function_time_of_function(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDayOfWeek>();
    factory.register_function<FunctionDayOfMonth>();
    factory.register_function<FunctionDayOfYear>();
    factory.register_function<FunctionWeekOfYear>();
    factory.register_function<FunctionYearWeek>();
    factory.register_function<FunctionWeekDay>();
    factory.register_function<FunctionDayOfWeekV2>();
    factory.register_function<FunctionDayOfMonthV2>();
    factory.register_function<FunctionDayOfYearV2>();
    factory.register_function<FunctionWeekOfYearV2>();
    factory.register_function<FunctionYearWeekV2>();
    factory.register_function<FunctionWeekDayV2>();
    factory.register_function<FunctionDateTimeV2WeekOfYear>();
    factory.register_function<FunctionDateTimeV2DayOfYear>();
    factory.register_function<FunctionDateTimeV2DayOfWeek>();
    factory.register_function<FunctionDateTimeV2DayOfMonth>();
    factory.register_function<FunctionDateTimeV2YearWeek>();
    factory.register_function<FunctionDateTimeV2WeekDay>();
}
} // namespace doris::vectorized