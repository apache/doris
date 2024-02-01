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

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <utility>

#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_time_v2.h" // IWYU pragma: keep
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function_date_or_datetime_to_something.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionWeekOfYear = FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekOfYearImpl<Int64>>;
using FunctionWeekOfYearV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekOfYearImpl<UInt32>>;
using FunctionDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeInt16, DayOfYearImpl<Int64>>;
using FunctionDayOfYearV2 = FunctionDateOrDateTimeToSomething<DataTypeInt16, DayOfYearImpl<UInt32>>;
using FunctionDayOfWeek = FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfWeekImpl<Int64>>;
using FunctionDayOfWeekV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfWeekImpl<UInt32>>;
using FunctionDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfMonthImpl<Int64>>;
using FunctionDayOfMonthV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfMonthImpl<UInt32>>;
using FunctionYearWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearWeekOneArgImpl<Int64>>;
using FunctionYearWeekV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearWeekOneArgImpl<UInt32>>;
using FunctionWeekDay = FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekDayImpl<Int64>>;
using FunctionWeekDayV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekDayImpl<UInt32>>;

using FunctionDateTimeV2WeekOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekOfYearImpl<UInt64>>;
using FunctionDateTimeV2DayOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, DayOfYearImpl<UInt64>>;
using FunctionDateTimeV2DayOfWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfWeekImpl<UInt64>>;
using FunctionDateTimeV2DayOfMonth =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfMonthImpl<UInt64>>;
using FunctionDateTimeV2YearWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearWeekOneArgImpl<UInt64>>;
using FunctionDateTimeV2WeekDay =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekDayImpl<UInt64>>;

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
