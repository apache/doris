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

#include <boost/iterator/iterator_facade.hpp>

#include "core/data_type/data_type_date_or_datetime_v2.h" // IWYU pragma: keep
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "exprs/function/date_time_transforms.h"
#include "exprs/function/function_date_or_datetime_to_something.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

using FunctionWeekOfYearV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekOfYearImpl<TYPE_DATEV2>>;
using FunctionDayOfYearV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, DayOfYearImpl<TYPE_DATEV2>>;
using FunctionDayOfWeekV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfWeekImpl<TYPE_DATEV2>>;
using FunctionDayOfMonthV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfMonthImpl<TYPE_DATEV2>>;
using FunctionYearWeekV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearWeekOneArgImpl<TYPE_DATEV2>>;
using FunctionWeekDayV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekDayImpl<TYPE_DATEV2>>;

using FunctionDateTimeV2WeekOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekOfYearImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2DayOfYear =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, DayOfYearImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2DayOfWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfWeekImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2DayOfMonth =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, DayOfMonthImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2YearWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearWeekOneArgImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2WeekDay =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, WeekDayImpl<TYPE_DATETIMEV2>>;

#define TIMESTAMP_NS_TIME_OF_FUNCTION(NAME, RESULT_TYPE, IMPL) \
    using FunctionTimeStampNs##NAME =                          \
            FunctionDateOrDateTimeToSomething<RESULT_TYPE, IMPL<TYPE_TIMESTAMP_NS>>

TIMESTAMP_NS_TIME_OF_FUNCTION(WeekOfYear, DataTypeInt8, WeekOfYearImpl);
TIMESTAMP_NS_TIME_OF_FUNCTION(DayOfYear, DataTypeInt16, DayOfYearImpl);
TIMESTAMP_NS_TIME_OF_FUNCTION(DayOfWeek, DataTypeInt8, DayOfWeekImpl);
TIMESTAMP_NS_TIME_OF_FUNCTION(DayOfMonth, DataTypeInt8, DayOfMonthImpl);
TIMESTAMP_NS_TIME_OF_FUNCTION(YearWeek, DataTypeInt32, ToYearWeekOneArgImpl);
TIMESTAMP_NS_TIME_OF_FUNCTION(WeekDay, DataTypeInt8, WeekDayImpl);

void register_function_time_of_function(SimpleFunctionFactory& factory) {
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
    factory.register_function<FunctionTimeStampNsWeekOfYear>();
    factory.register_function<FunctionTimeStampNsDayOfYear>();
    factory.register_function<FunctionTimeStampNsDayOfWeek>();
    factory.register_function<FunctionTimeStampNsDayOfMonth>();
    factory.register_function<FunctionTimeStampNsYearWeek>();
    factory.register_function<FunctionTimeStampNsWeekDay>();
}
} // namespace doris
