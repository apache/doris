
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

using FunctionYear = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearImpl<Int64>>;
using FunctionYearV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearImpl<UInt32>>;
using FunctionQuarter = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToQuarterImpl<Int64>>;
using FunctionQuarterV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToQuarterImpl<UInt32>>;
using FunctionMonth = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMonthImpl<Int64>>;
using FunctionMonthV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMonthImpl<UInt32>>;
using FunctionDay = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDayImpl<Int64>>;
using FunctionDayV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDayImpl<UInt32>>;
using FunctionWeek = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToWeekOneArgImpl<Int64>>;
using FunctionWeekV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToWeekOneArgImpl<UInt32>>;
using FunctionHour = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToHourImpl<Int64>>;
using FunctionHourV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToHourImpl<UInt32>>;
using FunctionMinute = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMinuteImpl<Int64>>;
using FunctionMinuteV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMinuteImpl<UInt32>>;
using FunctionSecond = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToSecondImpl<Int64>>;
using FunctionSecondV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToSecondImpl<UInt32>>;
using FunctionToDays = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<Int64>>;
using FunctionToDaysV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<UInt32>>;
using FunctionToDate = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToDateImpl<Int64>>;
using FunctionToDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<UInt32>>;
using FunctionDate = FunctionDateOrDateTimeToSomething<DataTypeDateTime, DateImpl<Int64>>;
using FunctionDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<UInt32>>;

using FunctionDateTimeV2Year = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearImpl<UInt64>>;
using FunctionDateTimeV2Quarter =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToQuarterImpl<UInt64>>;
using FunctionDateTimeV2Month =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMonthImpl<UInt64>>;
using FunctionDateTimeV2Day = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDayImpl<UInt64>>;
using FunctionDateTimeV2Week =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToWeekOneArgImpl<UInt64>>;
using FunctionDateTimeV2Hour = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToHourImpl<UInt64>>;
using FunctionDateTimeV2Minute =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMinuteImpl<UInt64>>;
using FunctionDateTimeV2Second =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToSecondImpl<UInt64>>;
using FunctionDateTimeV2ToDays =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<UInt64>>;
using FunctionDateTimeV2ToDate =
        FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<UInt64>>;
using FunctionDateTimeV2Date = FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<UInt64>>;

using FunctionTimeStamp = FunctionDateOrDateTimeToSomething<DataTypeDateTime, TimeStampImpl<Int64>>;

void register_function_to_time_function(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSecond>();
    factory.register_function<FunctionMinute>();
    factory.register_function<FunctionHour>();
    factory.register_function<FunctionDay>();
    factory.register_function<FunctionWeek>();
    factory.register_function<FunctionMonth>();
    factory.register_function<FunctionYear>();
    factory.register_function<FunctionQuarter>();
    factory.register_function<FunctionToDays>();
    factory.register_function<FunctionToDate>();
    factory.register_function<FunctionDate>();
    factory.register_function<FunctionTimeStamp>();
    factory.register_function<FunctionSecondV2>();
    factory.register_function<FunctionMinuteV2>();
    factory.register_function<FunctionHourV2>();
    factory.register_function<FunctionDayV2>();
    factory.register_function<FunctionWeekV2>();
    factory.register_function<FunctionMonthV2>();
    factory.register_function<FunctionYearV2>();
    factory.register_function<FunctionQuarterV2>();
    factory.register_function<FunctionToDaysV2>();
    factory.register_function<FunctionToDateV2>();
    factory.register_function<FunctionDateV2>();
    factory.register_function<FunctionDateTimeV2Second>();
    factory.register_function<FunctionDateTimeV2Minute>();
    factory.register_function<FunctionDateTimeV2Hour>();
    factory.register_function<FunctionDateTimeV2Day>();
    factory.register_function<FunctionDateTimeV2Week>();
    factory.register_function<FunctionDateTimeV2Month>();
    factory.register_function<FunctionDateTimeV2Year>();
    factory.register_function<FunctionDateTimeV2Quarter>();
    factory.register_function<FunctionDateTimeV2ToDays>();
    factory.register_function<FunctionDateTimeV2ToDate>();
    factory.register_function<FunctionDateTimeV2Date>();
    factory.register_alias("date", "datev2");
    factory.register_alias("to_date", "to_datev2");
}

} // namespace doris::vectorized
