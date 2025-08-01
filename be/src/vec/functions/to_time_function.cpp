
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

#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function_date_or_datetime_to_something.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionYear = FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearImpl<TYPE_DATETIME>>;
using FunctionYearV2 = FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearImpl<TYPE_DATEV2>>;
using FunctionYearOfWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearOfWeekImpl<TYPE_DATEV2>>;
using FunctionQuarter =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToQuarterImpl<TYPE_DATETIME>>;
using FunctionQuarterV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToQuarterImpl<TYPE_DATEV2>>;
using FunctionMonth = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMonthImpl<TYPE_DATETIME>>;
using FunctionMonthV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMonthImpl<TYPE_DATEV2>>;
using FunctionDay = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToDayImpl<TYPE_DATETIME>>;
using FunctionDayV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToDayImpl<TYPE_DATEV2>>;
using FunctionWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToWeekOneArgImpl<TYPE_DATETIME>>;
using FunctionWeekV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToWeekOneArgImpl<TYPE_DATEV2>>;
using FunctionHour = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToHourImpl<TYPE_DATETIME>>;
using FunctionHourV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToHourImpl<TYPE_DATEV2>>;
using FunctionMinute = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMinuteImpl<TYPE_DATETIME>>;
using FunctionMinuteV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMinuteImpl<TYPE_DATEV2>>;
using FunctionSecond = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToSecondImpl<TYPE_DATETIME>>;
using FunctionSecondV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToSecondImpl<TYPE_DATEV2>>;
using FunctionToDays = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<TYPE_DATETIME>>;
using FunctionToDaysV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<TYPE_DATEV2>>;
using FunctionToDate = FunctionDateOrDateTimeToSomething<DataTypeDate, ToDateImpl<TYPE_DATETIME>>;
using FunctionToDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<TYPE_DATEV2>>;
using FunctionDate = FunctionDateOrDateTimeToSomething<DataTypeDate, DateImpl<TYPE_DATETIME>>;
using FunctionDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<TYPE_DATEV2>>;

using FunctionDateTimeV2Year =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Quarter =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToQuarterImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Month =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMonthImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Day =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToDayImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Week =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToWeekOneArgImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Hour =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToHourImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Minute =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMinuteImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Second =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToSecondImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2MicroSecond =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMicroSecondImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2ToDays =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2ToDate =
        FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Date =
        FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<TYPE_DATETIMEV2>>;

using FunctionTimeStamp =
        FunctionDateOrDateTimeToSomething<DataTypeDateTime, TimeStampImpl<TYPE_DATETIME>>;
using FunctionTimeStampV2 =
        FunctionDateOrDateTimeToSomething<DataTypeDateTimeV2, TimeStampImpl<TYPE_DATETIMEV2>>;

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
    factory.register_function<FunctionTimeStampV2>();
    factory.register_function<FunctionSecondV2>();
    factory.register_function<FunctionMinuteV2>();
    factory.register_function<FunctionHourV2>();
    factory.register_function<FunctionDayV2>();
    factory.register_function<FunctionWeekV2>();
    factory.register_function<FunctionMonthV2>();
    factory.register_function<FunctionYearV2>();
    factory.register_function<FunctionYearOfWeek>();
    factory.register_function<FunctionQuarterV2>();
    factory.register_function<FunctionToDaysV2>();
    factory.register_function<FunctionToDateV2>();
    factory.register_function<FunctionDateV2>();
    factory.register_function<FunctionDateTimeV2MicroSecond>();
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
