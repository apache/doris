
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
#include <memory>
#include <utility>

#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function_date_or_datetime_to_something.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionYear = FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearImpl<Int64>>;
using FunctionYearV2 = FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearImpl<UInt32>>;
using FunctionQuarter = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToQuarterImpl<Int64>>;
using FunctionQuarterV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToQuarterImpl<UInt32>>;
using FunctionMonth = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMonthImpl<Int64>>;
using FunctionMonthV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMonthImpl<UInt32>>;
using FunctionDay = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToDayImpl<Int64>>;
using FunctionDayV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToDayImpl<UInt32>>;
using FunctionWeek = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToWeekOneArgImpl<Int64>>;
using FunctionWeekV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToWeekOneArgImpl<UInt32>>;
using FunctionHour = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToHourImpl<Int64>>;
using FunctionHourV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToHourImpl<UInt32>>;
using FunctionMinute = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMinuteImpl<Int64>>;
using FunctionMinuteV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMinuteImpl<UInt32>>;
using FunctionSecond = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToSecondImpl<Int64>>;
using FunctionSecondV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToSecondImpl<UInt32>>;
using FunctionToDays = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<Int64>>;
using FunctionToDaysV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<UInt32>>;
using FunctionToDate = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToDateImpl<Int64>>;
using FunctionToDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<UInt32>>;
using FunctionDate = FunctionDateOrDateTimeToSomething<DataTypeDateTime, DateImpl<Int64>>;
using FunctionDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<UInt32>>;

using FunctionDateTimeV2Year = FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearImpl<UInt64>>;
using FunctionDateTimeV2Quarter =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToQuarterImpl<UInt64>>;
using FunctionDateTimeV2Month =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMonthImpl<UInt64>>;
using FunctionDateTimeV2Day = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToDayImpl<UInt64>>;
using FunctionDateTimeV2Week =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToWeekOneArgImpl<UInt64>>;
using FunctionDateTimeV2Hour = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToHourImpl<UInt64>>;
using FunctionDateTimeV2Minute =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMinuteImpl<UInt64>>;
using FunctionDateTimeV2Second =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToSecondImpl<UInt64>>;
using FunctionDateTimeV2MicroSecond =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMicroSecondImpl<UInt64>>;
using FunctionDateTimeV2ToDays =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<UInt64>>;
using FunctionDateTimeV2ToDate =
        FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<UInt64>>;
using FunctionDateTimeV2Date = FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<UInt64>>;

using FunctionTimeStamp = FunctionDateOrDateTimeToSomething<DataTypeDateTime, TimeStampImpl<Int64>>;
using FunctionTimeStampV2 =
        FunctionDateOrDateTimeToSomething<DataTypeDateTimeV2, TimeStampImpl<UInt64>>;

/// @TEMPORARY: for be_exec_version=2
using FunctionYearOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearImpl<Int64>>;
using FunctionYearV2Old = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearImpl<UInt32>>;
using FunctionQuarterOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToQuarterImpl<Int64>>;
using FunctionQuarterV2Old =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToQuarterImpl<UInt32>>;
using FunctionMonthOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMonthImpl<Int64>>;
using FunctionMonthV2Old = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMonthImpl<UInt32>>;
using FunctionWeekOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToWeekOneArgImpl<Int64>>;
using FunctionWeekV2Old =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToWeekOneArgImpl<UInt32>>;
using FunctionDayOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDayImpl<Int64>>;
using FunctionDayV2Old = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDayImpl<UInt32>>;
using FunctionHourOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToHourImpl<Int64>>;
using FunctionHourV2Old = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToHourImpl<UInt32>>;
using FunctionMinuteOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMinuteImpl<Int64>>;
using FunctionMinuteV2Old = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMinuteImpl<UInt32>>;
using FunctionSecondOld = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToSecondImpl<Int64>>;
using FunctionSecondV2Old = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToSecondImpl<UInt32>>;
using FunctionDateTimeV2YearOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearImpl<UInt64>>;
using FunctionDateTimeV2QuarterOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToQuarterImpl<UInt64>>;
using FunctionDateTimeV2MonthOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMonthImpl<UInt64>>;
using FunctionDateTimeV2WeekOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToWeekOneArgImpl<UInt64>>;
using FunctionDateTimeV2DayOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDayImpl<UInt64>>;
using FunctionDateTimeV2HourOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToHourImpl<UInt64>>;
using FunctionDateTimeV2MinuteOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMinuteImpl<UInt64>>;
using FunctionDateTimeV2SecondOld =
        FunctionDateOrDateTimeToSomething<DataTypeInt32, ToSecondImpl<UInt64>>;

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

    /// @TEMPORARY: for be_exec_version=2
    factory.register_alternative_function<FunctionYearOld>();
    factory.register_alternative_function<FunctionQuarterOld>();
    factory.register_alternative_function<FunctionMonthOld>();
    factory.register_alternative_function<FunctionDayOld>();
    factory.register_alternative_function<FunctionWeekOld>();
    factory.register_alternative_function<FunctionHourOld>();
    factory.register_alternative_function<FunctionMinuteOld>();
    factory.register_alternative_function<FunctionSecondOld>();
    factory.register_alternative_function<FunctionYearV2Old>();
    factory.register_alternative_function<FunctionQuarterV2Old>();
    factory.register_alternative_function<FunctionMonthV2Old>();
    factory.register_alternative_function<FunctionWeekV2Old>();
    factory.register_alternative_function<FunctionDayV2Old>();
    factory.register_alternative_function<FunctionHourV2Old>();
    factory.register_alternative_function<FunctionMinuteV2Old>();
    factory.register_alternative_function<FunctionSecondV2Old>();
    factory.register_alternative_function<FunctionDateTimeV2YearOld>();
    factory.register_alternative_function<FunctionDateTimeV2QuarterOld>();
    factory.register_alternative_function<FunctionDateTimeV2MonthOld>();
    factory.register_alternative_function<FunctionDateTimeV2WeekOld>();
    factory.register_alternative_function<FunctionDateTimeV2DayOld>();
    factory.register_alternative_function<FunctionDateTimeV2HourOld>();
    factory.register_alternative_function<FunctionDateTimeV2MinuteOld>();
    factory.register_alternative_function<FunctionDateTimeV2SecondOld>();
}

} // namespace doris::vectorized
