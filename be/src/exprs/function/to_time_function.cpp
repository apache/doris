
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

#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/types.h"
#include "exprs/function/date_time_transforms.h"
#include "exprs/function/function_date_or_datetime_to_something.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

using FunctionYearV2 = FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearImpl<TYPE_DATEV2>>;
using FunctionYearOfWeek =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, ToYearOfWeekImpl<TYPE_DATEV2>>;
using FunctionQuarterV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToQuarterImpl<TYPE_DATEV2>>;
using FunctionMonthV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMonthImpl<TYPE_DATEV2>>;
using FunctionDayV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToDayImpl<TYPE_DATEV2>>;
using FunctionWeekV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt8, ToWeekOneArgImpl<TYPE_DATEV2>>;
using FunctionHourV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToHourImpl<TYPE_DATEV2>>;
using FunctionMinuteV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToMinuteImpl<TYPE_DATEV2>>;
using FunctionSecondV2 = FunctionDateOrDateTimeToSomething<DataTypeInt8, ToSecondImpl<TYPE_DATEV2>>;
using FunctionToDaysV2 = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl<TYPE_DATEV2>>;
using FunctionToDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<TYPE_DATEV2>>;
using FunctionDateV2 = FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<TYPE_DATEV2>>;
using FunctionToSeconds =
        FunctionDateOrDateTimeToSomething<DataTypeInt64, ToSecondsImpl<TYPE_DATETIMEV2>>;

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
using FunctionDateTimeV2ToDate =
        FunctionDateOrDateTimeToSomething<DataTypeDateV2, ToDateImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2Date =
        FunctionDateOrDateTimeToSomething<DataTypeDateV2, DateImpl<TYPE_DATETIMEV2>>;
using FunctionTimeStampV2 =
        FunctionDateOrDateTimeToSomething<DataTypeDateTimeV2, TimeStampImpl<TYPE_DATETIMEV2>>;
using FunctionCenturyV2 =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, ToCenturyImpl<TYPE_DATEV2>>;
using FunctionDateTimeV2Century =
        FunctionDateOrDateTimeToSomething<DataTypeInt16, ToCenturyImpl<TYPE_DATETIMEV2>>;

#define TIMESTAMP_NS_TO_TIME_FUNCTION(NAME, RESULT_TYPE, IMPL) \
    using FunctionTimeStampNs##NAME =                          \
            FunctionDateOrDateTimeToSomething<RESULT_TYPE, IMPL<TYPE_TIMESTAMP_NS>>

TIMESTAMP_NS_TO_TIME_FUNCTION(Year, DataTypeInt16, ToYearImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Quarter, DataTypeInt8, ToQuarterImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Month, DataTypeInt8, ToMonthImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Day, DataTypeInt8, ToDayImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Week, DataTypeInt8, ToWeekOneArgImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Hour, DataTypeInt8, ToHourImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Minute, DataTypeInt8, ToMinuteImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Second, DataTypeInt8, ToSecondImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(MicroSecond, DataTypeInt32, ToMicroSecondImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(ToDate, DataTypeDateV2, ToDateImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Date, DataTypeDateV2, DateImpl);
TIMESTAMP_NS_TO_TIME_FUNCTION(Century, DataTypeInt16, ToCenturyImpl);

void register_function_to_time_function(SimpleFunctionFactory& factory) {
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
    factory.register_function<FunctionDateTimeV2ToDate>();
    factory.register_function<FunctionDateTimeV2Date>();
    factory.register_function<FunctionCenturyV2>();
    factory.register_function<FunctionDateTimeV2Century>();
    factory.register_function<FunctionToSeconds>();
    factory.register_function<FunctionTimeStampNsYear>();
    factory.register_function<FunctionTimeStampNsQuarter>();
    factory.register_function<FunctionTimeStampNsMonth>();
    factory.register_function<FunctionTimeStampNsDay>();
    factory.register_function<FunctionTimeStampNsWeek>();
    factory.register_function<FunctionTimeStampNsHour>();
    factory.register_function<FunctionTimeStampNsMinute>();
    factory.register_function<FunctionTimeStampNsSecond>();
    factory.register_function<FunctionTimeStampNsMicroSecond>();
    factory.register_function<FunctionTimeStampNsToDate>();
    factory.register_function<FunctionTimeStampNsDate>();
    factory.register_function<FunctionTimeStampNsCentury>();
    factory.register_alias("date", "datev2");
    factory.register_alias("to_date", "to_datev2");
}

} // namespace doris
