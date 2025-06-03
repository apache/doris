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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionDateOrDatetimeToString.cpp
// and modified by Doris

#include "vec/functions/function_date_or_datetime_to_string.h"

#include "vec/data_types/data_type_date_or_datetime_v2.h" // IWYU pragma: keep
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionDayName = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATETIME>>;
using FunctionDayNameV2 = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATEV2>>;
using FunctionMonthName = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATETIME>>;
using FunctionMonthNameV2 = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATEV2>>;

using FunctionDateTimeV2DayName = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2MonthName = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATETIMEV2>>;

using FunctionDateIso8601 = FunctionDateOrDateTimeToString<ToIso8601Impl<TYPE_DATEV2>>;
using FunctionDateTimeIso8601 = FunctionDateOrDateTimeToString<ToIso8601Impl<TYPE_DATETIMEV2>>;

void register_function_date_time_to_string(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDayName>();
    factory.register_function<FunctionMonthName>();
    factory.register_function<FunctionDayNameV2>();
    factory.register_function<FunctionMonthNameV2>();
    factory.register_function<FunctionDateTimeV2DayName>();
    factory.register_function<FunctionDateTimeV2MonthName>();
    factory.register_function<FunctionDateIso8601>();
    factory.register_function<FunctionDateTimeIso8601>();
}

} // namespace doris::vectorized
