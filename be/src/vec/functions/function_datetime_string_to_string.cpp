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

#include "vec/functions/function_datetime_string_to_string.h"

#include "runtime/define_primitive_type.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h" // IWYU pragma: keep
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionDateFormatV2 = FunctionDateTimeStringToString<DateFormatImpl<TYPE_DATEV2>>;
using FunctionDateTimeV2DateFormat =
        FunctionDateTimeStringToString<DateFormatImpl<TYPE_DATETIMEV2>>;
// old version
using FunctionFromUnixTimeOneArg = FunctionDateTimeStringToString<FromUnixTimeImpl<false, false>>;
using FunctionFromUnixTimeTwoArg = FunctionDateTimeStringToString<FromUnixTimeImpl<true, false>>;
// new version
using FunctionFromUnixTimeNewOneArg = FunctionDateTimeStringToString<FromUnixTimeImpl<false, true>>;
using FunctionFromUnixTimeNewTwoArg = FunctionDateTimeStringToString<FromUnixTimeImpl<true, true>>;
using FunctionFromUnixTimeNewDecimalOneArg =
        FunctionDateTimeStringToString<FromUnixTimeDecimalImpl<false>>;
using FunctionFromUnixTimeNewDecimalTwoArg =
        FunctionDateTimeStringToString<FromUnixTimeDecimalImpl<true>>;

void register_function_date_time_string_to_string(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDateFormatV2>();
    factory.register_function<FunctionFromUnixTimeOneArg>();
    factory.register_function<FunctionFromUnixTimeTwoArg>();
    factory.register_function<FunctionFromUnixTimeNewOneArg>();
    factory.register_function<FunctionFromUnixTimeNewTwoArg>();
    factory.register_function<FunctionFromUnixTimeNewDecimalOneArg>();
    factory.register_function<FunctionFromUnixTimeNewDecimalTwoArg>();
    factory.register_function<FunctionDateTimeV2DateFormat>();
}

} // namespace doris::vectorized
