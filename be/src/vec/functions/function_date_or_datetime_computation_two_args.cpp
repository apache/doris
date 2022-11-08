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
using FunctionToYearWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<VecDateTimeValue, DataTypeDateTime, Int64>>;
using FunctionToYearWeekTwoArgsV2 = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<DateV2Value<DateV2ValueType>, DataTypeDateV2, UInt32>>;
using FunctionToWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<VecDateTimeValue, DataTypeDateTime, Int64>>;
using FunctionToWeekTwoArgsV2 = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<DateV2Value<DateV2ValueType>, DataTypeDateV2, UInt32>>;

using FunctionDatetimeV2ToYearWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToYearWeekTwoArgsImpl<DateV2Value<DateTimeV2ValueType>, DataTypeDateTimeV2, UInt64>>;
using FunctionDatetimeV2ToWeekTwoArgs = FunctionDateOrDateTimeComputation<
        ToWeekTwoArgsImpl<DateV2Value<DateTimeV2ValueType>, DataTypeDateTimeV2, UInt64>>;

void register_function_date_time_computation_two_args(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();
    factory.register_function<FunctionToYearWeekTwoArgsV2>();
    factory.register_function<FunctionToWeekTwoArgsV2>();
    factory.register_function<FunctionDatetimeV2ToYearWeekTwoArgs>();
    factory.register_function<FunctionDatetimeV2ToWeekTwoArgs>();
}


}