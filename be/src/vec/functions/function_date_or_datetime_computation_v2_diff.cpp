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

#define FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, TYPE1, TYPE2, ARG1, ARG2, DATE_VALUE1, \
                                      DATE_VALUE2)                                       \
    using NAME##_##TYPE1##_##TYPE2 = FunctionDateOrDateTimeComputation<                  \
            IMPL<DATE_VALUE1, DATE_VALUE2, TYPE1, TYPE2, ARG1, ARG2>>;

#define ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL)                                              \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateTimeV2, UInt64,      \
                                  UInt64, DateV2Value<DateTimeV2ValueType>,                        \
                                  DateV2Value<DateTimeV2ValueType>)                                \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateV2, UInt64, UInt32,  \
                                  DateV2Value<DateTimeV2ValueType>, DateV2Value<DateV2ValueType>)  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateTimeV2, UInt32, UInt64,  \
                                  DateV2Value<DateV2ValueType>, DateV2Value<DateTimeV2ValueType>)  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTimeV2, DataTypeDateTime, UInt64, Int64, \
                                  DateV2Value<DateTimeV2ValueType>, VecDateTimeValue)              \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTime, DataTypeDateTimeV2, Int64, UInt64, \
                                  VecDateTimeValue, DateV2Value<DateTimeV2ValueType>)              \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateTime, DataTypeDateV2, Int64, UInt32,     \
                                  VecDateTimeValue, DateV2Value<DateV2ValueType>)                  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateTime, UInt32, Int64,     \
                                  DateV2Value<DateV2ValueType>, VecDateTimeValue)                  \
    FUNCTION_DATEV2_WITH_TWO_ARGS(NAME, IMPL, DataTypeDateV2, DataTypeDateV2, UInt32, UInt32,      \
                                  DateV2Value<DateV2ValueType>, DateV2Value<DateV2ValueType>)

ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2DateDiff, DateDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2TimeDiff, TimeDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2YearsDiff, YearsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MonthsDiff, MonthsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2WeeksDiff, WeeksDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2HoursDiff, HoursDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2MinutesDiff, MintueSDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2SecondsDiff, SecondsDiffImpl)
ALL_FUNCTION_DATEV2_WITH_TWO_ARGS(FunctionDatetimeV2DaysDiff, DaysDiffImpl)

void register_function_date_time_computation_v2_diff(SimpleFunctionFactory& factory) {
#define REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, TYPE1, TYPE2) \
    factory.register_function<NAME##_##TYPE1##_##TYPE2>();

#define REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME)                                 \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateTimeV2) \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateV2)     \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateTimeV2)     \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTimeV2, DataTypeDateTime)   \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateTime)       \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateV2, DataTypeDateV2)         \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTime, DataTypeDateV2)       \
    REGISTER_DATEV2_FUNCTIONS_WITH_TWO_ARGS(NAME, DataTypeDateTime, DataTypeDateTimeV2)

    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2DateDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2TimeDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2YearsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MonthsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2WeeksDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2HoursDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2MinutesDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2SecondsDiff)
    REGISTER_ALL_DATEV2_FUNCTIONS_WITH_TWO_ARGS(FunctionDatetimeV2DaysDiff)
}
}