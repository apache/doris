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

using FunctionAddSeconds = FunctionDateOrDateTimeComputation<
        AddSecondsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddSecondsV2 = FunctionDateOrDateTimeComputation<
        AddSecondsImpl<DataTypeDateV2, UInt32, DataTypeDateTimeV2>>;
using FunctionAddMinutes = FunctionDateOrDateTimeComputation<
        AddMinutesImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddHours =
        FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddDays =
        FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddWeeks =
        FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddMonths =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddMinutesV2 = FunctionDateOrDateTimeComputation<
        AddMinutesImpl<DataTypeDateV2, UInt32, DataTypeDateTimeV2>>;
using FunctionAddHoursV2 =
        FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateV2, UInt32, DataTypeDateTimeV2>>;
using FunctionAddDaysV2 =
        FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddWeeksV2 =
        FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddMonthsV2 =
        FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<
        AddQuartersImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddQuartersV2 =
        FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;
using FunctionAddYears =
        FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateTime, Int64, DataTypeDateTime>>;
using FunctionAddYearsV2 =
        FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateV2, UInt32, DataTypeDateV2>>;

struct NowFunctionName {
    static constexpr auto name = "now";
};

struct CurrentTimestampFunctionName {
    static constexpr auto name = "current_timestamp";
};

struct LocalTimeFunctionName {
    static constexpr auto name = "localtime";
};

struct LocalTimestampFunctionName {
    static constexpr auto name = "localtimestamp";
};

using FunctionNow = FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, false>>;
using FunctionCurrentTimestamp =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<CurrentTimestampFunctionName, false>>;
using FunctionLocalTime =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimeFunctionName, false>>;
using FunctionLocalTimestamp =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimestampFunctionName, false>>;

using FunctionNowWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, true>>;
using FunctionCurrentTimestampWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<CurrentTimestampFunctionName, true>>;
using FunctionLocalTimeWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimeFunctionName, true>>;
using FunctionLocalTimestampWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<LocalTimestampFunctionName, true>>;

struct CurDateFunctionName {
    static constexpr auto name = "curdate";
};

struct CurrentDateFunctionName {
    static constexpr auto name = "current_date";
};

FunctionBuilderPtr createCurrentDateFunctionBuilderFunction() {
    return std::make_shared<CurrentDateFunctionBuilder<CurrentDateFunctionName>>();
}
FunctionBuilderPtr createCurDateFunctionBuilderFunction() {
    return std::make_shared<CurrentDateFunctionBuilder<CurDateFunctionName>>();
}

struct CurTimeFunctionName {
    static constexpr auto name = "curtime";
};
struct CurrentTimeFunctionName {
    static constexpr auto name = "current_time";
};

using FunctionCurTime = FunctionCurrentDateOrDateTime<CurrentTimeImpl<CurTimeFunctionName>>;
using FunctionCurrentTime = FunctionCurrentDateOrDateTime<CurrentTimeImpl<CurrentTimeFunctionName>>;
using FunctionUtcTimeStamp = FunctionCurrentDateOrDateTime<UtcTimestampImpl>;

void register_function_date_time_computation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAddSeconds>();
    factory.register_function<FunctionAddMinutes>();
    factory.register_function<FunctionAddHours>();
    factory.register_function<FunctionAddDays>();
    factory.register_function<FunctionAddSecondsV2>();
    factory.register_function<FunctionAddMinutesV2>();
    factory.register_function<FunctionAddHoursV2>();
    factory.register_function<FunctionAddDaysV2>();
    factory.register_function<FunctionAddWeeks>();
    factory.register_function<FunctionAddMonths>();
    factory.register_function<FunctionAddYears>();
    factory.register_function<FunctionAddQuarters>();
    factory.register_function<FunctionAddWeeksV2>();
    factory.register_function<FunctionAddMonthsV2>();
    factory.register_function<FunctionAddYearsV2>();
    factory.register_function<FunctionAddQuartersV2>();

    factory.register_function<FunctionNow>();
    factory.register_function<FunctionCurrentTimestamp>();
    factory.register_function<FunctionLocalTime>();
    factory.register_function<FunctionLocalTimestamp>();
    factory.register_function<FunctionNowWithPrecision>();
    factory.register_function<FunctionCurrentTimestampWithPrecision>();
    factory.register_function<FunctionLocalTimeWithPrecision>();
    factory.register_function<FunctionLocalTimestampWithPrecision>();
    factory.register_function(CurrentDateFunctionName::name,
                              &createCurrentDateFunctionBuilderFunction);
    factory.register_function(CurDateFunctionName::name, &createCurDateFunctionBuilderFunction);
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionCurrentTime>();
    factory.register_function<FunctionUtcTimeStamp>();

    // alias
    factory.register_alias("days_add", "date_add");
    factory.register_alias("days_add", "adddate");
    factory.register_alias("months_add", "add_months");
}

} // namespace doris::vectorized