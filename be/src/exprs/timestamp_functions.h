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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_TIMESTAMP_FUNCTIONS_H
#define DORIS_BE_SRC_QUERY_EXPRS_TIMESTAMP_FUNCTIONS_H

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/thread/thread.hpp>

#include "runtime/datetime_value.h"
#include "runtime/string_value.h"

namespace doris {

class Expr;
class OpcodeRegistry;
class TupleRow;

// The context used for timestamp function prepare phase,
// to save the converted date formatter, so that it doesn't
// need to be converted for each rows.
struct FormatCtx {
    // false means the format is invalid, and the function always return null
    bool is_valid = false;
    StringVal fmt;
};

// The context used for convert tz
struct ConvertTzCtx {
    // false means the format is invalid, and the function always return null
    bool is_valid = false;
    cctz::time_zone from_tz;
    cctz::time_zone to_tz;
};

class TimestampFunctions {
public:
    static void init();

    // Functions to extract parts of the timestamp, return integers.
    static doris_udf::IntVal year(doris_udf::FunctionContext* context,
                                  const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal quarter(doris_udf::FunctionContext* context,
                                     const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal month(doris_udf::FunctionContext* context,
                                   const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal day_of_week(doris_udf::FunctionContext* context,
                                         const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal day_of_month(doris_udf::FunctionContext* context,
                                          const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal day_of_year(doris_udf::FunctionContext* context,
                                         const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal week_of_year(doris_udf::FunctionContext* context,
                                          const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal year_week(doris_udf::FunctionContext *context,
                                       const doris_udf::DateTimeVal &ts_val);
    static doris_udf::IntVal year_week(doris_udf::FunctionContext *context,
                                       const doris_udf::DateTimeVal &ts_val,
                                       const doris_udf::IntVal &para);
    static doris_udf::IntVal week(doris_udf::FunctionContext *context,
                                  const doris_udf::DateTimeVal &ts_val);
    static doris_udf::IntVal week(doris_udf::FunctionContext *context,
                                  const doris_udf::DateTimeVal &ts_val,
                                  const doris_udf::IntVal &mode);
    static doris_udf::IntVal hour(doris_udf::FunctionContext* context,
                                  const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal minute(doris_udf::FunctionContext* context,
                                    const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal second(doris_udf::FunctionContext* context,
                                    const doris_udf::DateTimeVal& ts_val);

    // Date/time functions.
    static doris_udf::DateTimeVal make_date(doris_udf::FunctionContext* ctx,
                                           const doris_udf::IntVal& year,
                                           const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal to_date(doris_udf::FunctionContext* ctx,
                                          const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal date_diff(doris_udf::FunctionContext* ctx,
                                       const doris_udf::DateTimeVal& ts_val1,
                                       const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::DoubleVal time_diff(doris_udf::FunctionContext* ctx,
                                          const doris_udf::DateTimeVal& ts_val1,
                                          const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::DateTimeVal years_add(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal years_sub(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal months_add(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal months_sub(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal weeks_add(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal weeks_sub(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal days_add(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val,
                                           const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal days_sub(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val,
                                           const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal hours_add(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal hours_sub(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal minutes_add(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal minutes_sub(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal seconds_add(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal seconds_sub(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal micros_add(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal micros_sub(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& count);
    static doris_udf::StringVal date_format(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::StringVal& format);
    static doris_udf::DateTimeVal from_days(doris_udf::FunctionContext* ctx,
                                            const doris_udf::IntVal& days);
    static doris_udf::IntVal to_days(doris_udf::FunctionContext* ctx,
                                     const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal str_to_date(doris_udf::FunctionContext* ctx,
                                              const doris_udf::StringVal& str,
                                              const doris_udf::StringVal& format);
    static doris_udf::StringVal month_name(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val);
    static doris_udf::StringVal day_name(doris_udf::FunctionContext* ctx,
                                         const doris_udf::DateTimeVal& ts_val);

    // timestamp function
    template <TimeUnit unit>
    static doris_udf::BigIntVal timestamp_diff(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val1,
                                               const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::BigIntVal years_diff(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val1,
                                           const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::BigIntVal months_diff(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val1,
                                            const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::BigIntVal weeks_diff(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val1,
                                           const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::BigIntVal days_diff(doris_udf::FunctionContext* ctx,
                                          const doris_udf::DateTimeVal& ts_val1,
                                          const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::BigIntVal hours_diff(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val1,
                                           const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::BigIntVal minutes_diff(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val1,
                                             const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::BigIntVal seconds_diff(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val1,
                                             const doris_udf::DateTimeVal& ts_val2);

    // Period functions.
    template <TimeUnit unit, bool type>
    static doris_udf::DateTimeVal time_round(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period,
                                             const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal year_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal year_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal year_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal year_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period,
                                             const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal year_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal year_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal year_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal year_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period,
                                            const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal month_floor(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal month_floor(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal month_floor(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal month_floor(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& period,
                                              const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal month_ceil(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal month_ceil(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal month_ceil(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal month_ceil(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period,
                                             const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal week_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal week_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal week_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal week_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period,
                                             const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal week_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal week_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal week_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal week_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period,
                                            const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal day_floor(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal day_floor(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal day_floor(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal day_floor(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period,
                                            const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal day_ceil(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal day_ceil(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val,
                                           const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal day_ceil(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val,
                                           const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal day_ceil(doris_udf::FunctionContext* ctx,
                                           const doris_udf::DateTimeVal& ts_val,
                                           const doris_udf::IntVal& period,
                                           const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal hour_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal hour_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal hour_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal hour_floor(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::IntVal& period,
                                             const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal hour_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal hour_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal hour_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal hour_ceil(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& ts_val,
                                            const doris_udf::IntVal& period,
                                            const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal minute_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal minute_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val,
                                               const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal minute_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val,
                                               const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal minute_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val,
                                               const doris_udf::IntVal& period,
                                               const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal minute_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal minute_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal minute_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal minute_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& period,
                                              const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal second_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal second_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val,
                                               const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal second_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val,
                                               const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal second_floor(doris_udf::FunctionContext* ctx,
                                               const doris_udf::DateTimeVal& ts_val,
                                               const doris_udf::IntVal& period,
                                               const doris_udf::DateTimeVal& origin);

    static doris_udf::DateTimeVal second_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal second_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& period);
    static doris_udf::DateTimeVal second_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::DateTimeVal& origin);
    static doris_udf::DateTimeVal second_ceil(doris_udf::FunctionContext* ctx,
                                              const doris_udf::DateTimeVal& ts_val,
                                              const doris_udf::IntVal& period,
                                              const doris_udf::DateTimeVal& origin);

    // TimeZone correlation functions.
    static doris_udf::DateTimeVal timestamp(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DateTimeVal& val);
    // Helper for add/sub functions on the time portion.
    template <TimeUnit unit>
    static doris_udf::DateTimeVal timestamp_time_op(doris_udf::FunctionContext* ctx,
                                                    const doris_udf::DateTimeVal& ts_val,
                                                    const doris_udf::IntVal& count, bool is_add);
    static doris_udf::DateTimeVal now(doris_udf::FunctionContext* context);
    static doris_udf::DoubleVal curtime(doris_udf::FunctionContext* context);
    static doris_udf::DateTimeVal curdate(doris_udf::FunctionContext* context);
    static doris_udf::DateTimeVal utc_timestamp(doris_udf::FunctionContext* context);
    /// Returns the current time.
    static doris_udf::IntVal to_unix(FunctionContext* context, const DateTimeValue& ts_value);
    static doris_udf::IntVal to_unix(doris_udf::FunctionContext* context);
    /// Converts 'tv_val' to a unix time_t
    static doris_udf::IntVal to_unix(doris_udf::FunctionContext* context,
                                     const doris_udf::DateTimeVal& tv_val);
    /// Parses 'string_val' based on the format 'fmt'.
    static doris_udf::IntVal to_unix(doris_udf::FunctionContext* context,
                                     const doris_udf::StringVal& string_val,
                                     const doris_udf::StringVal& fmt);
    /// Return a timestamp string from a unix time_t
    /// Optional second argument is the format of the string.
    /// TIME is the integer type of the unix time argument.
    static doris_udf::StringVal from_unix(doris_udf::FunctionContext* context,
                                          const doris_udf::IntVal& unix_time);
    static doris_udf::StringVal from_unix(doris_udf::FunctionContext* context,
                                          const doris_udf::IntVal& unix_time,
                                          const doris_udf::StringVal& fmt);
    static doris_udf::DateTimeVal convert_tz(doris_udf::FunctionContext* ctx,
                                             const doris_udf::DateTimeVal& ts_val,
                                             const doris_udf::StringVal& from_tz,
                                             const doris_udf::StringVal& to_tz);

    // Helper function to check date/time format strings.
    // TODO: eventually return format converted from Java to Boost.
    static bool check_format(const StringVal& format, DateTimeValue& t);

    // In order to support 0.11 grayscale upgrade
    // Todo(kks): remove this method when 0.12 release
    static StringVal convert_format(doris_udf::FunctionContext* ctx, const StringVal& format);

    static std::string convert_format(const std::string& format);

    // Issue a warning for a bad format string.
    static void report_bad_format(const StringVal* format);

    static void format_prepare(doris_udf::FunctionContext* context,
                               doris_udf::FunctionContext::FunctionStateScope scope);

    static void format_close(doris_udf::FunctionContext* context,
                             doris_udf::FunctionContext::FunctionStateScope scope);

    static void convert_tz_prepare(doris_udf::FunctionContext* context,
                                   doris_udf::FunctionContext::FunctionStateScope scope);

    static void convert_tz_close(doris_udf::FunctionContext* context,
                                 doris_udf::FunctionContext::FunctionStateScope scope);
};
} // namespace doris

#endif
