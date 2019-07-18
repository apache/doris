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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>
#include "runtime/string_value.h"
#include "runtime/datetime_value.h"

namespace doris {

class Expr;
class OpcodeRegistry;
class TupleRow;

class TimestampFunctions {
public:
    static void init();
    /// Returns the current time.
    static doris_udf::IntVal to_unix(doris_udf::FunctionContext* context);
    /// Converts 'tv_val' to a unix time_t
    static doris_udf::IntVal to_unix(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& tv_val);
    /// Parses 'string_val' based on the format 'fmt'.
    static doris_udf::IntVal to_unix(
        doris_udf::FunctionContext* context, const doris_udf::StringVal& string_val,
        const doris_udf::StringVal& fmt);
    /// Return a timestamp string from a unix time_t
    /// Optional second argument is the format of the string.
    /// TIME is the integer type of the unix time argument.
    static doris_udf::StringVal from_unix(
        doris_udf::FunctionContext* context, const doris_udf::IntVal& unix_time);
    static doris_udf::StringVal from_unix(
        doris_udf::FunctionContext* context, const doris_udf::IntVal& unix_time,
        const doris_udf::StringVal& fmt);

    // Functions to extract parts of the timestamp, return integers.
    static doris_udf::IntVal year(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal quarter(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal month(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal day_of_week(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal day_of_month(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal day_of_year(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal week_of_year(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal hour(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal minute(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal second(
        doris_udf::FunctionContext* context, const doris_udf::DateTimeVal& ts_val);

    // Date/time functions.
    static doris_udf::DateTimeVal now(doris_udf::FunctionContext* context);
    static doris_udf::DateTimeVal curtime(doris_udf::FunctionContext* context);
    static doris_udf::DateTimeVal utc_timestamp(doris_udf::FunctionContext* context);
    static doris_udf::DateTimeVal to_date(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::IntVal date_diff(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val1,
        const doris_udf::DateTimeVal& ts_val2);
    static doris_udf::TimeVal time_diff(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val1,
        const doris_udf::DateTimeVal& ts_val2);

    static doris_udf::DateTimeVal years_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal years_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal months_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal months_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal weeks_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal weeks_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal days_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal days_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal hours_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal hours_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal minutes_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal minutes_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal seconds_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal seconds_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal micros_add(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);
    static doris_udf::DateTimeVal micros_sub(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::IntVal& count);

    static doris_udf::StringVal date_format(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
        const doris_udf::StringVal& format);
    static doris_udf::DateTimeVal from_days(
        doris_udf::FunctionContext* ctx, const doris_udf::IntVal& days);
    static doris_udf::IntVal to_days(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::DateTimeVal str_to_date(
        doris_udf::FunctionContext* ctx, const doris_udf::StringVal& str,
        const doris_udf::StringVal& format);
    static doris_udf::StringVal month_name(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val);
    static doris_udf::StringVal day_name(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val);

    static doris_udf::DateTimeVal timestamp(
        doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& val);

    // Helper for add/sub functions on the time portion.
    template <TimeUnit unit>
    static doris_udf::DateTimeVal timestamp_time_op(
            doris_udf::FunctionContext* ctx, const doris_udf::DateTimeVal& ts_val,
            const doris_udf::IntVal& count, bool is_add);

    // Convert a timestamp to or from a particular timezone based time.
    static void* from_utc(Expr* e, TupleRow* row);
    static void* to_utc(Expr* e, TupleRow* row);

    // Helper function to check date/time format strings.
    // TODO: eventually return format converted from Java to Boost.
    static bool check_format(const StringVal& format, DateTimeValue& t);

    // Issue a warning for a bad format string.
    static void report_bad_format(const StringVal* format);

};

// Functions to load and access the timestamp database.
class TimezoneDatabase {
public:
    TimezoneDatabase();
    ~TimezoneDatabase();

    static boost::local_time::time_zone_ptr find_timezone(const std::string& tz);

private:
    static const char* _s_timezone_database_str;
    static boost::local_time::tz_database _s_tz_database;
    static std::vector<std::string> _s_tz_region_list;
};

}

#endif
