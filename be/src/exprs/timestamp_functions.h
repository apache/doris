// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_TIMESTAMP_FUNCTIONS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_TIMESTAMP_FUNCTIONS_H

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>
#include "runtime/string_value.h"
#include "runtime/datetime_value.h"

namespace palo {

class Expr;
class OpcodeRegistry;
class TupleRow;

class TimestampFunctions {
public:
    static void init();
    /// Returns the current time.
    static palo_udf::IntVal to_unix(palo_udf::FunctionContext* context);
    /// Converts 'tv_val' to a unix time_t
    static palo_udf::IntVal to_unix(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& tv_val);
    /// Parses 'string_val' based on the format 'fmt'.
    static palo_udf::IntVal to_unix(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& string_val,
        const palo_udf::StringVal& fmt);
    /// Return a timestamp string from a unix time_t
    /// Optional second argument is the format of the string.
    /// TIME is the integer type of the unix time argument.
    static palo_udf::StringVal from_unix(
        palo_udf::FunctionContext* context, const palo_udf::IntVal& unix_time);
    static palo_udf::StringVal from_unix(
        palo_udf::FunctionContext* context, const palo_udf::IntVal& unix_time,
        const palo_udf::StringVal& fmt);

    // Functions to extract parts of the timestamp, return integers.
    static palo_udf::IntVal year(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal quarter(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal month(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal day_of_month(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal day_of_year(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal week_of_year(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal hour(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal minute(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal second(
        palo_udf::FunctionContext* context, const palo_udf::DateTimeVal& ts_val);

    // Date/time functions.
    static palo_udf::DateTimeVal now(palo_udf::FunctionContext* context);
    static palo_udf::DateTimeVal curtime(palo_udf::FunctionContext* context);
    static palo_udf::DateTimeVal to_date(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::IntVal date_diff(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val1,
        const palo_udf::DateTimeVal& ts_val2);
    static palo_udf::DateTimeVal time_diff(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val1,
        const palo_udf::DateTimeVal& ts_val2);

    static palo_udf::DateTimeVal years_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal years_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal months_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal months_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal weeks_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal weeks_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal days_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal days_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal hours_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal hours_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal minutes_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal minutes_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal seconds_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal seconds_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal micros_add(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);
    static palo_udf::DateTimeVal micros_sub(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::IntVal& count);

    static palo_udf::StringVal date_format(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
        const palo_udf::StringVal& format);
    static palo_udf::DateTimeVal from_days(
        palo_udf::FunctionContext* ctx, const palo_udf::IntVal& days);
    static palo_udf::IntVal to_days(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::DateTimeVal str_to_date(
        palo_udf::FunctionContext* ctx, const palo_udf::StringVal& str,
        const palo_udf::StringVal& format);
    static palo_udf::StringVal month_name(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val);
    static palo_udf::StringVal day_name(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val);

    static palo_udf::DateTimeVal timestamp(
        palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& val);

    // Helper for add/sub functions on the time portion.
    template <TimeUnit unit>
    static palo_udf::DateTimeVal timestamp_time_op(
            palo_udf::FunctionContext* ctx, const palo_udf::DateTimeVal& ts_val,
            const palo_udf::IntVal& count, bool is_add);

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
