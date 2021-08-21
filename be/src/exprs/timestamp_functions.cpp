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

#include "exprs/timestamp_functions.h"

#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/path_builder.h"
#include "util/timezone_utils.h"

namespace doris {

void TimestampFunctions::init() {}

// TODO: accept Java data/time format strings:
// http://docs.oracle.com/javase/1.4.2/docs/api/java/text/SimpleDateFormat.html
// Convert them to boost format strings.
bool TimestampFunctions::check_format(const StringVal& format, DateTimeValue& t) {
    // For now the format  must be of the form: yyyy-MM-dd HH:mm:ss
    // where the time part is optional.
    switch (format.len) {
    case 10:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd", 10) == 0) {
            t.set_type(TIME_DATE);
            return true;
        }

        break;

    case 19:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd HH:mm:ss", 19) == 0) {
            t.set_type(TIME_DATETIME);
            return true;
        }
        break;

    default:
        break;
    }

    report_bad_format(&format);
    return false;
}

std::string TimestampFunctions::convert_format(const std::string& format) {
    switch (format.size()) {
    case 8:
        if (strncmp(format.c_str(), "yyyyMMdd", 8) == 0) {
            return std::string("%Y%m%d");
        }
        break;
    case 10:
        if (strncmp(format.c_str(), "yyyy-MM-dd", 10) == 0) {
            return std::string("%Y-%m-%d");
        }
        break;
    case 19:
        if (strncmp(format.c_str(), "yyyy-MM-dd HH:mm:ss", 19) == 0) {
            return std::string("%Y-%m-%d %H:%i:%s");
        }
        break;
    default:
        break;
    }
    return format;
}

StringVal TimestampFunctions::convert_format(FunctionContext* ctx, const StringVal& format) {
    switch (format.len) {
    case 8:
        if (strncmp((const char*)format.ptr, "yyyyMMdd", 8) == 0) {
            std::string tmp("%Y%m%d");
            return AnyValUtil::from_string_temp(ctx, tmp);
        }
        break;
    case 10:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd", 10) == 0) {
            std::string tmp("%Y-%m-%d");
            return AnyValUtil::from_string_temp(ctx, tmp);
        }
        break;
    case 19:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd HH:mm:ss", 19) == 0) {
            std::string tmp("%Y-%m-%d %H:%i:%s");
            return AnyValUtil::from_string_temp(ctx, tmp);
        }
        break;
    default:
        break;
    }
    return format;
}

void TimestampFunctions::report_bad_format(const StringVal* format) {
    std::string format_str((char*)format->ptr, format->len);
    // LOG(WARNING) << "Bad date/time conversion format: " << format_str
    //             << " Format must be: 'yyyy-MM-dd[ HH:mm:ss]'";
}

IntVal TimestampFunctions::year(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.year());
}

IntVal TimestampFunctions::quarter(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal((ts_value.month() - 1) / 3 + 1);
}

IntVal TimestampFunctions::month(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.month());
}

IntVal TimestampFunctions::day_of_week(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (ts_value.is_valid_date()) {
        return IntVal((ts_value.weekday() + 1) % 7 + 1);
    }
    return IntVal::null();
}

IntVal TimestampFunctions::day_of_month(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.day());
}

IntVal TimestampFunctions::day_of_year(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (ts_value.is_valid_date()) {
        return IntVal(ts_value.day_of_year());
    }
    return IntVal::null();
}

IntVal TimestampFunctions::week_of_year(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (ts_value.is_valid_date()) {
        return IntVal(ts_value.week(mysql_week_mode(3)));
    }
    return IntVal::null();
}

IntVal TimestampFunctions::year_week(FunctionContext *context, const DateTimeVal &ts_val) {
    return year_week(context, ts_val, doris_udf::IntVal{0});
}

IntVal TimestampFunctions::year_week(FunctionContext *context, const DateTimeVal &ts_val, const doris_udf::IntVal &mode) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue &ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (ts_value.is_valid_date()) {
        return ts_value.year_week(mysql_week_mode(mode.val));
    }
    return IntVal::null();
}

IntVal TimestampFunctions::week(FunctionContext *context, const DateTimeVal &ts_val) {
    return week(context, ts_val, doris_udf::IntVal{0});
}

IntVal TimestampFunctions::week(FunctionContext *context, const DateTimeVal &ts_val, const doris_udf::IntVal& mode) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue &ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (ts_value.is_valid_date()) {
        return {ts_value.week(mysql_week_mode(mode.val))};
    }
    return IntVal::null();
}

IntVal TimestampFunctions::hour(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.hour());
}

IntVal TimestampFunctions::minute(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.minute());
}

IntVal TimestampFunctions::second(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.second());
}

DateTimeVal TimestampFunctions::make_date(FunctionContext *ctx, const IntVal &year, const IntVal &count) {
    if (count.val > 0) {
        // year-1-1
        DateTimeValue ts_value{year.val * 10000000000 + 101000000};
        ts_value.set_type(TIME_DATE);
        DateTimeVal ts_val;
        ts_value.to_datetime_val(&ts_val);
        return timestamp_time_op<DAY>(ctx, ts_val, {count.val - 1}, true);
    }
    return DateTimeVal::null();
}

DateTimeVal TimestampFunctions::to_date(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    ts_value.cast_to_date();
    DateTimeVal result;
    ts_value.to_datetime_val(&result);
    return result;
}

DateTimeVal TimestampFunctions::str_to_date(FunctionContext* ctx, const StringVal& str,
                                            const StringVal& format) {
    if (str.is_null || format.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value;
    if (!ts_value.from_date_format_str((const char*)format.ptr, format.len, (const char*)str.ptr,
                                       str.len)) {
        return DateTimeVal::null();
    }

    /// The return type of str_to_date depends on whether the time part is included in the format.
    /// If included, it is datetime, otherwise it is date.
    /// If the format parameter is not constant, the return type will be datetime.
    /// The above judgment has been completed in the FE query planning stage,
    /// so here we directly set the value type to the return type set in the query plan.
    ///
    /// For example:
    /// A table with one column k1 varchar, and has 2 lines:
    ///     "%Y-%m-%d"
    ///     "%Y-%m-%d %H:%i:%s"
    /// Query:
    ///     SELECT str_to_date("2020-09-01", k1) from tbl;
    /// Result will be:
    ///     2020-09-01 00:00:00
    ///     2020-09-01 00:00:00
    if (ctx->impl()->get_return_type().type == doris_udf::FunctionContext::Type::TYPE_DATETIME) {
        ts_value.to_datetime();
    }

    DateTimeVal ts_val;
    ts_value.to_datetime_val(&ts_val);
    return ts_val;
}

StringVal TimestampFunctions::month_name(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return StringVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    const char* name = ts_value.month_name();
    if (name == NULL) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, name);
}

StringVal TimestampFunctions::day_name(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return StringVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    const char* name = ts_value.day_name();
    if (name == NULL) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, name);
}

DateTimeVal TimestampFunctions::years_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                          const IntVal& count) {
    return timestamp_time_op<YEAR>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::years_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                          const IntVal& count) {
    return timestamp_time_op<YEAR>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::months_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                           const IntVal& count) {
    return timestamp_time_op<MONTH>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::months_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                           const IntVal& count) {
    return timestamp_time_op<MONTH>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::weeks_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                          const IntVal& count) {
    return timestamp_time_op<WEEK>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::weeks_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                          const IntVal& count) {
    return timestamp_time_op<WEEK>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::days_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                         const IntVal& count) {
    return timestamp_time_op<DAY>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::days_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                         const IntVal& count) {
    return timestamp_time_op<DAY>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::hours_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                          const IntVal& count) {
    return timestamp_time_op<HOUR>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::hours_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                          const IntVal& count) {
    return timestamp_time_op<HOUR>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::minutes_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                            const IntVal& count) {
    return timestamp_time_op<MINUTE>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::minutes_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                            const IntVal& count) {
    return timestamp_time_op<MINUTE>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::seconds_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                            const IntVal& count) {
    return timestamp_time_op<SECOND>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::seconds_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                            const IntVal& count) {
    return timestamp_time_op<SECOND>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::micros_add(FunctionContext* ctx, const DateTimeVal& ts_val,
                                           const IntVal& count) {
    return timestamp_time_op<MICROSECOND>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::micros_sub(FunctionContext* ctx, const DateTimeVal& ts_val,
                                           const IntVal& count) {
    return timestamp_time_op<MICROSECOND>(ctx, ts_val, count, false);
}

template <TimeUnit unit>
DateTimeVal TimestampFunctions::timestamp_time_op(FunctionContext* ctx, const DateTimeVal& ts_val,
                                                  const IntVal& count, bool is_add) {
    if (ts_val.is_null || count.is_null) {
        return DateTimeVal::null();
    }
    TimeInterval interval(unit, count.val, !is_add);

    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (!ts_value.date_add_interval(interval, unit)) {
        return DateTimeVal::null();
    }
    DateTimeVal new_ts_val;
    ts_value.to_datetime_val(&new_ts_val);

    return new_ts_val;
}

BigIntVal TimestampFunctions::years_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                         const DateTimeVal& ts_val2) {
    return timestamp_diff<YEAR>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::months_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                          const DateTimeVal& ts_val2) {
    return timestamp_diff<MONTH>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::weeks_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                         const DateTimeVal& ts_val2) {
    return timestamp_diff<WEEK>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::days_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                        const DateTimeVal& ts_val2) {
    return timestamp_diff<DAY>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::hours_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                         const DateTimeVal& ts_val2) {
    return timestamp_diff<HOUR>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::minutes_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                           const DateTimeVal& ts_val2) {
    return timestamp_diff<MINUTE>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::seconds_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                           const DateTimeVal& ts_val2) {
    return timestamp_diff<SECOND>(ctx, ts_val1, ts_val2);
}

template <TimeUnit unit>
BigIntVal TimestampFunctions::timestamp_diff(FunctionContext* ctx, const DateTimeVal& ts_val2,
                                             const DateTimeVal& ts_val1) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return BigIntVal::null();
    }

    DateTimeValue ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    DateTimeValue ts_value2 = DateTimeValue::from_datetime_val(ts_val2);

    return DateTimeValue::datetime_diff<unit>(ts_value1, ts_value2);
}

void TimestampFunctions::format_prepare(doris_udf::FunctionContext* context,
                                        doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || context->get_num_args() < 2 ||
        context->get_arg_type(1)->type != doris_udf::FunctionContext::Type::TYPE_VARCHAR ||
        !context->is_arg_constant(1)) {
        VLOG_TRACE << "format_prepare returned";
        return;
    }

    FormatCtx* fc = new FormatCtx();
    context->set_function_state(scope, fc);

    StringVal* format = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
    if (UNLIKELY(format->is_null)) {
        fc->is_valid = false;
        return;
    }

    fc->fmt = convert_format(context, *format);
    int format_len = DateTimeValue::compute_format_len((const char*)fc->fmt.ptr, fc->fmt.len);
    if (UNLIKELY(format_len >= 128)) {
        fc->is_valid = false;
        return;
    }

    fc->is_valid = true;
    return;
}

void TimestampFunctions::format_close(doris_udf::FunctionContext* context,
                                      doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    FormatCtx* fc = reinterpret_cast<FormatCtx*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (fc != nullptr) {
        delete fc;
    }
}

DateTimeVal from_olap_datetime(uint64_t datetime) {
    DateTimeValue ts_value;
    if (!ts_value.from_olap_datetime(datetime)) {
        return DateTimeVal::null();
    }

    DateTimeVal ts_val;
    ts_value.to_datetime_val(&ts_val);
    return ts_val;
}

#define _TR_4(TYPE, type, UNIT, unit)                                                              \
    DateTimeVal TimestampFunctions::unit##_##type(FunctionContext* ctx, const DateTimeVal& ts_val, \
                                                  const IntVal& period,                            \
                                                  const DateTimeVal& origin) {                     \
        return time_round<UNIT, TYPE>(ctx, ts_val, period, origin);                                \
    }                                                                                              \
    DateTimeVal TimestampFunctions::unit##_##type(FunctionContext* ctx, const DateTimeVal& ts_val, \
                                                  const DateTimeVal& origin) {                     \
        return time_round<UNIT, TYPE>(ctx, ts_val, IntVal(1), origin);                             \
    }

#define _TR_5(TYPE, type, UNIT, unit, ORIGIN)                                                      \
    DateTimeVal TimestampFunctions::unit##_##type(FunctionContext* ctx,                            \
                                                  const DateTimeVal& ts_val) {                     \
        return time_round<UNIT, TYPE>(ctx, ts_val, IntVal(1), ORIGIN);                             \
    }                                                                                              \
    DateTimeVal TimestampFunctions::unit##_##type(FunctionContext* ctx, const DateTimeVal& ts_val, \
                                                  const IntVal& period) {                          \
        return time_round<UNIT, TYPE>(ctx, ts_val, period, ORIGIN);                                \
    }

#define FLOOR 0
#define CEIL 1

static const DateTimeVal FIRST_DAY = from_olap_datetime(19700101000000);
static const DateTimeVal FIRST_SUNDAY = from_olap_datetime(19700104000000);

#define TIME_ROUND(UNIT, unit, ORIGIN) \
    _TR_4(FLOOR, floor, UNIT, unit)    \
    _TR_4(CEIL, ceil, UNIT, unit)      \
    _TR_5(FLOOR, floor, UNIT, unit, ORIGIN) _TR_5(CEIL, ceil, UNIT, unit, ORIGIN)

TIME_ROUND(YEAR, year, FIRST_DAY)
TIME_ROUND(MONTH, month, FIRST_DAY)
TIME_ROUND(WEEK, week, FIRST_SUNDAY)
TIME_ROUND(DAY, day, FIRST_DAY)
TIME_ROUND(HOUR, hour, FIRST_DAY)
TIME_ROUND(MINUTE, minute, FIRST_DAY)
TIME_ROUND(SECOND, second, FIRST_DAY)

template <TimeUnit unit, bool type>
DateTimeVal TimestampFunctions::time_round(FunctionContext* ctx, const DateTimeVal& ts_val,
                                           const IntVal& period, const DateTimeVal& origin) {
    if (ts_val.is_null || period.is_null || period.val < 1 || origin.is_null) {
        return DateTimeVal::null();
    }

    DateTimeValue ts1 = DateTimeValue::from_datetime_val(origin);
    DateTimeValue ts2 = DateTimeValue::from_datetime_val(ts_val);
    int64_t diff;
    switch (unit) {
    case YEAR: {
        int year = (ts2.year() - ts1.year());
        diff = year - (ts2.to_int64() % 10000000000 < ts1.to_int64() % 10000000000);
        break;
    }
    case MONTH: {
        int month = (ts2.year() - ts1.year()) * 12 + (ts2.month() - ts1.month());
        diff = month - (ts2.to_int64() % 100000000 < ts1.to_int64() % 100000000);
        break;
    }
    case WEEK: {
        int week = ts2.daynr() / 7 - ts1.daynr() / 7;
        diff = week - (ts2.daynr() % 7 < ts1.daynr() % 7 + (ts2.time_part_diff(ts1) < 0));
        break;
    }
    case DAY: {
        int day = ts2.daynr() - ts1.daynr();
        diff = day - (ts2.time_part_diff(ts1) < 0);
        break;
    }
    case HOUR: {
        int hour = (ts2.daynr() - ts1.daynr()) * 24 + (ts2.hour() - ts1.hour());
        diff = hour - ((ts2.minute() * 60 + ts2.second()) < (ts1.minute() * 60 - ts1.second()));
        break;
    }
    case MINUTE: {
        int minute = (ts2.daynr() - ts1.daynr()) * 24 * 60 + (ts2.hour() - ts1.hour()) * 60 +
                     (ts2.minute() - ts1.minute());
        diff = minute - (ts2.second() < ts1.second());
        break;
    }
    case SECOND: {
        diff = ts2.second_diff(ts1);
        break;
    }
    default:
        return DateTimeVal::null();
    }
    int64_t count = period.val;
    int64_t step = diff - (diff % count + count) % count + (type == FLOOR ? 0 : count);
    bool is_neg = step < 0;

    TimeInterval interval(unit, is_neg ? -step : step, is_neg);
    if (!ts1.date_add_interval(interval, unit)) {
        return DateTimeVal::null();
    }
    DateTimeVal new_ts_val;
    ts1.to_datetime_val(&new_ts_val);
    return new_ts_val;
}

StringVal TimestampFunctions::date_format(FunctionContext* ctx, const DateTimeVal& ts_val,
                                          const StringVal& format) {
    if (ts_val.is_null || format.is_null) {
        return StringVal::null();
    }

    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    FormatCtx* fc =
            reinterpret_cast<FormatCtx*>(ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (UNLIKELY(fc == nullptr)) {
        // prepare phase failed, calculate at runtime
        StringVal new_fmt = convert_format(ctx, format);
        if (DateTimeValue::compute_format_len((const char*)new_fmt.ptr, new_fmt.len) >= 128) {
            return StringVal::null();
        }

        char buf[128];
        if (!ts_value.to_format_string((const char*)new_fmt.ptr, new_fmt.len, buf)) {
            return StringVal::null();
        }
        return AnyValUtil::from_string_temp(ctx, buf);
    }

    if (!fc->is_valid) {
        return StringVal::null();
    }

    char buf[128];
    if (!ts_value.to_format_string((const char*)fc->fmt.ptr, fc->fmt.len, buf)) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, buf);
}

DateTimeVal TimestampFunctions::from_days(FunctionContext* ctx, const IntVal& days) {
    if (days.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value;
    if (!ts_value.from_date_daynr(days.val)) {
        return DateTimeVal::null();
    }
    DateTimeVal ts_val;
    ts_value.to_datetime_val(&ts_val);
    return ts_val;
}

IntVal TimestampFunctions::to_days(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.daynr());
}

DoubleVal TimestampFunctions::time_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                        const DateTimeVal& ts_val2) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return DoubleVal::null();
    }

    const DateTimeValue& ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    const DateTimeValue& ts_value2 = DateTimeValue::from_datetime_val(ts_val2);
    if (ts_value1.is_valid_date() && ts_value2.is_valid_date()) {
        return DoubleVal(ts_value1.second_diff(ts_value2));
    }
    return DoubleVal::null();
}

IntVal TimestampFunctions::date_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                     const DateTimeVal& ts_val2) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    const DateTimeValue& ts_value2 = DateTimeValue::from_datetime_val(ts_val2);
    return IntVal(ts_value1.daynr() - ts_value2.daynr());
}

// TimeZone correlation functions.
DateTimeVal TimestampFunctions::timestamp(FunctionContext* ctx, const DateTimeVal& val) {
    return val;
}

// FROM_UNIXTIME() without format
StringVal TimestampFunctions::from_unix(FunctionContext* context, const IntVal& unix_time) {
    if (unix_time.is_null || unix_time.val < 0 || unix_time.val > INT_MAX) {
        return StringVal::null();
    }

    DateTimeValue dtv;
    if (!dtv.from_unixtime(unix_time.val, context->impl()->state()->timezone_obj())) {
        return StringVal::null();
    }
    char buf[64];
    dtv.to_string(buf);
    return AnyValUtil::from_string_temp(context, buf);
}

// FROM_UNIXTIME() with format
StringVal TimestampFunctions::from_unix(FunctionContext* context, const IntVal& unix_time,
                                        const StringVal& fmt) {
    if (unix_time.is_null || fmt.is_null || unix_time.val < 0 || unix_time.val > INT_MAX) {
        return StringVal::null();
    }

    DateTimeValue dtv;
    if (!dtv.from_unixtime(unix_time.val, context->impl()->state()->timezone_obj())) {
        return StringVal::null();
    }

    FormatCtx* fc = reinterpret_cast<FormatCtx*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (UNLIKELY(fc == nullptr)) {
        // prepare phase failed, calculate at runtime
        StringVal new_fmt = convert_format(context, fmt);
        char buf[128];
        if (!dtv.to_format_string((const char*)new_fmt.ptr, new_fmt.len, buf)) {
            return StringVal::null();
        }
        return AnyValUtil::from_string_temp(context, buf);
    }

    if (!fc->is_valid) {
        return StringVal::null();
    }

    char buf[128];
    if (!dtv.to_format_string((const char*)fc->fmt.ptr, fc->fmt.len, buf)) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(context, buf);
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context) {
    return IntVal(context->impl()->state()->timestamp_ms() / 1000);
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context, const DateTimeValue& ts_value) {
    int64_t timestamp;
    if (!ts_value.unix_timestamp(&timestamp, context->impl()->state()->timezone_obj())) {
        return IntVal::null();
    } else {
        //To compatible to mysql, timestamp not between 1970-01-01 00:00:00 ~ 2038-01-01 00:00:00 return 0
        timestamp = timestamp < 0 ? 0 : timestamp;
        timestamp = timestamp > INT_MAX ? 0 : timestamp;
        return IntVal(timestamp);
    }
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context, const StringVal& string_val,
                                   const StringVal& fmt) {
    if (string_val.is_null || fmt.is_null) {
        return IntVal::null();
    }
    DateTimeValue tv;
    if (!tv.from_date_format_str((const char*)fmt.ptr, fmt.len, (const char*)string_val.ptr,
                                 string_val.len)) {
        return IntVal::null();
    }
    return to_unix(context, tv);
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    return to_unix(context, DateTimeValue::from_datetime_val(ts_val));
}

DateTimeVal TimestampFunctions::utc_timestamp(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000, "+00:00")) {
        return DateTimeVal::null();
    }

    DateTimeVal return_val;
    dtv.to_datetime_val(&return_val);
    return return_val;
}

DateTimeVal TimestampFunctions::now(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                           context->impl()->state()->timezone_obj())) {
        return DateTimeVal::null();
    }

    DateTimeVal return_val;
    dtv.to_datetime_val(&return_val);
    return return_val;
}

DoubleVal TimestampFunctions::curtime(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                           context->impl()->state()->timezone_obj())) {
        return DoubleVal::null();
    }

    return dtv.hour() * 3600 + dtv.minute() * 60 + dtv.second();
}

DateTimeVal TimestampFunctions::curdate(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000,
                           context->impl()->state()->timezone_obj())) {
        return DateTimeVal::null();
    }
    dtv.set_type(TIME_DATE);

    DateTimeVal return_val;
    dtv.to_datetime_val(&return_val);
    return return_val;
}

void TimestampFunctions::convert_tz_prepare(doris_udf::FunctionContext* context,
                                            doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || context->get_num_args() != 3 ||
        context->get_arg_type(1)->type != doris_udf::FunctionContext::Type::TYPE_VARCHAR ||
        context->get_arg_type(2)->type != doris_udf::FunctionContext::Type::TYPE_VARCHAR ||
        !context->is_arg_constant(1) || !context->is_arg_constant(2)) {
        return;
    }

    ConvertTzCtx* ctc = new ConvertTzCtx();
    context->set_function_state(scope, ctc);

    // find from timezone
    StringVal* from = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
    if (UNLIKELY(from->is_null)) {
        ctc->is_valid = false;
        return;
    }
    if (!TimezoneUtils::find_cctz_time_zone(std::string((char*)from->ptr, from->len),
                                            ctc->from_tz)) {
        ctc->is_valid = false;
        return;
    }

    // find to timezone
    StringVal* to = reinterpret_cast<StringVal*>(context->get_constant_arg(2));
    if (UNLIKELY(to->is_null)) {
        ctc->is_valid = false;
        return;
    }
    if (!TimezoneUtils::find_cctz_time_zone(std::string((char*)to->ptr, to->len), ctc->to_tz)) {
        ctc->is_valid = false;
        return;
    }

    ctc->is_valid = true;
    return;
}

DateTimeVal TimestampFunctions::convert_tz(FunctionContext* ctx, const DateTimeVal& ts_val,
                                           const StringVal& from_tz, const StringVal& to_tz) {
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    ConvertTzCtx* ctc = reinterpret_cast<ConvertTzCtx*>(
            ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (UNLIKELY(ctc == nullptr)) {
        int64_t timestamp;
        if (!ts_value.unix_timestamp(&timestamp, std::string((char*)from_tz.ptr, from_tz.len))) {
            return DateTimeVal::null();
        }
        DateTimeValue ts_value2;
        if (!ts_value2.from_unixtime(timestamp, std::string((char*)to_tz.ptr, to_tz.len))) {
            return DateTimeVal::null();
        }

        DateTimeVal return_val;
        ts_value2.to_datetime_val(&return_val);
        return return_val;
    }

    if (!ctc->is_valid) {
        return DateTimeVal::null();
    }

    int64_t timestamp;
    if (!ts_value.unix_timestamp(&timestamp, ctc->from_tz)) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value2;
    if (!ts_value2.from_unixtime(timestamp, ctc->to_tz)) {
        return DateTimeVal::null();
    }

    DateTimeVal return_val;
    ts_value2.to_datetime_val(&return_val);
    return return_val;
}

void TimestampFunctions::convert_tz_close(doris_udf::FunctionContext* context,
                                          doris_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    ConvertTzCtx* ctc = reinterpret_cast<ConvertTzCtx*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (ctc != nullptr) {
        delete ctc;
    }
}

} // namespace doris
