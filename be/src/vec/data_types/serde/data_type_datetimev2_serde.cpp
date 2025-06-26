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

#include "data_type_datetimev2_serde.h"

#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include <chrono> // IWYU pragma: keep
#include <cstdint>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "util/string_parser.hpp"
#include "vec/columns/column_const.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

enum {
    DIVISOR_FOR_SECOND = 1,
    DIVISOR_FOR_MILLI = 1000,
    DIVISOR_FOR_MICRO = 1000000,
    DIVISOR_FOR_NANO = 1000000000
};

namespace doris::vectorized {
static const int64_t micro_to_nano_second = 1000;
#include "common/compile_check_begin.h"

Status DataTypeDateTimeV2SerDe::from_string_batch(const ColumnString& col_str,
                                                  ColumnNullable& col_res,
                                                  const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnDateTimeV2&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_element(i);
        DateV2Value<DateTimeV2ValueType> res;
        Status st = _from_string_strict_mode(str, res, options.timezone);
        if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            st = _from_string(str, res, options.timezone);
        }
        if (st.ok()) {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res);
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            // if still invalid, set null
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] =
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(MIN_DATETIME_V2);
        } else {
            // some internal error
            return st;
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::from_string_strict_mode_batch(const ColumnString& col_str,
                                                              IColumn& col_res,
                                                              const FormatOptions& options) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnDateTimeV2&>(col_res);

    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_element(i);
        DateV2Value<DateTimeV2ValueType> res;
        RETURN_IF_ERROR(_from_string_strict_mode(str, res, options.timezone));
        col_data.get_data()[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(res);
    }
    return Status::OK();
}

/**
<datetime> ::= <whitespace>* <date> (<delimiter> <time> <whitespace>* <timezone>?)? <whitespace>*

<date> ::= <year> <separator> <month> <separator> <day>
<time> ::= <hour> <separator> <minute> <separator> <second> [<fraction>]

<year> ::= <digit>{4} | <digit>{2}
<month> ::= <digit>{1,2}
<day> ::= <digit>{1,2}
<hour> ::= <digit>{1,2}
<minute> ::= <digit>{1,2}
<second> ::= <digit>{1,2}

<separator> ::= ^(<digit> | <alpha>)
<delimiter> ::= " " | "T"

<fraction> ::= "." <digit>*

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<offset>         ::= ( "+" | "-" ) <hour-offset> [ ":"? <minute-offset> ]
                   | <tz-name>

<tz-name>        ::= <short-tz> | <long-tz> 

<short-tz>       ::= "CST" | "UTC" | "GMT" | "ZULU" | "Z"   ; 忽略大小写
<long-tz>        ::= <area> "/" <location>                  ; e.g. America/New_York

<hour-offset>    ::= <digit>{1,2}      ; 0–14
<minute-offset>  ::= <digit>{2}        ; 00/30/45

<area>           ::= <alpha>+
<location>       ::= (<alpha> | "_")+

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<whitespace> ::= " " | "\t" | "\n" | "\r" | "\v" | "\f"

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<alpha>          ::= "A" | … | "Z" | "a" | … | "z"
*/
Status DataTypeDateTimeV2SerDe::_from_string(const std::string& str,
                                             DateV2Value<DateTimeV2ValueType>& res,
                                             const cctz::time_zone* local_time_zone) const {
                                                return Status::OK(); // TODO: Implement this function
                                             }

/**
<datetime>       ::= <date> (("T" | " ") <time> <whitespace>* <offset>?)?

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<date>           ::= <year> "-" <month1> "-" <day1>
                   | <year> <month2> <day2>

<year>           ::= <digit>{2} | <digit>{4} ; 1970 为界
<month1>         ::= <digit>{1,2}            ; 01–12
<day1>           ::= <digit>{1,2}            ; 01–28/29/30/31 视月份而定

<month2>         ::= <digit>{2}              ; 01–12
<day2>           ::= <digit>{2}              ; 01–28/29/30/31 视月份而定

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<time>           ::= <hour1> (":" <minute1> (":" <second1> <fraction>?)?)?
                   | <hour2> (<minute2> (<second2> <fraction>?)?)?

<hour1>           ::= <digit>{1,2}      ; 00–23
<minute1>         ::= <digit>{1,2}      ; 00–59
<second1>         ::= <digit>{1,2}      ; 00–59

<hour2>           ::= <digit>{2}        ; 00–23
<minute2>         ::= <digit>{2}        ; 00–59
<second2>         ::= <digit>{2}        ; 00–59

<fraction>        ::= "." <digit>*

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<offset>         ::= ( "+" | "-" ) <hour-offset> [ ":"? <minute-offset> ]
                   | <tz-name>

<tz-name>        ::= <short-tz> | <long-tz> 

<short-tz>       ::= "CST" | "UTC" | "GMT" | "ZULU" | "Z"   ; 忽略大小写
<long-tz>        ::= <area> "/" <location>                  ; e.g. America/New_York

<hour-offset>    ::= <digit>{1,2}      ; 0–14
<minute-offset>  ::= <digit>{2}        ; 00/30/45

––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

<digit>          ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<area>           ::= <alpha>+
<location>       ::= (<alpha> | "_")+
<alpha>          ::= "A" | … | "Z" | "a" | … | "z"
<whitespace>     ::= " " | "\t" | "\n" | "\r" | "\v" | "\f"
*/
Status DataTypeDateTimeV2SerDe::_from_string_strict_mode(
        const std::string& str, DateV2Value<DateTimeV2ValueType>& res,
        const cctz::time_zone* local_time_zone) const {
    const char* ptr = str.data();
    const char* end = ptr + str.size();

    // date part
    RETURN_IF_ERROR(assert_within_bound(ptr, end, 5));
    if (is_digit_range(ptr, ptr + 5)) {
        // no delimiter here.
        uint32_t part[4];
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        if (is_numeric_ascii(*ptr)) {
            // 4 digits year
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[3])));
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                                      "invalid year {}", part[0] * 100 + part[1]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[2]),
                                      "invalid month {}", part[2]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[3]), "invalid day {}",
                                      part[3]);
        } else {
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::YEAR>(part[0]), "invalid year {}",
                                      part[0]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                      "invalid month {}", part[1]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[2]), "invalid day {}",
                                      part[2]);
        }
    } else {
        // has delimiter here.
        uint32_t part[4];
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
        if (*ptr == '-') {
            // 2 digits year
            ++ptr;
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));

            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::YEAR>(part[0]), "invalid year {}",
                                      part[0]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[1]),
                                      "invalid month {}", part[1]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[2]), "invalid day {}",
                                      part[2]);
        } else {
            // 4 digits year
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[3])));

            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                                      "invalid year {}", part[0] * 100 + part[1]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MONTH>(part[2]),
                                      "invalid month {}", part[2]);
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::DAY>(part[3]), "invalid day {}",
                                      part[3]);
        }
    }

    if (ptr == end) {
        // no time part, just return.
        res.unchecked_set_time_unit<TimeUnit::HOUR>(0);
        res.unchecked_set_time_unit<TimeUnit::MINUTE>(0);
        res.unchecked_set_time_unit<TimeUnit::SECOND>(0);
        res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
        return Status::OK();
    }

    RETURN_IF_ERROR(consume_one_delimiter(ptr, end));

    // time part.
    uint32_t part[3];
    bool has_second = false;
    // hour
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[0])));
    RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::HOUR>(part[0]), "invalid hour {}",
                              part[0]);
    RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
    if (*ptr == ':') {
        // with hour:minute:second
        if (consume_one_colon(ptr, end)) { // minute
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[1])));
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MINUTE>(part[1]),
                                      "invalid minute {}", part[1]);
            if (consume_one_colon(ptr, end)) { // second
                has_second = true;
                RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::SECOND>(part[2]),
                                          "invalid second {}", part[2]);
            } else {
                res.unchecked_set_time_unit<TimeUnit::SECOND>(0);
            }
        } else {
            res.unchecked_set_time_unit<TimeUnit::MINUTE>(0);
            res.unchecked_set_time_unit<TimeUnit::SECOND>(0);
        }
    } else {
        // no ':'
        if (consume_digit<UInt32, 2>(ptr, end, part[1])) {
            // has minute
            RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::MINUTE>(part[1]),
                                      "invalid minute {}", part[1]);
            if (consume_digit<UInt32, 2>(ptr, end, part[2])) {
                // has second
                has_second = true;
                RETURN_INVALID_ARG_IF_NOT(res.set_time_unit<TimeUnit::SECOND>(part[2]),
                                          "invalid second {}", part[2]);
            }
        }
    }

    // fractional part
    if (has_second && assert_within_bound(ptr, end, 0).ok() && *ptr == '.') {
        ++ptr;

        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        auto length = ptr - start;

        if (length > 0) {
            StringParser::ParseResult success;
            auto ms = StringParser::string_to_int_internal<uint32_t, true>(start, 6, &success);
            DCHECK(success ==
                   StringParser::PARSE_SUCCESS); // skip_any_digit ensured [start, ptr) is digit

            if (length > 6) {
                // round off to at most 6 digits
                if (auto remainder = *(start + 6) - '0'; remainder >= 5) {
                    ms++;
                    DCHECK(ms <= 1000000);
                    if (ms == 1000000) {
                        // overflow, round up to next second
                        res.date_add_interval<TimeUnit::SECOND>(
                                TimeInterval {TimeUnit::SECOND, 1, false});
                        ms = 0;
                    }
                }
                res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(ms);
            } else {
                res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(
                        (int32_t)ms * common::exp10_i32(6 - (int)length));
            }
        }
    } else {
        res.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
    }
    static_cast<void>(skip_any_digit(ptr, end));

    static_cast<void>(skip_any_whitespace(ptr, end));

    // timezone part
    if (ptr != end) {
        cctz::time_zone parsed_tz {};
        if (*ptr == '+' || *ptr == '-') {
            // offset
            const auto* start = ptr;
            ++ptr;
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[0])));
            RETURN_INVALID_ARG_IF_NOT(part[0] <= 14, "invalid hour offset {}", part[0]);
            if (assert_within_bound(ptr, end, 0).ok() && *ptr == ':') {
                ++ptr;
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
                RETURN_INVALID_ARG_IF_NOT(part[1] < 60, "invalid minute offset {}", part[1]);
            }

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone offset '{}'", std::string {start, ptr});
        } else {
            // timezone name
            const auto* start = ptr;
            RETURN_IF_ERROR(skip_tz_name(ptr, end));

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }
        // convert tz
        cctz::civil_second cs {res.year(), res.month(),  res.day(),
                               res.hour(), res.minute(), res.second()};

        auto given = cctz::convert(cs, parsed_tz);
        auto local = cctz::convert(given, *local_time_zone);
        res.unchecked_set_time_unit<TimeUnit::YEAR>((uint32_t)local.year());
        res.unchecked_set_time_unit<TimeUnit::MONTH>((uint32_t)local.month());
        res.unchecked_set_time_unit<TimeUnit::DAY>((uint32_t)local.day());
        res.unchecked_set_time_unit<TimeUnit::HOUR>((uint32_t)local.hour());
        res.unchecked_set_time_unit<TimeUnit::MINUTE>((uint32_t)local.minute());
        res.unchecked_set_time_unit<TimeUnit::SECOND>((uint32_t)local.second());

        static_cast<void>(skip_any_whitespace(ptr, end));
        RETURN_INVALID_ARG_IF_NOT(ptr == end,
                                  "invalid datetime string '{}', extra characters after timezone",
                                  std::string {ptr, end});
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                         int64_t end_idx, BufferWritable& bw,
                                                         FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateTimeV2SerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                           BufferWritable& bw,
                                                           FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    if (_nesting_level > 1) {
        bw.write('"');
    }
    UInt64 int_val =
            assert_cast<const ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(*ptr).get_element(
                    row_num);
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf);
    bw.write(buf, pos - buf - 1);

    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}
Status DataTypeDateTimeV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                               const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(column);
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    UInt64 val = 0;
    if (ReadBuffer rb(slice.data, slice.size);
        !read_datetime_v2_text_impl<UInt64>(val, rb, scale)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_arrow(const IColumn& column,
                                                      const NullMap* null_map,
                                                      arrow::ArrayBuilder* array_builder,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone& ctz) const {
    const auto& col_data = static_cast<const ColumnDateTimeV2&>(column).get_data();
    auto& timestamp_builder = assert_cast<arrow::TimestampBuilder&>(*array_builder);
    std::shared_ptr<arrow::TimestampType> timestamp_type =
            std::static_pointer_cast<arrow::TimestampType>(array_builder->type());
    const std::string& timezone = timestamp_type->timezone();
    const cctz::time_zone& real_ctz = timezone == "" ? cctz::utc_time_zone() : ctz;
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else {
            int64_t timestamp = 0;
            DateV2Value<DateTimeV2ValueType> datetime_val =
                    binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[i]);
            datetime_val.unix_timestamp(&timestamp, real_ctz);

            if (scale > 3) {
                uint32_t microsecond = datetime_val.microsecond();
                timestamp = (timestamp * 1000000) + microsecond;
            } else if (scale > 0) {
                uint32_t millisecond = datetime_val.microsecond() / 1000;
                timestamp = (timestamp * 1000) + millisecond;
            }
            RETURN_IF_ERROR(checkArrowStatus(timestamp_builder.Append(timestamp), column.get_name(),
                                             array_builder->type()->name()));
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::read_column_from_arrow(IColumn& column,
                                                       const arrow::Array* arrow_array,
                                                       int64_t start, int64_t end,
                                                       const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnDateTimeV2&>(column).get_data();
    int64_t divisor = 1;
    if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        const auto* concrete_array = dynamic_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: {
            divisor = DIVISOR_FOR_SECOND;
            break;
        }
        case arrow::TimeUnit::type::MILLI: {
            divisor = DIVISOR_FOR_MILLI;
            break;
        }
        case arrow::TimeUnit::type::MICRO: {
            divisor = DIVISOR_FOR_MICRO;
            break;
        }
        case arrow::TimeUnit::type::NANO: {
            divisor = DIVISOR_FOR_NANO;
            break;
        }
        default: {
            LOG(WARNING) << "not support convert to datetimev2 from time_unit:" << type->unit();
            return Status::InvalidArgument("not support convert to datetimev2 from time_unit: {}",
                                           type->unit());
        }
        }
        for (auto value_i = start; value_i < end; ++value_i) {
            auto utc_epoch = static_cast<UInt64>(concrete_array->Value(value_i));

            DateV2Value<DateTimeV2ValueType> v;
            // convert second
            v.from_unixtime(utc_epoch / divisor, ctz);
            // get rest time
            // add 0 on the right to make it 6 digits. DateTimeV2Value microsecond is 6 digits,
            // the scale decides to keep the first few digits, so the valid digits should be kept at the front.
            // "2022-01-01 11:11:11.111", utc_epoch = 1641035471111, divisor = 1000, set_microsecond(111000)
            v.set_microsecond((utc_epoch % divisor) * DIVISOR_FOR_MICRO / divisor);
            col_data.emplace_back(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v));
        }
    } else {
        LOG(WARNING) << "not support convert to datetimev2 from arrow type:"
                     << arrow_array->type()->id();
        return Status::InternalError("not support convert to datetimev2 from arrow type: {}",
                                     arrow_array->type()->id());
    }
    return Status::OK();
}

template <bool is_binary_format>
Status DataTypeDateTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                       MysqlRowBuffer<is_binary_format>& result,
                                                       int64_t row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnDateTimeV2&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    DateV2Value<DateTimeV2ValueType> date_val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(data[col_index]);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(date_val, scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<true>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<false>& row_buffer,
                                                      int64_t row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_orc(const std::string& timezone,
                                                    const IColumn& column, const NullMap* null_map,
                                                    orc::ColumnVectorBatch* orc_col_batch,
                                                    int64_t start, int64_t end,
                                                    std::vector<StringRef>& buffer_list) const {
    const auto& col_data = assert_cast<const ColumnDateTimeV2&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::TimestampVectorBatch*>(orc_col_batch);

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }

        int64_t timestamp = 0;
        DateV2Value<DateTimeV2ValueType> datetime_val =
                binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[row_id]);
        if (!datetime_val.unix_timestamp(&timestamp, timezone)) {
            return Status::InternalError("get unix timestamp error.");
        }

        cur_batch->data[row_id] = timestamp;
        cur_batch->nanoseconds[row_id] = datetime_val.microsecond() * micro_to_nano_second;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, uint64_t rows, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    if (rows < 1) [[unlikely]] {
        return Status::OK();
    }
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

void DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(IColumn& column,
                                                                      uint64_t times) const {
    if (times < 1) [[unlikely]] {
        return;
    }
    auto& col = assert_cast<ColumnDateTimeV2&>(column);
    auto sz = col.size();
    UInt64 val = col.get_element(sz - 1);
    col.insert_many_vals(val, times);
}

} // namespace doris::vectorized
