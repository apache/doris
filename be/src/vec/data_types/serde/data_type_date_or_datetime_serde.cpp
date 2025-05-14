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

#include "data_type_date_or_datetime_serde.h"

#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include "datelike_serde_common.hpp"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::serialize_column_to_json(
        const IColumn& column, int64_t start_idx, int64_t end_idx, BufferWritable& bw,
        typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::serialize_one_cell_to_json(
        const IColumn& column, int64_t row_num, BufferWritable& bw,
        typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    if (DataTypeNumberSerDe<T>::_nesting_level > 1) {
        bw.write('"');
    }
    Int64 int_val = assert_cast<const ColumnVector<T>&>(*ptr).get_element(row_num);
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    bw.write(buf, pos - buf - 1);
    if (DataTypeNumberSerDe<T>::_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnVector<T>&>(column);
    if (DataTypeNumberSerDe<T>::_nesting_level > 1) {
        slice.trim_quote();
    }
    Int64 val = 0;
    if (ReadBuffer rb(slice.data, slice.size); !read_date_text_impl<Int64>(val, rb)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                       int64_t end_idx, BufferWritable& bw,
                                                       FormatOptions& options) const {
        SERIALIZE_COLUMN_TO_JSON()}

Status DataTypeDateTimeSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                         BufferWritable& bw,
                                                         FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    Int64 int_val = assert_cast<const ColumnDateTime&>(*ptr).get_element(row_num);
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    bw.write(buf, pos - buf - 1);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                             const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnDateTime&>(column);
    Int64 val = 0;
    if (ReadBuffer rb(slice.data, slice.size); !read_datetime_text_impl<Int64>(val, rb)) {
        return Status::InvalidArgument("parse datetime fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int64_t start,
                                                     int64_t end,
                                                     const cctz::time_zone& ctz) const {
    return _read_column_from_arrow<false>(column, arrow_array, start, end, ctz);
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                   arrow::ArrayBuilder* array_builder,
                                                   int64_t start, int64_t end,
                                                   const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<const ColumnVector<T>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        char buf[64];
        const VecDateTimeValue* time_val = (const VecDateTimeValue*)(&col_data[i]);
        size_t len = time_val->to_buffer(buf);
        if (null_map && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                                             array_builder->type()->name()));
        } else {
            RETURN_IF_ERROR(checkArrowStatus(string_builder.Append(buf, cast_set<int32_t>(len)),
                                             column.get_name(), array_builder->type()->name()));
        }
    }
    return Status::OK();
}

static int64_t time_unit_divisor(arrow::TimeUnit::type unit) {
    // Doris only supports seconds
    switch (unit) {
    case arrow::TimeUnit::type::SECOND: {
        return 1L;
    }
    case arrow::TimeUnit::type::MILLI: {
        return 1000L;
    }
    case arrow::TimeUnit::type::MICRO: {
        return 1000000L;
    }
    case arrow::TimeUnit::type::NANO: {
        return 1000000000L;
    }
    default:
        return 0L;
    }
}

template <PrimitiveType T>
template <bool is_date>
Status DataTypeDateSerDe<T>::_read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int64_t start,
                                                     int64_t end,
                                                     const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnVector<T>&>(column).get_data();
    int64_t divisor = 1;
    int64_t multiplier = 1;
    if (arrow_array->type()->id() == arrow::Type::DATE64) {
        const auto* concrete_array = dynamic_cast<const arrow::Date64Array*>(arrow_array);
        divisor = 1000; //ms => secs
        for (auto value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        const auto* concrete_array = dynamic_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        divisor = time_unit_divisor(type->unit());
        if (divisor == 0L) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid Time Type: " + type->name());
        }
        for (auto value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::DATE32) {
        const auto* concrete_array = dynamic_cast<const arrow::Date32Array*>(arrow_array);
        multiplier = 24 * 60 * 60; // day => secs
        for (auto value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            v.cast_to_date();
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::STRING) {
        // to be compatible with old version, we use string type for date.
        const auto* concrete_array = dynamic_cast<const arrow::StringArray*>(arrow_array);
        for (auto value_i = start; value_i < end; ++value_i) {
            auto val_str = concrete_array->GetString(value_i);
            VecDateTimeValue v;
            v.from_date_str(val_str.c_str(), val_str.length(), ctz);
            if constexpr (is_date) {
                v.cast_to_date();
            }
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else {
        return Status::Error(doris::ErrorCode::INVALID_ARGUMENT,
                             "Unsupported Arrow Type: " + arrow_array->type()->name());
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::read_column_from_arrow(IColumn& column,
                                                    const arrow::Array* arrow_array, int64_t start,
                                                    int64_t end, const cctz::time_zone& ctz) const {
    return _read_column_from_arrow<true>(column, arrow_array, start, end, ctz);
}

template <PrimitiveType T>
template <bool is_binary_format>
Status DataTypeDateSerDe<T>::_write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<is_binary_format>& result, int64_t row_idx,
        bool col_const, const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnVector<T>&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    auto time_num = data[col_index];
    VecDateTimeValue time_val = binary_cast<Int64, VecDateTimeValue>(time_num);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (DataTypeNumberSerDe<T>::_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(time_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (DataTypeNumberSerDe<T>::_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<true>& row_buffer, int64_t row_idx, bool col_const,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::write_column_to_mysql(
        const IColumn& column, MysqlRowBuffer<false>& row_buffer, int64_t row_idx, bool col_const,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                 const NullMap* null_map,
                                                 orc::ColumnVectorBatch* orc_col_batch,
                                                 int64_t start, int64_t end,
                                                 std::vector<StringRef>& buffer_list) const {
    const auto& col_data = assert_cast<const ColumnVector<T>&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    // First pass: calculate total memory needed and collect serialized values
    std::vector<std::string> serialized_values;
    std::vector<size_t> valid_row_indices;
    size_t total_size = 0;
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            char buf[64];
            size_t len = binary_cast<Int64, VecDateTimeValue>(col_data[row_id]).to_buffer(buf);
            total_size += len;
            // avoid copy
            serialized_values.emplace_back(buf, len);
            valid_row_indices.push_back(row_id);
        }
    }
    // Allocate continues memory based on calculated size
    char* ptr = (char*)malloc(total_size);
    if (!ptr) {
        return Status::InternalError(
                "malloc memory {} error when write variant column data to orc file.", total_size);
    }
    StringRef bufferRef;
    bufferRef.data = ptr;
    bufferRef.size = total_size;
    buffer_list.emplace_back(bufferRef);
    // Second pass: copy data to allocated memory
    size_t offset = 0;
    for (size_t i = 0; i < serialized_values.size(); i++) {
        const auto& serialized_value = serialized_values[i];
        size_t row_id = valid_row_indices[i];
        size_t len = serialized_value.length();
        if (offset + len > total_size) {
            return Status::InternalError(
                    "Buffer overflow when writing column data to ORC file. offset {} with len {} "
                    "exceed total_size {} . ",
                    offset, len, total_size);
        }
        memcpy(const_cast<char*>(bufferRef.data) + offset, serialized_value.data(), len);
        cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + offset;
        cur_batch->length[row_id] = len;
        offset += len;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
template <PrimitiveType T>
Status DataTypeDateSerDe<T>::from_string_batch(
        const ColumnString& col_str, ColumnNullable& col_res,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnType&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_element(i);
        CppType res;
        Status st = _from_string_strict_mode(str, res, options.timezone);
        if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            st = _from_string(str, res, options.timezone);
        }
        if (st.ok()) {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(res);
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            // if still invalid, set null
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
        } else {
            // some internal error
            return st;
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status DataTypeDateSerDe<T>::from_string_strict_mode_batch(
        const ColumnString& col_str, IColumn& col_res,
        const typename DataTypeNumberSerDe<T>::FormatOptions& options,
        const NullMap::value_type* null_map) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnType&>(col_res);

    for (size_t i = 0; i < row; ++i) {
        if (null_map && null_map[i]) {
            continue;
        }
        auto str = col_str.get_element(i);
        CppType res;
        RETURN_IF_ERROR(_from_string_strict_mode(str, res, options.timezone));
        col_data.get_data()[i] = binary_cast<CppType, NativeType>(res);
    }
    return Status::OK();
}

// same with datetimev2
template <PrimitiveType T>
Status DataTypeDateSerDe<T>::_from_string(const std::string& str, CppType& res,
                                          const cctz::time_zone* local_time_zone) const {
    const char* ptr = str.data();
    const char* end = ptr + str.size();

    // skip leading whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    if (ptr == end) {
        return Status::InvalidArgument("empty datetime string");
    }

    // date part
    uint32_t year, month, day;

    // read year
    RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, year)));
    if (is_digit_range(ptr, ptr + 1)) {
        // continue by digit, it must be a 4-digit year
        uint32_t year2;
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, year2)));
        year = year * 100 + year2;
    } else {
        // otherwise, it must be a 2-digit year
        if (year < 100) {
            // Convert 2-digit year based on 1970 boundary
            year += (year >= 70) ? 1900 : 2000;
        }
    }

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // read month
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, month)));

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // read day
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, day)));

    if (!try_convert_set_zero_date(res, year, month, day)) {
        RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::YEAR>(year),
                                  "invalid year {}", year);
        RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MONTH>(month),
                                  "invalid month {}", month);
        RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::DAY>(day), "invalid day {}",
                                  day);
    }

    if (ptr == end) {
        // no time part, just return.
        _cast_to_type(res);
        return Status::OK();
    }

    // skip the delimiter if meet.
    if (*ptr == ' ' || *ptr == 'T') {
        ++ptr;
    }

    // time part
    uint32_t hour, minute, second;

    // hour
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, hour)));
    RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::HOUR>(hour), "invalid hour {}",
                              hour);

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // minute
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, minute)));
    RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MINUTE>(minute),
                              "invalid minute {}", minute);

    // check for separator
    RETURN_IF_ERROR(skip_one_non_alnum(ptr, end));

    // second
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, second)));
    RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::SECOND>(second),
                              "invalid second {}", second);

    // fractional part
    if (assert_within_bound(ptr, end, 0).ok() && *ptr == '.') {
        ++ptr;

        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        [[maybe_unused]] auto length = ptr - start;

        if constexpr (IsDatetime) {
            if (length > 0) {
                StringParser::ParseResult success;
                auto ms = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                        start, std::min<int>((int)length, 6), &success);
                RETURN_INVALID_ARG_IF_NOT(success == StringParser::PARSE_SUCCESS,
                                          "invalid fractional part in datetime string '{}'",
                                          std::string {start, ptr});
                // differ to datetimev2, we only need to process carrying on which caused by carrying on to digit 6-th.
                if (length > 6) {
                    // round off to at most 6 digits
                    if (auto remainder = *(start + 6) - '0'; remainder >= 5) {
                        ms++;
                        DCHECK(ms <= 1000000);
                        if (ms == 1000000) {
                            // overflow, round up to next second
                            res.template date_add_interval<TimeUnit::SECOND>(
                                    TimeInterval {TimeUnit::SECOND, 1, false});
                            ms = 0;
                        }
                    }
                }
            }
        }
    }

    // skip any whitespace after time
    static_cast<void>(skip_any_whitespace(ptr, end));

    // timezone part (if any)
    if (ptr != end) {
        cctz::time_zone parsed_tz {};
        if (*ptr == '+' || *ptr == '-') {
            // offset
            const char sign = *ptr;
            ++ptr;
            uint32_t hour_offset, minute_offset = 0;

            uint32_t length = count_digits(ptr, end);
            // hour
            if (length == 1 || length == 3) {
                RETURN_IF_ERROR((consume_digit<UInt32, 1>(ptr, end, hour_offset)));
            } else {
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, hour_offset)));
            }
            RETURN_INVALID_ARG_IF_NOT(hour_offset <= 14, "invalid hour offset {}", hour_offset);
            if (assert_within_bound(ptr, end, 0).ok()) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, minute_offset)));
                RETURN_INVALID_ARG_IF_NOT(
                        (minute_offset == 0 || minute_offset == 30 || minute_offset == 45),
                        "invalid minute offset {}", minute_offset);
            }
            if (hour_offset == 14 && minute_offset > 0) [[unlikely]] {
                return Status::InvalidArgument("invalid timezone offset '{}'",
                                               combine_tz_offset(sign, hour_offset, minute_offset));
            }

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(
                            combine_tz_offset(sign, hour_offset, minute_offset), parsed_tz),
                    "invalid timezone offset '{}'",
                    combine_tz_offset(sign, hour_offset, minute_offset));
        } else {
            // timezone name
            const auto* start = ptr;
            // short tzname, or something legal for tzdata. depends on our TimezoneUtils.
            RETURN_IF_ERROR(skip_tz_name_part(ptr, end));

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }
        if constexpr (IsDatetime) {
            // convert tz
            cctz::civil_second cs {res.year(), res.month(),  res.day(),
                                   res.hour(), res.minute(), res.second()};

            auto given = cctz::convert(cs, parsed_tz);
            auto local = cctz::convert(given, *local_time_zone);
            res.template unchecked_set_time_unit<TimeUnit::YEAR>((uint32_t)local.year());
            res.template unchecked_set_time_unit<TimeUnit::MONTH>((uint32_t)local.month());
            res.template unchecked_set_time_unit<TimeUnit::DAY>((uint32_t)local.day());
            res.template unchecked_set_time_unit<TimeUnit::HOUR>((uint32_t)local.hour());
            res.template unchecked_set_time_unit<TimeUnit::MINUTE>((uint32_t)local.minute());
            res.template unchecked_set_time_unit<TimeUnit::SECOND>((uint32_t)local.second());
        }
    }

    // skip trailing whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    RETURN_INVALID_ARG_IF_NOT(ptr == end,
                              "invalid datetime string '{}', extra characters after parsing",
                              std::string {ptr, end});

    _cast_to_type(res);
    return Status::OK();
}

// same with datetimev2
template <PrimitiveType T>
Status DataTypeDateSerDe<T>::_from_string_strict_mode(
        const std::string& str, CppType& res, const cctz::time_zone* local_time_zone) const {
    const char* ptr = str.data();
    const char* end = ptr + str.size();

    uint32_t part[4];
    bool has_second = false;

    // special `date` and `time` part format: 14-length digits string. parse it as YYYYMMDDHHMMSS
    if (assert_within_bound(ptr, end, 13) && is_digit_range(ptr, ptr + 14)) {
        // if the string is all digits, treat it as a date in YYYYMMDD format.
        RETURN_IF_ERROR((consume_digit<UInt32, 4>(ptr, end, part[0])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
            RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::YEAR>(part[0]),
                                      "invalid year {}", part[0]);
            RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MONTH>(part[1]),
                                      "invalid month {}", part[1]);
            RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::DAY>(part[2]),
                                      "invalid day {}", part[2]);
        }

        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::HOUR>(part[0]),
                                  "invalid hour {}", part[0]);
        RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MINUTE>(part[1]),
                                  "invalid minute {}", part[1]);
        RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::SECOND>(part[2]),
                                  "invalid second {}", part[2]);
        has_second = true;
        if (ptr == end) {
            // no fraction or timezone part, just return.
            return Status::OK();
        }
        goto FRAC;
    }

    // date part
    RETURN_IF_ERROR(assert_within_bound(ptr, end, 5));
    if (is_digit_range(ptr, ptr + 5)) {
        // no delimiter here.
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[2])));
        if (is_numeric_ascii(*ptr)) {
            // 4 digits year
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[3])));
            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.template set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                        "invalid year {}", part[0] * 100 + part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MONTH>(part[2]),
                                          "invalid month {}", part[2]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::DAY>(part[3]),
                                          "invalid day {}", part[3]);
            }
        } else {
            if (!try_convert_set_zero_date(res, complete_4digit_year(part[0]), part[1], part[2])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.template set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MONTH>(part[1]),
                                          "invalid month {}", part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::DAY>(part[2]),
                                          "invalid day {}", part[2]);
            }
        }
    } else {
        // has delimiter here.
        RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
        RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
        if (*ptr == '-') {
            // 2 digits year
            ++ptr; // consume one bar
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[1])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));

            if (!try_convert_set_zero_date(res, part[0], part[1], part[2])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.template set_time_unit<TimeUnit::YEAR>(complete_4digit_year(part[0])),
                        "invalid year {}", part[0]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MONTH>(part[1]),
                                          "invalid month {}", part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::DAY>(part[2]),
                                          "invalid day {}", part[2]);
            }
        } else {
            // 4 digits year
            RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
            RETURN_IF_ERROR((consume_one_bar(ptr, end)));
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[3])));

            if (!try_convert_set_zero_date(res, part[0] * 100 + part[1], part[2], part[3])) {
                RETURN_INVALID_ARG_IF_NOT(
                        res.template set_time_unit<TimeUnit::YEAR>(part[0] * 100 + part[1]),
                        "invalid year {}", part[0] * 100 + part[1]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MONTH>(part[2]),
                                          "invalid month {}", part[2]);
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::DAY>(part[3]),
                                          "invalid day {}", part[3]);
            }
        }
    }

    if (ptr == end) {
        // no time part, just return.
        _cast_to_type(res);
        return Status::OK();
    }

    RETURN_IF_ERROR(consume_one_delimiter(ptr, end));

    // time part.
    // hour
    RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[0])));
    RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::HOUR>(part[0]),
                              "invalid hour {}", part[0]);
    RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
    if (*ptr == ':') {
        // with hour:minute:second
        if (consume_one_colon(ptr, end)) { // minute
            RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[1])));
            RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MINUTE>(part[1]),
                                      "invalid minute {}", part[1]);
            if (consume_one_colon(ptr, end)) { // second
                has_second = true;
                RETURN_IF_ERROR((consume_digit<UInt32, 1, 2>(ptr, end, part[2])));
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::SECOND>(part[2]),
                                          "invalid second {}", part[2]);
            } else {
                res.template unchecked_set_time_unit<TimeUnit::SECOND>(0);
            }
        } else {
            res.template unchecked_set_time_unit<TimeUnit::MINUTE>(0);
            res.template unchecked_set_time_unit<TimeUnit::SECOND>(0);
        }
    } else {
        // no ':'
        if (consume_digit<UInt32, 2>(ptr, end, part[1])) {
            // has minute
            RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::MINUTE>(part[1]),
                                      "invalid minute {}", part[1]);
            if (consume_digit<UInt32, 2>(ptr, end, part[2])) {
                // has second
                has_second = true;
                RETURN_INVALID_ARG_IF_NOT(res.template set_time_unit<TimeUnit::SECOND>(part[2]),
                                          "invalid second {}", part[2]);
            }
        }
    }

FRAC:
    // fractional part
    if (has_second && assert_within_bound(ptr, end, 0).ok() && *ptr == '.') {
        ++ptr;

        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        [[maybe_unused]] auto length = ptr - start;

        if constexpr (IsDatetime) {
            if (length > 0) {
                StringParser::ParseResult success;
                auto ms = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                        start, std::min<int>((int)length, 6), &success);
                RETURN_INVALID_ARG_IF_NOT(success == StringParser::PARSE_SUCCESS,
                                          "invalid fractional part in datetime string '{}'",
                                          std::string {start, ptr});
                // differ to datetimev2, we only need to process carrying on which caused by carrying on to digit 6-th.
                if (length > 6) {
                    // round off to at most 6 digits
                    if (auto remainder = *(start + 6) - '0'; remainder >= 5) {
                        ms++;
                        DCHECK(ms <= 1000000);
                        if (ms == 1000000) {
                            // overflow, round up to next second
                            res.template date_add_interval<TimeUnit::SECOND>(
                                    TimeInterval {TimeUnit::SECOND, 1, false});
                            ms = 0;
                        }
                    }
                }
            }
        }
    }
    static_cast<void>(skip_any_digit(ptr, end));

    static_cast<void>(skip_any_whitespace(ptr, end));

    // timezone part
    if (ptr != end) {
        cctz::time_zone parsed_tz {};
        if (*ptr == '+' || *ptr == '-') {
            // offset
            const char sign = *ptr;
            ++ptr;
            part[1] = 0;

            uint32_t length = count_digits(ptr, end);
            // hour
            if (length == 1 || length == 3) {
                RETURN_IF_ERROR((consume_digit<UInt32, 1>(ptr, end, part[0])));
            } else {
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[0])));
            }
            RETURN_INVALID_ARG_IF_NOT(part[0] <= 14, "invalid hour offset {}", part[0]);
            if (assert_within_bound(ptr, end, 0).ok()) {
                if (*ptr == ':') {
                    ++ptr;
                }
                // minute
                RETURN_IF_ERROR((consume_digit<UInt32, 2>(ptr, end, part[1])));
                RETURN_INVALID_ARG_IF_NOT((part[1] == 0 || part[1] == 30 || part[1] == 45),
                                          "invalid minute offset {}", part[1]);
            }
            if (part[0] == 14 && part[1] > 0) [[unlikely]] {
                return Status::InvalidArgument("invalid timezone offset '{}'",
                                               combine_tz_offset(sign, part[0], part[1]));
            }

            RETURN_INVALID_ARG_IF_NOT(TimezoneUtils::find_cctz_time_zone(
                                              combine_tz_offset(sign, part[0], part[1]), parsed_tz),
                                      "invalid timezone offset '{}'",
                                      combine_tz_offset(sign, part[0], part[1]));
        } else {
            // timezone name
            const auto* start = ptr;
            // short tzname, or something legal for tzdata. depends on our TimezoneUtils.
            RETURN_IF_ERROR(skip_tz_name_part(ptr, end));

            RETURN_INVALID_ARG_IF_NOT(
                    TimezoneUtils::find_cctz_time_zone(std::string {start, ptr}, parsed_tz),
                    "invalid timezone name '{}'", std::string {start, ptr});
        }
        if constexpr (IsDatetime) {
            // convert tz
            cctz::civil_second cs {res.year(), res.month(),  res.day(),
                                   res.hour(), res.minute(), res.second()};

            auto given = cctz::convert(cs, parsed_tz);
            auto local = cctz::convert(given, *local_time_zone);
            res.template unchecked_set_time_unit<TimeUnit::YEAR>((uint32_t)local.year());
            res.template unchecked_set_time_unit<TimeUnit::MONTH>((uint32_t)local.month());
            res.template unchecked_set_time_unit<TimeUnit::DAY>((uint32_t)local.day());
            res.template unchecked_set_time_unit<TimeUnit::HOUR>((uint32_t)local.hour());
            res.template unchecked_set_time_unit<TimeUnit::MINUTE>((uint32_t)local.minute());
            res.template unchecked_set_time_unit<TimeUnit::SECOND>((uint32_t)local.second());
        }

        static_cast<void>(skip_any_whitespace(ptr, end));
        RETURN_INVALID_ARG_IF_NOT(ptr == end,
                                  "invalid datetime string '{}', extra characters after timezone",
                                  std::string {ptr, end});
    }
    _cast_to_type(res);
    return Status::OK();
}

static Status from_int(uint64_t uint_val, int length, VecDateTimeValue& val) {
    if (length == 3 || length == 4) {
        val.unchecked_set_time_unit<TimeUnit::YEAR>(2000);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>((uint32_t)uint_val / 100),
                                  "invalid month {}", uint_val / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 5) {
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::YEAR>(2000 + (uint32_t)uint_val / 10000),
                "invalid year {}", 2000 + uint_val / 10000);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>(uint_val % 10000 / 100),
                                  "invalid month {}", uint_val % 10000 / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 6) {
        uint32_t year = (uint32_t)uint_val / 10000;
        if (year < 70) {
            year += 2000;
        } else {
            year += 1900;
        }
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::YEAR>(year), "invalid year {}", year);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>(uint_val % 10000 / 100),
                                  "invalid month {}", uint_val % 10000 / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 8) {
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::YEAR>((uint32_t)uint_val / 10000),
                                  "invalid year {}", uint_val / 10000);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::MONTH>(uint_val % 10000 / 100),
                                  "invalid month {}", uint_val % 10000 / 100);
        RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::DAY>(uint_val % 100),
                                  "invalid day {}", uint_val % 100);
    } else if (length == 14) {
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::YEAR>(uint_val / common::exp10_i64(10)),
                "invalid year {}", uint_val / common::exp10_i64(10));
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::MONTH>((uint_val / common::exp10_i32(8)) % 100),
                "invalid month {}", (uint_val / common::exp10_i32(8)) % 100);
        RETURN_INVALID_ARG_IF_NOT(
                val.set_time_unit<TimeUnit::DAY>((uint_val / common::exp10_i32(6)) % 100),
                "invalid day {}", (uint_val / common::exp10_i32(6)) % 100);
        if (val.type() == TimeType::TIME_DATETIME) {
            RETURN_INVALID_ARG_IF_NOT(
                    val.set_time_unit<TimeUnit::HOUR>((uint_val / common::exp10_i32(4)) % 100),
                    "invalid hour {}", (uint_val / common::exp10_i32(4)) % 100);
            RETURN_INVALID_ARG_IF_NOT(
                    val.set_time_unit<TimeUnit::MINUTE>((uint_val / common::exp10_i32(2)) % 100),
                    "invalid minute {}", (uint_val / common::exp10_i32(2)) % 100);
            RETURN_INVALID_ARG_IF_NOT(val.set_time_unit<TimeUnit::SECOND>(uint_val % 100),
                                      "invalid second {}", uint_val % 100);
        }
    } else [[unlikely]] {
        return Status::InvalidArgument("invalid digits for datetimev2: {}", uint_val);
    }

    return Status::OK();
}

static void microsecond_carry_on(int64_t frac_part, uint32_t float_scale, VecDateTimeValue& val) {
    if (val.type() == TimeType::TIME_DATE) {
        // for date, we just ignore the fractional part
        return;
    }
    // normalize the fractional part to microseconds(6 digits)
    if (float_scale > 0) {
        if (float_scale > 6) {
            int ms = int(frac_part / common::exp10_i64(float_scale - 6));
            // if scale > 6, we need to round the fractional part
            int digit7 = frac_part % common::exp10_i32(float_scale - 6) /
                         common::exp10_i32(float_scale - 7);
            if (digit7 >= 5) {
                ms++;
                DCHECK(ms <= 1000000);
                if (ms == 1000000) {
                    // overflow, round up to next second
                    val.date_add_interval<TimeUnit::SECOND>(
                            TimeInterval {TimeUnit::SECOND, 1, false});
                    ms = 0;
                }
            }
        }
    }
}

template <PrimitiveType T>
template <typename IntDataType>
Status DataTypeDateSerDe<T>::from_int_batch(const IntDataType::ColumnType& int_col,
                                            ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    for (size_t i = 0; i < int_col.size(); ++i) {
        if (int_col.get_element(i) > std::numeric_limits<int64_t>::max() ||
            int_col.get_element(i) < std::numeric_limits<int64_t>::min()) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
            continue;
        }
        auto int_val = (int64_t)int_col.get_element(i);
        if (int_val <= 0) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
            continue;
        }
        int length = common::count_digits_fast(int_val);

        CppType val;
        if (auto st = from_int(int_val, length, val); st.ok()) [[likely]] {
            _cast_to_type(val);
            col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
            col_nullmap.get_data()[i] = false;
        } else if (st.template is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename IntDataType>
Status DataTypeDateSerDe<T>::from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                                        IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col);
    col_data.resize(int_col.size());

    for (size_t i = 0; i < int_col.size(); ++i) {
        if (int_col.get_element(i) > std::numeric_limits<int64_t>::max() ||
            int_col.get_element(i) < std::numeric_limits<int64_t>::min()) {
            return Status::InvalidArgument("invalid int value for time: {}",
                                           int_col.get_element(i));
        }
        auto int_val = (int64_t)int_col.get_element(i);
        if (int_val <= 0) {
            return Status::InvalidArgument("invalid int value for datetimev2: {}", int_val);
        }
        int length = common::count_digits_fast(int_val);

        CppType val;
        RETURN_IF_ERROR(from_int(int_val, length, val));
        _cast_to_type(val);
        col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename FloatDataType>
Status DataTypeDateSerDe<T>::from_float_batch(const FloatDataType::ColumnType& float_col,
                                              ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    for (size_t i = 0; i < float_col.size(); ++i) {
        double float_value = float_col.get_data()[i];
        if (float_value <= 0 || std::isnan(float_value) || std::isinf(float_value) ||
            float_value >= (double)std::numeric_limits<int64_t>::max()) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
            continue;
        }
        auto int_part = static_cast<int64_t>(float_value);
        int length = common::count_digits_fast(int_part);

        CppType val;
        if (auto st = from_int(int_part, length, val); st.ok()) [[likely]] {
            _cast_to_type(val);
            int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
            microsecond_carry_on(ms_part_7, 7, val);

            col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
            col_nullmap.get_data()[i] = false;
        } else if (st.template is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename FloatDataType>
Status DataTypeDateSerDe<T>::from_float_strict_mode_batch(
        const FloatDataType::ColumnType& float_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col);
    col_data.resize(float_col.size());

    for (size_t i = 0; i < float_col.size(); ++i) {
        double float_value = float_col.get_data()[i];
        if (float_value <= 0 || std::isnan(float_value) || std::isinf(float_value) ||
            float_value >= (double)std::numeric_limits<int64_t>::max()) {
            return Status::InvalidArgument("invalid float value for datetimev2: {}", float_value);
        }
        auto int_part = static_cast<int64_t>(float_value);
        int length = common::count_digits_fast(int_part);

        CppType val;
        RETURN_IF_ERROR(from_int(int_part, length, val));
        _cast_to_type(val);

        int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
        microsecond_carry_on(ms_part_7, 7, val);

        col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename DecimalDataType>
Status DataTypeDateSerDe<T>::from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                                                ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    for (size_t i = 0; i < decimal_col.size(); ++i) {
        if (decimal_col.get_intergral_part(i) > std::numeric_limits<int64_t>::max() ||
            decimal_col.get_intergral_part(i) < std::numeric_limits<int64_t>::min()) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
            continue;
        }
        auto int_part = (int64_t)decimal_col.get_intergral_part(i);
        if (int_part <= 0) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
            continue;
        }
        int length = common::count_digits_fast(int_part);

        CppType val;
        if (auto st = from_int(int_part, length, val); st.ok()) [[likely]] {
            _cast_to_type(val);
            microsecond_carry_on((int64_t)decimal_col.get_fractional_part(i),
                                 decimal_col.get_scale(), val);

            col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
            col_nullmap.get_data()[i] = false;
        } else if (st.template is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
            if constexpr (IsDatetime) {
                col_data.get_data()[i] = MIN_DATETIME_V2;
            } else {
                col_data.get_data()[i] = MIN_DATE_V2;
            }
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
template <typename DecimalDataType>
Status DataTypeDateSerDe<T>::from_decimal_strict_mode_batch(
        const DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnType&>(target_col);
    col_data.resize(decimal_col.size());

    for (size_t i = 0; i < decimal_col.size(); ++i) {
        if (decimal_col.get_intergral_part(i) > std::numeric_limits<int64_t>::max() ||
            decimal_col.get_intergral_part(i) < std::numeric_limits<int64_t>::min()) {
            return Status::InvalidArgument("invalid intergral value for time: {}",
                                           decimal_col.get_element(i));
        }
        auto int_part = (int64_t)decimal_col.get_intergral_part(i);
        if (int_part <= 0) {
            return Status::InvalidArgument("invalid decimal integral part for datetimev2: {}",
                                           int_part);
        }
        int length = common::count_digits_fast(int_part);

        CppType val;
        RETURN_IF_ERROR(from_int(int_part, length, val));
        _cast_to_type(val);

        microsecond_carry_on((int64_t)decimal_col.get_fractional_part(i), decimal_col.get_scale(),
                             val);
        col_data.get_data()[i] = binary_cast<CppType, NativeType>(val);
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// instantiation of template functions
template class DataTypeDateSerDe<TYPE_DATE>;
template class DataTypeDateSerDe<TYPE_DATETIME>;

template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATE>::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;

template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status
DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status
DataTypeDateSerDe<TYPE_DATETIME>::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;
} // namespace doris::vectorized
