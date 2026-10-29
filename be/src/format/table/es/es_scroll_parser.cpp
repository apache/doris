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

#include "format/table/es/es_scroll_parser.h"

#include <absl/strings/substitute.h>
#include <cctz/time_zone.h>
#include <glog/logging.h>
#include <rapidjson/allocators.h>
#include <rapidjson/encodings.h>
#include <stdint.h>
#include <string.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cmath>
#include <cstdlib>
#include <ostream>
#include <string>
#include <string_view>

#include "common/status.h"
#include "core/binary_cast.hpp"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/value/decimalv2_value.h"
#include "core/value/jsonb_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "exprs/function/cast/cast_to_datev2_impl.hpp"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/descriptors.h"
#include "util/string_parser.hpp"

namespace doris {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
static const char* FIELD_ID = "_id";

// get the original json data type
std::string json_type_to_string(rapidjson::Type type) {
    switch (type) {
    case rapidjson::kNumberType:
        return "Number";
    case rapidjson::kStringType:
        return "Varchar/Char";
    case rapidjson::kArrayType:
        return "Array";
    case rapidjson::kObjectType:
        return "Object";
    case rapidjson::kNullType:
        return "Null Type";
    case rapidjson::kFalseType:
    case rapidjson::kTrueType:
        return "True/False";
    default:
        return "Unknown Type";
    }
}

// transfer rapidjson::Value to string representation
std::string json_value_to_string(const rapidjson::Value& value) {
    rapidjson::StringBuffer scratch_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> temp_writer(scratch_buffer);
    value.Accept(temp_writer);
    return scratch_buffer.GetString();
}

static const std::string ERROR_INVALID_COL_DATA =
        "Data source returned inconsistent column data. "
        "Expected value of type {} based on column metadata. This likely indicates a "
        "problem with the data source library.";
static const std::string ERROR_MEM_LIMIT_EXCEEDED =
        "DataSourceScanNode::$0() failed to allocate "
        "$1 bytes for $2.";
static const std::string ERROR_COL_DATA_IS_ARRAY =
        "Data source returned an array for the type $0"
        "based on column metadata.";
static const std::string INVALID_NULL_VALUE =
        "Invalid null value occurs: Non-null column `$0` contains NULL";

#define RETURN_ERROR_IF_COL_IS_ARRAY(col, type, is_array)                    \
    do {                                                                     \
        if (col.IsArray() == is_array) {                                     \
            std::stringstream ss;                                            \
            ss << "Expected value of type: " << type_to_string(type)         \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Document slice is : " << json_value_to_string(col);     \
            return Status::RuntimeError(ss.str());                           \
        }                                                                    \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type)                            \
    do {                                                                        \
        if (!col.IsString()) {                                                  \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_PARSING_FAILED(result, col, type)                       \
    do {                                                                        \
        if (result != StringParser::PARSE_SUCCESS) {                            \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type)                     \
    do {                                                                 \
        std::stringstream ss;                                            \
        ss << "Expected value of type: " << type_to_string(type)         \
           << "; but found type: " << json_type_to_string(col.GetType()) \
           << "; Document slice is : " << json_value_to_string(col);     \
        return Status::RuntimeError(ss.str());                           \
    } while (false)

// ES integer coercion truncates decimal/scientific notation, e.g. "1.2e3" -> 1200.
// Shift the decimal point using digits so values near INT64_MAX never pass through double.
template <typename T>
static T parse_es_integer(const char* text, size_t length, StringParser::ParseResult* result) {
    text = skip_ascii_whitespaces(text, length);
    size_t i = 0;
    bool negative = length > 0 && text[0] == '-';
    if (length > 0 && (text[0] == '-' || text[0] == '+')) {
        ++i;
    }
    size_t start = i;
    int64_t digits = 0;
    int64_t integer_digits = 0;
    int64_t first_nonzero = -1;
    bool dot = false;
    for (; i < length; ++i) {
        char c = text[i];
        if (c >= '0' && c <= '9') {
            if (c != '0' && first_nonzero < 0) {
                first_nonzero = digits;
            }
            ++digits;
            integer_digits += !dot;
        } else if (c == '.' && !dot) {
            dot = true;
        } else {
            break;
        }
    }
    size_t end = i;
    int64_t exponent = 0;
    bool negative_exponent = false;
    if (i < length && (text[i] == 'e' || text[i] == 'E')) {
        ++i;
        if (i < length && (text[i] == '+' || text[i] == '-')) {
            negative_exponent = text[i++] == '-';
        }
        size_t exponent_start = i;
        for (; i < length && text[i] >= '0' && text[i] <= '9'; ++i) {
            // Saturation avoids overflow and work proportional to an untrusted exponent.
            exponent = std::min(exponent * 10 + text[i] - '0', static_cast<int64_t>(length) + 64);
        }
        if (i == exponent_start) {
            *result = StringParser::PARSE_FAILURE;
            return 0;
        }
    }
    if (digits == 0 || i != length) {
        *result = StringParser::PARSE_FAILURE;
        return 0;
    }
    int64_t count = integer_digits + (negative_exponent ? -exponent : exponent) - first_nonzero;
    if (first_nonzero < 0 || count <= 0) {
        *result = StringParser::PARSE_SUCCESS;
        return 0;
    }
    // 39 digits cover every signed 128-bit value; the final parser checks the exact type bounds.
    if (count > 39) {
        *result = StringParser::PARSE_OVERFLOW;
        return 0;
    }
    char integral[40];
    size_t size = 0;
    if (negative) {
        integral[size++] = '-';
    }
    int64_t digit = 0;
    for (i = start; i < end && count > 0; ++i) {
        if (text[i] != '.' && digit++ >= first_nonzero) {
            integral[size++] = text[i];
            --count;
        }
    }
    while (count-- > 0) {
        integral[size++] = '0';
    }
    return StringParser::string_to_int<T, true>(integral, size, result);
}

template <typename T>
static T parse_es_floating_string(const std::string& text, StringParser::ParseResult* result) {
    // Java's floating parser also accepts hexadecimal exponents, f/d suffixes and ASCII trim
    // characters. Keep the ordinary decimal fast path, and require the complete Java token.
    std::string_view token(text);
    while (!token.empty() && static_cast<unsigned char>(token.front()) <= 0x20) {
        token.remove_prefix(1);
    }
    while (!token.empty() && static_cast<unsigned char>(token.back()) <= 0x20) {
        token.remove_suffix(1);
    }
    auto magnitude = token;
    if (!magnitude.empty() && (magnitude.front() == '+' || magnitude.front() == '-')) {
        magnitude.remove_prefix(1);
    }
    if (magnitude == "NaN" || magnitude == "Infinity") {
        return StringParser::string_to_float<T>(token.data(), token.size(), result);
    }
    if (!magnitude.empty() &&
        std::string_view("fFdD").find(magnitude.back()) != std::string_view::npos) {
        token.remove_suffix(1);
        magnitude.remove_suffix(1);
        // Java permits outer whitespace, but not a gap between the number and its suffix.
        if (magnitude.empty() || static_cast<unsigned char>(magnitude.back()) <= 0x20) {
            *result = StringParser::PARSE_FAILURE;
            return 0;
        }
    }
    if (magnitude.empty() ||
        !((magnitude.front() >= '0' && magnitude.front() <= '9') || magnitude.front() == '.')) {
        *result = StringParser::PARSE_FAILURE;
        return 0;
    }
    const bool hexadecimal = magnitude.starts_with("0x") || magnitude.starts_with("0X");
    if (hexadecimal && magnitude.find_first_of("pP") == std::string_view::npos) {
        *result = StringParser::PARSE_FAILURE;
        return 0;
    }
    // Float.parseFloat rounds directly to float; an intermediate double can round a value
    // just above a float midpoint back to that midpoint and then to the wrong float.
    if (hexadecimal || std::is_same_v<T, float>) {
        char* end = nullptr;
        T number;
        if constexpr (std::is_same_v<T, float>) {
            number = std::strtof(token.data(), &end);
        } else {
            number = std::strtod(token.data(), &end);
        }
        *result = end == token.data() + token.size() ? StringParser::PARSE_SUCCESS
                                                     : StringParser::PARSE_FAILURE;
        return number;
    }
    return StringParser::string_to_float<T>(token.data(), token.size(), result);
}

template <typename T>
static T parse_es_narrow_integer(const std::string& text, StringParser::ParseResult* result) {
    // Unlike long's BigDecimal conversion, ES parses byte/short/integer strings through double:
    // "0.99999999999999999" rounds to 1 before truncation. Keep long's exact path separate.
    auto number = parse_es_floating_string<double>(text, result);
    if (*result != StringParser::PARSE_SUCCESS) {
        return 0;
    }
    // Java converts NaN to zero; casting it directly in C++ would be undefined.
    if (std::isnan(number)) {
        return 0;
    }
    using ParsedType = std::conditional_t<sizeof(T) == 1, int32_t, T>;
    if (number < StringParser::numeric_limits<ParsedType>(true) ||
        number > StringParser::numeric_limits<ParsedType>(false)) {
        *result = StringParser::PARSE_OVERFLOW;
        return 0;
    }
    auto parsed = static_cast<ParsedType>(number);
    // ES byte uses intValue() then checks the byte range, so "127.9" is valid. Short and
    // integer instead check their own double bounds before casting (e.g. short "32767.9" fails).
    if constexpr (sizeof(T) == 1) {
        if (parsed < StringParser::numeric_limits<T>(true) ||
            parsed > StringParser::numeric_limits<T>(false)) {
            *result = StringParser::PARSE_OVERFLOW;
            return 0;
        }
    }
    return static_cast<T>(parsed);
}

template <typename T>
Status get_int_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                     bool pure_doc_value) {
    const rapidjson::Value* value = &col;
    // A singleton source array has the same scalar value as its one docvalue. Keep rejecting
    // multi-value source arrays: selecting an arbitrary element would discard source semantics.
    if (col.IsArray() && !col.Empty() && (pure_doc_value || col.Size() == 1)) {
        value = &col[0];
    }
    RETURN_ERROR_IF_COL_IS_ARRAY((*value), type, true);
    T parsed;
    if (value->IsInt64() || value->IsUint64()) {
        __int128 number = value->IsInt64() ? static_cast<__int128>(value->GetInt64())
                                           : static_cast<__int128>(value->GetUint64());
        if (number < StringParser::numeric_limits<T>(true) ||
            number > StringParser::numeric_limits<T>(false)) {
            return Status::RuntimeError("ES integer value is out of range for {}",
                                        type_to_string(type));
        }
        parsed = static_cast<T>(number);
    } else if (value->IsNumber()) {
        // Jackson converts numeric tokens directly from double. Serializing INT64_MIN first
        // rounds its text below the valid range; Java also saturates the rounded upper endpoint.
        const double number = value->GetDouble();
        using ParsedType = std::conditional_t<(sizeof(T) < 4), int32_t, T>;
        const auto maximum = static_cast<double>(StringParser::numeric_limits<ParsedType>(false));
        if (!std::isfinite(number) ||
            number < static_cast<double>(StringParser::numeric_limits<ParsedType>(true)) ||
            number > maximum) {
            return Status::RuntimeError("ES integer value is out of range for {}",
                                        type_to_string(type));
        }
        const ParsedType converted = number == maximum
                                             ? StringParser::numeric_limits<ParsedType>(false)
                                             : static_cast<ParsedType>(number);
        if constexpr (sizeof(T) < 4) {
            if (converted < StringParser::numeric_limits<T>(true) ||
                converted > StringParser::numeric_limits<T>(false)) {
                return Status::RuntimeError("ES integer value is out of range for {}",
                                            type_to_string(type));
            }
        }
        parsed = static_cast<T>(converted);
    } else {
        RETURN_ERROR_IF_COL_IS_NOT_STRING((*value), type);
        std::string text(value->GetString(), value->GetStringLength());
        StringParser::ParseResult result;
        if constexpr (sizeof(T) < 8) {
            parsed = parse_es_narrow_integer<T>(text, &result);
        } else {
            parsed = parse_es_integer<T>(text.data(), text.size(), &result);
        }
        RETURN_ERROR_IF_PARSING_FAILED(result, (*value), type);
    }
    memcpy(slot, &parsed, sizeof(parsed));
    return Status::OK();
}

template <PrimitiveType T>
Status get_date_value_int(const rapidjson::Value& col, PrimitiveType type, bool is_date_str,
                          typename PrimitiveTypeTraits<T>::CppType* slot,
                          const cctz::time_zone& time_zone) {
    constexpr bool is_datetime_v1 = T == TYPE_DATE || T == TYPE_DATETIME;
    typename PrimitiveTypeTraits<T>::CppType dt_val;
    if (is_date_str) {
        const std::string str_date = col.GetString();
        int str_length = col.GetStringLength();
        bool success = false;
        if (str_length > 19) {
            std::chrono::system_clock::time_point tp;
            // time_zone suffix pattern
            // Z/+08:00/-04:30
            RE2 time_zone_pattern(R"([+-]\d{2}:?\d{2}|Z)");
            bool ok = false;
            std::string fmt;
            re2::StringPiece value;
            if (time_zone_pattern.Match(str_date, 0, str_date.size(), RE2::UNANCHORED, &value, 1)) {
                // with time_zone info
                // YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DDTHH:MM:SS+08:00
                // or 2022-08-08T12:10:10.000Z or YYYY-MM-DDTHH:MM:SS-08:00
                fmt = "%Y-%m-%dT%H:%M:%E*S%Ez";
                cctz::time_zone ctz;
                // find time_zone by time_zone suffix string
                TimezoneUtils::find_cctz_time_zone(value.as_string(), ctz);
                ok = cctz::parse(fmt, str_date, ctz, &tp);
            } else {
                // without time_zone info
                // 2022-08-08T12:10:10.000
                fmt = "%Y-%m-%dT%H:%M:%E*S";
                // If the time without time_zone info, ES will assume it is UTC time.
                // So we parse it in Doris with UTC time zone.
                ok = cctz::parse(fmt, str_date, cctz::utc_time_zone(), &tp);
            }
            if (ok) {
                // The local time zone can change by session variable `time_zone`
                // We should use the user specified time zone, not the actual system local time zone.
                success = true;
                dt_val.from_unixtime(std::chrono::system_clock::to_time_t(tp), time_zone);
            }
        } else if (str_length == 19) {
            // YYYY-MM-DDTHH:MM:SS
            if (*(str_date.c_str() + 10) == 'T') {
                std::chrono::system_clock::time_point tp;
                const bool ok =
                        cctz::parse("%Y-%m-%dT%H:%M:%S", str_date, cctz::utc_time_zone(), &tp);
                if (ok) {
                    success = true;
                    dt_val.from_unixtime(std::chrono::system_clock::to_time_t(tp), time_zone);
                }
            } else {
                // YYYY-MM-DD HH:MM:SS
                CastParameters params;
                if constexpr (is_datetime_v1) {
                    success = CastToDateOrDatetime::from_string_non_strict_mode<
                            DatelikeTargetType::DATE_TIME>({str_date.c_str(), (size_t)str_length},
                                                           dt_val, nullptr, params);
                } else if constexpr (T == TYPE_DATEV2) {
                    success = CastToDateV2::from_string_non_strict_mode(
                            {str_date.c_str(), (size_t)str_length}, dt_val, nullptr, params);
                } else {
                    success = CastToDatetimeV2::from_string_non_strict_mode(
                            {str_date.c_str(), (size_t)str_length}, dt_val, nullptr, -1, params);
                }
            }

        } else if (str_length == 13) {
            // string long like "1677895728000"
            int64_t time_long = std::atol(str_date.c_str());
            if (time_long > 0) {
                success = true;
                dt_val.from_unixtime(time_long / 1000, time_zone);
            }
        } else {
            // YYYY-MM-DD or others
            CastParameters params;
            if constexpr (is_datetime_v1) {
                success = CastToDateOrDatetime::from_string_non_strict_mode<
                        DatelikeTargetType::DATE_TIME>({str_date.c_str(), (size_t)str_length},
                                                       dt_val, nullptr, params);
            } else if constexpr (T == TYPE_DATEV2) {
                success = CastToDateV2::from_string_non_strict_mode(
                        {str_date.c_str(), (size_t)str_length}, dt_val, nullptr, params);
            } else {
                success = CastToDatetimeV2::from_string_non_strict_mode(
                        {str_date.c_str(), (size_t)str_length}, dt_val, nullptr, -1, params);
            }
        }

        if (!success) {
            RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
        }

    } else {
        dt_val.from_unixtime(col.GetInt64() / 1000, time_zone);
    }
    if constexpr (is_datetime_v1) {
        if (type == TYPE_DATE) {
            dt_val.cast_to_date();
        } else {
            dt_val.to_datetime();
        }
    }

    *slot = *reinterpret_cast<typename PrimitiveTypeTraits<T>::CppType*>(&dt_val);
    return Status::OK();
}

template <PrimitiveType T>
Status get_date_int(const rapidjson::Value& col, PrimitiveType type, bool pure_doc_value,
                    typename PrimitiveTypeTraits<T>::CppType* slot,
                    const cctz::time_zone& time_zone) {
    // this would happend just only when `enable_docvalue_scan = false`, and field has timestamp format date from _source
    if (col.IsNumber()) {
        // ES process date/datetime field would use millisecond timestamp for index or docvalue
        // processing date type field, if a number is encountered, Doris On ES will force it to be processed according to ms
        // Doris On ES needs to be consistent with ES, so just divided by 1000 because the unit for from_unixtime is seconds
        return get_date_value_int<T>(col, type, false, slot, time_zone);
    } else if (col.IsArray() && pure_doc_value && !col.Empty()) {
        // this would happened just only when `enable_docvalue_scan = true`
        // ES add default format for all field after ES 6.4, if we not provided format for `date` field ES would impose
        // a standard date-format for date field as `2020-06-16T00:00:00.000Z`
        // At present, we just process this string format date. After some PR were merged into Doris, we would impose `epoch_mills` for
        // date field's docvalue
        if (col[0].IsString()) {
            return get_date_value_int<T>(col[0], type, true, slot, time_zone);
        }
        // ES would return millisecond timestamp for date field, divided by 1000 because the unit for from_unixtime is seconds
        return get_date_value_int<T>(col[0], type, false, slot, time_zone);
    } else {
        // this would happened just only when `enable_docvalue_scan = false`, and field has string format date from _source
        RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
        RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);
        return get_date_value_int<T>(col, type, true, slot, time_zone);
    }
}
template <PrimitiveType T>
Status fill_date_int(const rapidjson::Value& col, PrimitiveType type, bool pure_doc_value,
                     IColumn* col_ptr, const cctz::time_zone& time_zone) {
    typename PrimitiveTypeTraits<T>::CppType data;
    RETURN_IF_ERROR((get_date_int<T>(col, type, pure_doc_value, &data, time_zone)));
    col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&data)), 0);
    return Status::OK();
}

template <typename T>
Status get_float_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                       bool pure_doc_value) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray() && !col.Empty()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col[0].GetFloat() : col[0].GetDouble());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string val(col.GetString(), col.GetStringLength());
    T v = parse_es_floating_string<T>(val, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);
    *reinterpret_cast<T*>(slot) = v;

    return Status::OK();
}

template <typename T>
Status insert_float_value(const rapidjson::Value& col, PrimitiveType type, IColumn* col_ptr,
                          bool pure_doc_value, bool nullable) {
    static_assert(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber() && nullable) {
        T value = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray() && !col.Empty() && nullable) {
        T value = (T)(sizeof(T) == 4 ? col[0].GetFloat() : col[0].GetDouble());
        col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&value)), 0);
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string val(col.GetString(), col.GetStringLength());
    T v = parse_es_floating_string<T>(val, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&v)), 0);

    return Status::OK();
}

template <typename T>
Status insert_int_value(const rapidjson::Value& col, PrimitiveType type, IColumn* col_ptr,
                        bool pure_doc_value, bool nullable) {
    // Empty arrays and singleton nulls have no scalar integer value, just like absent docvalues.
    if (nullable && col.IsArray() && (col.Empty() || (col.Size() == 1 && col[0].IsNull()))) {
        col_ptr->insert_data(nullptr, 0);
        return Status::OK();
    }
    T value;
    RETURN_IF_ERROR(get_int_value<T>(col, type, &value, pure_doc_value));
    col_ptr->insert_data(reinterpret_cast<const char*>(&value), 0);
    return Status::OK();
}

static bool parse_es_bool(const char* value, size_t length, StringParser::ParseResult* result) {
    // ES accepts an empty string as false in source, for scalar fields and array elements alike.
    if (length == 0) {
        *result = StringParser::PARSE_SUCCESS;
        return false;
    }
    return StringParser::string_to_bool(value, length, result);
}

template <PrimitiveType T>
Status handle_value(const rapidjson::Value& col, PrimitiveType sub_type, bool pure_doc_value,
                    typename PrimitiveTypeTraits<T>::CppType& val) {
    if constexpr (T == TYPE_TINYINT || T == TYPE_SMALLINT || T == TYPE_INT || T == TYPE_BIGINT ||
                  T == TYPE_LARGEINT) {
        RETURN_IF_ERROR(get_int_value<typename PrimitiveTypeTraits<T>::CppType>(col, sub_type, &val,
                                                                                pure_doc_value));
        return Status::OK();
    }
    if constexpr (T == TYPE_FLOAT) {
        RETURN_IF_ERROR(get_float_value<float>(col, sub_type, &val, pure_doc_value));
        return Status::OK();
    }
    if constexpr (T == TYPE_DOUBLE) {
        RETURN_IF_ERROR(get_float_value<double>(col, sub_type, &val, pure_doc_value));
        return Status::OK();
    }
    if constexpr (T == TYPE_STRING || T == TYPE_CHAR || T == TYPE_VARCHAR) {
        // When ES mapping is keyword/text but actual data is an array,
        // serialize the array to JSON string instead of throwing an error.
        // This is valid in ES since any field can hold array values.
        if (col.IsArray()) {
            val = json_value_to_string(col);
        } else if (!col.IsString()) {
            val = json_value_to_string(col);
        } else {
            val = col.GetString();
        }
        return Status::OK();
    }
    if constexpr (T == TYPE_BOOLEAN) {
        if (col.IsBool()) {
            val = col.GetBool();
            return Status::OK();
        }

        if (col.IsNumber()) {
            val = static_cast<typename PrimitiveTypeTraits<T>::CppType>(col.GetInt());
            return Status::OK();
        }

        bool is_nested_str = false;
        if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsBool()) {
            val = col[0].GetBool();
            return Status::OK();
        } else if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsString()) {
            is_nested_str = true;
        } else if (pure_doc_value && col.IsArray()) {
            return Status::InternalError(ERROR_INVALID_COL_DATA, "BOOLEAN");
        }

        const rapidjson::Value& str_col = is_nested_str ? col[0] : col;
        const std::string& str_val = str_col.GetString();
        size_t val_size = str_col.GetStringLength();
        StringParser::ParseResult result;
        val = parse_es_bool(str_val.c_str(), val_size, &result);
        RETURN_ERROR_IF_PARSING_FAILED(result, str_col, sub_type);
        return Status::OK();
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "Un-supported type: {}", type_to_string(T));
}

template <PrimitiveType T>
Status process_single_column(const rapidjson::Value& col, PrimitiveType sub_type,
                             bool pure_doc_value, Array& array) {
    // Source arrays retain null members that ES omits from docvalues. Numeric empty strings
    // have the same missing-value semantics; Boolean empty strings still mean false.
    if (col.IsNull() ||
        (is_number(T) && T != TYPE_BOOLEAN && col.IsString() && col.GetStringLength() == 0)) {
        array.push_back(Field {});
        return Status::OK();
    }
    typename PrimitiveTypeTraits<T>::CppType val;
    RETURN_IF_ERROR(handle_value<T>(col, sub_type, pure_doc_value, val));
    array.push_back(Field::create_field<T>(val));
    return Status::OK();
}

template <PrimitiveType T>
Status process_column_array(const rapidjson::Value& col, PrimitiveType sub_type,
                            bool pure_doc_value, Array& array) {
    for (const auto& sub_col : col.GetArray()) {
        RETURN_IF_ERROR(process_single_column<T>(sub_col, sub_type, pure_doc_value, array));
    }
    return Status::OK();
}

template <PrimitiveType T>
Status process_column(const rapidjson::Value& col, PrimitiveType sub_type, bool pure_doc_value,
                      Array& array) {
    if (!col.IsArray()) {
        return process_single_column<T>(col, sub_type, pure_doc_value, array);
    } else {
        return process_column_array<T>(col, sub_type, pure_doc_value, array);
    }
}

template <PrimitiveType T>
Status process_date_column(const rapidjson::Value& col, PrimitiveType sub_type, bool pure_doc_value,
                           Array& array, const cctz::time_zone& time_zone) {
    if (!col.IsArray()) {
        typename PrimitiveTypeTraits<T>::CppType data;
        RETURN_IF_ERROR((get_date_int<T>(col, sub_type, pure_doc_value, &data, time_zone)));
        array.push_back(Field::create_field<T>(data));
    } else {
        for (const auto& sub_col : col.GetArray()) {
            // Nullable source members must bypass date parsing, which requires a real value.
            if (sub_col.IsNull()) {
                array.push_back(Field {});
                continue;
            }
            typename PrimitiveTypeTraits<T>::CppType data;
            RETURN_IF_ERROR((get_date_int<T>(sub_col, sub_type, pure_doc_value, &data, time_zone)));
            array.push_back(Field::create_field<T>(data));
        }
    }
    return Status::OK();
}

Status process_jsonb_column(const rapidjson::Value& col, PrimitiveType sub_type,
                            bool pure_doc_value, Array& array) {
    if (!col.IsArray()) {
        JsonBinaryValue jsonb_value;
        RETURN_IF_ERROR(jsonb_value.from_json_string(json_value_to_string(col)));
        JsonbField json(jsonb_value.value(), jsonb_value.size());
        array.push_back(Field::create_field<TYPE_JSONB>(std::move(json)));
    } else {
        for (const auto& sub_col : col.GetArray()) {
            // SQL NULL members stay distinct from nonnull JSONB containers such as [null].
            if (sub_col.IsNull()) {
                array.push_back(Field {});
                continue;
            }
            JsonBinaryValue jsonb_value;
            RETURN_IF_ERROR(jsonb_value.from_json_string(json_value_to_string(sub_col)));
            JsonbField json(jsonb_value.value(), jsonb_value.size());
            array.push_back(Field::create_field<TYPE_JSONB>(json));
        }
    }
    return Status::OK();
}

Status ScrollParser::parse_column(const rapidjson::Value& col, PrimitiveType sub_type,
                                  bool pure_doc_value, Array& array,
                                  const cctz::time_zone& time_zone) {
    switch (sub_type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return process_column<TYPE_STRING>(col, sub_type, pure_doc_value, array);
    case TYPE_TINYINT:
        return process_column<TYPE_TINYINT>(col, sub_type, pure_doc_value, array);
    case TYPE_SMALLINT:
        return process_column<TYPE_SMALLINT>(col, sub_type, pure_doc_value, array);
    case TYPE_INT:
        return process_column<TYPE_INT>(col, sub_type, pure_doc_value, array);
    case TYPE_BIGINT:
        return process_column<TYPE_BIGINT>(col, sub_type, pure_doc_value, array);
    case TYPE_LARGEINT:
        return process_column<TYPE_LARGEINT>(col, sub_type, pure_doc_value, array);
    case TYPE_FLOAT:
        return process_column<TYPE_FLOAT>(col, sub_type, pure_doc_value, array);
    case TYPE_DOUBLE:
        return process_column<TYPE_DOUBLE>(col, sub_type, pure_doc_value, array);
    case TYPE_BOOLEAN:
        return process_column<TYPE_BOOLEAN>(col, sub_type, pure_doc_value, array);
    // date/datetime v2 is the default type for catalog table,
    // see https://github.com/apache/doris/pull/16304
    // No need to support date and datetime types.
    case TYPE_DATEV2: {
        return process_date_column<TYPE_DATEV2>(col, sub_type, pure_doc_value, array, time_zone);
    }
    case TYPE_DATETIMEV2: {
        return process_date_column<TYPE_DATETIMEV2>(col, sub_type, pure_doc_value, array,
                                                    time_zone);
    }
    case TYPE_JSONB: {
        return process_jsonb_column(col, sub_type, pure_doc_value, array);
    }
    default:
        LOG(ERROR) << "Do not support Array type: " << sub_type;
        return Status::InternalError("Unsupported type");
    }
}

ScrollParser::ScrollParser(bool doc_value_mode) : _size(0), _line_index(0) {}

ScrollParser::~ScrollParser() = default;

Status ScrollParser::parse(const std::string& scroll_result, bool exactly_once) {
    // rely on `_size !=0 ` to determine whether scroll ends
    _size = 0;
    _document_node.Parse(scroll_result.c_str(), scroll_result.length());
    if (_document_node.HasParseError()) {
        return Status::InternalError("Parsing json error, json is: {}", scroll_result);
    }

    if (!exactly_once && !_document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(WARNING) << "Document has not a scroll id field scroll response:" << scroll_result;
        return Status::InternalError("Document has not a scroll id field");
    }

    if (!exactly_once) {
        const rapidjson::Value& scroll_node = _document_node[FIELD_SCROLL_ID];
        _scroll_id = scroll_node.GetString();
    }
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value& outer_hits_node = _document_node[FIELD_HITS];
    // if has no inner hits, there has no data in this index
    if (!outer_hits_node.HasMember(FIELD_INNER_HITS)) {
        return Status::OK();
    }
    const rapidjson::Value& inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    // this happened just the end of scrolling
    if (!inner_hits_node.IsArray()) {
        return Status::OK();
    }
    _inner_hits_node.CopyFrom(inner_hits_node, _document_node.GetAllocator());
    // how many documents contains in this batch
    _size = _inner_hits_node.Size();
    return Status::OK();
}

int ScrollParser::get_size() const {
    return _size;
}

const std::string& ScrollParser::get_scroll_id() {
    return _scroll_id;
}

Status ScrollParser::fill_columns(const TupleDescriptor* tuple_desc,
                                  std::vector<MutableColumnPtr>& columns, bool* line_eof,
                                  const std::map<std::string, std::string>& docvalue_context,
                                  const cctz::time_zone& time_zone) {
    return fill_columns(tuple_desc->slots(), columns, line_eof, docvalue_context, time_zone);
}

// Keep identical ES-to-Doris type dispatch for the tuple and file-local projection entry points.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status ScrollParser::fill_columns(const std::vector<SlotDescriptor*>& slots,
                                  std::vector<MutableColumnPtr>& columns, bool* line_eof,
                                  const std::map<std::string, std::string>& docvalue_context,
                                  const cctz::time_zone& time_zone,
                                  std::span<const size_t> column_positions) {
    DORIS_CHECK(column_positions.empty() || column_positions.size() == slots.size());
    *line_eof = true;

    if (_size <= 0 || _line_index >= _size) {
        return Status::OK();
    }

    const rapidjson::Value& obj = _inner_hits_node[_line_index++];
    bool pure_doc_value = false;
    if (obj.HasMember("fields")) {
        pure_doc_value = true;
    }
    // obj may be neither have `_source` nor `fields` field.
    const rapidjson::Value* line = nullptr;
    if (obj.HasMember(FIELD_SOURCE)) {
        line = &obj[FIELD_SOURCE];
    } else if (obj.HasMember("fields")) {
        line = &obj["fields"];
    }

    for (size_t i = 0; i < slots.size(); ++i) {
        const SlotDescriptor* slot_desc = slots[i];
        auto* col_ptr = columns[column_positions.empty() ? i : column_positions[i]].get();

        if (slot_desc->col_name() == FIELD_ID) {
            // actually this branch will not be reached, this is guaranteed by Doris FE.
            if (pure_doc_value) {
                return Status::RuntimeError("obtain `_id` is not supported in doc_values mode");
            }
            // obj[FIELD_ID] must not be NULL
            std::string _id = obj[FIELD_ID].GetString();
            size_t len = _id.length();

            col_ptr->insert_data(const_cast<const char*>(_id.data()), len);
            continue;
        }

        const char* col_name = pure_doc_value ? docvalue_context.at(slot_desc->col_name()).c_str()
                                              : slot_desc->col_name().c_str();

        if (line == nullptr || line->FindMember(col_name) == line->MemberEnd()) {
            if (slot_desc->is_nullable()) {
                auto* nullable_column = reinterpret_cast<ColumnNullable*>(col_ptr);
                nullable_column->insert_data(nullptr, 0);
                continue;
            } else {
                std::string details = absl::Substitute(INVALID_NULL_VALUE, col_name);
                return Status::RuntimeError(details);
            }
        }

        auto type = slot_desc->type()->get_primitive_type();
        const rapidjson::Value& source_value = (*line)[col_name];
        // Source fallback changes every sibling's decoder. A scalar numeric/Boolean/date field
        // may hold [value], [] or [null]; preserve its single value or missing-value semantics.
        // STRING and JSONB keep their JSON representation; declared ARRAY keeps its container.
        bool scalar_source_array = !pure_doc_value && source_value.IsArray() &&
                                   type != TYPE_ARRAY && type != TYPE_JSONB &&
                                   !is_string_type(type);
        const rapidjson::Value& col =
                scalar_source_array && source_value.Size() == 1 ? source_value[0] : source_value;
        // Empty numeric source strings are ES missing values, including singleton arrays.
        bool is_null = col.IsNull() || (scalar_source_array && source_value.Empty()) ||
                       (!pure_doc_value && is_number(type) && type != TYPE_BOOLEAN &&
                        col.IsString() && col.GetStringLength() == 0);
        if (is_null && slot_desc->is_nullable()) {
            col_ptr->insert_data(nullptr, 0);
            continue;
        } else if (is_null && !slot_desc->is_nullable()) {
            std::string details = absl::Substitute(INVALID_NULL_VALUE, col_name);
            return Status::RuntimeError(details);
        }
        switch (type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            // sometimes elasticsearch user post some not-string value to Elasticsearch Index.
            // because of reading value from _source, we can not process all json type and then just transfer the value to original string representation
            // this may be a tricky, but we can work around this issue
            std::string val;
            if (pure_doc_value) {
                if (col.Empty()) {
                    break;
                } else if (col.Size() > 1) {
                    // doc_values with multiple elements means actual array data
                    // in ES keyword/text field, serialize as JSON array string
                    val = json_value_to_string(col);
                } else if (!col[0].IsString()) {
                    val = json_value_to_string(col[0]);
                } else {
                    val = col[0].GetString();
                }
            } else {
                // When ES mapping is keyword/text but actual data is an array,
                // serialize the array to JSON string instead of throwing an error.
                // This is valid in ES since any field can hold array values.
                if (col.IsArray()) {
                    val = json_value_to_string(col);
                } else if (!col.IsString()) {
                    val = json_value_to_string(col);
                } else {
                    val = col.GetString();
                }
            }
            size_t val_size = val.length();
            col_ptr->insert_data(const_cast<const char*>(val.data()), val_size);
            break;
        }

        case TYPE_TINYINT: {
            RETURN_IF_ERROR(insert_int_value<int8_t>(col, type, col_ptr, pure_doc_value,
                                                     slot_desc->is_nullable()));
            break;
        }

        case TYPE_SMALLINT: {
            RETURN_IF_ERROR(insert_int_value<int16_t>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_INT: {
            RETURN_IF_ERROR(insert_int_value<int32_t>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_BIGINT: {
            RETURN_IF_ERROR(insert_int_value<int64_t>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_LARGEINT: {
            RETURN_IF_ERROR(insert_int_value<__int128>(col, type, col_ptr, pure_doc_value,
                                                       slot_desc->is_nullable()));
            break;
        }

        case TYPE_DOUBLE: {
            RETURN_IF_ERROR(insert_float_value<double>(col, type, col_ptr, pure_doc_value,
                                                       slot_desc->is_nullable()));
            break;
        }

        case TYPE_FLOAT: {
            RETURN_IF_ERROR(insert_float_value<float>(col, type, col_ptr, pure_doc_value,
                                                      slot_desc->is_nullable()));
            break;
        }

        case TYPE_BOOLEAN: {
            if (col.IsBool()) {
                int8_t val = col.GetBool();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            }

            if (col.IsNumber()) {
                int8_t val = static_cast<int8_t>(col.GetInt());
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            }

            bool is_nested_str = false;
            if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsBool()) {
                int8_t val = col[0].GetBool();
                col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)), 0);
                break;
            } else if (pure_doc_value && col.IsArray() && !col.Empty() && col[0].IsString()) {
                is_nested_str = true;
            } else if (pure_doc_value && col.IsArray()) {
                return Status::InternalError(ERROR_INVALID_COL_DATA, "BOOLEAN");
            }

            const rapidjson::Value& str_col = is_nested_str ? col[0] : col;

            RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);

            const std::string& val = str_col.GetString();
            size_t val_size = str_col.GetStringLength();
            StringParser::ParseResult result;
            bool b = parse_es_bool(val.c_str(), val_size, &result);
            RETURN_ERROR_IF_PARSING_FAILED(result, str_col, type);
            col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&b)), 0);
            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value data;

            if (col.IsDouble()) {
                data.assign_from_double(col.GetDouble());
            } else {
                std::string val;
                if (pure_doc_value) {
                    if (col.Empty()) {
                        break;
                    } else if (!col[0].IsString()) {
                        val = json_value_to_string(col[0]);
                    } else {
                        val = col[0].GetString();
                    }
                } else {
                    RETURN_ERROR_IF_COL_IS_ARRAY(col, type, true);
                    if (!col.IsString()) {
                        val = json_value_to_string(col);
                    } else {
                        val = col.GetString();
                    }
                }
                data.parse_from_str(val.data(), static_cast<int32_t>(val.length()));
            }
            col_ptr->insert_data(const_cast<const char*>(reinterpret_cast<char*>(&data)), 0);
            break;
        }

        case TYPE_DATE:
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATE>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        case TYPE_DATETIME:
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATETIME>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        case TYPE_DATEV2:
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATEV2>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        case TYPE_DATETIMEV2: {
            RETURN_IF_ERROR(
                    fill_date_int<TYPE_DATETIMEV2>(col, type, pure_doc_value, col_ptr, time_zone));
            break;
        }
        case TYPE_ARRAY: {
            Array array;
            const auto& sub_type =
                    assert_cast<const DataTypeArray*>(remove_nullable(slot_desc->type()).get())
                            ->get_nested_type()
                            ->get_primitive_type();
            RETURN_IF_ERROR(parse_column(col, sub_type, pure_doc_value, array, time_zone));
            col_ptr->insert(Field::create_field<TYPE_ARRAY>(array));
            break;
        }
        case TYPE_JSONB: {
            JsonBinaryValue jsonb_value;
            RETURN_IF_ERROR(jsonb_value.from_json_string(json_value_to_string(col)));
            JsonbField json(jsonb_value.value(), jsonb_value.size());
            col_ptr->insert(Field::create_field<TYPE_JSONB>(json));
            break;
        }
        default: {
            LOG(ERROR) << "Unsupported data type: " << type_to_string(type);
            DCHECK(false);
            break;
        }
        }
    }

    *line_eof = false;
    return Status::OK();
}
} // namespace doris
