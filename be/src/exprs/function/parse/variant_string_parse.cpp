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

#include "exprs/function/parse/variant_string_parse.h"

#include <cctz/time_zone.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <simdjson.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "util/json/simd_json_parser.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

using variant_json::FormattedScalar;

void append_char(FormattedScalar* result, char value) {
    result->bytes[result->size++] = value;
}

void append_unsigned(FormattedScalar* result, uint64_t value, size_t minimum_digits = 1) {
    std::array<char, 32> reversed {};
    size_t digits = 0;
    do {
        reversed[digits++] = static_cast<char>('0' + value % 10);
        value /= 10;
    } while (value != 0);
    while (digits < minimum_digits) {
        reversed[digits++] = '0';
    }
    while (digits != 0) {
        append_char(result, reversed[--digits]);
    }
}

void append_year(FormattedScalar* result, int64_t year) {
    if (year >= 0 && year <= 9999) {
        append_unsigned(result, static_cast<uint64_t>(year), 4);
        return;
    }
    if (year >= 0) {
        append_char(result, '+');
        append_unsigned(result, static_cast<uint64_t>(year), 4);
        return;
    }
    append_char(result, '-');
    append_unsigned(result, static_cast<uint64_t>(-(year + 1)) + 1, 4);
}

void append_date_time(FormattedScalar* result, const cctz::civil_second& civil, bool include_time) {
    append_year(result, civil.year());
    append_char(result, '-');
    append_unsigned(result, civil.month(), 2);
    append_char(result, '-');
    append_unsigned(result, civil.day(), 2);
    if (!include_time) {
        return;
    }
    append_char(result, ' ');
    append_unsigned(result, civil.hour(), 2);
    append_char(result, ':');
    append_unsigned(result, civil.minute(), 2);
    append_char(result, ':');
    append_unsigned(result, civil.second(), 2);
}

void append_fraction(FormattedScalar* result, uint64_t fraction, uint8_t digits) {
    append_char(result, '.');
    append_unsigned(result, fraction, digits);
}

void append_offset(FormattedScalar* result, int offset_seconds) {
    const bool negative = offset_seconds < 0;
    const uint64_t magnitude_seconds =
            negative ? static_cast<uint64_t>(-(static_cast<int64_t>(offset_seconds)))
                     : static_cast<uint64_t>(offset_seconds);
    append_char(result, negative ? '-' : '+');
    append_unsigned(result, magnitude_seconds / 3600, 2);
    append_char(result, ':');
    append_unsigned(result, (magnitude_seconds % 3600) / 60, 2);
    if (magnitude_seconds % 60 != 0) {
        append_char(result, ':');
        append_unsigned(result, magnitude_seconds % 60, 2);
    }
}

std::pair<int64_t, uint64_t> split_epoch(int64_t value, int64_t units_per_second) {
    int64_t seconds = value / units_per_second;
    int64_t fraction = value % units_per_second;
    if (fraction < 0) {
        --seconds;
        fraction += units_per_second;
    }
    return {seconds, static_cast<uint64_t>(fraction)};
}

StringRef to_string_ref(std::string_view value) {
    return {value.data(), value.size()};
}

void require_json_key_length(std::string_view key, uint32_t maximum) {
    if (key.size() > maximum) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant JSON object key length {} exceeds maximum {} bytes", key.size(),
                        maximum);
    }
}

class JsonTreeCollector {
public:
    JsonTreeCollector(VariantBatchBuilder::Row& builder, const JsonToVariantOptions& options)
            : _builder(builder), _options(options) {}

    void collect(SimdJSONParser::Element element, uint32_t depth) {
        variant_json::require_json_depth(depth);
        if (element.isNull()) {
            _builder.add_null();
        } else if (element.isBool()) {
            _builder.add_bool(element.getBool());
        } else if (element.isInt64()) {
            _builder.add_int(element.getInt64());
        } else if (element.isUInt64()) {
            _builder.add_largeint(static_cast<__int128>(element.getUInt64()));
        } else if (element.isDouble()) {
            _builder.add_double(element.getDouble());
        } else if (element.isString()) {
            _builder.add_string(to_string_ref(element.getString()));
        } else if (element.isArray()) {
            collect_array(element.getArray(), depth);
        } else if (element.isObject()) {
            collect_object(element.getObject(), depth);
        }
    }

private:
    void collect_array(const SimdJSONParser::Array& array, uint32_t depth) {
        auto scope = _builder.start_array();
        for (SimdJSONParser::Element child : array) {
            collect(child, depth + 1);
        }
        scope.finish();
    }

    void collect_object(const SimdJSONParser::Object& object, uint32_t depth) {
        auto scope = _builder.start_object();
        for (const auto& [key, child] : object) {
            require_json_key_length(key, _options.max_json_key_length);
            scope.add_key(to_string_ref(key));
            collect(child, depth + 1);
        }
        scope.finish();
    }

    VariantBatchBuilder::Row& _builder;
    const JsonToVariantOptions& _options;
};

// The ondemand twin of JsonTreeCollector for a destination whose Schema Template rides with its
// type (it declares a DECIMAL or LARGEINT path): it sees each number's text, so a number on a
// DECIMAL path becomes an exact decimal and an integer beyond 64 bits on a LARGEINT path an exact
// 128-bit integer. It reads every document of a template with a DECIMAL field, and of any other
// only a document the DOM parser rejects (add_json_row). Any simdjson error makes the collect
// return false and the caller gives the text the DOM parser's verdict, the documented one. Builder
// exceptions (depth, key length, duplicate keys) propagate as they do there.
class OndemandTreeCollector {
public:
    OndemandTreeCollector(VariantBatchBuilder::Row& builder, const JsonToVariantOptions& options)
            : _builder(builder), _options(options), _paths(*options.template_paths) {}

    bool collect(simdjson::ondemand::value& value, uint32_t depth) {
        variant_json::require_json_depth(depth);
        simdjson::ondemand::json_type type;
        if (value.type().get(type) != simdjson::SUCCESS) {
            return false;
        }
        switch (type) {
        case simdjson::ondemand::json_type::null: {
            bool is_null = false;
            if (value.is_null().get(is_null) != simdjson::SUCCESS || !is_null) {
                return false;
            }
            _builder.add_null();
            return true;
        }
        case simdjson::ondemand::json_type::boolean: {
            bool flag = false;
            if (value.get_bool().get(flag) != simdjson::SUCCESS) {
                return false;
            }
            _builder.add_bool(flag);
            return true;
        }
        case simdjson::ondemand::json_type::string: {
            std::string_view text;
            if (value.get_string().get(text) != simdjson::SUCCESS) {
                return false;
            }
            _builder.add_string(to_string_ref(text));
            return true;
        }
        case simdjson::ondemand::json_type::number:
            return collect_number(value);
        case simdjson::ondemand::json_type::array:
            return collect_array(value, depth);
        case simdjson::ondemand::json_type::object:
            return collect_object(value, depth);
        }
        return false;
    }

private:
    static std::string_view trim_json_token(std::string_view token) {
        while (!token.empty() && (token.back() == ' ' || token.back() == '\t' ||
                                  token.back() == '\n' || token.back() == '\r')) {
            token.remove_suffix(1);
        }
        while (!token.empty() && (token.front() == ' ' || token.front() == '\t' ||
                                  token.front() == '\n' || token.front() == '\r')) {
            token.remove_prefix(1);
        }
        return token;
    }

    // Only a number that may be read from its text pays for the match (a glob is an fnmatch per
    // template field): an int64 / uint64 is stored as it is, and so is a double when no field is a
    // DECIMAL. Under an object that is an array element nothing is on a declared path
    // (collect_object), and neither is a value with no key above it: the root, or an element of
    // an array at the root.
    VariantNumberRead number_read() const {
        return _unmatched == 0 && _keys != 0 ? _paths.number_read(_path)
                                             : VariantNumberRead::NATIVE;
    }

    bool collect_number(simdjson::ondemand::value& value) {
        simdjson::ondemand::number number;
        const simdjson::error_code error = value.get_number().get(number);
        VariantNumberRead mode = VariantNumberRead::NATIVE;
        if (error == simdjson::SUCCESS) {
            switch (number.get_number_type()) {
            case simdjson::ondemand::number_type::signed_integer:
                _builder.add_int(number.get_int64());
                return true;
            case simdjson::ondemand::number_type::unsigned_integer:
                _builder.add_largeint(static_cast<__int128>(number.get_uint64()));
                return true;
            case simdjson::ondemand::number_type::floating_point_number:
                mode = _paths.reads_decimal_text() ? number_read() : VariantNumberRead::NATIVE;
                if (mode != VariantNumberRead::DECIMAL_TEXT) {
                    // also on a LARGEINT path: a fraction or an exponent is not an integer as
                    // written, so the double is kept and the template's cast decides
                    _builder.add_double(number.get_double());
                    return true;
                }
                break;
            case simdjson::ondemand::number_type::big_integer:
                mode = number_read();
                break;
            }
        } else if (error != simdjson::NUMBER_ERROR && error != simdjson::BIGINT_ERROR) {
            return false;
        } else {
            mode = number_read();
        }
        // What is left is a fraction or an exponent on a DECIMAL path, or a token simdjson holds
        // in no 64-bit type. It reports two valid JSON numbers that way (the big_integer type, or
        // NUMBER_ERROR / BIGINT_ERROR): an integer beyond 64 bits (18446744073709551616) and an
        // exponent a double cannot hold (1e400). The same errors also cover tokens that are not
        // JSON numbers at all, "01" or "-Infinity" among them, so each text reader enforces the
        // grammar itself and whatever it declines takes the DOM parser's verdict.
        switch (mode) {
        case VariantNumberRead::NATIVE:
            // The DOM parser fails the whole document on an integer beyond 64 bits, so off the
            // DECIMAL and LARGEINT paths such a document is rejected, or kept as its text by the
            // lenient serde, as it is today.
            return false;
        case VariantNumberRead::DECIMAL_TEXT:
            return collect_decimal_text(trim_json_token(value.raw_json_token()));
        case VariantNumberRead::LARGEINT_TEXT:
            return collect_largeint_text(trim_json_token(value.raw_json_token()));
        }
        return false;
    }

    // Beyond 38 significant digits, or at a scale no decimal has, the text is kept as it is and
    // the template's cast decides (a correctly rounded value or NULL). Only a value below 10^-39
    // is not kept: the reader answers the 0 it rounds to at every scale.
    bool collect_decimal_text(std::string_view token) {
        VariantDecimalText decimal;
        switch (parse_json_number_as_decimal(token, decimal)) {
        case JsonNumberToDecimal::OK:
            // the reader's width holds the scale too; the builder's own choice goes by magnitude
            _builder.add_decimal(decimal.unscaled, decimal.scale, decimal.width);
            return true;
        case JsonNumberToDecimal::OUT_OF_RANGE:
            _builder.add_string(to_string_ref(token));
            return true;
        case JsonNumberToDecimal::MALFORMED:
            return false;
        }
        return false;
    }

    // An integer beyond 64 bits on a LARGEINT path, read exactly instead of failing the document.
    // The builder stores a value up to 10^38 - 1 as a scale-0 decimal16 and a larger one as its
    // digits (add_largeint), and the template's cast to LARGEINT takes either. An integer no
    // LARGEINT holds keeps its text, as a decimal beyond 38 digits does, and that cast decides
    // (NULL). A doc-mode document column is written before the cast and keeps either as read.
    bool collect_largeint_text(std::string_view token) {
        __int128 integer = 0;
        switch (parse_json_integer_as_largeint(token, integer)) {
        case JsonNumberToLargeInt::OK:
            _builder.add_largeint(integer);
            return true;
        case JsonNumberToLargeInt::OUT_OF_RANGE:
            _builder.add_string(to_string_ref(token));
            return true;
        case JsonNumberToLargeInt::NOT_AN_INTEGER:
            return false;
        }
        return false;
    }

    // Elements keep their array's path, so an ARRAY<DECIMAL> or ARRAY<LARGEINT> template covers
    // the scalars directly inside it. The template addresses an array as a leaf, so an array that
    // is itself an array element is as far off every declared path as an object element:
    // {"prices":[[1.50]]} with 'prices' declared keeps the double 1.5 the shredder stores.
    bool collect_array(simdjson::ondemand::value& value, uint32_t depth) {
        simdjson::ondemand::array array;
        if (value.get_array().get(array) != simdjson::SUCCESS) {
            return false;
        }
        auto scope = _builder.start_array();
        const bool was_in_array = _in_array;
        if (was_in_array) {
            ++_unmatched;
        }
        _in_array = true;
        for (auto entry : array) {
            simdjson::ondemand::value element;
            if (std::move(entry).get(element) != simdjson::SUCCESS ||
                !collect(element, depth + 1)) {
                return false;
            }
        }
        _in_array = was_in_array;
        if (was_in_array) {
            --_unmatched;
        }
        scope.finish();
        return true;
    }

    bool collect_object(simdjson::ondemand::value& value, uint32_t depth) {
        simdjson::ondemand::object object;
        if (value.get_object().get(object) != simdjson::SUCCESS) {
            return false;
        }
        auto scope = _builder.start_object();
        // The template addresses an array as a leaf (an ARRAY<DECIMAL> field covers the scalars
        // directly inside it) and the shredder never types the members of an object element, so
        // nothing under such an object is on a declared path: {"a":[{"b":1.50}]} with 'a.b'
        // declared keeps the double 1.5 the shredder stores, instead of becoming the decimal 1.50.
        const bool element_of_array = _in_array;
        if (element_of_array) {
            ++_unmatched;
        }
        _in_array = false;
        for (auto entry : object) {
            simdjson::ondemand::field field;
            if (std::move(entry).get(field) != simdjson::SUCCESS) {
                return false;
            }
            std::string_view key;
            if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
                return false;
            }
            require_json_key_length(key, _options.max_json_key_length);
            scope.add_key(to_string_ref(key));
            // The shredder joins every key below the first with '.', an empty one included:
            // {"":{"b":1}} is the path ".b", which a field declared 'b' does not match. So the
            // separator goes by the number of keys above, not by whether the path is still empty.
            const size_t path_size = _path.size();
            if (_keys != 0) {
                _path.push_back('.');
            }
            _path.append(key);
            ++_keys;
            simdjson::ondemand::value child = field.value();
            const bool collected = collect(child, depth + 1);
            --_keys;
            _path.resize(path_size);
            if (!collected) {
                return false;
            }
        }
        scope.finish();
        _in_array = element_of_array;
        if (element_of_array) {
            --_unmatched;
        }
        return true;
    }

    VariantBatchBuilder::Row& _builder;
    const JsonToVariantOptions& _options;
    const VariantTemplatePaths& _paths;
    std::string _path;
    // The number of object keys `_path` joins: 0 for a value with no key above it.
    uint32_t _keys = 0;
    // Set while an array's elements are collected; an object or array element raises `_unmatched`
    // for its whole subtree. A `false` return abandons the collector with its row, so none of the
    // three is restored on that path.
    bool _in_array = false;
    uint32_t _unmatched = 0;
};

} // namespace

namespace variant_json {

void require_json_depth(uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant JSON traversal exceeds maximum depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

void require_exact_json_value(VariantRef value) {
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant value has {} trailing bytes after its {} byte root",
                        value.value.size - encoded_size, encoded_size);
    }
}

void require_valid_json_utf8(StringRef value, const char* description) {
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::CORRUPTION, "Variant {} is not valid UTF-8", description);
    }
}

void require_json_object_key(StringRef key, StringRef previous_key, uint32_t field_index) {
    require_valid_json_utf8(key, "object key");
    if (field_index != 0 && previous_key.compare(key) >= 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant object keys are not strictly byte-sorted at field {}",
                        field_index);
    }
}

[[noreturn]] void throw_unsupported_json_primitive(VariantPrimitiveId id) {
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Unsupported Variant JSON primitive id {}",
                    static_cast<uint8_t>(id));
}

FormattedScalar format_json_int(int64_t value) {
    FormattedScalar result;
    char* end = fmt::format_to(result.bytes.data(), FMT_COMPILE("{}"), value);
    result.size = end - result.bytes.data();
    return result;
}

FormattedScalar format_json_float(float value) {
    FormattedScalar result;
    char* end = fmt::format_to(result.bytes.data(), FMT_COMPILE("{:.{}g}"), value,
                               std::numeric_limits<float>::digits10 + 1);
    result.size = end - result.bytes.data();
    return result;
}

FormattedScalar format_json_double(double value) {
    FormattedScalar result;
    char* end = fmt::format_to(result.bytes.data(), FMT_COMPILE("{:.{}g}"), value,
                               std::numeric_limits<double>::digits10 + 1);
    result.size = end - result.bytes.data();
    return result;
}

FormattedScalar format_json_decimal(VariantDecimal value) {
    FormattedScalar result;
    const bool negative = value.unscaled < 0;
    std::array<char, 39> reversed {};
    size_t digits = 0;
    unsigned __int128 remaining = variant_unsigned_magnitude(value.unscaled);
    do {
        reversed[digits++] = static_cast<char>('0' + remaining % 10);
        remaining /= 10;
    } while (remaining != 0);

    if (negative) {
        append_char(&result, '-');
    }
    if (value.scale == 0) {
        while (digits != 0) {
            append_char(&result, reversed[--digits]);
        }
        return result;
    }
    if (digits <= value.scale) {
        append_char(&result, '0');
        append_char(&result, '.');
        for (size_t zero = digits; zero < value.scale; ++zero) {
            append_char(&result, '0');
        }
        while (digits != 0) {
            append_char(&result, reversed[--digits]);
        }
        return result;
    }

    while (digits > value.scale) {
        append_char(&result, reversed[--digits]);
    }
    append_char(&result, '.');
    while (digits != 0) {
        append_char(&result, reversed[--digits]);
    }
    return result;
}

FormattedScalar format_json_date(int32_t days_since_epoch) {
    constexpr int64_t SECONDS_PER_DAY = 86'400;
    const int64_t seconds = static_cast<int64_t>(days_since_epoch) * SECONDS_PER_DAY;
    const auto lookup =
            cctz::utc_time_zone().lookup(cctz::time_point<cctz::seconds>(cctz::seconds(seconds)));
    FormattedScalar result;
    append_date_time(&result, lookup.cs, false);
    return result;
}

FormattedScalar format_json_timestamp(int64_t value, uint8_t fractional_digits, bool utc_adjusted,
                                      const cctz::time_zone* timezone) {
    if (fractional_digits != 6 && fractional_digits != 9) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant timestamp JSON precision must be 6 or 9, got {}",
                        fractional_digits);
    }
    const int64_t units_per_second = fractional_digits == 6 ? 1'000'000 : 1'000'000'000;
    const auto [seconds, fraction] = split_epoch(value, units_per_second);
    const cctz::time_zone& zone =
            utc_adjusted && timezone != nullptr ? *timezone : cctz::utc_time_zone();
    const auto lookup = zone.lookup(cctz::time_point<cctz::seconds>(cctz::seconds(seconds)));
    FormattedScalar result;
    append_date_time(&result, lookup.cs, true);
    append_fraction(&result, fraction, fractional_digits);
    if (utc_adjusted) {
        append_offset(&result, lookup.offset);
    }
    return result;
}

FormattedScalar format_json_time_micros(int64_t value) {
    constexpr int64_t MICROS_PER_SECOND = 1'000'000;
    constexpr int64_t MICROS_PER_DAY = 86'400 * MICROS_PER_SECOND;
    if (value < 0 || value >= MICROS_PER_DAY) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant time value {} is outside [0, {}) microseconds", value,
                        MICROS_PER_DAY);
    }
    const auto [seconds, micros] = split_epoch(value, MICROS_PER_SECOND);
    FormattedScalar result;
    append_unsigned(&result, static_cast<uint64_t>(seconds / 3600), 2);
    append_char(&result, ':');
    append_unsigned(&result, static_cast<uint64_t>((seconds % 3600) / 60), 2);
    append_char(&result, ':');
    append_unsigned(&result, static_cast<uint64_t>(seconds % 60), 2);
    append_fraction(&result, micros, 6);
    return result;
}

FormattedScalar format_json_uuid(const std::array<uint8_t, 16>& value) {
    static constexpr char HEX[] = "0123456789abcdef";
    FormattedScalar result;
    for (size_t index = 0; index < value.size(); ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10) {
            append_char(&result, '-');
        }
        append_char(&result, HEX[value[index] >> 4]);
        append_char(&result, HEX[value[index] & 0x0F]);
    }
    return result;
}

} // namespace variant_json

JsonToVariantOptions JsonToVariantOptions::current_config() {
    return {.max_json_key_length = static_cast<uint32_t>(config::variant_max_json_key_length)};
}

struct JsonStringToVariantEncoder::Impl {
    enum class State : uint8_t { COLLECTING, FINISHED, FAILED };

    explicit Impl(JsonToVariantOptions options_) : options(options_) {
        if (options.max_json_key_length == 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant maximum JSON key length must be positive");
        }
    }

    void require_collecting() const {
        if (state == State::FINISHED) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant JSON encoder is already finished");
        }
        if (state == State::FAILED) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant JSON encoder is in a terminal failed state");
        }
    }

    void add_json_row(StringRef json) {
        if (json.size != 0 && json.data == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant JSON input has a null data pointer");
        }
        // A root scalar has no path, so the ondemand pass could only decline it after copying and
        // indexing the text: the "null" the parse functions add for every SQL NULL row is the
        // commonest one. Only an object or an array is given to it.
        const bool has_template = options.template_paths != nullptr &&
                                  !options.template_paths->empty() && starts_a_container(json);
        // The DOM parser reads a fraction into a binary64 without complaint, so under a template
        // with a DECIMAL field every document is read from its text first. Under one without
        // (LARGEINT fields only) the two reads differ only on an integer beyond 64 bits, which
        // fails the DOM parser: that parser goes first, and the text read gets what it rejects.
        if (has_template && options.template_paths->reads_decimal_text() &&
            add_row_with_template_paths(json)) {
            return;
        }
        SimdJSONParser::Element root;
        const bool parsed = json.size != 0 && parser.parse(json.data, json.size, root);
        if (!parsed && has_template && !options.template_paths->reads_decimal_text() &&
            add_row_with_template_paths(json)) {
            return;
        }
        // The DOM parser's verdict, the one the text has without a template.
        auto row = builder.begin_row();
        if (!parsed) {
            if (options.throw_on_invalid_json) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Failed to parse JSON as Variant");
            }
            row.add_string(json);
            row.finish();
            return;
        }
        JsonTreeCollector(row, options).collect(root, 0);
        row.finish();
    }

    // Whether the first byte of the document opens an object or an array, past what simdjson
    // itself passes over before a document: a UTF-8 byte order mark and JSON whitespace.
    static bool starts_a_container(StringRef json) {
        size_t pos = 0;
        if (json.size >= 3 && std::memcmp(json.data, "\xEF\xBB\xBF", 3) == 0) {
            pos = 3;
        }
        for (; pos < json.size; ++pos) {
            const char c = json.data[pos];
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return c == '{' || c == '[';
            }
        }
        return false;
    }

    // Adds the document as the ondemand pass reads it, or nothing: false means the row it began
    // was rolled back, and the caller gives the text the DOM parser's verdict.
    bool add_row_with_template_paths(StringRef json) {
        auto row = builder.begin_row();
        bool collected = false;
        if (options.throw_on_invalid_json) {
            collected = collect_with_template_paths(row, json);
        } else {
            // The lenient serde keeps a document the DOM parser rejects as a string row, and the
            // DOM parser reads the whole text before any depth or key check runs. The ondemand
            // pass checks as it reads, so on a document whose prefix is over-deep or
            // duplicate-keyed AND whose tail is malformed, {"o":{"a":1,"a":2},"k":x}, it would
            // throw where the DOM path stored the text. Let the DOM parser give the verdict: a
            // well-formed document meets the same check there and fails as it always did.
            try {
                collected = collect_with_template_paths(row, json);
            } catch (const Exception& exception) {
                if (exception.code() != ErrorCode::INVALID_ARGUMENT) {
                    throw;
                }
            }
        }
        if (!collected) {
            // `row` ends here and rolls its partial content back
            return false;
        }
        row.finish();
        return true;
    }

    // A root object or array read with the ondemand parser; false on any simdjson error, and for a
    // root scalar, which has no path.
    bool collect_with_template_paths(VariantBatchBuilder::Row& row, StringRef json) {
        // One padded copy per encoder, grown to the largest document seen: ondemand values point
        // into it, and the builder copies a row's bytes before the next document is read.
        const size_t padded_size = json.size + simdjson::SIMDJSON_PADDING;
        if (padding_buffer.size() < padded_size) {
            padding_buffer.resize(padded_size);
        }
        std::memcpy(padding_buffer.data(), json.data, json.size);
        const std::string_view padded(padding_buffer.data(), json.size);
        simdjson::ondemand::document document;
        if (ondemand_parser.iterate(padded, padding_buffer.size()).get(document) !=
            simdjson::SUCCESS) {
            return false;
        }
        simdjson::ondemand::json_type type;
        if (document.type().get(type) != simdjson::SUCCESS) {
            return false;
        }
        if (type != simdjson::ondemand::json_type::object &&
            type != simdjson::ondemand::json_type::array) {
            return false;
        }
        simdjson::ondemand::value root;
        if (document.get_value().get(root) != simdjson::SUCCESS) {
            return false;
        }
        if (!OndemandTreeCollector(row, options).collect(root, 0)) {
            return false;
        }
        return document.at_end();
    }

    JsonToVariantOptions options;
    VariantBatchBuilder builder;
    SimdJSONParser parser;
    simdjson::ondemand::parser ondemand_parser;
    // The ondemand parser's input: a padded copy of the current document, reused across rows and
    // freed with the encoder. Untracked, like the DOM parser's own copy of the document.
    std::string padding_buffer;
    State state = State::COLLECTING;
};

JsonStringToVariantEncoder::JsonStringToVariantEncoder()
        : JsonStringToVariantEncoder(JsonToVariantOptions::current_config()) {}

JsonStringToVariantEncoder::JsonStringToVariantEncoder(JsonToVariantOptions options)
        : _impl(std::make_unique<Impl>(options)) {}

JsonStringToVariantEncoder::~JsonStringToVariantEncoder() = default;
JsonStringToVariantEncoder::JsonStringToVariantEncoder(JsonStringToVariantEncoder&&) noexcept =
        default;
JsonStringToVariantEncoder& JsonStringToVariantEncoder::operator=(
        JsonStringToVariantEncoder&&) noexcept = default;

void JsonStringToVariantEncoder::add_json(StringRef json) {
    _impl->require_collecting();
    try {
        _impl->add_json_row(json);
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

Status JsonStringToVariantEncoder::try_add_json(StringRef json) {
    _impl->require_collecting();
    try {
        _impl->add_json_row(json);
        return Status::OK();
    } catch (const Exception& exception) {
        if (exception.code() == ErrorCode::INVALID_ARGUMENT) {
            return exception.to_status();
        }
        _impl->state = Impl::State::FAILED;
        throw;
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

VariantBatchBuilder JsonStringToVariantEncoder::finish_batch() {
    _impl->require_collecting();
    try {
        VariantBatchBuilder block = _impl->builder.finish_batch();
        _impl->state = Impl::State::FINISHED;
        return block;
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

} // namespace doris
