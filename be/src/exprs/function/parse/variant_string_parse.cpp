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

// The ondemand twin of JsonTreeCollector for a destination with DECIMAL template paths: it sees each
// number's text, so a number on such a path becomes an exact decimal. Any simdjson error makes the
// collect return false and the caller hands the text to the DOM parser, whose verdict is the
// documented one. Builder exceptions (depth, key length, duplicate keys) propagate as they do there.
class OndemandTreeCollector {
public:
    OndemandTreeCollector(VariantBatchBuilder::Row& builder, const JsonToVariantOptions& options)
            : _builder(builder), _options(options), _paths(*options.decimal_paths) {}

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

    bool collect_number(simdjson::ondemand::value& value) {
        simdjson::ondemand::number number;
        const simdjson::error_code error = value.get_number().get(number);
        const std::string_view token = trim_json_token(value.raw_json_token());
        const bool decimal_path = _paths.matches(_path);
        if (error == simdjson::SUCCESS) {
            switch (number.get_number_type()) {
            case simdjson::ondemand::number_type::signed_integer:
                _builder.add_int(number.get_int64());
                return true;
            case simdjson::ondemand::number_type::unsigned_integer:
                _builder.add_largeint(static_cast<__int128>(number.get_uint64()));
                return true;
            case simdjson::ondemand::number_type::floating_point_number:
                if (!decimal_path) {
                    _builder.add_double(number.get_double());
                    return true;
                }
                break;
            case simdjson::ondemand::number_type::big_integer:
                if (!decimal_path) {
                    return false;
                }
                break;
            }
        } else if (error != simdjson::NUMBER_ERROR && error != simdjson::BIGINT_ERROR) {
            return false;
        } else if (!decimal_path) {
            // an integer beyond 64 bits off a DECIMAL path: the DOM parser's verdict, as today
            return false;
        }
        // A number on a DECIMAL path is read from its text. Beyond 38 significant digits the text is
        // kept as it is and the template's cast decides (a correctly rounded value or NULL).
        VariantDecimalText decimal;
        switch (parse_json_number_as_decimal(token, &decimal)) {
        case JsonNumberToDecimal::OK:
            _builder.add_decimal(decimal.unscaled, decimal.scale);
            return true;
        case JsonNumberToDecimal::OUT_OF_RANGE:
            _builder.add_string(to_string_ref(token));
            return true;
        case JsonNumberToDecimal::MALFORMED:
            return false;
        }
        return false;
    }

    // Elements keep their array's path, so an ARRAY<DECIMAL> template covers them.
    bool collect_array(simdjson::ondemand::value& value, uint32_t depth) {
        simdjson::ondemand::array array;
        if (value.get_array().get(array) != simdjson::SUCCESS) {
            return false;
        }
        auto scope = _builder.start_array();
        for (auto entry : array) {
            simdjson::ondemand::value element;
            if (std::move(entry).get(element) != simdjson::SUCCESS || !collect(element, depth + 1)) {
                return false;
            }
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
            const size_t path_size = _path.size();
            if (!_path.empty()) {
                _path.push_back('.');
            }
            _path.append(key);
            simdjson::ondemand::value child = field.value();
            const bool collected = collect(child, depth + 1);
            _path.resize(path_size);
            if (!collected) {
                return false;
            }
        }
        scope.finish();
        return true;
    }

    VariantBatchBuilder::Row& _builder;
    const JsonToVariantOptions& _options;
    const VariantDecimalPathSet& _paths;
    std::string _path;
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
        if (json.size != 0 && options.decimal_paths != nullptr && !options.decimal_paths->empty()) {
            auto row = builder.begin_row();
            if (collect_with_decimal_paths(row, json)) {
                row.finish();
                return;
            }
            // `row` ends here and rolls its partial content back; the DOM parser below gives the
            // text the verdict it has today.
        }
        auto row = builder.begin_row();
        SimdJSONParser::Element root;
        if (json.size == 0 || !parser.parse(json.data, json.size, root)) {
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

    // A root object or array read with the ondemand parser; false on any simdjson error, and for a
    // root scalar, which has no path.
    bool collect_with_decimal_paths(VariantBatchBuilder::Row& row, StringRef json) {
        simdjson::padded_string padded(json.data, json.size);
        simdjson::ondemand::document document;
        if (ondemand_parser.iterate(padded).get(document) != simdjson::SUCCESS) {
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
    std::string quoted;
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
