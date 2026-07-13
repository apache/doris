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

#include "util/variant/variant_json.h"

#include <cctz/time_zone.h>
#include <fmt/compile.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "util/json/simd_json_parser.h"
#include "util/utf8_check.h"
#include "util/variant/variant_block_builder.h"
#include "util/variant/variant_encoding.h"

namespace doris {
namespace {

using variant_json_detail::FormattedScalar;

unsigned __int128 magnitude(__int128 value) {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    return value < 0 ? ~unsigned_value + 1 : unsigned_value;
}

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
    JsonTreeCollector(VariantBlockBuilder::Row& builder, const JsonToVariantOptions& options)
            : _builder(builder), _options(options) {}

    void collect(SimdJSONParser::Element element, uint32_t depth) {
        variant_json_detail::require_json_depth(depth);
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
        if (!_options.check_duplicate_json_path) {
            for (const auto& [key, child] : object) {
                require_json_key_length(key, _options.max_json_key_length);
                scope.add_key(to_string_ref(key));
                collect(child, depth + 1);
            }
            scope.finish();
            return;
        }

        std::unordered_set<std::string_view> seen_keys;
        seen_keys.reserve(object.size());
        for (const auto& [key, child] : object) {
            require_json_key_length(key, _options.max_json_key_length);
            if (seen_keys.emplace(key).second) {
                scope.add_key(to_string_ref(key));
                collect(child, depth + 1);
            } else {
                validate_ignored(child, depth + 1);
            }
        }
        scope.finish();
    }

    void validate_ignored(SimdJSONParser::Element element, uint32_t depth) const {
        variant_json_detail::require_json_depth(depth);
        if (element.isArray()) {
            for (SimdJSONParser::Element child : element.getArray()) {
                validate_ignored(child, depth + 1);
            }
        } else if (element.isObject()) {
            for (const auto& [key, child] : element.getObject()) {
                require_json_key_length(key, _options.max_json_key_length);
                validate_ignored(child, depth + 1);
            }
        }
    }

    VariantBlockBuilder::Row& _builder;
    const JsonToVariantOptions& _options;
};

} // namespace

namespace variant_json_detail {

void require_json_depth(uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant JSON traversal exceeds maximum depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

void require_exact_json_value(VariantValueRef value) {
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant value has {} trailing bytes after its {} byte root",
                        value.size - encoded_size, encoded_size);
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
    unsigned __int128 remaining = magnitude(value.unscaled);
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

} // namespace variant_json_detail

JsonToVariantOptions JsonToVariantOptions::current_config() {
    return {.max_json_key_length = static_cast<uint32_t>(config::variant_max_json_key_length),
            .throw_on_invalid_json = config::variant_throw_exeception_on_invalid_json,
            .check_duplicate_json_path = config::variant_enable_duplicate_json_path_check};
}

struct JsonToVariantEncoder::Impl {
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
        auto row = builder.begin_row();
        if (json.size != 0 && json.data == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant JSON input has a null data pointer");
        }
        if (json.size == 0) {
            auto object = row.start_object();
            object.finish();
            row.finish();
            return;
        }

        SimdJSONParser::Element root;
        if (!parser.parse(json.data, json.size, root)) {
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

    JsonToVariantOptions options;
    VariantBlockBuilder builder;
    SimdJSONParser parser;
    State state = State::COLLECTING;
};

JsonToVariantEncoder::JsonToVariantEncoder()
        : JsonToVariantEncoder(JsonToVariantOptions::current_config()) {}

JsonToVariantEncoder::JsonToVariantEncoder(JsonToVariantOptions options)
        : _impl(std::make_unique<Impl>(options)) {}

JsonToVariantEncoder::~JsonToVariantEncoder() = default;
JsonToVariantEncoder::JsonToVariantEncoder(JsonToVariantEncoder&&) noexcept = default;
JsonToVariantEncoder& JsonToVariantEncoder::operator=(JsonToVariantEncoder&&) noexcept = default;

void JsonToVariantEncoder::add_json(StringRef json) {
    _impl->require_collecting();
    try {
        _impl->add_json_row(json);
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

Status JsonToVariantEncoder::try_add_json(StringRef json) {
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

VariantEncodedBlock JsonToVariantEncoder::finish_block() {
    _impl->require_collecting();
    try {
        VariantEncodedBlock block = _impl->builder.finish_block();
        _impl->state = Impl::State::FINISHED;
        return block;
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

} // namespace doris
