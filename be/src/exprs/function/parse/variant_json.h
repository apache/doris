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

#pragma once

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "core/value/variant/variant_encoded_block.h"
#include "core/value/variant/variant_value.h"

namespace cctz {
class time_zone;
}

namespace doris {

// Formatting state supplied by the column adapter. A null time zone means UTC.
struct VariantJsonFormatOptions {
    const cctz::time_zone* timezone = nullptr;
};

struct JsonToVariantOptions {
    uint32_t max_json_key_length = 255;
    bool throw_on_invalid_json = false;
    bool check_duplicate_json_path = false;

    // Takes a value snapshot. Later changes to mutable config do not affect an encoder.
    static JsonToVariantOptions current_config();
};

// Reuses one JSON parser and one block builder. Each add_json() collects into a stack-only row;
// finish_block() performs pass 2. A failed add_json() is terminal, as is a successful
// finish_block(). try_add_json() rolls back invalid input rows and keeps collecting.
class JsonToVariantEncoder {
public:
    JsonToVariantEncoder();
    explicit JsonToVariantEncoder(JsonToVariantOptions options);
    ~JsonToVariantEncoder();

    JsonToVariantEncoder(const JsonToVariantEncoder&) = delete;
    JsonToVariantEncoder& operator=(const JsonToVariantEncoder&) = delete;
    JsonToVariantEncoder(JsonToVariantEncoder&&) noexcept;
    JsonToVariantEncoder& operator=(JsonToVariantEncoder&&) noexcept;

    void add_json(StringRef json);
    Status try_add_json(StringRef json);
    VariantEncodedBlock finish_block();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

namespace variant_json_detail {

struct FormattedScalar {
    std::array<char, 128> bytes {};
    size_t size = 0;
};

void require_json_depth(uint32_t depth);
void require_exact_json_value(VariantRef value);
void require_valid_json_utf8(StringRef value, const char* description);
void require_json_object_key(StringRef key, StringRef previous_key, uint32_t field_index);
[[noreturn]] void throw_unsupported_json_primitive(VariantPrimitiveId id);

FormattedScalar format_json_int(int64_t value);
FormattedScalar format_json_float(float value);
FormattedScalar format_json_double(double value);
FormattedScalar format_json_decimal(VariantDecimal value);
FormattedScalar format_json_date(int32_t days_since_epoch);
FormattedScalar format_json_timestamp(int64_t value, uint8_t fractional_digits, bool utc_adjusted,
                                      const cctz::time_zone* timezone);
FormattedScalar format_json_time_micros(int64_t value);
FormattedScalar format_json_uuid(const std::array<uint8_t, 16>& value);

template <typename Writer, size_t N>
void write_literal(Writer& out, const char (&value)[N]) {
    out.write(value, N - 1);
}

template <typename Writer>
void write_scalar(Writer& out, const FormattedScalar& value) {
    out.write(value.bytes.data(), value.size);
}

template <typename Writer>
void write_quoted_scalar(Writer& out, const FormattedScalar& value) {
    write_literal(out, "\"");
    write_scalar(out, value);
    write_literal(out, "\"");
}

template <typename Writer>
void write_escaped_json_string(Writer& out, StringRef value) {
    write_literal(out, "\"");
    size_t plain_begin = 0;
    size_t index = 0;
    while (index < value.size) {
        const auto byte = static_cast<uint8_t>(value.data[index]);
        const char* escaped = nullptr;
        size_t escaped_size = 0;
        size_t consumed = 1;
        switch (byte) {
        case '"':
            escaped = "\\\"";
            escaped_size = 2;
            break;
        case '\\':
            escaped = "\\\\";
            escaped_size = 2;
            break;
        case '\b':
            escaped = "\\b";
            escaped_size = 2;
            break;
        case '\f':
            escaped = "\\f";
            escaped_size = 2;
            break;
        case '\n':
            escaped = "\\n";
            escaped_size = 2;
            break;
        case '\r':
            escaped = "\\r";
            escaped_size = 2;
            break;
        case '\t':
            escaped = "\\t";
            escaped_size = 2;
            break;
        default:
            if (byte < 0x20) {
                static constexpr char HEX[] = "0123456789ABCDEF";
                char unicode_escape[] = {'\\', 'u', '0', '0', HEX[byte >> 4], HEX[byte & 0x0F]};
                if (index != plain_begin) {
                    out.write(value.data + plain_begin, index - plain_begin);
                }
                out.write(unicode_escape, sizeof(unicode_escape));
                ++index;
                plain_begin = index;
                continue;
            }
            if (index + 2 < value.size && byte == 0xE2 &&
                static_cast<uint8_t>(value.data[index + 1]) == 0x80 &&
                (static_cast<uint8_t>(value.data[index + 2]) == 0xA8 ||
                 static_cast<uint8_t>(value.data[index + 2]) == 0xA9)) {
                escaped =
                        static_cast<uint8_t>(value.data[index + 2]) == 0xA8 ? "\\u2028" : "\\u2029";
                escaped_size = 6;
                consumed = 3;
            }
            break;
        }
        if (escaped == nullptr) {
            ++index;
            continue;
        }
        if (index != plain_begin) {
            out.write(value.data + plain_begin, index - plain_begin);
        }
        out.write(escaped, escaped_size);
        index += consumed;
        plain_begin = index;
    }
    if (plain_begin != value.size) {
        out.write(value.data + plain_begin, value.size - plain_begin);
    }
    write_literal(out, "\"");
}

template <typename Writer>
void write_json_string(Writer& out, StringRef value, const char* description) {
    require_valid_json_utf8(value, description);
    write_escaped_json_string(out, value);
}

template <typename Writer>
void write_json_binary(Writer& out, StringRef value) {
    static constexpr char BASE64[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    write_literal(out, "\"");
    std::array<char, 1024> encoded {};
    size_t encoded_size = 0;
    size_t index = 0;
    const auto flush = [&]() {
        if (encoded_size != 0) {
            out.write(encoded.data(), encoded_size);
            encoded_size = 0;
        }
    };
    while (index + 3 <= value.size) {
        if (encoded.size() - encoded_size < 4) {
            flush();
        }
        const auto first = static_cast<uint8_t>(value.data[index]);
        const auto second = static_cast<uint8_t>(value.data[index + 1]);
        const auto third = static_cast<uint8_t>(value.data[index + 2]);
        encoded[encoded_size++] = BASE64[first >> 2];
        encoded[encoded_size++] = BASE64[((first & 0x03) << 4) | (second >> 4)];
        encoded[encoded_size++] = BASE64[((second & 0x0F) << 2) | (third >> 6)];
        encoded[encoded_size++] = BASE64[third & 0x3F];
        index += 3;
    }
    if (index != value.size) {
        if (encoded.size() - encoded_size < 4) {
            flush();
        }
        const auto first = static_cast<uint8_t>(value.data[index]);
        encoded[encoded_size++] = BASE64[first >> 2];
        if (index + 1 == value.size) {
            encoded[encoded_size++] = BASE64[(first & 0x03) << 4];
            encoded[encoded_size++] = '=';
            encoded[encoded_size++] = '=';
        } else {
            const auto second = static_cast<uint8_t>(value.data[index + 1]);
            encoded[encoded_size++] = BASE64[((first & 0x03) << 4) | (second >> 4)];
            encoded[encoded_size++] = BASE64[(second & 0x0F) << 2];
            encoded[encoded_size++] = '=';
        }
    }
    flush();
    write_literal(out, "\"");
}

template <typename Writer>
class Printer {
public:
    Printer(Writer& out, const VariantJsonFormatOptions& options) : _out(out), _options(options) {}

    void write(VariantRef value, uint32_t depth) {
        require_json_depth(depth);
        switch (value.basic_type()) {
        case VariantBasicType::SHORT_STRING:
            write_json_string(_out, value.get_string(), "string");
            return;
        case VariantBasicType::PRIMITIVE:
            write_primitive(value);
            return;
        case VariantBasicType::OBJECT:
            write_object(value, depth);
            return;
        case VariantBasicType::ARRAY:
            write_array(value, depth);
            return;
        }
    }

private:
    void write_primitive(VariantRef value) {
        const VariantPrimitiveId id = value.primitive_id();
        switch (id) {
        case VariantPrimitiveId::NULL_VALUE:
            write_literal(_out, "null");
            return;
        case VariantPrimitiveId::TRUE_VALUE:
            write_literal(_out, "true");
            return;
        case VariantPrimitiveId::FALSE_VALUE:
            write_literal(_out, "false");
            return;
        case VariantPrimitiveId::INT8:
        case VariantPrimitiveId::INT16:
        case VariantPrimitiveId::INT32:
        case VariantPrimitiveId::INT64:
            write_scalar(_out, format_json_int(value.get_int()));
            return;
        case VariantPrimitiveId::DOUBLE: {
            const double number = value.get_double();
            if (!std::isfinite(number)) {
                write_non_finite(number);
                return;
            }
            write_scalar(_out, format_json_double(number));
            return;
        }
        case VariantPrimitiveId::DECIMAL4:
        case VariantPrimitiveId::DECIMAL8:
        case VariantPrimitiveId::DECIMAL16:
            write_scalar(_out, format_json_decimal(value.get_decimal()));
            return;
        case VariantPrimitiveId::DATE:
            write_quoted_scalar(_out, format_json_date(value.get_date()));
            return;
        case VariantPrimitiveId::TIMESTAMP_MICROS:
            write_quoted_scalar(_out, format_json_timestamp(value.get_timestamp_micros(), 6, true,
                                                            _options.timezone));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
            write_quoted_scalar(_out, format_json_timestamp(value.get_timestamp_ntz_micros(), 6,
                                                            false, nullptr));
            return;
        case VariantPrimitiveId::FLOAT: {
            const float number = value.get_float();
            if (!std::isfinite(number)) {
                write_non_finite(number);
                return;
            }
            write_scalar(_out, format_json_float(number));
            return;
        }
        case VariantPrimitiveId::BINARY:
            write_json_binary(_out, value.get_binary());
            return;
        case VariantPrimitiveId::STRING:
            write_json_string(_out, value.get_string(), "string");
            return;
        case VariantPrimitiveId::TIME_NTZ_MICROS:
            write_quoted_scalar(_out, format_json_time_micros(value.get_time_ntz_micros()));
            return;
        case VariantPrimitiveId::TIMESTAMP_NANOS:
            write_quoted_scalar(_out, format_json_timestamp(value.get_timestamp_nanos(), 9, true,
                                                            _options.timezone));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
            write_quoted_scalar(_out, format_json_timestamp(value.get_timestamp_ntz_nanos(), 9,
                                                            false, nullptr));
            return;
        case VariantPrimitiveId::UUID:
            write_quoted_scalar(_out, format_json_uuid(value.get_uuid()));
            return;
        }
        throw_unsupported_json_primitive(id);
    }

    template <typename Number>
    void write_non_finite(Number value) {
        if (std::isnan(value)) {
            write_literal(_out, "\"NaN\"");
        } else if (value > 0) {
            write_literal(_out, "\"Infinity\"");
        } else {
            write_literal(_out, "\"-Infinity\"");
        }
    }

    void write_object(VariantRef value, uint32_t depth) {
        write_literal(_out, "{");
        const uint32_t count = value.num_elements();
        StringRef previous_key;
        for (uint32_t index = 0; index < count; ++index) {
            if (index != 0) {
                write_literal(_out, ",");
            }
            uint32_t field_id = 0;
            VariantRef child = value.object_value_at(index, &field_id);
            const StringRef key = value.metadata.key_at(field_id);
            require_json_object_key(key, previous_key, index);
            write_escaped_json_string(_out, key);
            write_literal(_out, ":");
            write(child, depth + 1);
            previous_key = key;
        }
        write_literal(_out, "}");
    }

    void write_array(VariantRef value, uint32_t depth) {
        write_literal(_out, "[");
        const uint32_t count = value.num_elements();
        for (uint32_t index = 0; index < count; ++index) {
            if (index != 0) {
                write_literal(_out, ",");
            }
            write(value.array_at(index), depth + 1);
        }
        write_literal(_out, "]");
    }

    Writer& _out;
    const VariantJsonFormatOptions& _options;
};

} // namespace variant_json_detail

// Writer is statically dispatched and only needs write(const char*, size_t).
template <typename Writer>
void to_json(VariantRef value, Writer& out,
             const VariantJsonFormatOptions& options = VariantJsonFormatOptions {}) {
    variant_json_detail::require_exact_json_value(value);
    variant_json_detail::Printer<Writer>(out, options).write(value, 0);
}

} // namespace doris
