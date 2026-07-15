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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "util/variant/variant_block_builder.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_value.h"
#include "variant_test_utils.h"

namespace doris {
namespace {

constexpr std::string_view ARTIFACT = "org.apache.parquet:parquet-variant:1.17.0";
constexpr std::string_view JAR_SHA256 =
        "daecf8161e7bba63f7ba9fd62c1e8a77730c9a9d76a335191dc9d0a0fcaaec52";

struct GoldenVector {
    std::string name;
    std::string provenance;
    std::string root_type;
    std::string expected;
    std::string metadata;
    std::string value;
};

struct EncodedVariant {
    std::string metadata;
    std::string value;
};

[[noreturn]] void fixture_error(const std::filesystem::path& path, size_t line,
                                std::string_view reason) {
    throw std::runtime_error(path.string() + ":" + std::to_string(line) + ": " +
                             std::string(reason));
}

std::vector<std::string_view> split_tabs(std::string_view line) {
    std::vector<std::string_view> fields;
    size_t begin = 0;
    while (true) {
        const size_t delimiter = line.find('\t', begin);
        if (delimiter == std::string_view::npos) {
            fields.push_back(line.substr(begin));
            return fields;
        }
        fields.push_back(line.substr(begin, delimiter - begin));
        begin = delimiter + 1;
    }
}

uint8_t decode_hex_digit(char digit) {
    if (digit >= '0' && digit <= '9') {
        return static_cast<uint8_t>(digit - '0');
    }
    if (digit >= 'a' && digit <= 'f') {
        return static_cast<uint8_t>(digit - 'a' + 10);
    }
    throw std::runtime_error("golden hex must use lowercase hexadecimal digits");
}

std::string decode_hex(std::string_view encoded) {
    if (encoded.empty() || encoded.size() % 2 != 0) {
        throw std::runtime_error("golden hex must be non-empty and have even length");
    }
    std::string decoded;
    decoded.reserve(encoded.size() / 2);
    for (size_t index = 0; index < encoded.size(); index += 2) {
        const uint8_t high = decode_hex_digit(encoded[index]);
        const uint8_t low = decode_hex_digit(encoded[index + 1]);
        decoded.push_back(static_cast<char>((high << 4) | low));
    }
    return decoded;
}

std::filesystem::path fixture_directory() {
    const std::filesystem::path source_path(__FILE__);
    const std::array candidates {
            source_path.parent_path() / "testdata",
            std::filesystem::current_path() / "be/test/util/variant/testdata",
    };
    for (const auto& candidate : candidates) {
        if (std::filesystem::is_directory(candidate)) {
            return std::filesystem::canonical(candidate);
        }
    }
    std::ostringstream message;
    message << "Variant golden fixture directory is missing; __FILE__=" << __FILE__
            << ", cwd=" << std::filesystem::current_path();
    throw std::runtime_error(message.str());
}

std::map<std::string, GoldenVector> load_corpus(std::string_view filename,
                                                std::string_view expected_corpus) {
    const std::filesystem::path path = fixture_directory() / filename;
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Cannot open Variant golden corpus " + path.string());
    }

    const std::array expected_headers {
            std::string("# Licensed to the Apache Software Foundation (ASF) under one"),
            std::string("# or more contributor license agreements.  See the NOTICE file"),
            std::string("# distributed with this work for additional information"),
            std::string("# regarding copyright ownership.  The ASF licenses this file"),
            std::string("# to you under the Apache License, Version 2.0 (the"),
            std::string("# \"License\"); you may not use this file except in compliance"),
            std::string("# with the License.  You may obtain a copy of the License at"),
            std::string("#"),
            std::string("#   http://www.apache.org/licenses/LICENSE-2.0"),
            std::string("#"),
            std::string("# Unless required by applicable law or agreed to in writing,"),
            std::string("# software distributed under the License is distributed on an"),
            std::string("# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY"),
            std::string("# KIND, either express or implied.  See the License for the"),
            std::string("# specific language governing permissions and limitations"),
            std::string("# under the License."),
            std::string("#"),
            std::string("# variant-golden-v1"),
            std::string("# artifact=") + std::string(ARTIFACT),
            std::string("# jar_sha256=") + std::string(JAR_SHA256),
            std::string("# corpus=") + std::string(expected_corpus),
            std::string(
                    "# "
                    "columns=name\\tprovenance\\troot_type\\texpected\\tmetadata_hex\\tvalue_hex"),
    };
    std::string line;
    size_t line_number = 0;
    for (const std::string& expected_header : expected_headers) {
        ++line_number;
        if (!std::getline(input, line) || line != expected_header) {
            fixture_error(path, line_number, "unexpected or missing corpus header");
        }
    }

    std::map<std::string, GoldenVector> result;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.empty() || line.starts_with('#')) {
            fixture_error(path, line_number, "blank lines and extra comments are forbidden");
        }
        const std::vector<std::string_view> fields = split_tabs(line);
        if (fields.size() != 6) {
            fixture_error(path, line_number, "record must contain exactly six columns");
        }
        for (std::string_view field : fields) {
            if (field.empty()) {
                fixture_error(path, line_number, "record columns must be non-empty");
            }
        }
        GoldenVector vector {
                .name = std::string(fields[0]),
                .provenance = std::string(fields[1]),
                .root_type = std::string(fields[2]),
                .expected = std::string(fields[3]),
                .metadata = {},
                .value = {},
        };
        try {
            vector.metadata = decode_hex(fields[4]);
            vector.value = decode_hex(fields[5]);
        } catch (const std::exception& error) {
            fixture_error(path, line_number, error.what());
        }
        if (!result.emplace(vector.name, std::move(vector)).second) {
            fixture_error(path, line_number, "duplicate vector name");
        }
    }
    if (!input.eof()) {
        throw std::runtime_error("Failed while reading Variant golden corpus " + path.string());
    }
    return result;
}

StringRef string_ref(std::string_view value) {
    return {value.data(), value.size()};
}

void append_hex(std::string& output, const char* data, size_t size) {
    constexpr char DIGITS[] = "0123456789abcdef";
    output.reserve(output.size() + size * 2);
    for (size_t index = 0; index < size; ++index) {
        const auto byte = static_cast<uint8_t>(data[index]);
        output.push_back(DIGITS[byte >> 4]);
        output.push_back(DIGITS[byte & 0x0F]);
    }
}

std::string hex(StringRef value) {
    std::string result;
    append_hex(result, value.data, value.size);
    return result;
}

std::string int128_to_string(__int128 value) {
    if (value == 0) {
        return "0";
    }
    const bool negative = value < 0;
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    unsigned __int128 magnitude = negative ? ~unsigned_value + 1 : unsigned_value;
    std::string result;
    while (magnitude != 0) {
        result.push_back(static_cast<char>('0' + magnitude % 10));
        magnitude /= 10;
    }
    if (negative) {
        result.push_back('-');
    }
    std::ranges::reverse(result);
    return result;
}

__int128 parse_int128(std::string_view value) {
    if (value.empty()) {
        throw std::invalid_argument("empty int128 literal");
    }
    __int128 result = 0;
    for (char digit : value) {
        if (!std::isdigit(static_cast<unsigned char>(digit))) {
            throw std::invalid_argument("invalid int128 literal");
        }
        result = result * 10 + (digit - '0');
    }
    return result;
}

std::string fixed_hex(uint64_t value, size_t digits) {
    std::array<char, 17> buffer {};
    const int written = std::snprintf(buffer.data(), buffer.size(), "%016llx",
                                      static_cast<unsigned long long>(value));
    if (written != 16 || digits > 16) {
        throw std::runtime_error("Failed to format floating-point bits");
    }
    return {buffer.data() + 16 - digits, digits};
}

std::string root_type(VariantValueRef value) {
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        return "string";
    case VariantBasicType::OBJECT:
        return "object";
    case VariantBasicType::ARRAY:
        return "array";
    case VariantBasicType::PRIMITIVE:
        break;
    }
    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
        return "null";
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return "boolean";
    case VariantPrimitiveId::INT8:
        return "byte";
    case VariantPrimitiveId::INT16:
        return "short";
    case VariantPrimitiveId::INT32:
        return "int";
    case VariantPrimitiveId::INT64:
        return "long";
    case VariantPrimitiveId::DOUBLE:
        return "double";
    case VariantPrimitiveId::DECIMAL4:
        return "decimal4";
    case VariantPrimitiveId::DECIMAL8:
        return "decimal8";
    case VariantPrimitiveId::DECIMAL16:
        return "decimal16";
    case VariantPrimitiveId::DATE:
        return "date";
    case VariantPrimitiveId::TIMESTAMP_MICROS:
        return "timestamp_tz";
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
        return "timestamp_ntz";
    case VariantPrimitiveId::FLOAT:
        return "float";
    case VariantPrimitiveId::BINARY:
        return "binary";
    case VariantPrimitiveId::STRING:
        return "string";
    case VariantPrimitiveId::TIME_NTZ_MICROS:
        return "time";
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        return "timestamp_nanos_tz";
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return "timestamp_nanos_ntz";
    case VariantPrimitiveId::UUID:
        return "uuid";
    }
    throw std::runtime_error("Unknown Variant primitive type in golden vector");
}

std::string describe(VariantValueRef value);

std::string describe_object(VariantValueRef value) {
    std::string result = "object{";
    for (uint32_t index = 0; index < value.num_elements(); ++index) {
        if (index != 0) {
            result.push_back(';');
        }
        uint32_t field_id = std::numeric_limits<uint32_t>::max();
        const VariantValueRef child = value.object_value_at(index, &field_id);
        const StringRef key = value.metadata.key_at(field_id);
        VariantValueRef found;
        if (!value.object_find(key, &found)) {
            throw std::runtime_error("Doris object lookup missed an iterated golden field");
        }
        const std::string child_description = describe(child);
        if (describe(found) != child_description) {
            throw std::runtime_error("Doris object lookup disagrees with golden iteration");
        }
        append_hex(result, key.data, key.size);
        result.push_back('=');
        result.append(child_description);
    }
    result.push_back('}');
    return result;
}

std::string describe_array(VariantValueRef value) {
    std::string result = "array[";
    for (uint32_t index = 0; index < value.num_elements(); ++index) {
        if (index != 0) {
            result.push_back(';');
        }
        result.append(describe(value.array_at(index)));
    }
    result.push_back(']');
    return result;
}

std::string describe_uuid(VariantValueRef value) {
    const std::array bytes = value.get_uuid();
    std::string encoded;
    for (size_t index = 0; index < bytes.size(); ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10) {
            encoded.push_back('-');
        }
        const char byte = static_cast<char>(bytes[index]);
        append_hex(encoded, &byte, 1);
    }
    return "uuid:" + encoded;
}

std::string describe(VariantValueRef value) {
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        return "string:" + hex(value.get_string());
    case VariantBasicType::OBJECT:
        return describe_object(value);
    case VariantBasicType::ARRAY:
        return describe_array(value);
    case VariantBasicType::PRIMITIVE:
        break;
    }
    const VariantPrimitiveId id = value.primitive_id();
    switch (id) {
    case VariantPrimitiveId::NULL_VALUE:
        return "null";
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return std::string("bool:") + (value.get_bool() ? "true" : "false");
    case VariantPrimitiveId::INT8:
        return "int8:" + std::to_string(value.get_int());
    case VariantPrimitiveId::INT16:
        return "int16:" + std::to_string(value.get_int());
    case VariantPrimitiveId::INT32:
        return "int32:" + std::to_string(value.get_int());
    case VariantPrimitiveId::INT64:
        return "int64:" + std::to_string(value.get_int());
    case VariantPrimitiveId::DOUBLE:
        return "double:" + fixed_hex(std::bit_cast<uint64_t>(value.get_double()), 16);
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16: {
        const VariantDecimal decimal = value.get_decimal();
        return root_type(value) + ":" + int128_to_string(decimal.unscaled) + ":" +
               std::to_string(decimal.scale);
    }
    case VariantPrimitiveId::DATE:
        return "date:" + std::to_string(value.get_date());
    case VariantPrimitiveId::TIMESTAMP_MICROS:
        return "timestamp_tz:" + std::to_string(value.get_timestamp_micros());
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
        return "timestamp_ntz:" + std::to_string(value.get_timestamp_ntz_micros());
    case VariantPrimitiveId::FLOAT:
        return "float:" +
               fixed_hex(std::bit_cast<uint32_t>(value.get_float()), sizeof(uint32_t) * 2);
    case VariantPrimitiveId::BINARY:
        return "binary:" + hex(value.get_binary());
    case VariantPrimitiveId::STRING:
        return "string:" + hex(value.get_string());
    case VariantPrimitiveId::TIME_NTZ_MICROS:
        return "time:" + std::to_string(value.get_time_ntz_micros());
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        return "timestamp_nanos_tz:" + std::to_string(value.get_timestamp_nanos());
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return "timestamp_nanos_ntz:" + std::to_string(value.get_timestamp_ntz_nanos());
    case VariantPrimitiveId::UUID:
        return describe_uuid(value);
    }
    throw std::runtime_error("Unknown Variant primitive in golden semantic decoder");
}

EncodedVariant build_doris(const std::function<void(VariantBlockBuilder::Row&)>& append) {
    VariantBlockBuilder builder;
    auto row = builder.begin_row();
    append(row);
    row.finish();
    VariantEncodedBlock block = builder.finish_block();
    const VariantMetadataRef metadata = block.metadata_ref();
    const VariantValueRef value = block.value_at(0);
    return {.metadata = std::string(metadata.data, metadata.size),
            .value = std::string(value.data, value.size)};
}

// NOLINTNEXTLINE(readability-function-size) -- keep the cross-language vector matrix together.
std::map<std::string, EncodedVariant> doris_vectors() {
    std::map<std::string, EncodedVariant> vectors;
    auto add = [&vectors](std::string name,
                          const std::function<void(VariantBlockBuilder::Row&)>& append) {
        vectors.emplace(std::move(name), build_doris(append));
    };

    add("doris_null", [](VariantBlockBuilder::Row& builder) { builder.add_null(); });
    add("doris_true", [](VariantBlockBuilder::Row& builder) { builder.add_bool(true); });
    add("doris_false", [](VariantBlockBuilder::Row& builder) { builder.add_bool(false); });
    add("doris_int8", [](VariantBlockBuilder::Row& builder) { builder.add_int(-7); });
    add("doris_int8_min", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int8_t>::min());
    });
    add("doris_int8_max", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int8_t>::max());
    });
    add("doris_int16", [](VariantBlockBuilder::Row& builder) { builder.add_int(128); });
    add("doris_int16_min", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int16_t>::min());
    });
    add("doris_int16_max", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int16_t>::max());
    });
    add("doris_int32", [](VariantBlockBuilder::Row& builder) { builder.add_int(32768); });
    add("doris_int32_min", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int32_t>::min());
    });
    add("doris_int32_max", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int32_t>::max());
    });
    add("doris_int64", [](VariantBlockBuilder::Row& builder) { builder.add_int(2147483648L); });
    add("doris_int64_min", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int64_t>::min());
    });
    add("doris_int64_max", [](VariantBlockBuilder::Row& builder) {
        builder.add_int(std::numeric_limits<int64_t>::max());
    });
    add("doris_double", [](VariantBlockBuilder::Row& builder) {
        builder.add_double(std::bit_cast<double>(uint64_t {0x400921fb54442d18}));
    });
    add("doris_decimal4",
        [](VariantBlockBuilder::Row& builder) { builder.add_decimal(1234567, 2); });
    add("doris_decimal8",
        [](VariantBlockBuilder::Row& builder) { builder.add_decimal(123456789012345LL, 5); });
    add("doris_decimal16", [](VariantBlockBuilder::Row& builder) {
        builder.add_decimal(parse_int128("12345678901234567891234567890"), 10);
    });
    add("doris_decimal4_precision9_max",
        [](VariantBlockBuilder::Row& builder) { builder.add_decimal(999'999'999, 2); });
    add("doris_decimal4_precision9_min",
        [](VariantBlockBuilder::Row& builder) { builder.add_decimal(-999'999'999, 2); });
    add("doris_decimal8_precision10_min",
        [](VariantBlockBuilder::Row& builder) { builder.add_decimal(1'000'000'000, 0); });
    add("doris_decimal8_precision18_max", [](VariantBlockBuilder::Row& builder) {
        builder.add_decimal(999'999'999'999'999'999LL, 0);
    });
    add("doris_decimal16_precision19_min", [](VariantBlockBuilder::Row& builder) {
        builder.add_decimal(parse_int128("1000000000000000000"), 0);
    });
    add("doris_decimal16_precision38_max", [](VariantBlockBuilder::Row& builder) {
        builder.add_decimal(parse_int128("99999999999999999999999999999999999999"), 0);
    });
    add("doris_decimal4_scale38_min_unit",
        [](VariantBlockBuilder::Row& builder) { builder.add_decimal(-1, 38); });
    add("doris_decimal4_trailing_zero",
        [](VariantBlockBuilder::Row& builder) { builder.add_decimal(12'300, 4); });
    add("doris_date", [](VariantBlockBuilder::Row& builder) { builder.add_date(-12345); });
    add("doris_timestamp_tz", [](VariantBlockBuilder::Row& builder) {
        builder.add_timestamp_micros(-1234567890123L, true);
    });
    add("doris_timestamp_ntz", [](VariantBlockBuilder::Row& builder) {
        builder.add_timestamp_micros(2234567890123L, false);
    });
    add("doris_float", [](VariantBlockBuilder::Row& builder) { builder.add_float(1.5F); });
    add("doris_binary", [](VariantBlockBuilder::Row& builder) {
        const std::string binary("\0\x01\xFF", 3);
        builder.add_binary(StringRef(binary));
    });
    add("doris_long_string", [](VariantBlockBuilder::Row& builder) {
        const std::string text(64, 'L');
        builder.add_string(StringRef(text));
    });
    add("doris_time",
        [](VariantBlockBuilder::Row& builder) { builder.add_time_ntz_micros(86'399'999'999L); });
    add("doris_timestamp_nanos_tz", [](VariantBlockBuilder::Row& builder) {
        builder.add_timestamp_nanos(-3234567890123L, true);
    });
    add("doris_timestamp_nanos_ntz", [](VariantBlockBuilder::Row& builder) {
        builder.add_timestamp_nanos(4234567890123L, false);
    });
    add("doris_uuid", [](VariantBlockBuilder::Row& builder) {
        const std::array<uint8_t, 16> uuid {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                                            0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF};
        builder.add_uuid(uuid);
    });
    add("doris_short_empty",
        [](VariantBlockBuilder::Row& builder) { builder.add_string(StringRef("", 0)); });
    add("doris_short_63", [](VariantBlockBuilder::Row& builder) {
        const std::string text(63, 's');
        builder.add_string(StringRef(text));
    });
    add("doris_unicode_string", [](VariantBlockBuilder::Row& builder) {
        constexpr std::string_view text = "A\xC3\xA9\xE4\xB8\xAD\xF0\x90\x80\x80";
        builder.add_string(string_ref(text));
    });
    add("doris_object", [](VariantBlockBuilder::Row& builder) {
        auto object = builder.start_object();
        object.add_key(string_ref("z"));
        builder.add_int(1);
        object.add_key(string_ref("a"));
        builder.add_string(string_ref("x"));
        object.finish();
    });
    add("doris_array", [](VariantBlockBuilder::Row& builder) {
        auto array = builder.start_array();
        builder.add_int(1);
        builder.add_string(string_ref("x"));
        builder.add_bool(false);
        array.finish();
    });
    add("doris_nested", [](VariantBlockBuilder::Row& builder) {
        auto root = builder.start_object();
        root.add_key(string_ref("arr"));
        auto array = builder.start_array();
        builder.add_int(7);
        auto inner = builder.start_object();
        inner.add_key(string_ref("inside"));
        builder.add_bool(false);
        inner.finish();
        array.finish();
        root.finish();
    });
    add("doris_unicode_object", [](VariantBlockBuilder::Row& builder) {
        constexpr std::string_view first = "\xC3\xA9";
        constexpr std::string_view second = "\xEE\x80\x80";
        auto object = builder.start_object();
        object.add_key(string_ref(second));
        builder.add_int(2);
        object.add_key(string_ref(first));
        builder.add_int(1);
        object.finish();
    });
    return vectors;
}

const std::set<std::string>& expected_java_vector_names() {
    static const std::set<std::string> names {
            "array_count_255",
            "array_count_256",
            "array_offset_bytes_255",
            "array_offset_bytes_256",
            "array_offset_bytes_65535",
            "array_offset_bytes_65536",
            "array_ordered",
            "empty_array",
            "empty_object",
            "metadata_bytes_255",
            "metadata_bytes_256",
            "metadata_bytes_65535",
            "metadata_bytes_65536",
            "nested_object_array",
            "object_id_count_256",
            "object_id_count_257",
            "object_unsorted_metadata",
            "primitive_binary",
            "primitive_date",
            "primitive_decimal16",
            "primitive_decimal16_precision19_min",
            "primitive_decimal16_precision38_max",
            "primitive_decimal4",
            "primitive_decimal4_precision9_max",
            "primitive_decimal4_precision9_min",
            "primitive_decimal4_scale38_min_unit",
            "primitive_decimal4_trailing_zero",
            "primitive_decimal8",
            "primitive_decimal8_precision10_min",
            "primitive_decimal8_precision18_max",
            "primitive_double",
            "primitive_false",
            "primitive_float",
            "primitive_int16",
            "primitive_int16_max",
            "primitive_int16_min",
            "primitive_int32",
            "primitive_int32_max",
            "primitive_int32_min",
            "primitive_int64",
            "primitive_int64_max",
            "primitive_int64_min",
            "primitive_int8",
            "primitive_int8_max",
            "primitive_int8_min",
            "primitive_long_string",
            "primitive_null",
            "primitive_time",
            "primitive_timestamp_nanos_ntz",
            "primitive_timestamp_nanos_tz",
            "primitive_timestamp_ntz",
            "primitive_timestamp_tz",
            "primitive_true",
            "primitive_uuid",
            "raw_array_offset_width_4",
            "raw_metadata_width_4",
            "raw_object_id_width_3",
            "raw_object_id_width_4",
            "raw_object_nonmonotonic_offsets",
            "short_string_63",
            "short_string_empty",
            "short_string_unicode",
            "unicode_bmp_order",
            "unicode_supplementary_order",
    };
    return names;
}

const std::set<uint8_t>& all_primitive_ids() {
    static const std::set<uint8_t> ids {0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
                                        11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    return ids;
}

std::set<std::string> names_of(const std::map<std::string, GoldenVector>& vectors) {
    std::set<std::string> names;
    for (const auto& [name, unused] : vectors) {
        static_cast<void>(unused);
        names.insert(name);
    }
    return names;
}

std::set<std::string> names_of(const std::map<std::string, EncodedVariant>& vectors) {
    std::set<std::string> names;
    for (const auto& [name, unused] : vectors) {
        static_cast<void>(unused);
        names.insert(name);
    }
    return names;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- each assertion is a golden gate.
TEST(VariantGoldenVectorTest, ParquetJavaBytesDecodeWithDoris) {
    const auto vectors = load_corpus("parquet_java_vectors.tsv", "java-to-doris");
    ASSERT_EQ(names_of(vectors), expected_java_vector_names());
    std::set<uint8_t> decoded_primitive_ids;
    for (const auto& [name, vector] : vectors) {
        SCOPED_TRACE(name);
        const std::string expected_provenance =
                name.starts_with("raw_") ? "spec-raw-java-validated" : "parquet-java-builder";
        ASSERT_EQ(vector.provenance, expected_provenance);
        VariantMetadataRef metadata {
                .data = vector.metadata.data(),
                .size = vector.metadata.size(),
        };
        ASSERT_NO_THROW(metadata.validate());
        VariantValueRef value {
                .metadata = metadata,
                .data = vector.value.data(),
                .size = vector.value.size(),
        };
        ASSERT_EQ(value.value_size(), vector.value.size());
        ASSERT_EQ(root_type(value), vector.root_type);
        ASSERT_EQ(describe(value), vector.expected);
        if (value.basic_type() == VariantBasicType::PRIMITIVE) {
            decoded_primitive_ids.insert(static_cast<uint8_t>(value.primitive_id()));
        }
    }
    ASSERT_EQ(decoded_primitive_ids, all_primitive_ids());
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- each assertion is a golden gate.
TEST(VariantGoldenVectorTest, DorisBytesMatchJavaDecodedCorpus) {
    const auto golden = load_corpus("doris_java_verified_vectors.tsv", "doris-to-java");
    const auto generated = doris_vectors();
    ASSERT_EQ(names_of(golden), names_of(generated));
    std::set<uint8_t> encoded_primitive_ids;
    for (const auto& [name, encoded] : generated) {
        SCOPED_TRACE(name);
        const GoldenVector& expected = golden.at(name);
        ASSERT_EQ(expected.provenance, "doris-block-builder-java-validated");
        ASSERT_EQ(encoded.metadata, expected.metadata);
        ASSERT_EQ(encoded.value, expected.value);
        VariantMetadataRef metadata {
                .data = encoded.metadata.data(),
                .size = encoded.metadata.size(),
        };
        VariantValueRef value {
                .metadata = metadata,
                .data = encoded.value.data(),
                .size = encoded.value.size(),
        };
        ASSERT_NO_THROW(validate_canonical(value));
        ASSERT_EQ(root_type(value), expected.root_type);
        ASSERT_EQ(describe(value), expected.expected);
        if (value.basic_type() == VariantBasicType::PRIMITIVE) {
            encoded_primitive_ids.insert(static_cast<uint8_t>(value.primitive_id()));
        }
    }
    ASSERT_EQ(encoded_primitive_ids, all_primitive_ids());
}

} // namespace
} // namespace doris
