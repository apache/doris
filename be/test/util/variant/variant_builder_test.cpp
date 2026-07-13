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

#include "util/variant/variant_builder.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/variant/variant_block_builder.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_scalar_encoding.h"
#include "variant_test_utils.h"

namespace doris {
namespace {

StringRef string_ref(std::string_view value) {
    return {value.data(), value.size()};
}

VariantValueRef value_ref(const VariantMetadataBuilder& metadata, const std::string& value) {
    return {.metadata = metadata.metadata_ref(), .data = value.data(), .size = value.size()};
}

std::string finish_row(VariantBuilder& builder) {
    std::string value;
    builder.finish_row(value);
    return value;
}

std::string encode_plan(const VariantScalarEncodingPlan& plan) {
    std::string encoded(plan.size(), '\0');
    plan.write(encoded.data(), encoded.size());
    return encoded;
}

template <typename AddValue>
std::string encode_builder_scalar(AddValue&& add_value) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    add_value(builder);
    metadata.seal();
    return finish_row(builder);
}

template <typename Function>
void expect_builder_exception_code(int code, Function&& function) {
    try {
        function();
        FAIL() << "Expected doris::Exception";
    } catch (const Exception& exception) {
        EXPECT_EQ(exception.code(), code) << exception.what();
    }
}

struct OwnedBuilderValue {
    std::string metadata;
    std::string value;

    VariantValueRef ref() const {
        return {.metadata = {.data = metadata.data(), .size = metadata.size()},
                .data = value.data(),
                .size = value.size()};
    }
};

template <typename Fill>
OwnedBuilderValue build_owned_value(Fill&& fill) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    fill(builder);
    metadata.seal();
    std::string value = finish_row(builder);
    const StringRef encoded_metadata = metadata.encoded_metadata();
    return {.metadata = std::string(encoded_metadata.data, encoded_metadata.size),
            .value = std::move(value)};
}

OwnedBuilderValue make_nested_owned_value() {
    return build_owned_value([](VariantBuilder& builder) {
        auto object = builder.start_object();
        object.add_key(string_ref("array"));
        auto array = builder.start_array();
        builder.add_null();
        auto child = builder.start_object();
        child.add_key(string_ref("leaf"));
        builder.add_string(string_ref("value"));
        child.finish();
        array.finish();
        object.finish();
    });
}

OwnedBuilderValue make_nested_noncanonical_owned_value() {
    std::string metadata {char {0x01}, char {0x02}, char {0x00}, char {0x01},
                          char {0x02}, 'b',         'a'};
    const std::string object {
            char {static_cast<uint8_t>(VariantBasicType::OBJECT)},
            char {0x02},
            char {0x01},
            char {0x00},
            char {0x01},
            char {0x00},
            char {0x02},
            char {static_cast<uint8_t>(VariantPrimitiveId::FALSE_VALUE)
                  << VARIANT_VALUE_HEADER_SHIFT},
            char {static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE)
                  << VARIANT_VALUE_HEADER_SHIFT},
    };
    std::string value {char {static_cast<uint8_t>(VariantBasicType::ARRAY)}, char {0x01},
                       char {0x00}, static_cast<char>(object.size())};
    value.append(object);
    return {.metadata = std::move(metadata), .value = std::move(value)};
}

VariantValueRef required_field(VariantValueRef object, std::string_view key) {
    VariantValueRef result;
    EXPECT_TRUE(object.object_find(string_ref(key), &result));
    return result;
}

unsigned __int128 power_of_ten(uint8_t exponent) {
    unsigned __int128 value = 1;
    for (uint8_t index = 0; index < exponent; ++index) {
        value *= 10;
    }
    return value;
}

std::string numbered_key(uint32_t number) {
    return "key_" + std::to_string(1000 + number);
}

std::string decimal_bytes(VariantPrimitiveId id, __int128 unscaled, uint8_t width) {
    std::string encoded;
    encoded.push_back(static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT));
    encoded.push_back(0);
    const auto unsigned_value = static_cast<unsigned __int128>(unscaled);
    for (uint8_t byte = 0; byte < width; ++byte) {
        encoded.push_back(static_cast<char>(unsigned_value >> (byte * 8)));
    }
    return encoded;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- exhaustive scalar plan parity matrix.
TEST(VariantBuilderTest, ScalarEncodingPlanMatchesBuilder) {
    const auto expect_parity = [](const VariantScalarEncodingPlan& plan, auto&& add_value) {
        EXPECT_EQ(encode_plan(plan), encode_builder_scalar(add_value));
    };

    expect_parity(VariantScalarEncodingPlan::null_value(),
                  [](VariantBuilder& builder) { builder.add_null(); });
    expect_parity(VariantScalarEncodingPlan::boolean(false),
                  [](VariantBuilder& builder) { builder.add_bool(false); });
    expect_parity(VariantScalarEncodingPlan::boolean(true),
                  [](VariantBuilder& builder) { builder.add_bool(true); });
    for (const int64_t value : {int64_t {-129}, int64_t {-128}, int64_t {127}, int64_t {128},
                                int64_t {32768}, int64_t {1} << 40}) {
        expect_parity(VariantScalarEncodingPlan::integer(value),
                      [value](VariantBuilder& builder) { builder.add_int(value); });
    }
    const std::array<std::pair<int64_t, uint8_t>, 4> fixed_integers {
            std::pair {int64_t {-128}, uint8_t {1}},
            std::pair {int64_t {-32768}, uint8_t {2}},
            std::pair {int64_t {std::numeric_limits<int32_t>::min()}, uint8_t {4}},
            std::pair {std::numeric_limits<int64_t>::min(), uint8_t {8}},
    };
    for (const auto& value_and_width : fixed_integers) {
        const auto value = value_and_width.first;
        const auto width = value_and_width.second;
        expect_parity(VariantScalarEncodingPlan::integer(value, width),
                      [value](VariantBuilder& builder) { builder.add_int(value); });
    }
    const std::string empty_metadata("\x11\0\0", 3);
    const std::array<std::pair<uint8_t, VariantPrimitiveId>, 4> requested_integers {
            std::pair {uint8_t {1}, VariantPrimitiveId::INT8},
            std::pair {uint8_t {2}, VariantPrimitiveId::INT16},
            std::pair {uint8_t {4}, VariantPrimitiveId::INT32},
            std::pair {uint8_t {8}, VariantPrimitiveId::INT64},
    };
    for (const auto& [width, id] : requested_integers) {
        const std::string encoded = encode_plan(VariantScalarEncodingPlan::integer(1, width));
        const VariantValueRef decoded {
                .metadata = {.data = empty_metadata.data(), .size = empty_metadata.size()},
                .data = encoded.data(),
                .size = encoded.size()};
        EXPECT_EQ(decoded.primitive_id(), id);
        EXPECT_EQ(decoded.get_int(), 1);
        EXPECT_EQ(decoded.value_size(), static_cast<size_t>(width) + 1);
    }
    expect_parity(VariantScalarEncodingPlan::float32(-1.25F),
                  [](VariantBuilder& builder) { builder.add_float(-1.25F); });
    expect_parity(VariantScalarEncodingPlan::float64(123.5),
                  [](VariantBuilder& builder) { builder.add_double(123.5); });
    expect_parity(VariantScalarEncodingPlan::decimal(-123456789, 7),
                  [](VariantBuilder& builder) { builder.add_decimal(-123456789, 7); });
    expect_parity(
            VariantScalarEncodingPlan::decimal(-123456789012345678LL, 7, 8),
            [](VariantBuilder& builder) { builder.add_decimal(-123456789012345678LL, 7, 8); });
    const std::array<std::pair<__int128, uint8_t>, 3> fixed_decimals {
            std::pair {static_cast<__int128>(999'999'999), uint8_t {4}},
            std::pair {static_cast<__int128>(999'999'999'999'999'999LL), uint8_t {8}},
            std::pair {static_cast<__int128>(1'000'000'000'000'000'000LL), uint8_t {16}},
    };
    for (const auto& unscaled_and_width : fixed_decimals) {
        const auto unscaled = unscaled_and_width.first;
        const auto width = unscaled_and_width.second;
        expect_parity(VariantScalarEncodingPlan::decimal(unscaled, 3, width),
                      [unscaled, width](VariantBuilder& builder) {
                          builder.add_decimal(unscaled, 3, width);
                      });
    }
    const std::array<std::pair<uint8_t, VariantPrimitiveId>, 3> requested_decimals {
            std::pair {uint8_t {4}, VariantPrimitiveId::DECIMAL4},
            std::pair {uint8_t {8}, VariantPrimitiveId::DECIMAL8},
            std::pair {uint8_t {16}, VariantPrimitiveId::DECIMAL16},
    };
    for (const auto& [width, id] : requested_decimals) {
        const std::string encoded = encode_plan(VariantScalarEncodingPlan::decimal(1, 3, width));
        const VariantValueRef decoded {
                .metadata = {.data = empty_metadata.data(), .size = empty_metadata.size()},
                .data = encoded.data(),
                .size = encoded.size()};
        EXPECT_EQ(decoded.primitive_id(), id);
        EXPECT_EQ(decoded.get_decimal(), (VariantDecimal {1, 3, width}));
    }
    expect_parity(VariantScalarEncodingPlan::date(-20000),
                  [](VariantBuilder& builder) { builder.add_date(-20000); });
    expect_parity(VariantScalarEncodingPlan::timestamp_micros(-1234567890, true),
                  [](VariantBuilder& builder) { builder.add_timestamp_micros(-1234567890, true); });
    expect_parity(VariantScalarEncodingPlan::timestamp_micros(2234567890, false),
                  [](VariantBuilder& builder) { builder.add_timestamp_micros(2234567890, false); });
    expect_parity(VariantScalarEncodingPlan::timestamp_nanos(-3234567890, true),
                  [](VariantBuilder& builder) { builder.add_timestamp_nanos(-3234567890, true); });
    expect_parity(VariantScalarEncodingPlan::timestamp_nanos(4234567890, false),
                  [](VariantBuilder& builder) { builder.add_timestamp_nanos(4234567890, false); });
    expect_parity(VariantScalarEncodingPlan::time_ntz_micros(5234567890),
                  [](VariantBuilder& builder) { builder.add_time_ntz_micros(5234567890); });

    const std::string binary("\0\xFF\x01", 3);
    expect_parity(VariantScalarEncodingPlan::binary(StringRef(binary)),
                  [&binary](VariantBuilder& builder) { builder.add_binary(StringRef(binary)); });
    const std::string short_text(63, 's');
    const std::string long_text(64, 'L');
    expect_parity(
            VariantScalarEncodingPlan::string(StringRef(short_text)),
            [&short_text](VariantBuilder& builder) { builder.add_string(StringRef(short_text)); });
    expect_parity(
            VariantScalarEncodingPlan::string(StringRef(long_text)),
            [&long_text](VariantBuilder& builder) { builder.add_string(StringRef(long_text)); });

    std::array<uint8_t, 16> uuid {};
    for (uint8_t index = 0; index < uuid.size(); ++index) {
        uuid[index] = index;
    }
    expect_parity(VariantScalarEncodingPlan::uuid(uuid),
                  [&uuid](VariantBuilder& builder) { builder.add_uuid(uuid); });
    const auto decimal38 = static_cast<__int128>(power_of_ten(38) - 1);
    expect_parity(VariantScalarEncodingPlan::largeint(decimal38),
                  [decimal38](VariantBuilder& builder) { builder.add_largeint(decimal38); });
    const auto outside_decimal38 = static_cast<__int128>(power_of_ten(38));
    const VariantScalarEncodingPlan fallback =
            VariantScalarEncodingPlan::largeint(outside_decimal38);
    EXPECT_TRUE(fallback.used_string_fallback());
    expect_parity(fallback, [outside_decimal38](VariantBuilder& builder) {
        builder.add_largeint(outside_decimal38);
    });
    for (const __int128 value : {-outside_decimal38, std::numeric_limits<__int128>::min()}) {
        const VariantScalarEncodingPlan negative_fallback =
                VariantScalarEncodingPlan::largeint(value);
        EXPECT_TRUE(negative_fallback.used_string_fallback());
        expect_parity(negative_fallback,
                      [value](VariantBuilder& builder) { builder.add_largeint(value); });
    }

    std::string borrowed_short = "borrowed";
    const VariantScalarEncodingPlan borrowed_short_plan =
            VariantScalarEncodingPlan::string(StringRef(borrowed_short));
    borrowed_short.front() = 'B';
    EXPECT_EQ(encode_plan(borrowed_short_plan),
              encode_builder_scalar([&borrowed_short](VariantBuilder& builder) {
                  builder.add_string(StringRef(borrowed_short));
              }));
    std::string borrowed_long(64, 'x');
    const VariantScalarEncodingPlan borrowed_long_plan =
            VariantScalarEncodingPlan::string(StringRef(borrowed_long));
    borrowed_long.back() = 'y';
    EXPECT_EQ(encode_plan(borrowed_long_plan),
              encode_builder_scalar([&borrowed_long](VariantBuilder& builder) {
                  builder.add_string(StringRef(borrowed_long));
              }));

    const VariantScalarEncodingPlan integer = VariantScalarEncodingPlan::integer(1);
    std::array<char, 4> unchanged {'a', 'b', 'c', 'd'};
    EXPECT_THROW(integer.write(unchanged.data(), integer.size() - 1), Exception);
    EXPECT_EQ(unchanged, (std::array<char, 4> {'a', 'b', 'c', 'd'}));
    EXPECT_THROW(integer.write(nullptr, integer.size()), Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::integer(128, 1), Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::integer(1, 3), Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::decimal(1, 39), Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::decimal(1, 0, 3), Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::decimal(1'000'000'000, 0, 4), Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::decimal(
                         static_cast<__int128>(1'000'000'000'000'000'000LL), 0, 8),
                 Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::decimal(outside_decimal38, 0, 16), Exception);
    const std::string invalid_utf8("\xC3\x28", 2);
    EXPECT_THROW(VariantScalarEncodingPlan::string(StringRef(invalid_utf8)), Exception);
    const StringRef null_bytes(static_cast<const char*>(nullptr), 1);
    EXPECT_THROW(VariantScalarEncodingPlan::string(null_bytes), Exception);
    EXPECT_THROW(VariantScalarEncodingPlan::binary(null_bytes), Exception);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros expand assertions.
TEST(VariantBuilderTest, ScalarAndNestedValuesRoundTrip) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto root_scope = builder.start_object();

    root_scope.add_key(string_ref("z_null"));
    builder.add_null();
    root_scope.add_key(string_ref("bool"));
    builder.add_bool(true);
    root_scope.add_key(string_ref("int"));
    builder.add_int(-12345);
    root_scope.add_key(string_ref("float"));
    builder.add_float(-1.25F);
    root_scope.add_key(string_ref("double"));
    builder.add_double(123.5);
    root_scope.add_key(string_ref("decimal"));
    builder.add_decimal(-123456789012345678LL, 7);
    root_scope.add_key(string_ref("date"));
    builder.add_date(-20000);
    root_scope.add_key(string_ref("timestamp"));
    builder.add_timestamp_micros(-1234567890, true);
    root_scope.add_key(string_ref("timestamp_ntz"));
    builder.add_timestamp_micros(2234567890, false);
    root_scope.add_key(string_ref("timestamp_nanos"));
    builder.add_timestamp_nanos(-3234567890, true);
    root_scope.add_key(string_ref("timestamp_ntz_nanos"));
    builder.add_timestamp_nanos(4234567890, false);
    root_scope.add_key(string_ref("time"));
    builder.add_time_ntz_micros(5234567890);
    const std::string binary("\0\xFF\x01", 3);
    root_scope.add_key(string_ref("binary"));
    builder.add_binary(StringRef(binary));
    root_scope.add_key(string_ref("short"));
    builder.add_string(string_ref("short text"));
    const std::string long_text(64, 'L');
    root_scope.add_key(string_ref("long"));
    builder.add_string(StringRef(long_text));
    std::array<uint8_t, 16> uuid {};
    for (uint8_t index = 0; index < uuid.size(); ++index) {
        uuid[index] = index;
    }
    root_scope.add_key(string_ref("uuid"));
    builder.add_uuid(uuid);
    root_scope.add_key(string_ref("nested"));
    auto array_scope = builder.start_array();
    builder.add_int(7);
    builder.add_string(string_ref("array value"));
    auto nested_object_scope = builder.start_object();
    nested_object_scope.add_key(string_ref("inside"));
    builder.add_bool(false);
    nested_object_scope.finish();
    array_scope.finish();
    root_scope.finish();

    metadata.seal();
    const std::string encoded = finish_row(builder);
    const VariantValueRef root = value_ref(metadata, encoded);
    validate_canonical(root);

    EXPECT_TRUE(required_field(root, "z_null").is_null());
    EXPECT_TRUE(required_field(root, "bool").get_bool());
    EXPECT_EQ(required_field(root, "int").get_int(), -12345);
    EXPECT_EQ(required_field(root, "float").get_float(), -1.25F);
    EXPECT_EQ(required_field(root, "double").get_double(), 123.5);
    EXPECT_EQ(required_field(root, "decimal").get_decimal(),
              (VariantDecimal {-123456789012345678LL, 7, 8}));
    EXPECT_EQ(required_field(root, "date").get_date(), -20000);
    EXPECT_EQ(required_field(root, "timestamp").get_timestamp_micros(), -1234567890);
    EXPECT_EQ(required_field(root, "timestamp_ntz").get_timestamp_ntz_micros(), 2234567890);
    EXPECT_EQ(required_field(root, "timestamp_nanos").get_timestamp_nanos(), -3234567890);
    EXPECT_EQ(required_field(root, "timestamp_ntz_nanos").get_timestamp_ntz_nanos(), 4234567890);
    EXPECT_EQ(required_field(root, "time").get_time_ntz_micros(), 5234567890);
    EXPECT_EQ(required_field(root, "binary").get_binary(), StringRef(binary));
    EXPECT_EQ(required_field(root, "short").get_string(), string_ref("short text"));
    EXPECT_EQ(required_field(root, "long").get_string(), StringRef(long_text));
    EXPECT_EQ(required_field(root, "uuid").get_uuid(), uuid);

    const VariantValueRef nested = required_field(root, "nested");
    ASSERT_EQ(nested.num_elements(), 3);
    EXPECT_EQ(nested.array_at(0).get_int(), 7);
    EXPECT_EQ(nested.array_at(1).get_string(), string_ref("array value"));
    EXPECT_FALSE(required_field(nested.array_at(2), "inside").get_bool());
}

TEST(VariantBuilderTest, DictionaryRemapUsesUnsignedUtf8Ordering) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    const std::array<std::string_view, 4> keys {"\xC3\xBF", "z", "\xC2\x80", "a"};
    for (int64_t index = 0; index < keys.size(); ++index) {
        object_scope.add_key(string_ref(keys[index]));
        builder.add_int(index);
    }
    object_scope.finish();

    metadata.seal();
    const std::string encoded = finish_row(builder);
    const VariantValueRef object = value_ref(metadata, encoded);
    validate_canonical(object);

    const std::array<std::string_view, 4> sorted_keys {"a", "z", "\xC2\x80", "\xC3\xBF"};
    const std::array<int64_t, 4> expected_values {3, 1, 2, 0};
    for (uint32_t final_id = 0; final_id < sorted_keys.size(); ++final_id) {
        EXPECT_EQ(metadata.metadata_ref().key_at(final_id), string_ref(sorted_keys[final_id]));
        uint32_t decoded_id = std::numeric_limits<uint32_t>::max();
        EXPECT_EQ(object.object_value_at(final_id, &decoded_id).get_int(),
                  expected_values[final_id]);
        EXPECT_EQ(decoded_id, final_id);
    }
}

void expect_reverse_object_is_canonical(uint32_t count) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    for (uint32_t index = count; index != 0; --index) {
        const std::string key = numbered_key(index - 1);
        object_scope.add_key(StringRef(key));
        builder.add_int(index - 1);
    }
    object_scope.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef object = value_ref(metadata, encoded);
    validate_canonical(object);
    for (uint32_t index = 0; index < count; ++index) {
        EXPECT_EQ(required_field(object, numbered_key(index)).get_int(), index);
    }
}

TEST(VariantBuilderTest, ObjectPlanningSortsAcrossSmallObjectThreshold) {
    // Pass 2 uses insertion sort through 16 fields and std::sort above that threshold.
    expect_reverse_object_is_canonical(16);
    expect_reverse_object_is_canonical(17);
}

TEST(VariantBuilderTest, BlockRowsShareOneMetadataDictionary) {
    VariantMetadataBuilder metadata;
    VariantBuilder first(metadata);
    VariantBuilder second(metadata);
    VariantBuilder third(metadata);

    auto first_object = first.start_object();
    first_object.add_key(string_ref("z"));
    first.add_int(1);
    first_object.finish();

    auto second_object = second.start_object();
    second_object.add_key(string_ref("a"));
    second.add_int(2);
    second_object.add_key(string_ref("z"));
    second.add_int(3);
    second_object.finish();

    third.add_string(string_ref("scalar row"));
    metadata.seal();

    const std::string first_value = finish_row(first);
    const std::string second_value = finish_row(second);
    const std::string third_value = finish_row(third);
    const std::vector<VariantValueRef> rows {value_ref(metadata, first_value),
                                             value_ref(metadata, second_value),
                                             value_ref(metadata, third_value)};
    validate_canonical(metadata.metadata_ref(), rows);

    ASSERT_EQ(metadata.metadata_ref().dict_size(), 2);
    EXPECT_EQ(metadata.metadata_ref().key_at(0), string_ref("a"));
    EXPECT_EQ(metadata.metadata_ref().key_at(1), string_ref("z"));
    EXPECT_EQ(required_field(rows[0], "z").get_int(), 1);
    EXPECT_EQ(required_field(rows[1], "a").get_int(), 2);
    EXPECT_EQ(required_field(rows[1], "z").get_int(), 3);
    EXPECT_EQ(rows[2].get_string(), string_ref("scalar row"));
    EXPECT_EQ(rows[0].metadata.data, rows[1].metadata.data);
    EXPECT_EQ(rows[1].metadata.data, rows[2].metadata.data);
}

TEST(VariantBuilderTest, SingleRowDictionaryContainsOnlyReferencedKeys) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    object_scope.add_key(string_ref("only"));
    builder.add_bool(true);
    object_scope.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    EXPECT_EQ(metadata.metadata_ref().dict_size(), 1);
    EXPECT_EQ(metadata.metadata_ref().key_at(0), string_ref("only"));
}

TEST(VariantBuilderTest, AbortedRowsDoNotPolluteSharedMetadata) {
    VariantMetadataBuilder metadata;
    VariantBuilder discarded(metadata);
    VariantBuilder retained(metadata);

    auto discarded_object = discarded.start_object();
    discarded_object.add_key(string_ref("discarded_only"));
    discarded.add_null();
    discarded_object.add_key(string_ref("shared"));
    discarded.add_null();
    discarded_object.finish();

    auto retained_object = retained.start_object();
    retained_object.add_key(string_ref("retained_only"));
    retained.add_int(1);
    retained_object.add_key(string_ref("shared"));
    retained.add_int(2);
    retained_object.finish();

    discarded.abort();
    metadata.seal();
    EXPECT_THROW(discarded.abort(), Exception);

    const std::string encoded = finish_row(retained);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    ASSERT_EQ(metadata.metadata_ref().dict_size(), 2);
    EXPECT_EQ(metadata.metadata_ref().key_at(0), string_ref("retained_only"));
    EXPECT_EQ(metadata.metadata_ref().key_at(1), string_ref("shared"));
}

TEST(VariantBuilderTest, DestroyedRowsReleaseCompletedAndIncompleteKeys) {
    VariantMetadataBuilder metadata;
    {
        VariantBuilder completed(metadata);
        auto object_scope = completed.start_object();
        object_scope.add_key(string_ref("completed_but_discarded"));
        completed.add_null();
        object_scope.finish();
    }
    {
        VariantBuilder incomplete(metadata);
        auto object_scope = incomplete.start_object();
        object_scope.add_key(string_ref("incomplete_and_discarded"));
    }

    VariantBuilder retained(metadata);
    auto retained_object = retained.start_object();
    retained_object.add_key(string_ref("retained"));
    retained.add_null();
    retained_object.finish();
    metadata.seal();

    const std::string encoded = finish_row(retained);
    validate_canonical(value_ref(metadata, encoded));
    ASSERT_EQ(metadata.metadata_ref().dict_size(), 1);
    EXPECT_EQ(metadata.metadata_ref().key_at(0), string_ref("retained"));
}

TEST(VariantBuilderTest, IncompleteRowsBlockSealUntilAborted) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    object_scope.add_key(string_ref("pending"));
    EXPECT_THROW(metadata.seal(), Exception);
    builder.abort();
    metadata.seal();
    EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    EXPECT_THROW(finish_row(builder), Exception);
}

TEST(VariantBuilderTest, IntegerAndDecimalWidthsAreMinimal) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto array_scope = builder.start_array();
    const std::array<int64_t, 12> integers {
            std::numeric_limits<int8_t>::min() - 1LL,
            std::numeric_limits<int8_t>::min(),
            std::numeric_limits<int8_t>::max(),
            std::numeric_limits<int8_t>::max() + 1LL,
            std::numeric_limits<int16_t>::min() - 1LL,
            std::numeric_limits<int16_t>::min(),
            std::numeric_limits<int16_t>::max(),
            std::numeric_limits<int16_t>::max() + 1LL,
            static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1,
            std::numeric_limits<int32_t>::min(),
            std::numeric_limits<int32_t>::max(),
            static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1,
    };
    for (int64_t value : integers) {
        builder.add_int(value);
    }
    const std::array<__int128, 9> decimals {
            0,
            999'999'999,
            1'000'000'000,
            -999'999'999,
            -1'000'000'000,
            static_cast<__int128>(999'999'999'999'999'999),
            static_cast<__int128>(1'000'000'000'000'000'000),
            -static_cast<__int128>(999'999'999'999'999'999),
            -static_cast<__int128>(1'000'000'000'000'000'000),
    };
    for (__int128 value : decimals) {
        builder.add_decimal(value, 1);
    }
    builder.add_decimal(static_cast<__int128>(power_of_ten(38) - 1), 38);
    array_scope.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    for (uint32_t index = 0; index < integers.size(); ++index) {
        EXPECT_EQ(value.array_at(index).get_int(), integers[index]);
    }
    const std::array<uint8_t, 9> expected_widths {4, 4, 8, 4, 8, 8, 16, 8, 16};
    for (uint32_t index = 0; index < decimals.size(); ++index) {
        const VariantDecimal decimal = value.array_at(integers.size() + index).get_decimal();
        EXPECT_EQ(decimal.unscaled, decimals[index]);
        EXPECT_EQ(decimal.width, expected_widths[index]);
    }
    EXPECT_EQ(value.array_at(integers.size() + decimals.size()).get_decimal().width, 16);
}

TEST(VariantBuilderTest, DecimalValidationAndLargeIntFallback) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    EXPECT_THROW(builder.add_decimal(1, 39), Exception);
    EXPECT_THROW(builder.add_decimal(static_cast<__int128>(power_of_ten(38)), 0), Exception);

    auto array_scope = builder.start_array();
    builder.add_decimal(1, 0);
    builder.add_largeint(42);
    builder.add_largeint(std::numeric_limits<__int128>::max());
    builder.add_largeint(std::numeric_limits<__int128>::min());
    array_scope.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    EXPECT_EQ(value.array_at(0).get_decimal(), (VariantDecimal {1, 0, 4}));
    EXPECT_EQ(value.array_at(1).get_decimal(), (VariantDecimal {42, 0, 16}));
    EXPECT_EQ(value.array_at(2).get_string(),
              string_ref("170141183460469231731687303715884105727"));
    EXPECT_EQ(value.array_at(3).get_string(),
              string_ref("-170141183460469231731687303715884105728"));
}

TEST(VariantBuilderTest,              // NOLINT(readability-function-cognitive-complexity) --
     ExplicitDecimalPhysicalWidths) { // GoogleTest assertion macros inflate the metric.
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto array = builder.start_array();
    builder.add_decimal(1, 38, 4);
    builder.add_decimal(1, 38, 8);
    builder.add_decimal(1, 38, 16);
    array.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    EXPECT_EQ(value.array_at(0).get_decimal(), (VariantDecimal {1, 38, 4}));
    EXPECT_EQ(value.array_at(1).get_decimal(), (VariantDecimal {1, 38, 8}));
    EXPECT_EQ(value.array_at(2).get_decimal(), (VariantDecimal {1, 38, 16}));

    for (uint8_t invalid_width : {uint8_t {0}, uint8_t {1}, uint8_t {5}, uint8_t {17}}) {
        VariantMetadataBuilder invalid_metadata;
        VariantBuilder invalid(invalid_metadata);
        EXPECT_THROW(invalid.add_decimal(1, 0, invalid_width), Exception);
    }

    VariantMetadataBuilder decimal4_metadata;
    VariantBuilder decimal4(decimal4_metadata);
    EXPECT_THROW(decimal4.add_decimal(1'000'000'000, 0, 4), Exception);
    VariantMetadataBuilder decimal8_metadata;
    VariantBuilder decimal8(decimal8_metadata);
    EXPECT_THROW(decimal8.add_decimal(static_cast<__int128>(1'000'000'000'000'000'000), 0, 8),
                 Exception);
}

TEST(VariantBuilderTest, StringLengthBoundaryUsesShortAndLongForms) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto array_scope = builder.start_array();
    const std::string short_text(63, 's');
    const std::string long_text(64, 'l');
    builder.add_string(StringRef(short_text));
    builder.add_string(StringRef(long_text));
    array_scope.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    EXPECT_EQ(value.array_at(0).basic_type(), VariantBasicType::SHORT_STRING);
    EXPECT_EQ(value.array_at(1).primitive_id(), VariantPrimitiveId::STRING);
    EXPECT_EQ(value.array_at(0).get_string(), StringRef(short_text));
    EXPECT_EQ(value.array_at(1).get_string(), StringRef(long_text));
}

TEST(VariantBuilderTest, Utf8ValidationPreservesBuilderStateAndBinaryIsExempt) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    const std::string invalid_key("\xC0\xAF", 2);
    EXPECT_THROW(object_scope.add_key(StringRef(invalid_key)), Exception);

    object_scope.add_key(string_ref("text"));
    const std::string invalid_short("\xE2\x28\xA1", 3);
    EXPECT_THROW(builder.add_string(StringRef(invalid_short)), Exception);
    std::string invalid_long(64, 'x');
    invalid_long.back() = static_cast<char>(0xFF);
    EXPECT_THROW(builder.add_string(StringRef(invalid_long)), Exception);
    const std::string valid_multibyte("\xE4\xB8\xAD", 3);
    builder.add_string(StringRef(valid_multibyte));

    object_scope.add_key(string_ref("\xE9\x94\xAE"));
    const std::string non_utf8_binary("\xFF\xC0\xAF", 3);
    builder.add_binary(StringRef(non_utf8_binary));
    object_scope.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    EXPECT_EQ(required_field(value, "text").get_string(), StringRef(valid_multibyte));
    EXPECT_EQ(required_field(value, "\xE9\x94\xAE").get_binary(), StringRef(non_utf8_binary));
    ASSERT_EQ(metadata.metadata_ref().dict_size(), 2);
}

TEST(VariantBuilderTest, Utf8ValidationAcceptsLongMultibyteStrings) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    std::string value;
    for (uint8_t index = 0; index < 22; ++index) {
        value.append("\xE4\xB8\xAD", 3);
    }
    builder.add_string(StringRef(value));
    metadata.seal();

    const std::string encoded = finish_row(builder);
    validate_canonical(value_ref(metadata, encoded));
    EXPECT_EQ(value_ref(metadata, encoded).get_string(), StringRef(value));
    EXPECT_EQ(value_ref(metadata, encoded).primitive_id(), VariantPrimitiveId::STRING);
}

std::string build_array_with_nulls(uint32_t count, uint8_t* value_header_out) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto array_scope = builder.start_array();
    for (uint32_t index = 0; index < count; ++index) {
        builder.add_null();
    }
    array_scope.finish();
    metadata.seal();
    std::string encoded = finish_row(builder);
    validate_canonical(value_ref(metadata, encoded));
    *value_header_out = static_cast<uint8_t>(encoded[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    EXPECT_EQ(value_ref(metadata, encoded).num_elements(), count);
    return encoded;
}

TEST(VariantBuilderTest, CountAndOffsetWidthsCrossAt255And256) {
    uint8_t small_header = 0;
    uint8_t large_header = 0;
    const std::string small = build_array_with_nulls(255, &small_header);
    const std::string large = build_array_with_nulls(256, &large_header);
    EXPECT_EQ(small_header & VARIANT_ARRAY_LARGE_MASK, 0);
    EXPECT_NE(large_header & VARIANT_ARRAY_LARGE_MASK, 0);
    EXPECT_EQ(((small_header >> VARIANT_ARRAY_OFFSET_SIZE_SHIFT) & 0x03U) + 1, 1);
    EXPECT_EQ(((large_header >> VARIANT_ARRAY_OFFSET_SIZE_SHIFT) & 0x03U) + 1, 2);
    EXPECT_LT(small.size(), large.size());
}

struct ObjectBoundaryResult {
    uint8_t id_width;
    uint8_t offset_width;
    bool is_large;
};

ObjectBoundaryResult build_object_boundary(uint32_t count) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    for (uint32_t index = 0; index < count; ++index) {
        const std::string key = numbered_key(index);
        object_scope.add_key(StringRef(key));
        builder.add_null();
    }
    object_scope.finish();
    metadata.seal();
    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    EXPECT_EQ(value.num_elements(), count);

    const uint8_t header = static_cast<uint8_t>(encoded[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    return {.id_width =
                    static_cast<uint8_t>(((header >> VARIANT_OBJECT_ID_SIZE_SHIFT) & 0x03U) + 1),
            .offset_width = static_cast<uint8_t>(
                    ((header >> VARIANT_OBJECT_OFFSET_SIZE_SHIFT) & 0x03U) + 1),
            .is_large = (header & VARIANT_OBJECT_LARGE_MASK) != 0};
}

TEST(VariantBuilderTest, ObjectIdWidthCrossesAfterFinalId255) {
    const ObjectBoundaryResult count255 = build_object_boundary(255);
    const ObjectBoundaryResult count256 = build_object_boundary(256);
    const ObjectBoundaryResult count257 = build_object_boundary(257);
    EXPECT_EQ(count255.id_width, 1);
    EXPECT_EQ(count255.offset_width, 1);
    EXPECT_FALSE(count255.is_large);
    EXPECT_EQ(count256.id_width, 1);
    EXPECT_EQ(count256.offset_width, 2);
    EXPECT_TRUE(count256.is_large);
    EXPECT_EQ(count257.id_width, 2);
    EXPECT_EQ(count257.offset_width, 2);
    EXPECT_TRUE(count257.is_large);
}

uint8_t build_metadata_with_key_size(size_t key_size) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    const std::string key(key_size, 'k');
    object_scope.add_key(StringRef(key));
    builder.add_null();
    object_scope.finish();
    metadata.seal();
    const std::string encoded = finish_row(builder);
    validate_canonical(value_ref(metadata, encoded));
    return metadata.metadata_ref().offset_size();
}

TEST(VariantBuilderTest, MetadataOffsetWidthCrossesAt255And256Bytes) {
    EXPECT_EQ(build_metadata_with_key_size(255), 1);
    EXPECT_EQ(build_metadata_with_key_size(256), 2);
}

TEST(VariantBuilderTest, CanonicalValidatorRejectsIndependentNonCanonicalBytes) {
    VariantMetadataBuilder empty_metadata;
    empty_metadata.seal();

    std::string wide_integer;
    wide_integer.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT32)
                                             << VARIANT_VALUE_HEADER_SHIFT));
    wide_integer.push_back(5);
    wide_integer.append(3, '\0');
    EXPECT_THROW(validate_canonical(value_ref(empty_metadata, wide_integer)), Exception);

    std::string long_form_for_short_string;
    long_form_for_short_string.push_back(static_cast<char>(
            static_cast<uint8_t>(VariantPrimitiveId::STRING) << VARIANT_VALUE_HEADER_SHIFT));
    long_form_for_short_string.push_back(1);
    long_form_for_short_string.append(3, '\0');
    long_form_for_short_string.push_back('x');
    EXPECT_THROW(validate_canonical(value_ref(empty_metadata, long_form_for_short_string)),
                 Exception);

    VariantMetadataBuilder object_metadata;
    VariantBuilder object_builder(object_metadata);
    auto object_scope = object_builder.start_object();
    object_scope.add_key(string_ref("a"));
    object_builder.add_null();
    object_scope.add_key(string_ref("b"));
    object_builder.add_null();
    object_scope.finish();
    object_metadata.seal();
    const std::string canonical_object = finish_row(object_builder);
    validate_canonical(value_ref(object_metadata, canonical_object));
    std::string reversed_ids = canonical_object;
    std::swap(reversed_ids[2], reversed_ids[3]);
    EXPECT_THROW(validate_canonical(value_ref(object_metadata, reversed_ids)), Exception);

    VariantMetadataBuilder unused_metadata;
    unused_metadata.register_key(string_ref("unused"));
    VariantBuilder scalar_builder(unused_metadata);
    scalar_builder.add_null();
    unused_metadata.seal();
    const std::string scalar = finish_row(scalar_builder);
    validate_canonical(value_ref(unused_metadata, scalar));
    EXPECT_EQ(unused_metadata.metadata_ref().dict_size(), 0);
}

TEST(VariantBuilderTest, CanonicalValidatorChecksDecimalPrecisionAndUtf8Independently) {
    VariantMetadataBuilder empty_metadata;
    empty_metadata.seal();

    const std::string undersized_decimal4 =
            decimal_bytes(VariantPrimitiveId::DECIMAL4, 1'000'000'000, 4);
    EXPECT_THROW(validate_canonical(value_ref(empty_metadata, undersized_decimal4)), Exception);
    const std::string undersized_decimal8 = decimal_bytes(
            VariantPrimitiveId::DECIMAL8, static_cast<__int128>(1'000'000'000'000'000'000), 8);
    EXPECT_THROW(validate_canonical(value_ref(empty_metadata, undersized_decimal8)), Exception);
    const std::string valid_wide_decimal = decimal_bytes(VariantPrimitiveId::DECIMAL16, 42, 16);
    validate_canonical(value_ref(empty_metadata, valid_wide_decimal));

    std::string invalid_short;
    invalid_short.push_back(
            static_cast<char>((1 << VARIANT_VALUE_HEADER_SHIFT) |
                              static_cast<uint8_t>(VariantBasicType::SHORT_STRING)));
    invalid_short.push_back(static_cast<char>(0xFF));
    EXPECT_THROW(validate_canonical(value_ref(empty_metadata, invalid_short)), Exception);

    std::string invalid_long;
    invalid_long.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::STRING)
                                             << VARIANT_VALUE_HEADER_SHIFT));
    invalid_long.push_back(64);
    invalid_long.append(3, '\0');
    invalid_long.append(63, 'x');
    invalid_long.push_back(static_cast<char>(0xFF));
    EXPECT_THROW(validate_canonical(value_ref(empty_metadata, invalid_long)), Exception);

    std::string invalid_binary;
    invalid_binary.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::BINARY)
                                               << VARIANT_VALUE_HEADER_SHIFT));
    invalid_binary.push_back(1);
    invalid_binary.append(3, '\0');
    invalid_binary.push_back(static_cast<char>(0xFF));
    validate_canonical(value_ref(empty_metadata, invalid_binary));

    std::string invalid_metadata {
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK),
            1,
            0,
            1,
            static_cast<char>(0xFF),
    };
    const std::string object_with_invalid_key {
            static_cast<char>(VariantBasicType::OBJECT), 1, 0, 0, 1, 0};
    const VariantMetadataRef metadata_ref {.data = invalid_metadata.data(),
                                           .size = invalid_metadata.size()};
    const VariantValueRef row {.metadata = metadata_ref,
                               .data = object_with_invalid_key.data(),
                               .size = object_with_invalid_key.size()};
    EXPECT_THROW(validate_canonical(row), Exception);
}

TEST(VariantBuilderTest, FinishBeforeMetadataSealDoesNotChangeDestination) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    builder.add_null();

    std::string destination = "prefix";
    EXPECT_THROW(builder.finish_row(destination), Exception);
    EXPECT_EQ(destination, "prefix");
}

TEST(VariantBuilderTest, DuplicateKeysFailBeforeSealAndBadRowCanBeDiscarded) {
    VariantMetadataBuilder metadata;
    VariantBuilder bad(metadata);
    auto bad_object = bad.start_object();
    bad_object.add_key(string_ref("bad_only"));
    bad.add_null();
    bad_object.add_key(string_ref("shared"));
    bad.add_int(1);
    bad_object.add_key(string_ref("shared"));
    bad.add_int(2);
    EXPECT_THROW(bad_object.finish(), Exception);
    bad.abort();

    VariantBuilder good(metadata);
    auto good_object = good.start_object();
    good_object.add_key(string_ref("shared"));
    good.add_int(3);
    good_object.add_key(string_ref("good_only"));
    good.add_bool(true);
    good_object.finish();
    metadata.seal();

    const std::string encoded = finish_row(good);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    ASSERT_EQ(metadata.metadata_ref().dict_size(), 2);
    EXPECT_EQ(metadata.metadata_ref().key_at(0), string_ref("good_only"));
    EXPECT_EQ(metadata.metadata_ref().key_at(1), string_ref("shared"));
    EXPECT_EQ(required_field(value, "shared").get_int(), 3);
    EXPECT_TRUE(required_field(value, "good_only").get_bool());
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros expand assertions.
TEST(VariantBuilderTest, ScopeAndFinishMisuseIsRejectedWithoutStateCorruption) {
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        builder.add_null();
        std::string output = "prefix";
        EXPECT_THROW(builder.finish_row(output), Exception);
        EXPECT_EQ(output, "prefix");
        metadata.seal();
        builder.finish_row(output);
        EXPECT_EQ(output.size(), 7);
        const std::string encoded = output.substr(6);
        validate_canonical(value_ref(metadata, encoded));
        EXPECT_THROW(builder.finish_row(output), Exception);
    }
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        auto object_scope = builder.start_object();
        object_scope.add_key(string_ref("a"));
        builder.add_null();
        object_scope.add_key(string_ref("b"));
        EXPECT_THROW(object_scope.add_key(string_ref("missing_value")), Exception);
        EXPECT_THROW(object_scope.finish(), Exception);
        builder.add_bool(true);
        object_scope.finish();
        EXPECT_THROW(object_scope.finish(), Exception);
        metadata.seal();
        const std::string encoded = finish_row(builder);
        validate_canonical(value_ref(metadata, encoded));
    }
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        auto object_scope = builder.start_object();
        object_scope.add_key(string_ref("array"));
        auto array_scope = builder.start_array();
        builder.add_null();
        EXPECT_THROW(object_scope.finish(), Exception);
        EXPECT_THROW(object_scope.add_key(string_ref("later")), Exception);
        array_scope.finish();
        object_scope.add_key(string_ref("later"));
        builder.add_int(1);
        object_scope.finish();
        metadata.seal();
        const std::string encoded = finish_row(builder);
        validate_canonical(value_ref(metadata, encoded));
    }
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        auto array_scope = builder.start_array();
        builder.add_null();
        EXPECT_THROW(metadata.seal(), Exception);
        std::string output;
        EXPECT_THROW(builder.finish_row(output), Exception);
        EXPECT_TRUE(output.empty());
        array_scope.finish();
        EXPECT_THROW(builder.add_null(), Exception);
        metadata.seal();
        builder.finish_row(output);
        validate_canonical(value_ref(metadata, output));
        EXPECT_THROW(metadata.seal(), Exception);
    }
    {
        VariantMetadataBuilder metadata;
        EXPECT_THROW(metadata.encoded_metadata(), Exception);
        EXPECT_THROW(metadata.final_id(0), Exception);
        metadata.seal();
        EXPECT_THROW(metadata.register_key(string_ref("late")), Exception);
        EXPECT_THROW({ VariantBuilder late_builder(metadata); }, Exception);
        EXPECT_THROW(metadata.seal(), Exception);
    }
}

TEST(VariantBlockBuilderTest, SharedMetadataCoversMultipleRowsAndNestedContainers) {
    VariantBlockBuilder builder({.rows = 3,
                                 .metadata_keys = 3,
                                 .scalar_bytes = 32,
                                 .nodes = 32,
                                 .containers = 16,
                                 .children = 32});
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(string_ref("z"));
        row.add_int(7);
        object.add_key(string_ref("nested"));
        std::vector<VariantBlockBuilder::Row::ArrayScope> arrays;
        for (uint32_t depth = 0; depth < 8; ++depth) {
            arrays.emplace_back(row.start_array());
        }
        row.add_null();
        for (auto& array : std::ranges::reverse_view(arrays)) {
            array.finish();
        }
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_bool(true);
        auto object = row.start_object();
        object.add_key(string_ref("a"));
        row.add_string(string_ref("value"));
        object.finish();
        array.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_string(string_ref("scalar"));
        row.finish();
    }

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 3);
    ASSERT_EQ(block.metadata_ref().dict_size(), 3);
    EXPECT_EQ(block.metadata_ref().key_at(0), string_ref("a"));
    EXPECT_EQ(block.metadata_ref().key_at(1), string_ref("nested"));
    EXPECT_EQ(block.metadata_ref().key_at(2), string_ref("z"));
    EXPECT_EQ(block.value_at(0).metadata.data, block.value_at(1).metadata.data);
    EXPECT_EQ(block.value_at(1).metadata.data, block.value_at(2).metadata.data);
    std::vector<VariantValueRef> rows;
    rows.reserve(block.num_rows());
    for (size_t index = 0; index < block.num_rows(); ++index) {
        rows.push_back(block.value_at(index));
    }
    validate_canonical(block.metadata_ref(), rows);

    VariantValueRef nested = required_field(block.value_at(0), "nested");
    for (uint32_t depth = 0; depth < 8; ++depth) {
        ASSERT_EQ(nested.num_elements(), 1);
        nested = nested.array_at(0);
    }
    EXPECT_TRUE(nested.is_null());
}

TEST(VariantBlockBuilderTest, EnforcesSingleActiveRowAndTerminalBlockState) {
    VariantBlockBuilder builder;
    auto row = builder.begin_row();
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT,
                                  [&] { static_cast<void>(builder.begin_row()); });
    row.add_null();
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT,
                                  [&] { static_cast<void>(builder.finish_block()); });
    row.finish();
    EXPECT_TRUE(row.is_finished());

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 1);
    EXPECT_TRUE(block.value_at(0).is_null());
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT,
                                  [&] { static_cast<void>(builder.begin_row()); });
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT,
                                  [&] { static_cast<void>(builder.finish_block()); });
}

TEST(VariantBlockBuilderTest, MovedFromRowCannotMutateTheActiveRow) {
    VariantBlockBuilder builder;
    auto moved_from = builder.begin_row();
    auto active = std::move(moved_from);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- Negative API contract.
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        // NOLINTNEXTLINE(clang-analyzer-cplusplus.Move) -- Negative API contract.
        moved_from.add_null();
    });
    active.add_int(1);
    active.finish();

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 1);
    EXPECT_EQ(block.value_at(0).get_int(), 1);
}

TEST(VariantBlockBuilderTest, MovedFromScopeCannotMutateTheActiveScope) {
    VariantBlockBuilder builder;
    auto object_row = builder.begin_row();
    auto moved_from = object_row.start_object();
    auto active_object = std::move(moved_from);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- Negative API contract.
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        // NOLINTNEXTLINE(clang-analyzer-cplusplus.Move) -- Negative API contract.
        moved_from.add_key(string_ref("stale"));
    });
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] { moved_from.finish(); });
    active_object.add_key(string_ref("active"));
    object_row.add_bool(true);
    active_object.finish();
    object_row.finish();

    auto array_row = builder.begin_row();
    auto moved_array = array_row.start_array();
    auto active_array = std::move(moved_array);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- Negative API contract.
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        // NOLINTNEXTLINE(clang-analyzer-cplusplus.Move) -- Negative API contract.
        moved_array.finish();
    });
    array_row.add_null();
    active_array.finish();
    array_row.finish();

    VariantEncodedBlock block = builder.finish_block();
    const std::vector<VariantValueRef> rows {block.value_at(0), block.value_at(1)};
    validate_canonical(block.metadata_ref(), rows);
    EXPECT_TRUE(required_field(block.value_at(0), "active").get_bool());
    ASSERT_EQ(block.value_at(1).num_elements(), 1);
    EXPECT_TRUE(block.value_at(1).array_at(0).is_null());
}

TEST(VariantBlockBuilderTest, RowMoveKeepsActiveScopesUsableAndScopesRejectStaleGeneration) {
    VariantBlockBuilder builder;
    auto row = builder.begin_row();
    auto object = row.start_object();
    object.add_key(string_ref("nested"));
    auto array = row.start_array();

    auto moved_row = std::move(row);
    // NOLINTNEXTLINE(bugprone-use-after-move) -- Negative API contract.
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        // NOLINTNEXTLINE(clang-analyzer-cplusplus.Move) -- Negative API contract.
        row.add_null();
    });
    moved_row.add_int(17);
    array.finish();
    object.finish();
    moved_row.finish();

    auto aborted = builder.begin_row();
    auto stale_scope = aborted.start_object();
    aborted.abort();
    auto current = builder.begin_row();
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT,
                                  [&] { stale_scope.add_key(string_ref("stale")); });
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] { stale_scope.finish(); });
    current.add_null();
    current.finish();

    VariantEncodedBlock block = builder.finish_block();
    const std::vector<VariantValueRef> rows {block.value_at(0), block.value_at(1)};
    validate_canonical(block.metadata_ref(), rows);
    const VariantValueRef nested = required_field(block.value_at(0), "nested");
    ASSERT_EQ(nested.num_elements(), 1);
    EXPECT_EQ(nested.array_at(0).get_int(), 17);
    EXPECT_TRUE(block.value_at(1).is_null());
}

TEST(VariantBlockBuilderTest, OldFinishedAndAbortedRowsCannotCrossGenerations) {
    VariantBlockBuilder builder;
    auto finished = builder.begin_row();
    finished.add_null();
    finished.finish();

    auto aborted = builder.begin_row();
    aborted.abort();

    auto current = builder.begin_row();
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] { finished.add_bool(true); });
    expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] { aborted.add_int(7); });
    current.add_string(string_ref("current"));
    current.finish();

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 2);
    const std::vector<VariantValueRef> rows {block.value_at(0), block.value_at(1)};
    validate_canonical(block.metadata_ref(), rows);
    EXPECT_TRUE(block.value_at(0).is_null());
    EXPECT_EQ(block.value_at(1).get_string(), string_ref("current"));
}

TEST(VariantBlockBuilderTest, EmptyBlockHasMinimalMetadataAndOnlyZeroOffset) {
    VariantBlockBuilder builder;
    VariantEncodedBlock block = builder.finish_block();
    const std::string expected_metadata("\x11\0\0", 3);
    EXPECT_EQ(std::string(block.metadata_ref().data, block.metadata_ref().size), expected_metadata);
    EXPECT_EQ(block.value_bytes().size, 0);
    ASSERT_EQ(block.value_offsets().size(), 1);
    EXPECT_EQ(block.value_offsets().front(), 0);
    EXPECT_EQ(block.num_rows(), 0);
    EXPECT_NO_THROW(block.metadata_ref().validate());
}

#ifdef BE_TEST
TEST(VariantBlockBuilderTest, SmallCanonicalScalarsStayInlineWithoutArena) {
    VariantBlockBuilder builder;
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_null();
        row.add_bool(true);
        row.add_int(-128);
        row.add_int(128);
        row.add_string(string_ref("abc"));
        array.finish();
        row.finish();
    }

    EXPECT_EQ(builder.test_counters().scalar_byte_capacity, 0);

    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_null();
        row.add_int(1);
        array.finish();
        row.abort();
    }
    EXPECT_EQ(builder.test_counters().scalar_byte_capacity, 0);

    {
        auto row = builder.begin_row();
        row.add_string(string_ref("abcd"));
        row.finish();
    }
    EXPECT_GT(builder.test_counters().scalar_byte_capacity, 0);

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 2);
    const VariantValueRef value = block.value_at(0);
    ASSERT_EQ(value.basic_type(), VariantBasicType::ARRAY);
    ASSERT_EQ(value.num_elements(), 5);
    EXPECT_TRUE(value.array_at(0).is_null());
    EXPECT_TRUE(value.array_at(1).get_bool());
    EXPECT_EQ(value.array_at(2).get_int(), -128);
    EXPECT_EQ(value.array_at(3).get_int(), 128);
    EXPECT_EQ(value.array_at(4).get_string(), string_ref("abc"));
    validate_canonical(value);
    EXPECT_EQ(block.value_at(1).get_string(), string_ref("abcd"));
    validate_canonical(block.value_at(1));
}

TEST(VariantBlockBuilderTest, InlineScalarPathsMatchPlanFactoryAndLegacyBytes) {
    const OwnedBuilderValue legacy = build_owned_value([](VariantBuilder& builder) {
        auto array = builder.start_array();
        builder.add_null();
        builder.add_bool(true);
        builder.add_int(-128);
        builder.add_int(128);
        builder.add_string(string_ref("abc"));
        array.finish();
    });
    const VariantValueRef legacy_value = legacy.ref();
    const std::array<std::string, 5> plan_bytes {
            encode_plan(VariantScalarEncodingPlan::null_value()),
            encode_plan(VariantScalarEncodingPlan::boolean(true)),
            encode_plan(VariantScalarEncodingPlan::integer(-128)),
            encode_plan(VariantScalarEncodingPlan::integer(128)),
            encode_plan(VariantScalarEncodingPlan::string(string_ref("abc"))),
    };
    for (size_t index = 0; index < plan_bytes.size(); ++index) {
        const VariantValueRef child = legacy_value.array_at(static_cast<uint32_t>(index));
        EXPECT_EQ(std::string(child.data, child.size), plan_bytes[index]);
    }

    VariantBlockBuilder builder;
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_null();
        row.add_bool(true);
        row.add_int(-128);
        row.add_int(128);
        row.add_string(string_ref("abc"));
        array.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_value(legacy.ref());
        row.finish();
    }
    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 2);
    for (size_t row = 0; row < block.num_rows(); ++row) {
        EXPECT_EQ(std::string(block.value_at(row).data, block.value_at(row).size), legacy.value);
        validate_canonical(block.value_at(row));
    }
}

TEST(VariantBlockBuilderTest, ReserveHintIsVisibleThroughOwningBufferCounters) {
    const VariantBlockBuilder::ReserveHint hint {.rows = 4,
                                                 .metadata_keys = 2,
                                                 .scalar_bytes = 64,
                                                 .nodes = 16,
                                                 .containers = 4,
                                                 .children = 12};
    VariantBlockBuilder builder(hint);
    const VariantBlockBuilder::TestCounters initial = builder.test_counters();
    EXPECT_GE(initial.row_root_capacity, hint.rows);
    EXPECT_GE(initial.metadata_key_capacity, hint.metadata_keys);
    EXPECT_GE(initial.scalar_byte_capacity, hint.scalar_bytes);
    EXPECT_GE(initial.node_capacity, hint.nodes);
    EXPECT_GE(initial.container_capacity, hint.containers);
    EXPECT_GE(initial.child_capacity, hint.children);
    EXPECT_GE(initial.scope_stack_capacity, hint.containers);
    EXPECT_GE(initial.object_id_scratch_capacity, hint.children);
    EXPECT_GE(initial.key_reference_capacity, hint.children);
    EXPECT_GE(initial.container_plan_capacity, hint.containers);
    EXPECT_GE(initial.planned_object_child_capacity, hint.children);
    EXPECT_GE(initial.previous_object_token_capacity, hint.containers);
    EXPECT_GE(initial.pending_object_token_capacity, hint.containers);

    auto row = builder.begin_row();
    auto object = row.start_object();
    object.add_key(string_ref("key"));
    row.add_string(string_ref("value"));
    object.finish();
    row.finish();
    static_cast<void>(builder.finish_block());
    EXPECT_EQ(builder.test_counters().total_capacity_growths(), 0);
}

TEST(VariantBlockBuilderTest, AbortedRowCapacityGrowthIsObservedOnceAtRowBoundary) {
    VariantBlockBuilder builder;
    auto row = builder.begin_row();
    auto array = row.start_array();
    for (int64_t value = 0; value < 1'024; ++value) {
        row.add_int(value);
    }
    row.add_int(32'768);
    array.finish();
    row.abort();

    const VariantBlockBuilder::TestCounters counters = builder.test_counters();
    EXPECT_EQ(counters.scalar_capacity_growths, 1);
    EXPECT_EQ(counters.node_capacity_growths, 1);
    EXPECT_EQ(counters.container_capacity_growths, 1);
    EXPECT_EQ(counters.child_capacity_growths, 1);
    EXPECT_EQ(counters.scope_stack_capacity_growths, 1);
    EXPECT_EQ(counters.row_root_capacity_growths, 0);
}

TEST(VariantBlockBuilderTest, PreviousObjectSchemaCacheTracksSuccessfulTransitions) {
    VariantBlockBuilder builder;
    const auto add_object_row = [&builder](std::initializer_list<std::string_view> keys) {
        auto row = builder.begin_row();
        auto object = row.start_object();
        int64_t value = 0;
        for (std::string_view key : keys) {
            // Use fresh storage on every row: a cache hit is byte identity, not pointer identity.
            const std::string copied_key(key);
            object.add_key(StringRef(copied_key));
            row.add_int(value++);
        }
        object.finish();
        row.finish();
    };

    add_object_row({});
    add_object_row({});
    add_object_row({"a", "b"});
    add_object_row({"a", "b"});
    add_object_row({"b", "a"});
    add_object_row({"b", "a"});
    add_object_row({"a"});
    add_object_row({"a"});
    add_object_row({"a", "b", "c"});
    add_object_row({"a", "b", "c"});

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 10);
    ASSERT_EQ(block.metadata_ref().dict_size(), 3);
    std::vector<VariantValueRef> rows;
    rows.reserve(block.num_rows());
    for (size_t index = 0; index < block.num_rows(); ++index) {
        rows.push_back(block.value_at(index));
    }
    validate_canonical(block.metadata_ref(), rows);
    EXPECT_EQ(block.value_at(0).num_elements(), 0);
    EXPECT_EQ(block.value_at(9).num_elements(), 3);

    const VariantBlockBuilder::TestCounters counters = builder.test_counters();
    EXPECT_EQ(counters.object_schema_hits, 5);
    EXPECT_EQ(counters.object_schema_fallbacks, 5);
    EXPECT_EQ(counters.object_plan_reuses, 5);
    EXPECT_EQ(counters.object_plan_fallbacks, 5);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest macros inflate the cache matrix.
TEST(VariantBlockBuilderTest, PreviousObjectSchemaCachePublishesOnlySuccessfulRows) {
    VariantBlockBuilder builder;
    const auto add_pair_row = [&builder] {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(string_ref("a"));
        row.add_int(1);
        object.add_key(string_ref("b"));
        row.add_int(2);
        object.finish();
        row.finish();
    };
    const auto add_array_object_row = [&builder](std::initializer_list<std::string_view> keys) {
        auto row = builder.begin_row();
        auto array = row.start_array();
        auto object = row.start_object();
        for (std::string_view key : keys) {
            object.add_key(string_ref(key));
            row.add_null();
        }
        object.finish();
        array.finish();
        row.finish();
    };
    const auto add_array_objects_row = [&builder](std::initializer_list<std::string_view> keys) {
        auto row = builder.begin_row();
        auto array = row.start_array();
        for (std::string_view key : keys) {
            auto object = row.start_object();
            object.add_key(string_ref(key));
            row.add_null();
            object.finish();
        }
        array.finish();
        row.finish();
    };

    add_pair_row();
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        const std::string invalid_key(1, static_cast<char>(0xFF));
        expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT,
                                      [&] { object.add_key(StringRef(invalid_key)); });
        row.abort();
    }
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(string_ref("a"));
        row.add_null();
        object.add_key(string_ref("ghost"));
        row.add_null();
        object.finish();
        row.abort();
    }
    add_pair_row();
    {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    add_pair_row();

    const OwnedBuilderValue imported = build_owned_value([](VariantBuilder& source) {
        auto object = source.start_object();
        object.add_key(string_ref("a"));
        source.add_int(3);
        object.add_key(string_ref("b"));
        source.add_int(4);
        object.finish();
    });
    {
        auto row = builder.begin_row();
        row.add_value(imported.ref());
        row.finish();
    }
    add_array_object_row({"a", "b"});
    add_array_object_row({"a", "b"});
    add_array_objects_row({"left", "right"});
    add_array_objects_row({"right"});
    add_array_objects_row({"right"});

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 10);
    ASSERT_EQ(block.metadata_ref().dict_size(), 4);
    EXPECT_EQ(block.metadata_ref().key_at(0), string_ref("a"));
    EXPECT_EQ(block.metadata_ref().key_at(1), string_ref("b"));
    EXPECT_EQ(block.metadata_ref().key_at(2), string_ref("left"));
    EXPECT_EQ(block.metadata_ref().key_at(3), string_ref("right"));
    std::vector<VariantValueRef> rows;
    rows.reserve(block.num_rows());
    for (size_t index = 0; index < block.num_rows(); ++index) {
        rows.push_back(block.value_at(index));
    }
    validate_canonical(block.metadata_ref(), rows);
    EXPECT_TRUE(block.value_at(2).is_null());
    ASSERT_EQ(block.value_at(5).num_elements(), 1);
    EXPECT_EQ(block.value_at(5).array_at(0).num_elements(), 2);
    EXPECT_EQ(block.value_at(9).num_elements(), 1);

    const VariantBlockBuilder::TestCounters counters = builder.test_counters();
    EXPECT_EQ(counters.object_schema_hits, 5);
    EXPECT_EQ(counters.object_schema_fallbacks, 6);
    EXPECT_EQ(counters.object_plan_reuses, 5);
    EXPECT_EQ(counters.object_plan_fallbacks, 5);
}

TEST(VariantBlockBuilderTest, PreviousObjectSchemaCacheRejectsDuplicatesAndRecovers) {
    VariantBlockBuilder builder;
    const auto add_unique_row = [&builder] {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(string_ref("duplicate"));
        row.add_null();
        object.finish();
        row.finish();
    };
    add_unique_row();
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(string_ref("duplicate"));
        row.add_null();
        object.add_key(string_ref("duplicate"));
        row.add_null();
        expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] { object.finish(); });
        row.abort();
    }
    add_unique_row();

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 2);
    ASSERT_EQ(block.metadata_ref().dict_size(), 1);
    const std::vector<VariantValueRef> rows {block.value_at(0), block.value_at(1)};
    validate_canonical(block.metadata_ref(), rows);
    const VariantBlockBuilder::TestCounters counters = builder.test_counters();
    EXPECT_EQ(counters.object_schema_hits, 1);
    EXPECT_EQ(counters.object_schema_fallbacks, 1);
    EXPECT_EQ(counters.object_plan_reuses, 1);
    EXPECT_EQ(counters.object_plan_fallbacks, 1);
}

TEST(VariantBlockBuilderTest, PreviousObjectSchemaCacheAllowsSameSchemaAtShiftedOrdinal) {
    VariantBlockBuilder builder;
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        for (int64_t value : {1, 2}) {
            auto object = row.start_object();
            object.add_key(string_ref("value"));
            row.add_int(value);
            object.finish();
        }
        array.finish();
        row.finish();
    }
    {
        // The first logical item disappeared. The remaining item now occupies ordinal zero, and
        // byte-identical schema makes reuse safe even though its logical path shifted.
        auto row = builder.begin_row();
        auto array = row.start_array();
        auto object = row.start_object();
        object.add_key(string_ref("value"));
        row.add_int(2);
        object.finish();
        array.finish();
        row.finish();
    }

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 2);
    const std::vector<VariantValueRef> rows {block.value_at(0), block.value_at(1)};
    validate_canonical(block.metadata_ref(), rows);
    EXPECT_EQ(required_field(block.value_at(1).array_at(0), "value").get_int(), 2);
    const VariantBlockBuilder::TestCounters counters = builder.test_counters();
    EXPECT_EQ(counters.object_schema_hits, 1);
    EXPECT_EQ(counters.object_schema_fallbacks, 2);
    EXPECT_EQ(counters.object_plan_reuses, 1);
    EXPECT_EQ(counters.object_plan_fallbacks, 2);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest macros inflate the boundary matrix.
TEST(VariantBlockBuilderTest, PreviousObjectSchemaCacheCrossesObjectCountBoundary) {
    VariantBlockBuilder builder;
    const auto add_object_row = [&builder](uint32_t count) {
        auto row = builder.begin_row();
        auto object = row.start_object();
        for (uint32_t index = 0; index < count; ++index) {
            const std::string key = numbered_key(index);
            object.add_key(StringRef(key));
            row.add_null();
        }
        object.finish();
        row.finish();
    };
    add_object_row(255);
    add_object_row(255);
    add_object_row(256);
    add_object_row(256);

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 4);
    ASSERT_EQ(block.metadata_ref().dict_size(), 256);
    std::vector<VariantValueRef> rows;
    rows.reserve(block.num_rows());
    for (size_t index = 0; index < block.num_rows(); ++index) {
        rows.push_back(block.value_at(index));
    }
    validate_canonical(block.metadata_ref(), rows);
    EXPECT_EQ(block.value_at(0).num_elements(), 255);
    EXPECT_EQ(block.value_at(1).num_elements(), 255);
    EXPECT_EQ(block.value_at(2).num_elements(), 256);
    EXPECT_EQ(block.value_at(3).num_elements(), 256);
    const uint8_t small_header =
            static_cast<uint8_t>(block.value_at(0).data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    const uint8_t large_header =
            static_cast<uint8_t>(block.value_at(2).data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    EXPECT_EQ(small_header & VARIANT_OBJECT_LARGE_MASK, 0);
    EXPECT_NE(large_header & VARIANT_OBJECT_LARGE_MASK, 0);

    const VariantBlockBuilder::TestCounters counters = builder.test_counters();
    EXPECT_EQ(counters.object_schema_hits, 2);
    EXPECT_EQ(counters.object_schema_fallbacks, 2);
    EXPECT_EQ(counters.object_plan_reuses, 2);
    EXPECT_EQ(counters.object_plan_fallbacks, 2);
}

TEST(VariantBlockBuilderTest, ArrayAndScalarRowsDoNotAllocateObjectCacheScratch) {
    VariantBlockBuilder builder;
    for (size_t index = 0; index < 32; ++index) {
        auto row = builder.begin_row();
        if (index % 2 == 0) {
            auto array = row.start_array();
            row.add_int(static_cast<int64_t>(index));
            array.finish();
        } else {
            row.add_null();
        }
        row.finish();
    }
    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 32);
    const VariantBlockBuilder::TestCounters counters = builder.test_counters();
    EXPECT_EQ(counters.object_schema_hits, 0);
    EXPECT_EQ(counters.object_schema_fallbacks, 0);
    EXPECT_EQ(counters.object_plan_reuses, 0);
    EXPECT_EQ(counters.object_plan_fallbacks, 0);
    EXPECT_EQ(counters.object_id_scratch_capacity_growths, 0);
    EXPECT_EQ(counters.object_id_scratch_capacity, 0);
    EXPECT_EQ(counters.object_token_capacity_growths, 0);
    EXPECT_EQ(counters.previous_object_token_capacity, 0);
    EXPECT_EQ(counters.pending_object_token_capacity, 0);
}
#endif

TEST(VariantBlockBuilderTest, AbortAndRowErrorsRollbackBeforeTheNextRow) {
    VariantBlockBuilder builder;
    {
        auto discarded = builder.begin_row();
        auto object = discarded.start_object();
        object.add_key(string_ref("destroyed"));
        discarded.add_null();
        object.finish();
    }
    {
        auto bad = builder.begin_row();
        auto object = bad.start_object();
        object.add_key(string_ref("duplicate"));
        bad.add_null();
        object.add_key(string_ref("duplicate"));
        bad.add_bool(true);
        expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT, [&] { object.finish(); });
        bad.abort();
    }
    {
        std::string source_metadata {char {0x11}, char {0x01}, char {0x00}, char {0x0D}};
        source_metadata.append("failed_import");
        const std::string source_value {char {0x02},
                                        char {0x01},
                                        char {0x00},
                                        char {0x00},
                                        char {0x02},
                                        char {0x05},
                                        static_cast<char>(0xFF)};
        const VariantValueRef source {
                .metadata = {.data = source_metadata.data(), .size = source_metadata.size()},
                .data = source_value.data(),
                .size = source_value.size()};
        auto bad = builder.begin_row();
        expect_builder_exception_code(ErrorCode::CORRUPTION, [&] { bad.add_value(source); });
        bad.abort();
    }
    {
        auto retained = builder.begin_row();
        auto object = retained.start_object();
        object.add_key(string_ref("retained"));
        retained.add_int(9);
        object.finish();
        retained.finish();
    }

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 1);
    ASSERT_EQ(block.metadata_ref().dict_size(), 1);
    EXPECT_EQ(block.metadata_ref().key_at(0), string_ref("retained"));
    EXPECT_EQ(required_field(block.value_at(0), "retained").get_int(), 9);
}

TEST(VariantBlockBuilderTest, CopiesBorrowedKeysStringsAndBinaryBeforeRowReturns) {
    VariantBlockBuilder builder;
    std::string key = "borrowed";
    std::string text = "text before mutation";
    std::string binary("\0\xFF", 2);
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef(key));
        auto array = row.start_array();
        row.add_string(StringRef(text));
        row.add_binary(StringRef(binary));
        array.finish();
        object.finish();
        row.finish();
    }
    key.assign("changed");
    text.assign("changed");
    binary.assign("changed");

    VariantEncodedBlock block = builder.finish_block();
    const VariantValueRef array = required_field(block.value_at(0), "borrowed");
    ASSERT_EQ(array.num_elements(), 2);
    EXPECT_EQ(array.array_at(0).get_string(), string_ref("text before mutation"));
    EXPECT_EQ(array.array_at(1).get_binary(), StringRef(std::string("\0\xFF", 2)));
}

TEST(VariantBlockBuilderTest, RowAddValueCanonicalizesNestedBorrowedArrayChild) {
    OwnedBuilderValue source = make_nested_noncanonical_owned_value();
    VariantBlockBuilder builder;
    {
        auto row = builder.begin_row();
        row.add_value(source.ref());
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_int(7);
        row.add_value(source.ref());
        source.metadata.assign("invalidated");
        source.value.assign("invalidated");
        array.finish();
        row.finish();
    }

    VariantEncodedBlock block = builder.finish_block();
    ASSERT_EQ(block.num_rows(), 2);
    ASSERT_EQ(block.metadata_ref().dict_size(), 2);
    EXPECT_EQ(block.metadata_ref().key_at(0), string_ref("a"));
    EXPECT_EQ(block.metadata_ref().key_at(1), string_ref("b"));
    EXPECT_EQ(block.value_at(0).metadata.data, block.value_at(1).metadata.data);
    const std::vector<VariantValueRef> rows {block.value_at(0), block.value_at(1)};
    validate_canonical(block.metadata_ref(), rows);

    const VariantValueRef direct_object = block.value_at(0).array_at(0);
    EXPECT_TRUE(required_field(direct_object, "a").get_bool());
    EXPECT_FALSE(required_field(direct_object, "b").get_bool());
    const VariantValueRef array_child = block.value_at(1).array_at(1).array_at(0);
    EXPECT_TRUE(required_field(array_child, "a").get_bool());
    EXPECT_FALSE(required_field(array_child, "b").get_bool());
}

TEST(VariantBuilderAddValueTest, ImportsEveryPrimitiveClassAndCopiesBorrowedInput) {
    OwnedBuilderValue source = build_owned_value([](VariantBuilder& builder) {
        auto array = builder.start_array();
        builder.add_null();
        builder.add_bool(false);
        builder.add_bool(true);
        builder.add_int(1);
        builder.add_int(200);
        builder.add_int(70'000);
        builder.add_int(5'000'000'000);
        builder.add_double(-1.25);
        builder.add_decimal(123, 2, 4);
        builder.add_decimal(12'345'678'901, 3, 8);
        builder.add_decimal(static_cast<__int128>(1) << 80, 4, 16);
        builder.add_date(-20'000);
        builder.add_timestamp_micros(-1, true);
        builder.add_timestamp_micros(1, false);
        builder.add_float(1.5F);
        const std::string binary("\0\xFF", 2);
        builder.add_binary(StringRef(binary));
        builder.add_string(string_ref("short"));
        const std::string long_string(64, 'x');
        builder.add_string(StringRef(long_string));
        builder.add_time_ntz_micros(1);
        builder.add_timestamp_nanos(-1, true);
        builder.add_timestamp_nanos(1, false);
        const std::array<uint8_t, 16> uuid {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        builder.add_uuid(uuid);
        array.finish();
    });
    const std::string expected_metadata = source.metadata;
    const std::string expected_value = source.value;

    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    builder.add_value(source.ref());
    source.metadata.assign("invalidated");
    source.value.assign("invalidated");
    metadata.seal();
    const std::string encoded = finish_row(builder);
    const StringRef encoded_metadata = metadata.encoded_metadata();
    EXPECT_EQ(std::string(encoded_metadata.data, encoded_metadata.size), expected_metadata);
    EXPECT_EQ(encoded, expected_value);
    const VariantValueRef root = value_ref(metadata, encoded);
    validate_canonical(root);
    ASSERT_EQ(root.num_elements(), 22);
    EXPECT_EQ(root.array_at(16).basic_type(), VariantBasicType::SHORT_STRING);
    EXPECT_EQ(root.array_at(17).primitive_id(), VariantPrimitiveId::STRING);
    EXPECT_EQ(root.array_at(21).primitive_id(), VariantPrimitiveId::UUID);
}

TEST(VariantBuilderAddValueTest, ImportsNestedValueAsActiveArrayChild) {
    const OwnedBuilderValue source = make_nested_owned_value();
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto array = builder.start_array();
    builder.add_int(1);
    builder.add_value(source.ref());
    array.finish();
    metadata.seal();
    const std::string encoded = finish_row(builder);
    const VariantValueRef root = value_ref(metadata, encoded);
    validate_canonical(root);
    ASSERT_EQ(root.num_elements(), 2);
    EXPECT_EQ(root.array_at(0).get_int(), 1);
    const VariantValueRef imported = root.array_at(1);
    ASSERT_EQ(required_field(imported, "array").num_elements(), 2);
    EXPECT_EQ(required_field(required_field(imported, "array").array_at(1), "leaf").get_string(),
              string_ref("value"));
}

TEST(VariantBuilderAddValueTest, ImportsNestedValueAsActiveObjectChild) {
    const OwnedBuilderValue source = make_nested_owned_value();
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object = builder.start_object();
    object.add_key(string_ref("wrapped"));
    builder.add_value(source.ref());
    object.finish();
    metadata.seal();
    const std::string encoded = finish_row(builder);
    const VariantValueRef root = value_ref(metadata, encoded);
    validate_canonical(root);
    const VariantValueRef imported = required_field(root, "wrapped");
    EXPECT_EQ(required_field(required_field(imported, "array").array_at(1), "leaf").get_string(),
              string_ref("value"));
}

TEST(VariantBuilderAddValueTest, CanonicalizesLegalNonCanonicalInputAndCopiesIt) {
    std::string source_metadata {char {0x01}, char {0x02}, char {0x00}, char {0x01},
                                 char {0x02}, 'b',         'a'};
    std::string source_value {
            char {0x02},
            char {0x02},
            char {0x01},
            char {0x00},
            char {0x01},
            char {0x00},
            char {0x02},
            char {static_cast<uint8_t>(VariantPrimitiveId::FALSE_VALUE)
                  << VARIANT_VALUE_HEADER_SHIFT},
            char {static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE)
                  << VARIANT_VALUE_HEADER_SHIFT},
    };
    const VariantValueRef source {
            .metadata = {.data = source_metadata.data(), .size = source_metadata.size()},
            .data = source_value.data(),
            .size = source_value.size()};

    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    builder.add_value(source);
    source_metadata.assign("invalidated");
    source_value.assign("invalidated");
    metadata.seal();
    const std::string encoded = finish_row(builder);
    const VariantValueRef canonical = value_ref(metadata, encoded);
    validate_canonical(canonical);
    ASSERT_EQ(metadata.metadata_ref().dict_size(), 2);
    EXPECT_EQ(metadata.metadata_ref().key_at(0), string_ref("a"));
    EXPECT_EQ(metadata.metadata_ref().key_at(1), string_ref("b"));
    EXPECT_TRUE(required_field(canonical, "a").get_bool());
    EXPECT_FALSE(required_field(canonical, "b").get_bool());
}

TEST(VariantBuilderAddValueTest, RejectsTrailingBytesDepthOverflowAndDuplicateFields) {
    const std::string empty_metadata("\x11\0\0", 3);
    const std::string trailing_nulls(2, '\0');
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        const VariantValueRef trailing {
                .metadata = {.data = empty_metadata.data(), .size = empty_metadata.size()},
                .data = trailing_nulls.data(),
                .size = trailing_nulls.size()};
        expect_builder_exception_code(ErrorCode::CORRUPTION, [&] { builder.add_value(trailing); });
        builder.abort();
        metadata.seal();
        EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    }

    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        const std::string decimal_overflow = decimal_bytes(
                VariantPrimitiveId::DECIMAL16, static_cast<__int128>(power_of_ten(38)), 16);
        const VariantValueRef source {
                .metadata = {.data = empty_metadata.data(), .size = empty_metadata.size()},
                .data = decimal_overflow.data(),
                .size = decimal_overflow.size()};
        expect_builder_exception_code(ErrorCode::CORRUPTION, [&] { builder.add_value(source); });
        builder.abort();
        metadata.seal();
        EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    }

    VariantMetadataBuilder source_metadata;
    VariantBuilder source_builder(source_metadata);
    std::vector<VariantBuilder::ArrayScope> arrays;
    for (uint32_t depth = 0; depth <= VARIANT_MAX_NESTING_DEPTH; ++depth) {
        arrays.emplace_back(source_builder.start_array());
    }
    source_builder.add_null();
    for (auto& array : std::ranges::reverse_view(arrays)) {
        array.finish();
    }
    source_metadata.seal();
    const std::string too_deep_value = finish_row(source_builder);
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        const VariantValueRef too_deep {.metadata = source_metadata.metadata_ref(),
                                        .data = too_deep_value.data(),
                                        .size = too_deep_value.size()};
        expect_builder_exception_code(ErrorCode::INVALID_ARGUMENT,
                                      [&] { builder.add_value(too_deep); });
        builder.abort();
        metadata.seal();
        EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    }

    const std::string one_key_metadata {char {0x11}, char {0x01}, char {0x00}, char {0x01}, 'a'};
    const std::string duplicate_object {char {0x02}, char {0x02}, char {0x00},
                                        char {0x00}, char {0x00}, char {0x01},
                                        char {0x02}, char {0x00}, char {0x00}};
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        const VariantValueRef duplicate {
                .metadata = {.data = one_key_metadata.data(), .size = one_key_metadata.size()},
                .data = duplicate_object.data(),
                .size = duplicate_object.size()};
        expect_builder_exception_code(ErrorCode::CORRUPTION, [&] { builder.add_value(duplicate); });
        builder.abort();
        metadata.seal();
        EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    }

    const std::string two_key_metadata {char {0x11}, char {0x02}, char {0x00}, char {0x01},
                                        char {0x02}, 'a',         'b'};
    const auto expect_invalid_object_partition = [&](const std::string& object) {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        const VariantValueRef source {
                .metadata = {.data = two_key_metadata.data(), .size = two_key_metadata.size()},
                .data = object.data(),
                .size = object.size()};
        expect_builder_exception_code(ErrorCode::CORRUPTION, [&] { builder.add_value(source); });
        builder.abort();
        metadata.seal();
        EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    };
    const std::string overlapping_object {char {0x02}, char {0x02}, char {0x00}, char {0x01},
                                          char {0x00}, char {0x00}, char {0x01}, char {0x00}};
    const std::string object_with_gap {char {0x02}, char {0x02}, char {0x00}, char {0x01},
                                       char {0x00}, char {0x02}, char {0x03}, char {0x00},
                                       char {0x00}, char {0x00}};
    const std::string object_with_trailing_value {
            char {0x02}, char {0x02}, char {0x00}, char {0x01}, char {0x00},
            char {0x01}, char {0x03}, char {0x00}, char {0x00}, char {0x00}};
    expect_invalid_object_partition(overlapping_object);
    expect_invalid_object_partition(object_with_gap);
    expect_invalid_object_partition(object_with_trailing_value);
}

TEST(VariantBuilderAddValueTest, InvalidUtf8StringAndKeyAbortWithoutRetainingMetadata) {
    const std::string empty_metadata("\x11\0\0", 3);
    const std::string invalid_string {char {0x05}, static_cast<char>(0xFF)};
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        const VariantValueRef source {
                .metadata = {.data = empty_metadata.data(), .size = empty_metadata.size()},
                .data = invalid_string.data(),
                .size = invalid_string.size()};
        expect_builder_exception_code(ErrorCode::CORRUPTION, [&] { builder.add_value(source); });
        builder.abort();
        metadata.seal();
        EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    }

    const std::string invalid_key_metadata {char {0x11}, char {0x01}, char {0x00}, char {0x01},
                                            static_cast<char>(0xFF)};
    const std::string object_value {char {0x02}, char {0x01}, char {0x00},
                                    char {0x00}, char {0x01}, char {0x00}};
    {
        VariantMetadataBuilder metadata;
        VariantBuilder builder(metadata);
        const VariantValueRef source {.metadata = {.data = invalid_key_metadata.data(),
                                                   .size = invalid_key_metadata.size()},
                                      .data = object_value.data(),
                                      .size = object_value.size()};
        expect_builder_exception_code(ErrorCode::CORRUPTION, [&] { builder.add_value(source); });
        builder.abort();
        metadata.seal();
        EXPECT_EQ(metadata.metadata_ref().dict_size(), 0);
    }
}

#ifdef BE_TEST
VariantBlockBuilder::TestCounters collect_capacity_counters(size_t rows) {
    VariantBlockBuilder builder;
    for (size_t index = 0; index < rows; ++index) {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(string_ref("value"));
        row.add_int(static_cast<int64_t>(index) + 32'768);
        object.finish();
        row.finish();
    }
    VariantEncodedBlock block = builder.finish_block();
    EXPECT_EQ(block.num_rows(), rows);
    std::vector<VariantValueRef> values;
    values.reserve(block.num_rows());
    for (size_t index = 0; index < block.num_rows(); ++index) {
        values.push_back(block.value_at(index));
    }
    validate_canonical(block.metadata_ref(), values);
    return builder.test_counters();
}

std::array<size_t, 12> owning_buffer_growths(const VariantBlockBuilder::TestCounters& value) {
    return {value.metadata_capacity_growths,
            value.scalar_capacity_growths,
            value.node_capacity_growths,
            value.container_capacity_growths,
            value.child_capacity_growths,
            value.scope_stack_capacity_growths,
            value.object_id_scratch_capacity_growths,
            value.key_reference_capacity_growths,
            value.container_plan_capacity_growths,
            value.planned_object_child_capacity_growths,
            value.row_root_capacity_growths,
            value.object_token_capacity_growths};
}

TEST(VariantBlockBuilderTest, CapacityGrowthIsBlockBoundedRatherThanPerRow) {
    const VariantBlockBuilder::TestCounters small = collect_capacity_counters(4'096);
    const VariantBlockBuilder::TestCounters large = collect_capacity_counters(8'192);
    for (const VariantBlockBuilder::TestCounters* counters : {&small, &large}) {
        for (size_t growths : owning_buffer_growths(*counters)) {
            EXPECT_GT(growths, 0);
        }
    }
    EXPECT_EQ(small.metadata_unique_keys, 1);
    EXPECT_EQ(large.metadata_unique_keys, 1);
    EXPECT_GT(small.total_capacity_growths(), 0);
    EXPECT_LE(large.total_capacity_growths(), small.total_capacity_growths() + 12);
    EXPECT_GT(small.previous_object_token_capacity, 0);
    EXPECT_GT(small.pending_object_token_capacity, 0);
    EXPECT_GE(large.previous_object_token_capacity, small.previous_object_token_capacity);
    EXPECT_GE(large.pending_object_token_capacity, small.pending_object_token_capacity);
}

TEST(VariantBlockBuilderTest, BlockLifetimeAllocationsAreMemTrackerVisible) {
    constexpr size_t ROWS = 2'048;
    constexpr size_t PAYLOAD_BYTES = 4'096;
    const std::string payload(PAYLOAD_BYTES, 'x');
    const auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                          "VariantBlockBuilderTrackedAllocations");
    auto scoped_tracker = SwitchThreadMemTrackerLimiter(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const int64_t baseline = tracker->consumption();

    {
        VariantBlockBuilder builder(
                {.rows = ROWS, .scalar_bytes = ROWS * (PAYLOAD_BYTES + 5), .nodes = ROWS});
        for (size_t index = 0; index < ROWS; ++index) {
            auto row = builder.begin_row();
            row.add_string(StringRef(payload));
            row.finish();
        }
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_GT(tracker->consumption(),
                  baseline + static_cast<int64_t>(ROWS * PAYLOAD_BYTES / 2));

        VariantEncodedBlock block = builder.finish_block();
        ASSERT_EQ(block.num_rows(), ROWS);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_GT(tracker->consumption(),
                  baseline + static_cast<int64_t>(ROWS * PAYLOAD_BYTES / 2));
    }

    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(tracker->consumption(), baseline);
}
#endif

TEST(VariantBuilderTest, MillionFieldObjectIsCanonicalAndReadable) {
    constexpr uint32_t FIELD_COUNT = 1'000'000;
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    auto object_scope = builder.start_object();
    char key[7] {'k', '0', '0', '0', '0', '0', '0'};
    for (uint32_t index = 0; index < FIELD_COUNT; ++index) {
        uint32_t remaining = index;
        for (uint8_t digit = 0; digit < 6; ++digit) {
            key[6 - digit] = static_cast<char>('0' + remaining % 10);
            remaining /= 10;
        }
        object_scope.add_key({key, sizeof(key)});
        builder.add_null();
    }
    object_scope.finish();
    metadata.seal();

    const std::string encoded = finish_row(builder);
    const VariantValueRef value = value_ref(metadata, encoded);
    validate_canonical(value);
    ASSERT_EQ(metadata.metadata_ref().dict_size(), FIELD_COUNT);
    ASSERT_EQ(value.num_elements(), FIELD_COUNT);
    EXPECT_EQ(metadata.metadata_ref().offset_size(), 3);
    EXPECT_TRUE(value.object_value_at(0, nullptr).is_null());
    EXPECT_TRUE(value.object_value_at(FIELD_COUNT / 2, nullptr).is_null());
    EXPECT_TRUE(value.object_value_at(FIELD_COUNT - 1, nullptr).is_null());
    VariantValueRef found;
    EXPECT_TRUE(value.object_find(string_ref("k000000"), &found));
    EXPECT_TRUE(found.is_null());
    EXPECT_TRUE(value.object_find(string_ref("k999999"), &found));
    EXPECT_TRUE(found.is_null());
}

} // namespace
} // namespace doris
