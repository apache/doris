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
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/string_buffer.hpp"
#include "core/value/jsonb_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exec/common/variant_util.h"
#include "variant_test_utils.h"

namespace doris {
namespace {

StringRef string_ref(std::string_view value) {
    return {value.data(), value.size()};
}

struct OwnedValue {
    std::string metadata;
    std::string value;

    VariantRef ref() const {
        return {.metadata = {.data = metadata.data(), .size = metadata.size()},
                .value = {value.data(), value.size()}};
    }
};

template <typename Fill>
OwnedValue build_value(Fill&& fill) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    fill(row);
    row.finish();
    VariantBatchBuilder block = builder.finish_batch();
    const VariantMetadataRef metadata = block.metadata_ref();
    const VariantRef value = block.value_at(0);
    return {.metadata = std::string(metadata.data, metadata.size),
            .value = std::string(value.value.data, value.value.size)};
}

struct StringWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

std::string print_json(VariantRef value,
                       const VariantJsonFormatOptions& options = VariantJsonFormatOptions {}) {
    StringWriter writer;
    to_json(value, writer, options);
    return writer.value;
}

VariantBatchBuilder encode_jsons(const std::vector<std::string_view>& jsons,
                                 JsonToVariantOptions options = {}) {
    JsonStringToVariantEncoder encoder(options);
    for (std::string_view json : jsons) {
        encoder.add_json(string_ref(json));
    }
    return encoder.finish_batch();
}

std::string block_value_bytes(const VariantBatchBuilder& block) {
    const StringRef bytes = block.value_bytes();
    return {bytes.data, bytes.size};
}

std::vector<uint32_t> block_value_offsets(const VariantBatchBuilder& block) {
    const std::span<const uint32_t> offsets = block.value_offsets();
    return {offsets.begin(), offsets.end()};
}

template <typename Function>
void expect_exception_code(int code, Function&& function) {
    try {
        function();
        FAIL() << "Expected doris::Exception";
    } catch (const Exception& exception) {
        EXPECT_EQ(exception.code(), code) << exception.what();
    }
}

std::string nested_array_json(uint32_t array_count) {
    std::string json(array_count, '[');
    json.push_back('0');
    json.append(array_count, ']');
    return json;
}

OwnedValue nested_array_value(uint32_t array_count) {
    return build_value([&](VariantBatchBuilder::Row& builder) {
        std::vector<VariantBatchBuilder::Row::ArrayScope> scopes;
        scopes.reserve(array_count);
        for (uint32_t depth = 0; depth < array_count; ++depth) {
            scopes.emplace_back(builder.start_array());
        }
        builder.add_int(0);
        for (VariantBatchBuilder::Row::ArrayScope& scope : std::ranges::reverse_view(scopes)) {
            scope.finish();
        }
    });
}

OwnedValue raw_object_value(const std::vector<std::string>& keys, bool sorted_metadata,
                            const std::vector<uint8_t>& field_ids,
                            const std::vector<uint8_t>& value_offsets,
                            std::string physical_values) {
    std::string metadata;
    metadata.push_back(
            static_cast<char>(VARIANT_ENCODING_VERSION |
                              (sorted_metadata ? VARIANT_METADATA_SORTED_STRINGS_MASK : 0)));
    metadata.push_back(static_cast<char>(keys.size()));
    metadata.push_back(0);
    uint8_t key_offset = 0;
    for (const std::string& key : keys) {
        key_offset += static_cast<uint8_t>(key.size());
        metadata.push_back(static_cast<char>(key_offset));
    }
    for (const std::string& key : keys) {
        metadata.append(key);
    }

    std::string value;
    value.push_back(static_cast<char>(VariantBasicType::OBJECT));
    value.push_back(static_cast<char>(field_ids.size()));
    value.append(reinterpret_cast<const char*>(field_ids.data()), field_ids.size());
    value.append(reinterpret_cast<const char*>(value_offsets.data()), value_offsets.size());
    value.push_back(static_cast<char>(physical_values.size()));
    value.append(physical_values);
    return {.metadata = std::move(metadata), .value = std::move(value)};
}

TEST(VariantJsonTest, AllPrimitiveIdsUseStableJsonMappings) {
    const std::string long_string(64, 's');
    const std::string binary("\0\x01\x02\xFF", 4);
    std::array<uint8_t, 16> uuid {};
    for (uint8_t index = 0; index < uuid.size(); ++index) {
        uuid[index] = index;
    }

    const OwnedValue owned = build_value([&](VariantBatchBuilder::Row& builder) {
        auto array = builder.start_array();
        builder.add_null();
        builder.add_bool(true);
        builder.add_bool(false);
        builder.add_int(-128);
        builder.add_int(-129);
        builder.add_int(32768);
        builder.add_int(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1);
        builder.add_double(1.25);
        builder.add_decimal(-12345, 2);
        builder.add_decimal(1'000'000'000, 3);
        builder.add_decimal(static_cast<__int128>(1'000'000'000'000'000'000LL), 0);
        builder.add_date(0);
        builder.add_timestamp_micros(0, true);
        builder.add_timestamp_micros(-1, false);
        builder.add_float(-0.0F);
        builder.add_binary(StringRef(binary));
        builder.add_string(StringRef(long_string));
        builder.add_time_ntz_micros(1);
        builder.add_timestamp_nanos(-1, true);
        builder.add_timestamp_nanos(-1, false);
        builder.add_uuid(uuid);
        array.finish();
    });
    const VariantRef root = owned.ref();
    validate_canonical(root);
    ASSERT_EQ(root.num_elements(), VARIANT_MAX_PRIMITIVE_ID + 1);

    const std::array<std::string, 21> expected_json {
            "null",
            "true",
            "false",
            "-128",
            "-129",
            "32768",
            "2147483648",
            "1.25",
            "-123.45",
            "1000000.000",
            "1000000000000000000",
            "\"1970-01-01\"",
            "\"1970-01-01 00:00:00.000000+00:00\"",
            "\"1969-12-31 23:59:59.999999\"",
            "-0",
            "\"AAEC/w==\"",
            "\"" + long_string + "\"",
            "\"00:00:00.000001\"",
            "\"1969-12-31 23:59:59.999999999+00:00\"",
            "\"1969-12-31 23:59:59.999999999\"",
            "\"00010203-0405-0607-0809-0a0b0c0d0e0f\"",
    };
    for (uint8_t id = 0; id <= VARIANT_MAX_PRIMITIVE_ID; ++id) {
        const VariantRef value = root.array_at(id);
        EXPECT_EQ(value.primitive_id(), static_cast<VariantPrimitiveId>(id));
        EXPECT_EQ(print_json(value), expected_json[id]);
    }
}

TEST(VariantJsonTest, FloatingPointSpecialValuesAreQuoted) {
    const OwnedValue owned = build_value([](VariantBatchBuilder::Row& builder) {
        auto array = builder.start_array();
        builder.add_float(std::numeric_limits<float>::quiet_NaN());
        builder.add_float(std::numeric_limits<float>::infinity());
        builder.add_double(-std::numeric_limits<double>::infinity());
        array.finish();
    });
    EXPECT_EQ(print_json(owned.ref()), R"(["NaN","Infinity","-Infinity"])");
}

TEST(VariantJsonTest, TimestampPrecisionTimezoneAndNegativeEpochAreStable) {
    const OwnedValue owned = build_value([](VariantBatchBuilder::Row& builder) {
        auto array = builder.start_array();
        builder.add_timestamp_micros(0, true);
        builder.add_timestamp_micros(-1, true);
        builder.add_timestamp_micros(-1, false);
        builder.add_timestamp_nanos(0, true);
        builder.add_timestamp_nanos(-1, true);
        builder.add_timestamp_nanos(-1, false);
        array.finish();
    });
    const cctz::time_zone timezone = cctz::fixed_time_zone(std::chrono::seconds(19'800));
    const VariantJsonFormatOptions options {.timezone = &timezone};
    EXPECT_EQ(
            print_json(owned.ref(), options),
            R"(["1970-01-01 05:30:00.000000+05:30","1970-01-01 05:29:59.999999+05:30","1969-12-31 23:59:59.999999","1970-01-01 05:30:00.000000000+05:30","1970-01-01 05:29:59.999999999+05:30","1969-12-31 23:59:59.999999999"])");
}

TEST(VariantJsonTest, EncoderCopiesRowsBeforeParserReuseAndProducesCanonicalValues) {
    const std::vector<std::string_view> inputs {R"({"z":1,"a":[true,null,"x"]})",
                                                R"(18446744073709551615)", R"("last")"};
    VariantBatchBuilder block = encode_jsons(inputs);
    ASSERT_EQ(block.num_rows(), inputs.size());
    EXPECT_EQ(print_json(block.value_at(0)), R"({"a":[true,null,"x"],"z":1})");
    EXPECT_EQ(print_json(block.value_at(1)), "18446744073709551615");
    EXPECT_EQ(print_json(block.value_at(2)), R"("last")");
    std::vector<VariantRef> rows;
    rows.reserve(block.num_rows());
    for (size_t row = 0; row < block.num_rows(); ++row) {
        rows.push_back(block.value_at(row));
    }
    validate_canonical(block.metadata_ref(), std::span<const VariantRef>(rows));

    VariantBatchBuilder reparsed = encode_jsons({print_json(block.value_at(0))});
    EXPECT_TRUE(canonical_equals(block.value_at(0), reparsed.value_at(0)));
}

TEST(VariantJsonTest, DuplicateKeysKeepFirstCompleteSubtreeWhenEnabled) {
    JsonToVariantOptions options;
    options.check_duplicate_json_path = true;
    VariantBatchBuilder block = encode_jsons(
            {R"({"a":1,"a":[2]})", R"({"a":[1],"a":2})", R"({"a":{"b":1},"a":{"c":2}})"}, options);
    ASSERT_EQ(block.num_rows(), 3);
    EXPECT_EQ(print_json(block.value_at(0)), R"({"a":1})");
    EXPECT_EQ(print_json(block.value_at(1)), R"({"a":[1]})");
    EXPECT_EQ(print_json(block.value_at(2)), R"({"a":{"b":1}})");

    // This is member-level first-wins semantics. It deliberately removes the old PathInData
    // flatten/merge artifact; it is not evidence that the broader T0.2 comparison is complete.
    VariantBatchBuilder dotted = encode_jsons({R"({"a.b":1,"a":{"b":2}})"}, options);
    EXPECT_EQ(print_json(dotted.value_at(0)), R"({"a":{"b":2},"a.b":1})");
}

TEST(VariantJsonTest, DuplicateKeysFailWithoutPerObjectHashWhenDisabled) {
    JsonToVariantOptions options;
    options.check_duplicate_json_path = false;
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(encode_jsons({R"({"a":1,"a":2})"}, options)); });
}

TEST(VariantJsonTest, RepeatedDuplicateSchemasStayStableAcrossSchemaAndScalarRows) {
    JsonToVariantOptions options;
    options.check_duplicate_json_path = true;
    VariantBatchBuilder block =
            encode_jsons({R"({"a":1,"a":2})", R"({"a":3,"a":4})", "null", R"({"a":5,"a":6})",
                          R"({"b":7,"b":8})", R"({"b":9,"b":10})"},
                         options);
    ASSERT_EQ(block.num_rows(), 6);
    EXPECT_EQ(print_json(block.value_at(0)), R"({"a":1})");
    EXPECT_EQ(print_json(block.value_at(1)), R"({"a":3})");
    EXPECT_TRUE(block.value_at(2).is_null());
    EXPECT_EQ(print_json(block.value_at(3)), R"({"a":5})");
    EXPECT_EQ(print_json(block.value_at(4)), R"({"b":7})");
    EXPECT_EQ(print_json(block.value_at(5)), R"({"b":9})");
    std::vector<VariantRef> rows;
    rows.reserve(block.num_rows());
    for (size_t index = 0; index < block.num_rows(); ++index) {
        rows.push_back(block.value_at(index));
    }
    validate_canonical(block.metadata_ref(), rows);
}

TEST(VariantJsonTest, IgnoredDuplicateSubtreeStillValidatesKeysAndDepth) {
    JsonToVariantOptions options;
    options.max_json_key_length = 255;
    options.check_duplicate_json_path = true;
    const std::string oversized_key(256, 'k');
    const std::string oversized = R"({"a":1,"a":{")" + oversized_key + R"(":2}})";
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(encode_jsons({oversized}, options)); });

    const std::string too_deep =
            R"({"a":1,"a":)" + nested_array_json(VARIANT_MAX_NESTING_DEPTH) + "}";
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(encode_jsons({too_deep}, options)); });
}

TEST(VariantJsonTest, KeyLengthBoundaryIsMeasuredInUtf8Bytes) {
    JsonToVariantOptions options;
    options.max_json_key_length = 255;
    const std::string key255(255, 'k');
    const std::string key256(256, 'k');
    VariantBatchBuilder block = encode_jsons({R"({")" + key255 + R"(":1})"}, options);
    EXPECT_EQ(block.metadata_ref().key_at(0), StringRef(key255));
    expect_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        static_cast<void>(encode_jsons({R"({")" + key256 + R"(":1})"}, options));
    });
}

TEST(VariantJsonTest, EmptyAndInvalidInputFollowExplicitPolicy) {
    JsonToVariantOptions permissive;
    permissive.throw_on_invalid_json = false;
    VariantBatchBuilder block = encode_jsons({std::string_view {}, "{"}, permissive);
    ASSERT_EQ(block.num_rows(), 2);
    EXPECT_EQ(print_json(block.value_at(0)), "{}");
    EXPECT_EQ(print_json(block.value_at(1)), R"("{")");

    JsonToVariantOptions strict;
    strict.throw_on_invalid_json = true;
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(encode_jsons({"{"}, strict)); });

    const std::string invalid_utf8(1, static_cast<char>(0xFF));
    expect_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        static_cast<void>(encode_jsons({std::string_view(invalid_utf8)}, permissive));
    });

    JsonStringToVariantEncoder null_input(permissive);
    expect_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        null_input.add_json({static_cast<const char*>(nullptr), 1});
    });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(null_input.finish_batch()); });
}

TEST(VariantJsonTest, RecoverableRowErrorRollsBackAndContinues) {
    JsonToVariantOptions strict;
    strict.throw_on_invalid_json = true;
    JsonStringToVariantEncoder encoder(strict);

    encoder.add_json(string_ref(R"({"before":1})"));
    const Status invalid = encoder.try_add_json(string_ref("{"));
    EXPECT_FALSE(invalid.ok());
    EXPECT_EQ(invalid.code(), ErrorCode::INVALID_ARGUMENT) << invalid.to_string();
    ASSERT_TRUE(encoder.try_add_json(string_ref(R"([2,{"after":3}])")).ok());

    VariantBatchBuilder block = encoder.finish_batch();
    ASSERT_EQ(block.num_rows(), 2);
    EXPECT_EQ(print_json(block.value_at(0)), R"({"before":1})");
    EXPECT_EQ(print_json(block.value_at(1)), R"([2,{"after":3}])");
}

TEST(VariantJsonTest, DepthBoundaryMatchesCanonicalTraversal) {
    const std::string accepted_json = nested_array_json(VARIANT_MAX_NESTING_DEPTH);
    VariantBatchBuilder accepted = encode_jsons({accepted_json});
    EXPECT_NO_THROW(static_cast<void>(print_json(accepted.value_at(0))));
    EXPECT_TRUE(canonical_equals(accepted.value_at(0), accepted.value_at(0)));

    const std::string rejected_json = nested_array_json(VARIANT_MAX_NESTING_DEPTH + 1);
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(encode_jsons({rejected_json})); });

    const OwnedValue too_deep = nested_array_value(VARIANT_MAX_NESTING_DEPTH + 1);
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(print_json(too_deep.ref())); });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT, [&] {
        static_cast<void>(canonical_equals(too_deep.ref(), too_deep.ref()));
    });
}

TEST(VariantJsonTest, StringsKeysAndBinaryAreEscapedWithoutFlattening) {
    VariantBatchBuilder block =
            encode_jsons({R"({"a.b":1,"a":{"b":2},"line\u2028key":"x\n\t\"\\"})"});
    EXPECT_EQ(print_json(block.value_at(0)),
              "{\"a\":{\"b\":2},\"a.b\":1,\"line\\u2028key\":\"x\\n\\t\\\"\\\\\"}");
}

TEST(VariantJsonTest, BlockOwnsStableBoundariesAndStateTransitionsAreTerminal) {
    JsonStringToVariantEncoder empty_encoder;
    VariantBatchBuilder empty = empty_encoder.finish_batch();
    EXPECT_EQ(empty.num_rows(), 0);
    EXPECT_EQ(block_value_offsets(empty), (std::vector<uint32_t> {0}));
    EXPECT_EQ(block_value_bytes(empty), "");
    EXPECT_NO_THROW(empty.metadata_ref().validate());
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(empty.value_at(0)); });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(empty_encoder.finish_batch()); });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { empty_encoder.add_json(string_ref("null")); });

    JsonToVariantOptions strict;
    strict.throw_on_invalid_json = true;
    JsonStringToVariantEncoder failed(strict);
    expect_exception_code(ErrorCode::INVALID_ARGUMENT, [&] { failed.add_json(string_ref("{")); });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { failed.add_json(string_ref("null")); });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(failed.finish_batch()); });
}

TEST(VariantJsonBlockTest, SharedBuilderIsByteEquivalentToExistingJsonAndTerminal) {
    const std::string expected_metadata {char {0x11}, char {0x02}, char {0x00}, char {0x01},
                                         char {0x02}, 'a',         'z'};
    const std::string expected_values {
            char {0x02}, char {0x02}, char {0x00}, char {0x01}, char {0x00}, char {0x0A},
            char {0x0C}, char {0x03}, char {0x03}, char {0x00}, char {0x01}, char {0x02},
            char {0x04}, char {0x04}, char {0x00}, char {0x05}, 'x',         char {0x0C},
            char {0x01}, char {0x19}, 's',         'c',         'a',         'l',
            'a',         'r',         char {0x02}, char {0x00}, char {0x00},
    };
    const std::vector<uint32_t> expected_offsets {0, 19, 26, 29};

    JsonStringToVariantEncoder json_encoder;
    json_encoder.add_json(string_ref(R"({"z":1,"a":[true,null,"x"]})"));
    json_encoder.add_json(string_ref(R"("scalar")"));
    json_encoder.add_json(string_ref("{}"));
    VariantBatchBuilder json_block = json_encoder.finish_batch();
    EXPECT_EQ(std::string(json_block.metadata_ref().data, json_block.metadata_ref().size),
              expected_metadata);
    EXPECT_EQ(block_value_bytes(json_block), expected_values);
    EXPECT_EQ(block_value_offsets(json_block), expected_offsets);
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { json_encoder.add_json(string_ref("null")); });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(json_encoder.finish_batch()); });

    VariantBatchBuilder builder({.rows = 3,
                                 .metadata_keys = 2,
                                 .scalar_bytes = 16,
                                 .nodes = 8,
                                 .containers = 3,
                                 .children = 5});
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(string_ref("z"));
        row.add_int(1);
        object.add_key(string_ref("a"));
        auto array = row.start_array();
        row.add_bool(true);
        row.add_null();
        row.add_string(string_ref("x"));
        array.finish();
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_string(string_ref("scalar"));
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.finish();
        row.finish();
    }

    VariantBatchBuilder block = builder.finish_batch();
    EXPECT_EQ(std::string(block.metadata_ref().data, block.metadata_ref().size), expected_metadata);
    EXPECT_EQ(block_value_bytes(block), expected_values);
    EXPECT_EQ(block_value_offsets(block), expected_offsets);
    const std::vector<VariantRef> rows {block.value_at(0), block.value_at(1), block.value_at(2)};
    validate_canonical(block.metadata_ref(), rows);
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(builder.begin_row()); });
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { static_cast<void>(builder.finish_batch()); });
}

TEST(VariantJsonTest, CurrentConfigIsSnapshottedAndExplicitOptionsAreValidated) {
    const JsonToVariantOptions options = JsonToVariantOptions::current_config();
    EXPECT_EQ(options.max_json_key_length,
              static_cast<uint32_t>(config::variant_max_json_key_length));
    EXPECT_EQ(options.throw_on_invalid_json, config::variant_throw_exeception_on_invalid_json);
    EXPECT_EQ(options.check_duplicate_json_path, config::variant_enable_duplicate_json_path_check);

    JsonToVariantOptions invalid;
    invalid.max_json_key_length = 0;
    expect_exception_code(ErrorCode::INVALID_ARGUMENT,
                          [&] { JsonStringToVariantEncoder encoder(invalid); });
}

TEST(VariantJsonTest, StaticDispatchWritesDirectlyToBufferWritable) {
    VariantBatchBuilder block = encode_jsons({R"({"a":1,"b":[true,"x"]})"});
    auto column = ColumnString::create();
    BufferWritable writer(*column);
    to_json(block.value_at(0), writer);
    writer.commit();
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(column->get_data_at(0), string_ref(R"({"a":1,"b":[true,"x"]})"));
}

TEST(VariantJsonTest, ExternalMalformedValueReturnsExplicitErrors) {
    OwnedValue string_value = build_value(
            [](VariantBatchBuilder::Row& builder) { builder.add_string(string_ref("valid")); });
    string_value.value.back() = static_cast<char>(0xFF);
    expect_exception_code(ErrorCode::CORRUPTION,
                          [&] { static_cast<void>(print_json(string_value.ref())); });

    OwnedValue trailing =
            build_value([](VariantBatchBuilder::Row& builder) { builder.add_int(1); });
    trailing.value.push_back('\0');
    expect_exception_code(ErrorCode::CORRUPTION,
                          [&] { static_cast<void>(print_json(trailing.ref())); });

    const std::string two_nulls(2, '\0');
    const OwnedValue reverse_keys = raw_object_value({"b", "a"}, false, {0, 1}, {0, 1}, two_nulls);
    expect_exception_code(ErrorCode::CORRUPTION,
                          [&] { static_cast<void>(print_json(reverse_keys.ref())); });

    const OwnedValue duplicate_key = raw_object_value({"a", "b"}, true, {0, 0}, {0, 1}, two_nulls);
    expect_exception_code(ErrorCode::CORRUPTION,
                          [&] { static_cast<void>(print_json(duplicate_key.ref())); });

    std::string physical_values;
    physical_values.push_back(static_cast<char>(
            static_cast<uint8_t>(VariantPrimitiveId::FALSE_VALUE) << VARIANT_VALUE_HEADER_SHIFT));
    physical_values.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE)
                                                << VARIANT_VALUE_HEADER_SHIFT));
    const OwnedValue nonmonotonic_offsets =
            raw_object_value({"a", "b"}, true, {0, 1}, {1, 0}, std::move(physical_values));
    EXPECT_EQ(print_json(nonmonotonic_offsets.ref()), R"({"a":true,"b":false})");
}

struct Mb1Workload {
    const char* name;
    std::string json;
    uint32_t rows;
};

std::array<Mb1Workload, 5> make_mb1_workloads() {
    std::string narrow = "{";
    for (uint32_t index = 0; index < 8; ++index) {
        if (index != 0) {
            narrow.push_back(',');
        }
        narrow += "\"k" + std::to_string(index) + "\":" + std::to_string(index);
    }
    narrow.push_back('}');

    std::string wide = "{";
    for (uint32_t index = 0; index < 1000; ++index) {
        if (index != 0) {
            wide.push_back(',');
        }
        wide += "\"wide_" + std::to_string(index) + "\":" + std::to_string(index);
    }
    wide.push_back('}');

    std::string deep = "1";
    for (uint32_t depth = 0; depth < 8; ++depth) {
        deep = "{\"d" + std::to_string(depth) + "\":" + deep + "}";
    }

    std::string array_heavy = "[";
    for (uint32_t index = 0; index < 100; ++index) {
        if (index != 0) {
            array_heavy.push_back(',');
        }
        array_heavy += std::to_string(index);
    }
    array_heavy.push_back(']');

    return {{{.name = "narrow-8", .json = std::move(narrow), .rows = 20'000},
             {.name = "wide-1000", .json = std::move(wide), .rows = 1'000},
             {.name = "deep-8", .json = std::move(deep), .rows = 16'000},
             {.name = "array-100", .json = std::move(array_heavy), .rows = 16'000},
             {.name = "mixed",
              .json = R"({"id":7,"ok":true,"s":"text","a":[1,null,{"x":2.5}]})",
              .rows = 12'536}}};
}

template <typename ParseBlock>
std::pair<double, size_t> measure_mb1(const std::array<Mb1Workload, 5>& workloads,
                                      ParseBlock&& parse_block) {
    constexpr uint32_t BLOCK_SIZE = 128;
    size_t checksum = 0;
    const auto start = std::chrono::steady_clock::now();
    for (const Mb1Workload& workload : workloads) {
        for (uint32_t begin = 0; begin < workload.rows; begin += BLOCK_SIZE) {
            const uint32_t count = std::min(BLOCK_SIZE, workload.rows - begin);
            checksum += parse_block(workload.json, count);
        }
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start);
    return {elapsed.count(), checksum};
}

// MB1 is intentionally disabled in normal UT runs. The milestone command executes this exact test
// under RELEASE and records elapsed seconds, rows/s, and ratios; it is evidence, not a threshold.
TEST(VariantJsonTest, DISABLED_MB1Initial) {
    const auto workloads = make_mb1_workloads();
    uint64_t row_count = 0;
    for (const Mb1Workload& workload : workloads) {
        row_count += workload.rows;
    }
    ASSERT_EQ(row_count, 65'536);
    const auto rows = static_cast<double>(row_count);

    const auto [new_seconds, new_checksum] =
            measure_mb1(workloads, [](const std::string& json, uint32_t count) {
                JsonStringToVariantEncoder encoder;
                for (uint32_t row = 0; row < count; ++row) {
                    encoder.add_json(StringRef(json));
                }
                VariantBatchBuilder block = encoder.finish_batch();
                return block.value_bytes().size + block.metadata_ref().size;
            });

    const auto [jsonb_seconds, jsonb_checksum] =
            measure_mb1(workloads, [](const std::string& json, uint32_t count) {
                JsonBinaryValue jsonb;
                size_t bytes = 0;
                for (uint32_t row = 0; row < count; ++row) {
                    const Status status = jsonb.from_json_string(json);
                    if (!status.ok()) {
                        throw Exception(status);
                    }
                    bytes += jsonb.size();
                }
                return bytes;
            });

    const auto [old_seconds, old_checksum] =
            measure_mb1(workloads, [](const std::string& json, uint32_t count) {
                auto strings = ColumnString::create();
                for (uint32_t row = 0; row < count; ++row) {
                    strings->insert_data(json.data(), json.size());
                }
                auto variant = ColumnVariant::create(0, true);
                ParseConfig config;
                config.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
                variant_util::parse_json_to_variant(*variant, *strings, config);
                return variant->rows();
            });

    EXPECT_GT(new_checksum, 0);
    EXPECT_GT(jsonb_checksum, 0);
    EXPECT_GT(old_checksum, 0);
    std::cout << "MB1 RELEASE rows=" << row_count << " new_seconds=" << new_seconds
              << " new_rows_per_second=" << rows / new_seconds << " jsonb_seconds=" << jsonb_seconds
              << " new_throughput_over_jsonb=" << jsonb_seconds / new_seconds
              << " old_parse_seconds=" << old_seconds
              << " new_throughput_over_old_parse=" << old_seconds / new_seconds << std::endl;
}

} // namespace
} // namespace doris
