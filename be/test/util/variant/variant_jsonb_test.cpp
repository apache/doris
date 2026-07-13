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

#include "util/variant/variant_jsonb.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <limits>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "util/jsonb_writer.h"
#include "util/variant/variant_builder.h"
#include "util/variant/variant_canonical.h"
#include "variant_test_utils.h"

namespace doris {
namespace {

struct OwnedVariant {
    std::string metadata;
    std::string value;

    VariantValueRef ref() const {
        return {.metadata = {.data = metadata.data(), .size = metadata.size()},
                .data = value.data(),
                .size = value.size()};
    }
};

StringRef string_ref(std::string_view value) {
    return {value.data(), value.size()};
}

template <typename Function>
void expect_jsonb_exception_code(int code, Function&& function) {
    try {
        function();
        FAIL() << "Expected doris::Exception";
    } catch (const Exception& exception) {
        EXPECT_EQ(exception.code(), code) << exception.what();
    }
}

std::string jsonb_bytes(JsonbWriter& writer) {
    return {writer.getOutput()->getBuffer(), static_cast<size_t>(writer.getOutput()->getSize())};
}

OwnedVariant encode_jsonb(std::string_view document) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    jsonb_to_variant(string_ref(document), builder);
    metadata.seal();
    std::string value;
    builder.finish_row(value);
    validate_canonical(
            {.metadata = metadata.metadata_ref(), .data = value.data(), .size = value.size()});
    const StringRef encoded_metadata = metadata.encoded_metadata();
    return {.metadata = std::string(encoded_metadata.data, encoded_metadata.size),
            .value = std::move(value)};
}

std::string block_value_bytes(const VariantEncodedBlock& block) {
    const StringRef bytes = block.value_bytes();
    return {bytes.data, bytes.size};
}

std::vector<uint32_t> block_value_offsets(const VariantEncodedBlock& block) {
    const std::span<const uint32_t> offsets = block.value_offsets();
    return {offsets.begin(), offsets.end()};
}

void expect_jsonb_failure_and_aborted( // NOLINT(readability-function-cognitive-complexity) --
        std::string_view document) {   // GoogleTest exception macros inflate the metric.
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    EXPECT_THROW(jsonb_to_variant(string_ref(document), builder), Exception);
    EXPECT_NO_THROW(metadata.seal());
    std::string encoded;
    EXPECT_THROW(builder.finish_row(encoded), Exception);
}

template <typename T>
void append_pod(std::string& bytes, const T& value) {
    bytes.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

std::string nested_jsonb_arrays(uint32_t count) {
    std::string value(1, static_cast<char>(JsonbType::T_Null));
    for (uint32_t index = 0; index < count; ++index) {
        std::string parent;
        parent.push_back(static_cast<char>(JsonbType::T_Array));
        const auto size = static_cast<uint32_t>(value.size());
        append_pod(parent, size);
        parent.append(value);
        value = std::move(parent);
    }
    value.insert(value.begin(), static_cast<char>(JSONB_VER));
    return value;
}

__int128 power_of_ten_128(uint8_t exponent) {
    __int128 value = 1;
    for (uint8_t digit = 0; digit < exponent; ++digit) {
        value *= 10;
    }
    return value;
}

wide::Int256 power_of_ten_256(uint8_t exponent) {
    wide::Int256 value = 1;
    for (uint8_t digit = 0; digit < exponent; ++digit) {
        value *= 10;
    }
    return value;
}

template <typename Fill>
OwnedVariant build_variant(Fill&& fill) {
    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    fill(builder);
    metadata.seal();
    std::string value;
    builder.finish_row(value);
    validate_canonical(
            {.metadata = metadata.metadata_ref(), .data = value.data(), .size = value.size()});
    const StringRef encoded_metadata = metadata.encoded_metadata();
    return {.metadata = std::string(encoded_metadata.data, encoded_metadata.size),
            .value = std::move(value)};
}

std::string encode_variant_jsonb(VariantValueRef value, const VariantJsonFormatOptions& options =
                                                                VariantJsonFormatOptions {}) {
    JsonbWriter writer;
    variant_to_jsonb(value, writer, options);
    return jsonb_bytes(writer);
}

const JsonbValue* jsonb_root(const std::string& document) {
    const JsonbDocument* parsed = nullptr;
    const Status status =
            JsonbDocument::checkAndCreateDocument(document.data(), document.size(), &parsed);
    if (!status.ok()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Failed to inspect generated JSONB: {}",
                        status.to_string());
    }
    return parsed->getValue();
}

std::string_view jsonb_blob(const JsonbValue* value) {
    const auto* blob = value->unpack<JsonbBinaryVal>();
    return {blob->getBlob(), blob->getBlobLen()};
}

OwnedVariant nested_variant_arrays(uint32_t count) {
    return build_variant([&](VariantBuilder& builder) {
        std::vector<VariantBuilder::ArrayScope> scopes;
        scopes.reserve(count);
        for (uint32_t index = 0; index < count; ++index) {
            scopes.push_back(builder.start_array());
        }
        builder.add_null();
        for (auto& scope : std::ranges::reverse_view(scopes)) {
            scope.finish();
        }
    });
}

TEST(VariantJsonbTest, JsonbNullToVariant) {
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeNull());
    EXPECT_TRUE(encode_jsonb(jsonb_bytes(writer)).ref().is_null());
    EXPECT_TRUE(encode_jsonb({}).ref().is_null());
}

TEST(VariantJsonbTest, AllJsonbTypesMapToCanonicalVariant) {
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartArray());
    ASSERT_TRUE(writer.writeNull());
    ASSERT_TRUE(writer.writeBool(true));
    ASSERT_TRUE(writer.writeBool(false));
    ASSERT_TRUE(writer.writeInt8(7));
    ASSERT_TRUE(writer.writeInt16(300));
    ASSERT_TRUE(writer.writeInt32(70'000));
    ASSERT_TRUE(writer.writeInt64(5'000'000'000));
    ASSERT_TRUE(writer.writeDouble(-1.25));
    ASSERT_TRUE(writer.writeStartString());
    const std::string string_with_nul("a\0", 2);
    ASSERT_TRUE(writer.writeString(string_with_nul.data(), string_with_nul.size()));
    ASSERT_TRUE(writer.writeEndString());
    ASSERT_TRUE(writer.writeStartBinary());
    const std::string binary("\0\xFF", 2);
    ASSERT_TRUE(writer.writeBinary(binary.data(), binary.size()));
    ASSERT_TRUE(writer.writeEndBinary());
    ASSERT_TRUE(writer.writeStartObject());
    ASSERT_TRUE(writer.writeKey("k", 1));
    ASSERT_TRUE(writer.writeNull());
    ASSERT_TRUE(writer.writeEndObject());
    ASSERT_TRUE(writer.writeStartArray());
    ASSERT_TRUE(writer.writeEndArray());
    ASSERT_TRUE(writer.writeInt128(power_of_ten_128(20)));
    ASSERT_TRUE(writer.writeFloat(1.5F));
    ASSERT_TRUE(writer.writeDecimal(Decimal32 {123}, 3, 38));
    ASSERT_TRUE(writer.writeDecimal(Decimal64 {12'345'678'901}, 11, 38));
    ASSERT_TRUE(writer.writeDecimal(Decimal128V3 {power_of_ten_128(20)}, 21, 6));
    ASSERT_TRUE(writer.writeDecimal(Decimal256 {wide::Int256(12'345)}, 5, 3));
    ASSERT_TRUE(writer.writeEndArray());

    const OwnedVariant encoded = encode_jsonb(jsonb_bytes(writer));
    const VariantValueRef root = encoded.ref();
    ASSERT_EQ(root.num_elements(), 18);
    EXPECT_TRUE(root.array_at(0).is_null());
    EXPECT_TRUE(root.array_at(1).get_bool());
    EXPECT_FALSE(root.array_at(2).get_bool());
    EXPECT_EQ(root.array_at(3).primitive_id(), VariantPrimitiveId::INT8);
    EXPECT_EQ(root.array_at(4).primitive_id(), VariantPrimitiveId::INT16);
    EXPECT_EQ(root.array_at(5).primitive_id(), VariantPrimitiveId::INT32);
    EXPECT_EQ(root.array_at(6).primitive_id(), VariantPrimitiveId::INT64);
    EXPECT_DOUBLE_EQ(root.array_at(7).get_double(), -1.25);
    EXPECT_EQ(root.array_at(8).get_string(), StringRef(string_with_nul));
    EXPECT_EQ(root.array_at(9).get_binary(), StringRef(binary));
    VariantValueRef object_child;
    ASSERT_TRUE(root.array_at(10).object_find(string_ref("k"), &object_child));
    EXPECT_TRUE(object_child.is_null());
    EXPECT_EQ(root.array_at(11).num_elements(), 0);
    EXPECT_EQ(root.array_at(12).get_decimal(), (VariantDecimal {power_of_ten_128(20), 0, 16}));
    EXPECT_FLOAT_EQ(root.array_at(13).get_float(), 1.5F);
    EXPECT_EQ(root.array_at(14).get_decimal(), (VariantDecimal {123, 38, 4}));
    EXPECT_EQ(root.array_at(15).get_decimal(), (VariantDecimal {12'345'678'901, 38, 8}));
    EXPECT_EQ(root.array_at(16).get_decimal(), (VariantDecimal {power_of_ten_128(20), 6, 16}));
    EXPECT_EQ(root.array_at(17).get_string(), string_ref("12.345"));

    const OwnedVariant semantic_roundtrip = encode_jsonb(encode_variant_jsonb(root));
    EXPECT_TRUE(canonical_equals(root, semantic_roundtrip.ref()));
}

TEST(VariantJsonbTest, Int128BoundariesUseDecimal16OrStringFallback) {
    const __int128 maximum_decimal = power_of_ten_128(38) - 1;
    const __int128 first_fallback = power_of_ten_128(38);
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartArray());
    for (__int128 value :
         {maximum_decimal, -maximum_decimal, first_fallback, -first_fallback,
          std::numeric_limits<__int128>::max(), std::numeric_limits<__int128>::min()}) {
        ASSERT_TRUE(writer.writeInt128(value));
    }
    ASSERT_TRUE(writer.writeEndArray());

    const OwnedVariant encoded = encode_jsonb(jsonb_bytes(writer));
    const VariantValueRef root = encoded.ref();
    EXPECT_EQ(root.array_at(0).get_decimal(), (VariantDecimal {maximum_decimal, 0, 16}));
    EXPECT_EQ(root.array_at(1).get_decimal(), (VariantDecimal {-maximum_decimal, 0, 16}));
    EXPECT_EQ(root.array_at(2).get_string(), string_ref("100000000000000000000000000000000000000"));
    EXPECT_EQ(root.array_at(3).get_string(),
              string_ref("-100000000000000000000000000000000000000"));
    EXPECT_EQ(root.array_at(4).get_string(), string_ref("170141183460469231731687303715884105727"));
    EXPECT_EQ(root.array_at(5).get_string(),
              string_ref("-170141183460469231731687303715884105728"));
}

TEST(VariantJsonbTest, // NOLINT(readability-function-cognitive-complexity) --
     DecimalValidationAndDecimal256Fallback) { // GoogleTest assertions inflate the metric.
    JsonbWriter valid;
    ASSERT_TRUE(valid.writeStartArray());
    ASSERT_TRUE(valid.writeDecimal(Decimal32 {1}, 1, 38));
    ASSERT_TRUE(valid.writeDecimal(Decimal64 {1}, 1, 38));
    ASSERT_TRUE(valid.writeDecimal(Decimal128V3 {-1}, 1, 38));
    ASSERT_TRUE(valid.writeDecimal(Decimal256 {wide::Int256(-1)}, 1, 76));
    ASSERT_TRUE(valid.writeEndArray());
    const OwnedVariant encoded = encode_jsonb(jsonb_bytes(valid));
    EXPECT_EQ(encoded.ref().array_at(0).get_decimal(), (VariantDecimal {1, 38, 4}));
    EXPECT_EQ(encoded.ref().array_at(1).get_decimal(), (VariantDecimal {1, 38, 8}));
    EXPECT_EQ(encoded.ref().array_at(2).get_decimal(), (VariantDecimal {-1, 38, 16}));
    EXPECT_EQ(encoded.ref().array_at(3).get_string(),
              string_ref("-0."
                         "0000000000000000000000000000000000000000000000000000000000000000000000000"
                         "001"));

    const auto expect_invalid_decimal32 = [](int32_t value, uint32_t precision, uint32_t scale) {
        JsonbWriter writer;
        EXPECT_TRUE(writer.writeDecimal(Decimal32 {value}, precision, scale));
        expect_jsonb_failure_and_aborted(jsonb_bytes(writer));
    };
    expect_invalid_decimal32(1, 0, 0);
    expect_invalid_decimal32(1, 10, 0);
    expect_invalid_decimal32(100, 2, 0);
    expect_invalid_decimal32(1, 1, 39);

    const auto expect_invalid_decimal64 = [](int64_t value, uint32_t precision, uint32_t scale) {
        JsonbWriter writer;
        EXPECT_TRUE(writer.writeDecimal(Decimal64 {value}, precision, scale));
        expect_jsonb_failure_and_aborted(jsonb_bytes(writer));
    };
    expect_invalid_decimal64(1, 0, 0);
    expect_invalid_decimal64(1, 19, 0);
    expect_invalid_decimal64(100, 2, 0);
    expect_invalid_decimal64(1, 1, 39);

    const auto expect_invalid_decimal128 = [](__int128 value, uint32_t precision, uint32_t scale) {
        JsonbWriter writer;
        EXPECT_TRUE(writer.writeDecimal(Decimal128V3 {value}, precision, scale));
        expect_jsonb_failure_and_aborted(jsonb_bytes(writer));
    };
    expect_invalid_decimal128(1, 0, 0);
    expect_invalid_decimal128(1, 39, 0);
    expect_invalid_decimal128(100, 2, 0);
    expect_invalid_decimal128(1, 1, 39);

    const wide::Int256 maximum_decimal256 = power_of_ten_256(76) - 1;
    JsonbWriter maximum_decimal256_writer;
    ASSERT_TRUE(maximum_decimal256_writer.writeDecimal(Decimal256 {maximum_decimal256}, 76, 38));
    const OwnedVariant maximum_encoded = encode_jsonb(jsonb_bytes(maximum_decimal256_writer));
    EXPECT_EQ(maximum_encoded.ref().get_string(),
              StringRef(std::string(38, '9') + "." + std::string(38, '9')));

    const auto expect_invalid_decimal256 = [](const wide::Int256& value, uint32_t precision,
                                              uint32_t scale) {
        JsonbWriter writer;
        EXPECT_TRUE(writer.writeDecimal(Decimal256 {value}, precision, scale));
        expect_jsonb_failure_and_aborted(jsonb_bytes(writer));
    };
    expect_invalid_decimal256(power_of_ten_256(76), 76, 0);
    expect_invalid_decimal256(wide::Int256(1), 77, 0);
    expect_invalid_decimal256(wide::Int256(100), 2, 0);
    expect_invalid_decimal256(wide::Int256(1), 1, 77);

    JsonbWriter negative_maximum_decimal256_writer;
    ASSERT_TRUE(negative_maximum_decimal256_writer.writeDecimal(Decimal256 {-maximum_decimal256},
                                                                76, 76));
    const OwnedVariant negative_maximum_encoded =
            encode_jsonb(jsonb_bytes(negative_maximum_decimal256_writer));
    EXPECT_EQ(negative_maximum_encoded.ref().get_string(),
              StringRef(std::string("-0.") + std::string(76, '9')));
}

TEST(VariantJsonbTest, // NOLINT(readability-function-cognitive-complexity) --
     FixedWidthRootTruncationAndNullPointerFail) { // GoogleTest assertions inflate the metric.
    std::vector<std::string> documents;
    const auto add_document = [&](auto&& write) {
        JsonbWriter writer;
        ASSERT_TRUE(write(writer));
        documents.push_back(jsonb_bytes(writer));
    };
    add_document([](JsonbWriter& writer) { return writer.writeInt8(1); });
    add_document([](JsonbWriter& writer) { return writer.writeInt16(1); });
    add_document([](JsonbWriter& writer) { return writer.writeInt32(1); });
    add_document([](JsonbWriter& writer) { return writer.writeInt64(1); });
    add_document([](JsonbWriter& writer) { return writer.writeInt128(1); });
    add_document([](JsonbWriter& writer) { return writer.writeFloat(1.0F); });
    add_document([](JsonbWriter& writer) { return writer.writeDouble(1.0); });
    add_document([](JsonbWriter& writer) { return writer.writeDecimal(Decimal32 {1}, 1, 0); });
    add_document([](JsonbWriter& writer) { return writer.writeDecimal(Decimal64 {1}, 1, 0); });
    add_document([](JsonbWriter& writer) { return writer.writeDecimal(Decimal128V3 {1}, 1, 0); });
    add_document([](JsonbWriter& writer) {
        return writer.writeDecimal(Decimal256 {wide::Int256(1)}, 1, 0);
    });

    for (const std::string& document : documents) {
        for (size_t size = 1; size < document.size(); ++size) {
            expect_jsonb_failure_and_aborted(std::string_view(document.data(), size));
        }
    }

    VariantMetadataBuilder metadata;
    VariantBuilder builder(metadata);
    EXPECT_THROW(jsonb_to_variant({static_cast<const char*>(nullptr), 1}, builder), Exception);
    EXPECT_NO_THROW(metadata.seal());
    std::string encoded;
    EXPECT_THROW(builder.finish_row(encoded), Exception);
}

TEST(VariantJsonbTest, ContainersEmptyKeyAndMaximumKeyLength) {
    const std::string maximum_key(255, 'k');
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartObject());
    ASSERT_TRUE(writer.writeKey("", 0));
    ASSERT_TRUE(writer.writeStartObject());
    ASSERT_TRUE(writer.writeEndObject());
    ASSERT_TRUE(writer.writeKey(maximum_key.data(), static_cast<uint8_t>(maximum_key.size())));
    ASSERT_TRUE(writer.writeStartArray());
    ASSERT_TRUE(writer.writeNull());
    ASSERT_TRUE(writer.writeEndArray());
    ASSERT_TRUE(writer.writeEndObject());

    const OwnedVariant encoded = encode_jsonb(jsonb_bytes(writer));
    VariantValueRef child;
    ASSERT_TRUE(encoded.ref().object_find(string_ref(""), &child));
    EXPECT_EQ(child.num_elements(), 0);
    ASSERT_TRUE(encoded.ref().object_find(StringRef(maximum_key), &child));
    ASSERT_EQ(child.num_elements(), 1);
    EXPECT_TRUE(child.array_at(0).is_null());
}

TEST(VariantJsonbTest, ExternalDictionaryIdsAndDuplicateKeysFail) {
    JsonbWriter dictionary_key;
    ASSERT_TRUE(dictionary_key.writeStartObject());
    ASSERT_TRUE(dictionary_key.writeKey(JsonbKeyValue::keyid_type {42}));
    ASSERT_TRUE(dictionary_key.writeNull());
    ASSERT_TRUE(dictionary_key.writeEndObject());
    expect_jsonb_failure_and_aborted(jsonb_bytes(dictionary_key));

    JsonbWriter duplicate;
    ASSERT_TRUE(duplicate.writeStartObject());
    ASSERT_TRUE(duplicate.writeKey("a", 1));
    ASSERT_TRUE(duplicate.writeNull());
    ASSERT_TRUE(duplicate.writeKey("a", 1));
    ASSERT_TRUE(duplicate.writeBool(true));
    ASSERT_TRUE(duplicate.writeEndObject());
    expect_jsonb_failure_and_aborted(jsonb_bytes(duplicate));
}

TEST(VariantJsonbTest, JsonbContainerDepthUsesWriterLimit) {
    EXPECT_NO_THROW(static_cast<void>(encode_jsonb(nested_jsonb_arrays(MaxNestingLevel))));
    expect_jsonb_failure_and_aborted(nested_jsonb_arrays(MaxNestingLevel + 1));
}

TEST(VariantJsonbTest, // NOLINT(readability-function-cognitive-complexity) --
     MalformedAndTruncatedDocumentsFailDeepValidation) { // GoogleTest assertions inflate metric.
    JsonbWriter string_writer;
    ASSERT_TRUE(string_writer.writeStartString());
    ASSERT_TRUE(string_writer.writeString("value", 5));
    ASSERT_TRUE(string_writer.writeEndString());
    JsonbWriter binary_writer;
    ASSERT_TRUE(binary_writer.writeStartBinary());
    ASSERT_TRUE(binary_writer.writeBinary("value", 5));
    ASSERT_TRUE(binary_writer.writeEndBinary());
    JsonbWriter object_writer;
    ASSERT_TRUE(object_writer.writeStartObject());
    ASSERT_TRUE(object_writer.writeKey("key", 3));
    ASSERT_TRUE(object_writer.writeInt64(1));
    ASSERT_TRUE(object_writer.writeEndObject());
    JsonbWriter decimal_writer;
    ASSERT_TRUE(decimal_writer.writeDecimal(Decimal256 {wide::Int256(123)}, 3, 2));

    for (const std::string& valid : {jsonb_bytes(string_writer), jsonb_bytes(binary_writer),
                                     jsonb_bytes(object_writer), jsonb_bytes(decimal_writer)}) {
        for (size_t size = 1; size < valid.size(); ++size) {
            expect_jsonb_failure_and_aborted(std::string_view(valid.data(), size));
        }
    }

    std::string bad_version = jsonb_bytes(string_writer);
    bad_version[0] = 2;
    expect_jsonb_failure_and_aborted(bad_version);
    std::string unknown_type {static_cast<char>(JSONB_VER), static_cast<char>(0x7F)};
    expect_jsonb_failure_and_aborted(unknown_type);
    std::string trailing = jsonb_bytes(string_writer);
    trailing.push_back('\0');
    expect_jsonb_failure_and_aborted(trailing);

    JsonbWriter invalid_utf8_string;
    ASSERT_TRUE(invalid_utf8_string.writeStartString());
    const char invalid_byte = static_cast<char>(0xFF);
    ASSERT_TRUE(invalid_utf8_string.writeString(&invalid_byte, 1));
    ASSERT_TRUE(invalid_utf8_string.writeEndString());
    expect_jsonb_failure_and_aborted(jsonb_bytes(invalid_utf8_string));

    JsonbWriter invalid_utf8_key;
    ASSERT_TRUE(invalid_utf8_key.writeStartObject());
    ASSERT_TRUE(invalid_utf8_key.writeKey(&invalid_byte, 1));
    ASSERT_TRUE(invalid_utf8_key.writeNull());
    ASSERT_TRUE(invalid_utf8_key.writeEndObject());
    expect_jsonb_failure_and_aborted(jsonb_bytes(invalid_utf8_key));
}

TEST(VariantJsonbTest, // NOLINT(readability-function-cognitive-complexity) --
     AllVariantPrimitiveTypesMapDirectlyToJsonb) { // GoogleTest assertions inflate the metric.
    std::array<uint8_t, 16> uuid {};
    for (uint8_t index = 0; index < uuid.size(); ++index) {
        uuid[index] = index;
    }
    const std::string binary("\0\x01\x02\xFF", 4);
    const std::string long_string(64, 's');
    const OwnedVariant encoded = build_variant([&](VariantBuilder& builder) {
        auto array = builder.start_array();
        builder.add_null();
        builder.add_bool(true);
        builder.add_bool(false);
        builder.add_int(-128);
        builder.add_int(-129);
        builder.add_int(32'768);
        builder.add_int(2'147'483'648LL);
        builder.add_double(1.25);
        builder.add_decimal(-12'345, 2, 4);
        builder.add_decimal(1'000'000'000, 3, 8);
        builder.add_decimal(static_cast<__int128>(1'000'000'000'000'000'000LL), 0, 16);
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
        builder.add_string(string_ref("short"));
        array.finish();
    });

    const std::string document = encode_variant_jsonb(encoded.ref());
    const JsonbValue* root = jsonb_root(document);
    ASSERT_EQ(root->type, JsonbType::T_Array);
    const auto* array = root->unpack<ArrayVal>();
    ASSERT_EQ(array->numElem(), 22);
    const std::array<JsonbType, 22> expected_types {
            JsonbType::T_Null,      JsonbType::T_True,       JsonbType::T_False,
            JsonbType::T_Int8,      JsonbType::T_Int16,      JsonbType::T_Int32,
            JsonbType::T_Int64,     JsonbType::T_Double,     JsonbType::T_Decimal32,
            JsonbType::T_Decimal64, JsonbType::T_Decimal128, JsonbType::T_String,
            JsonbType::T_String,    JsonbType::T_String,     JsonbType::T_Float,
            JsonbType::T_Binary,    JsonbType::T_String,     JsonbType::T_String,
            JsonbType::T_String,    JsonbType::T_String,     JsonbType::T_String,
            JsonbType::T_String,
    };
    for (uint32_t index = 0; index < expected_types.size(); ++index) {
        EXPECT_EQ(array->get(index)->type, expected_types[index]) << index;
    }

    const auto* decimal4 = array->get(8)->unpack<JsonbDecimal32>();
    EXPECT_EQ(decimal4->precision, 9);
    EXPECT_EQ(decimal4->scale, 2);
    EXPECT_EQ(decimal4->val(), -12'345);
    const auto* decimal8 = array->get(9)->unpack<JsonbDecimal64>();
    EXPECT_EQ(decimal8->precision, 18);
    EXPECT_EQ(decimal8->scale, 3);
    EXPECT_EQ(decimal8->val(), 1'000'000'000);
    const auto* decimal16 = array->get(10)->unpack<JsonbDecimal128>();
    EXPECT_EQ(decimal16->precision, 38);
    EXPECT_EQ(decimal16->scale, 0);
    EXPECT_EQ(decimal16->val(), static_cast<__int128>(1'000'000'000'000'000'000LL));

    EXPECT_EQ(jsonb_blob(array->get(11)), "1970-01-01");
    EXPECT_EQ(jsonb_blob(array->get(12)), "1970-01-01 00:00:00.000000+00:00");
    EXPECT_EQ(jsonb_blob(array->get(13)), "1969-12-31 23:59:59.999999");
    EXPECT_EQ(jsonb_blob(array->get(15)), std::string_view(binary));
    EXPECT_EQ(jsonb_blob(array->get(16)), std::string_view(long_string));
    EXPECT_EQ(jsonb_blob(array->get(17)), "00:00:00.000001");
    EXPECT_EQ(jsonb_blob(array->get(18)), "1969-12-31 23:59:59.999999999+00:00");
    EXPECT_EQ(jsonb_blob(array->get(19)), "1969-12-31 23:59:59.999999999");
    EXPECT_EQ(jsonb_blob(array->get(20)), "00010203-0405-0607-0809-0a0b0c0d0e0f");
    EXPECT_EQ(jsonb_blob(array->get(21)), "short");
}

TEST(VariantJsonbTest, TimestampFallbackUsesSharedTimezoneContract) {
    const OwnedVariant encoded = build_variant([](VariantBuilder& builder) {
        auto array = builder.start_array();
        builder.add_timestamp_micros(-1, true);
        builder.add_timestamp_micros(-1, false);
        builder.add_timestamp_nanos(-1, true);
        builder.add_timestamp_nanos(-1, false);
        array.finish();
    });
    const cctz::time_zone timezone = cctz::fixed_time_zone(std::chrono::seconds(19'800));
    const VariantJsonFormatOptions options {.timezone = &timezone};
    const std::string document = encode_variant_jsonb(encoded.ref(), options);
    const auto* array = jsonb_root(document)->unpack<ArrayVal>();
    EXPECT_EQ(jsonb_blob(array->get(0)), "1970-01-01 05:29:59.999999+05:30");
    EXPECT_EQ(jsonb_blob(array->get(1)), "1969-12-31 23:59:59.999999");
    EXPECT_EQ(jsonb_blob(array->get(2)), "1970-01-01 05:29:59.999999999+05:30");
    EXPECT_EQ(jsonb_blob(array->get(3)), "1969-12-31 23:59:59.999999999");
}

TEST(VariantJsonbTest, DecimalScale38OutputIsAcceptedByJsonbReader) {
    const OwnedVariant source = build_variant([](VariantBuilder& builder) {
        auto array = builder.start_array();
        builder.add_decimal(1, 38, 4);
        builder.add_decimal(-1, 38, 8);
        builder.add_decimal(42, 0, 16);
        array.finish();
    });
    const OwnedVariant roundtrip = encode_jsonb(encode_variant_jsonb(source.ref()));
    EXPECT_EQ(roundtrip.ref().array_at(0).get_decimal(), (VariantDecimal {1, 38, 4}));
    EXPECT_EQ(roundtrip.ref().array_at(1).get_decimal(), (VariantDecimal {-1, 38, 8}));
    EXPECT_EQ(roundtrip.ref().array_at(2).get_decimal(), (VariantDecimal {42, 0, 16}));
}

TEST(VariantJsonbTest, Int128AndDecimal256RoundTripUseDocumentedFallbackTypes) {
    JsonbWriter small_int128;
    ASSERT_TRUE(small_int128.writeInt128(42));
    const OwnedVariant small_variant = encode_jsonb(jsonb_bytes(small_int128));
    const std::string small_document = encode_variant_jsonb(small_variant.ref());
    const JsonbValue* small_root = jsonb_root(small_document);
    ASSERT_EQ(small_root->type, JsonbType::T_Decimal128);
    const auto* small_decimal = small_root->unpack<JsonbDecimal128>();
    EXPECT_EQ(small_decimal->precision, 38);
    EXPECT_EQ(small_decimal->scale, 0);
    EXPECT_EQ(small_decimal->val(), 42);

    JsonbWriter large_int128;
    ASSERT_TRUE(large_int128.writeInt128(std::numeric_limits<__int128>::max()));
    const OwnedVariant large_variant = encode_jsonb(jsonb_bytes(large_int128));
    const std::string large_document = encode_variant_jsonb(large_variant.ref());
    const JsonbValue* large_root = jsonb_root(large_document);
    ASSERT_EQ(large_root->type, JsonbType::T_String);
    EXPECT_EQ(jsonb_blob(large_root), "170141183460469231731687303715884105727");

    JsonbWriter decimal256;
    ASSERT_TRUE(decimal256.writeDecimal(Decimal256 {wide::Int256(-12'345)}, 5, 3));
    const OwnedVariant decimal_variant = encode_jsonb(jsonb_bytes(decimal256));
    const std::string decimal_document = encode_variant_jsonb(decimal_variant.ref());
    const JsonbValue* decimal_root = jsonb_root(decimal_document);
    ASSERT_EQ(decimal_root->type, JsonbType::T_String);
    EXPECT_EQ(jsonb_blob(decimal_root), "-12.345");
}

TEST(VariantJsonbTest, VariantEmptyContainersAndJsonbKeyBoundaries) {
    const std::string maximum_key(255, 'm');
    const OwnedVariant source = build_variant([&](VariantBuilder& builder) {
        auto root = builder.start_array();
        auto empty_object = builder.start_object();
        empty_object.finish();
        auto empty_array = builder.start_array();
        empty_array.finish();
        auto object = builder.start_object();
        object.add_key(string_ref(""));
        builder.add_null();
        object.add_key(StringRef(maximum_key));
        builder.add_bool(true);
        object.finish();
        root.finish();
    });

    const std::string document = encode_variant_jsonb(source.ref());
    const auto* root = jsonb_root(document)->unpack<ArrayVal>();
    ASSERT_EQ(root->numElem(), 3);
    EXPECT_EQ(root->get(0)->unpack<ObjectVal>()->numElem(), 0);
    EXPECT_EQ(root->get(1)->unpack<ArrayVal>()->numElem(), 0);
    const auto* object = root->get(2)->unpack<ObjectVal>();
    auto field = object->begin();
    ASSERT_NE(field, object->end());
    EXPECT_EQ(field->klen(), 0);
    EXPECT_EQ(field->getKeyId(), std::numeric_limits<JsonbKeyValue::keyid_type>::max());
    EXPECT_TRUE(field->value()->isNull());
    ++field;
    ASSERT_NE(field, object->end());
    EXPECT_EQ(field->klen(), 255);
    EXPECT_EQ(std::string_view(field->getKeyStr(), field->klen()), maximum_key);
    EXPECT_TRUE(field->value()->isTrue());
}

TEST(VariantJsonbTest, LegalNonCanonicalVariantObjectConvertsWithoutCanonicalAssumptions) {
    const std::string metadata {char {0x01}, char {0x02}, char {0x00}, char {0x01},
                                char {0x02}, 'b',         'a'};
    const std::string value {
            char {0x02},
            char {0x02},
            char {0x01},
            char {0x00},
            char {0x01},
            char {0x00},
            char {0x02},
            char {static_cast<uint8_t>(VariantPrimitiveId::FALSE_VALUE) << 2},
            char {static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE) << 2},
    };
    const VariantValueRef ref {.metadata = {.data = metadata.data(), .size = metadata.size()},
                               .data = value.data(),
                               .size = value.size()};
    EXPECT_FALSE(ref.metadata.sorted_strings());
    const std::string document = encode_variant_jsonb(ref);
    const auto* object = jsonb_root(document)->unpack<ObjectVal>();
    auto field = object->begin();
    ASSERT_NE(field, object->end());
    EXPECT_EQ(std::string_view(field->getKeyStr(), field->klen()), "a");
    EXPECT_TRUE(field->value()->isTrue());
    ++field;
    ASSERT_NE(field, object->end());
    EXPECT_EQ(std::string_view(field->getKeyStr(), field->klen()), "b");
    EXPECT_TRUE(field->value()->isFalse());
    ++field;
    EXPECT_EQ(field, object->end());
}

TEST(VariantJsonbTest, VariantContainerDepthUsesWriterLimit) {
    const OwnedVariant maximum = nested_variant_arrays(MaxNestingLevel);
    EXPECT_NO_THROW(static_cast<void>(encode_variant_jsonb(maximum.ref())));

    const OwnedVariant too_deep = nested_variant_arrays(MaxNestingLevel + 1);
    JsonbWriter writer;
    EXPECT_THROW(variant_to_jsonb(too_deep.ref(), writer), Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);
}

TEST(VariantJsonbTest, // NOLINT(readability-function-cognitive-complexity) --
     VariantFailuresResetWriterAndRejectUnrepresentableKeys) { // GoogleTest macros inflate metric.
    const OwnedVariant valid = build_variant([](VariantBuilder& builder) { builder.add_null(); });
    OwnedVariant trailing = valid;
    trailing.value.push_back('\0');

    JsonbWriter writer;
    ASSERT_TRUE(writer.writeBool(true));
    EXPECT_THROW(variant_to_jsonb(trailing.ref(), writer), Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);
    EXPECT_NO_THROW(variant_to_jsonb(valid.ref(), writer));
    EXPECT_TRUE(jsonb_root(jsonb_bytes(writer))->isNull());

    const std::string oversized_key(256, 'k');
    const OwnedVariant oversized = build_variant([&](VariantBuilder& builder) {
        auto object = builder.start_object();
        object.add_key(StringRef(oversized_key));
        builder.add_null();
        object.finish();
    });
    EXPECT_THROW(variant_to_jsonb(oversized.ref(), writer), Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);

    const OwnedVariant invalid_time =
            build_variant([](VariantBuilder& builder) { builder.add_time_ntz_micros(-1); });
    EXPECT_THROW(variant_to_jsonb(invalid_time.ref(), writer), Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);
}

TEST(VariantJsonbTest, MalformedVariantStringsAndDecimalsFailExplicitly) {
    VariantMetadataBuilder empty_metadata;
    empty_metadata.seal();

    std::string invalid_string;
    invalid_string.push_back(
            static_cast<char>((1U << VARIANT_VALUE_HEADER_SHIFT) |
                              static_cast<uint8_t>(VariantBasicType::SHORT_STRING)));
    invalid_string.push_back(static_cast<char>(0xFF));
    JsonbWriter writer;
    EXPECT_THROW(variant_to_jsonb({empty_metadata.metadata_ref(), invalid_string.data(),
                                   invalid_string.size()},
                                  writer),
                 Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);

    std::string invalid_decimal;
    invalid_decimal.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::DECIMAL4)
                                                << VARIANT_VALUE_HEADER_SHIFT));
    invalid_decimal.push_back('\0');
    const int32_t ten_digit_value = 1'000'000'000;
    append_pod(invalid_decimal, ten_digit_value);
    EXPECT_THROW(variant_to_jsonb({empty_metadata.metadata_ref(), invalid_decimal.data(),
                                   invalid_decimal.size()},
                                  writer),
                 Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);

    const std::string object_metadata {char {0x11}, char {0x02}, char {0x00}, char {0x01},
                                       char {0x02}, 'a',         'b'};
    const std::string duplicate_key_object {
            char {0x02}, char {0x02}, char {0x00}, char {0x00}, char {0x00},
            char {0x01}, char {0x02}, char {0x00}, char {0x00},
    };
    EXPECT_THROW(variant_to_jsonb({{object_metadata.data(), object_metadata.size()},
                                   duplicate_key_object.data(),
                                   duplicate_key_object.size()},
                                  writer),
                 Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);

    const std::string null_value(1, '\0');
    const std::string truncated_metadata(1, '\x01');
    ASSERT_TRUE(writer.writeBool(true));
    EXPECT_THROW(variant_to_jsonb({{truncated_metadata.data(), truncated_metadata.size()},
                                   null_value.data(),
                                   null_value.size()},
                                  writer),
                 Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);

    const OwnedVariant valid_array = build_variant([](VariantBuilder& builder) {
        auto array = builder.start_array();
        builder.add_null();
        array.finish();
    });
    OwnedVariant truncated_array = valid_array;
    truncated_array.value.pop_back();
    ASSERT_TRUE(writer.writeBool(true));
    EXPECT_THROW(variant_to_jsonb(truncated_array.ref(), writer), Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);

    const std::string reverse_key_object {
            char {0x02}, char {0x02}, char {0x01}, char {0x00}, char {0x00},
            char {0x01}, char {0x02}, char {0x00}, char {0x04},
    };
    ASSERT_TRUE(writer.writeBool(true));
    EXPECT_THROW(variant_to_jsonb({{object_metadata.data(), object_metadata.size()},
                                   reverse_key_object.data(),
                                   reverse_key_object.size()},
                                  writer),
                 Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), 0);
}

TEST(VariantJsonbBlockTest, ExplicitNullPlaceholderProducesOneNullRow) {
    JsonbToVariantEncoder encoder({.rows = 1, .scalar_bytes = 1, .nodes = 1});
    encoder.add_null();
    VariantEncodedBlock block = encoder.finish_block();
    ASSERT_EQ(block.num_rows(), 1);
    EXPECT_TRUE(block.value_at(0).is_null());
    EXPECT_EQ(block.metadata_ref().dict_size(), 0);
}

TEST(VariantJsonbBlockTest, RealJsonbAndEmptyDocumentsProduceNullRows) {
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeNull());
    const std::string document = jsonb_bytes(writer);

    JsonbToVariantEncoder encoder;
    encoder.add_jsonb(StringRef(document));
    encoder.add_jsonb(StringRef {});
    VariantEncodedBlock block = encoder.finish_block();
    ASSERT_EQ(block.num_rows(), 2);
    EXPECT_TRUE(block.value_at(0).is_null());
    EXPECT_TRUE(block.value_at(1).is_null());
    EXPECT_EQ(block.value_at(0).metadata.data, block.value_at(1).metadata.data);
    EXPECT_EQ(block.metadata_ref().dict_size(), 0);
}

TEST(VariantJsonbBlockTest, EmptyEncoderProducesMinimalEmptyBlock) {
    JsonbToVariantEncoder encoder(VariantBlockBuilder::ReserveHint {});
    VariantEncodedBlock block = encoder.finish_block();
    const std::string expected_metadata("\x11\0\0", 3);
    EXPECT_EQ(std::string(block.metadata_ref().data, block.metadata_ref().size), expected_metadata);
    EXPECT_EQ(block_value_bytes(block), "");
    EXPECT_EQ(block_value_offsets(block), (std::vector<uint32_t> {0}));
    EXPECT_EQ(block.num_rows(), 0);
}

TEST(VariantJsonbBlockTest, AddCopiesBorrowedDocumentBeforeCallerMutatesIt) {
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartObject());
    ASSERT_TRUE(writer.writeKey("key", 3));
    ASSERT_TRUE(writer.writeStartString());
    ASSERT_TRUE(writer.writeString("value", 5));
    ASSERT_TRUE(writer.writeEndString());
    ASSERT_TRUE(writer.writeEndObject());
    std::string document = jsonb_bytes(writer);
    const OwnedVariant expected = encode_jsonb(document);

    JsonbToVariantEncoder encoder({.rows = 1,
                                   .metadata_keys = 1,
                                   .scalar_bytes = document.size(),
                                   .nodes = 2,
                                   .containers = 1,
                                   .children = 1});
    encoder.add_jsonb(StringRef(document));
    document.assign(document.size(), '\0');
    VariantEncodedBlock block = encoder.finish_block();
    ASSERT_EQ(block.num_rows(), 1);
    EXPECT_EQ(std::string(block.metadata_ref().data, block.metadata_ref().size), expected.metadata);
    EXPECT_EQ(block_value_bytes(block), expected.value);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate this matrix.
TEST(VariantJsonbBlockTest, SharesDictionaryAcrossObjectsArraysAndNullRows) {
    JsonbWriter first_writer;
    ASSERT_TRUE(first_writer.writeStartObject());
    ASSERT_TRUE(first_writer.writeKey("z", 1));
    ASSERT_TRUE(first_writer.writeInt8(1));
    ASSERT_TRUE(first_writer.writeEndObject());
    const std::string first = jsonb_bytes(first_writer);

    JsonbWriter second_writer;
    ASSERT_TRUE(second_writer.writeStartArray());
    ASSERT_TRUE(second_writer.writeNull());
    ASSERT_TRUE(second_writer.writeStartObject());
    ASSERT_TRUE(second_writer.writeKey("a", 1));
    ASSERT_TRUE(second_writer.writeBool(true));
    ASSERT_TRUE(second_writer.writeKey("z", 1));
    ASSERT_TRUE(second_writer.writeInt16(2));
    ASSERT_TRUE(second_writer.writeEndObject());
    ASSERT_TRUE(second_writer.writeEndArray());
    const std::string second = jsonb_bytes(second_writer);

    JsonbWriter null_writer;
    ASSERT_TRUE(null_writer.writeNull());
    const std::string null_document = jsonb_bytes(null_writer);

    JsonbToVariantEncoder encoder;
    encoder.add_jsonb(StringRef(first));
    encoder.add_jsonb(StringRef(second));
    encoder.add_jsonb(StringRef(null_document));
    VariantEncodedBlock block = encoder.finish_block();
    ASSERT_EQ(block.num_rows(), 3);
    ASSERT_EQ(block.metadata_ref().dict_size(), 2);
    EXPECT_EQ(block.metadata_ref().key_at(0), string_ref("a"));
    EXPECT_EQ(block.metadata_ref().key_at(1), string_ref("z"));
    EXPECT_EQ(block.value_at(0).metadata.data, block.value_at(1).metadata.data);
    EXPECT_EQ(block.value_at(1).metadata.data, block.value_at(2).metadata.data);
    std::vector<VariantValueRef> rows;
    rows.reserve(block.num_rows());
    for (size_t row = 0; row < block.num_rows(); ++row) {
        rows.push_back(block.value_at(row));
    }
    validate_canonical(block.metadata_ref(), rows);

    VariantValueRef child;
    ASSERT_TRUE(block.value_at(0).object_find(string_ref("z"), &child));
    EXPECT_EQ(child.get_int(), 1);
    ASSERT_EQ(block.value_at(1).num_elements(), 2);
    EXPECT_TRUE(block.value_at(1).array_at(0).is_null());
    ASSERT_TRUE(block.value_at(1).array_at(1).object_find(string_ref("a"), &child));
    EXPECT_TRUE(child.get_bool());
    EXPECT_TRUE(block.value_at(2).is_null());
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate this matrix.
TEST(VariantJsonbBlockTest, SingleCellOutputMatchesTheExistingPerRowConverter) {
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartArray());
    ASSERT_TRUE(writer.writeInt64(5'000'000'000));
    ASSERT_TRUE(writer.writeStartString());
    ASSERT_TRUE(writer.writeString("value", 5));
    ASSERT_TRUE(writer.writeEndString());
    ASSERT_TRUE(writer.writeStartObject());
    ASSERT_TRUE(writer.writeKey("key", 3));
    ASSERT_TRUE(writer.writeNull());
    ASSERT_TRUE(writer.writeEndObject());
    ASSERT_TRUE(writer.writeEndArray());
    const std::string document = jsonb_bytes(writer);
    const OwnedVariant expected = encode_jsonb(document);

    JsonbToVariantEncoder encoder;
    encoder.add_jsonb(StringRef(document));
    VariantEncodedBlock block = encoder.finish_block();
    ASSERT_EQ(block.num_rows(), 1);
    EXPECT_EQ(std::string(block.metadata_ref().data, block.metadata_ref().size), expected.metadata);
    EXPECT_EQ(block_value_bytes(block), expected.value);
    EXPECT_EQ(block_value_offsets(block),
              (std::vector<uint32_t> {0, static_cast<uint32_t>(expected.value.size())}));
    expect_jsonb_exception_code(ErrorCode::INVALID_ARGUMENT,
                                [&] { encoder.add_jsonb(StringRef(document)); });
    expect_jsonb_exception_code(ErrorCode::INVALID_ARGUMENT,
                                [&] { static_cast<void>(encoder.finish_block()); });
}

TEST(VariantJsonbBlockTest, InvalidDuplicateAndDeepInputsAreTerminal) {
    const std::string bad_version(1, static_cast<char>(JSONB_VER + 1));
    JsonbToVariantEncoder bad_version_encoder;
    expect_jsonb_exception_code(ErrorCode::CORRUPTION,
                                [&] { bad_version_encoder.add_jsonb(StringRef(bad_version)); });
    expect_jsonb_exception_code(ErrorCode::INVALID_ARGUMENT,
                                [&] { static_cast<void>(bad_version_encoder.finish_block()); });

    JsonbWriter duplicate_writer;
    ASSERT_TRUE(duplicate_writer.writeStartObject());
    ASSERT_TRUE(duplicate_writer.writeKey("a", 1));
    ASSERT_TRUE(duplicate_writer.writeNull());
    ASSERT_TRUE(duplicate_writer.writeKey("a", 1));
    ASSERT_TRUE(duplicate_writer.writeBool(true));
    ASSERT_TRUE(duplicate_writer.writeEndObject());
    const std::string duplicate = jsonb_bytes(duplicate_writer);
    JsonbToVariantEncoder duplicate_encoder;
    expect_jsonb_exception_code(ErrorCode::INVALID_ARGUMENT,
                                [&] { duplicate_encoder.add_jsonb(StringRef(duplicate)); });
    expect_jsonb_exception_code(ErrorCode::INVALID_ARGUMENT,
                                [&] { duplicate_encoder.add_jsonb(StringRef {}); });

    const std::string too_deep = nested_jsonb_arrays(MaxNestingLevel + 1);
    JsonbToVariantEncoder deep_encoder;
    expect_jsonb_exception_code(ErrorCode::INVALID_ARGUMENT,
                                [&] { deep_encoder.add_jsonb(StringRef(too_deep)); });
    expect_jsonb_exception_code(ErrorCode::INVALID_ARGUMENT,
                                [&] { static_cast<void>(deep_encoder.finish_block()); });
}

enum class JsonbBlockBenchmarkPath : uint8_t { OLD_PER_ROW, SHARED_BLOCK };

struct JsonbBlockBenchmarkResult {
    double seconds = 0;
    uint64_t checksum = 0;
};

uint64_t run_jsonb_block_benchmark_path(JsonbBlockBenchmarkPath path, StringRef document,
                                        uint32_t rows) {
    if (path == JsonbBlockBenchmarkPath::OLD_PER_ROW) {
        VariantMetadataBuilder metadata;
        std::deque<VariantBuilder> builders;
        for (uint32_t row = 0; row < rows; ++row) {
            builders.emplace_back(metadata);
            jsonb_to_variant(document, builders.back());
        }
        metadata.seal();
        std::string values;
        for (VariantBuilder& builder : builders) {
            builder.finish_row(values);
        }
        return values.size() + metadata.encoded_metadata().size;
    }

    const size_t row_count = rows;
    JsonbToVariantEncoder encoder({.rows = rows,
                                   .metadata_keys = 9,
                                   .scalar_bytes = document.size * row_count,
                                   .nodes = 112 * row_count,
                                   .containers = 2 * row_count,
                                   .children = 109 * row_count});
    for (uint32_t row = 0; row < rows; ++row) {
        encoder.add_jsonb(document);
    }
    VariantEncodedBlock block = encoder.finish_block();
    return block.value_bytes().size + block.metadata_ref().size;
}

JsonbBlockBenchmarkResult measure_jsonb_block_benchmark(JsonbBlockBenchmarkPath path,
                                                        StringRef document, uint32_t rows) {
    static_cast<void>(run_jsonb_block_benchmark_path(path, document, 128));
    const auto start = std::chrono::steady_clock::now();
    JsonbBlockBenchmarkResult result;
    result.checksum = run_jsonb_block_benchmark_path(path, document, rows);
    result.seconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    return result;
}

std::string make_jsonb_block_benchmark_document() {
    JsonbWriter writer;
    EXPECT_TRUE(writer.writeStartObject());
    for (uint8_t index = 0; index < 8; ++index) {
        const std::string key = "key_" + std::to_string(index);
        EXPECT_TRUE(writer.writeKey(key.data(), static_cast<uint8_t>(key.size())));
        EXPECT_TRUE(writer.writeInt32(index));
    }
    EXPECT_TRUE(writer.writeKey("array", 5));
    EXPECT_TRUE(writer.writeStartArray());
    for (int32_t value = 0; value < 100; ++value) {
        EXPECT_TRUE(writer.writeInt32(value));
    }
    EXPECT_TRUE(writer.writeEndArray());
    EXPECT_TRUE(writer.writeEndObject());
    return jsonb_bytes(writer);
}

// Run each disabled test as an exact filter in a fresh run-be-ut.sh process wrapped by
// /usr/bin/time -v: one unmeasured process warmup, then five measured processes. Compare median
// seconds and peak RSS, and require the printed checksums to match before applying the thresholds.
TEST(VariantJsonbBlockTest, DISABLED_OldPerRowJsonbBenchmark) {
    const std::string document = make_jsonb_block_benchmark_document();
    constexpr uint32_t ROWS = 65'536;
    const JsonbBlockBenchmarkResult result = measure_jsonb_block_benchmark(
            JsonbBlockBenchmarkPath::OLD_PER_ROW, StringRef(document), ROWS);
    EXPECT_GT(result.checksum, 0);
    std::cout << "JSONB old per-row RELEASE rows=" << ROWS << " seconds=" << result.seconds
              << " checksum=" << result.checksum << std::endl;
}

TEST(VariantJsonbBlockTest, DISABLED_SharedBlockJsonbBenchmark) {
    const std::string document = make_jsonb_block_benchmark_document();
    constexpr uint32_t ROWS = 65'536;
    const JsonbBlockBenchmarkResult result = measure_jsonb_block_benchmark(
            JsonbBlockBenchmarkPath::SHARED_BLOCK, StringRef(document), ROWS);
    EXPECT_GT(result.checksum, 0);
    std::cout << "JSONB shared block RELEASE rows=" << ROWS << " seconds=" << result.seconds
              << " checksum=" << result.checksum << std::endl;
}

} // namespace
} // namespace doris
