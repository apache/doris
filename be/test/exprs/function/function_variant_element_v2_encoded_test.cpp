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

#include <array>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "exprs/function/function_variant_element_v2.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_field.h"
#include "util/variant/variant_json.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

using Segment = VariantElementV2PathSegment;

VariantField encode_json(std::string_view json) {
    JsonToVariantEncoder encoder({.max_json_key_length = 1024,
                                  .throw_on_invalid_json = true,
                                  .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantEncodedBlock block = encoder.finish_block();
    return VariantField::encode(block.value_at(0));
}

void append_json(ColumnVariantV2& column, std::string_view json) {
    const VariantField field = encode_json(json);
    insert_encoded_field(column, field);
}

std::unique_ptr<ResolvedVariantElementV2Path> resolve(std::vector<Segment> segments) {
    std::unique_ptr<ResolvedVariantElementV2Path> result;
    Status status = resolve_variant_element_v2_path(segments, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

ColumnPtr extract(const ColumnVariantV2& source, const ResolvedVariantElementV2Path& path,
                  std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    Status status = extract_variant_element_v2(source, path, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

const ColumnNullable& nullable_result(const ColumnPtr& result) {
    return assert_cast<const ColumnNullable&>(*result);
}

const ColumnVariantV2& variant_result(const ColumnPtr& result) {
    return assert_cast<const ColumnVariantV2&>(nullable_result(result).get_nested_column());
}

std::string_view bytes(VariantMetadataRef metadata) {
    return {metadata.data, metadata.size};
}

void append_unsigned(std::string& output, uint64_t value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

VariantField legal_noncanonical_object() {
    std::string metadata;
    metadata.push_back(static_cast<char>(VARIANT_ENCODING_VERSION));
    append_unsigned(metadata, 3, 1);
    append_unsigned(metadata, 0, 1);
    append_unsigned(metadata, 1, 1);
    append_unsigned(metadata, 2, 1);
    append_unsigned(metadata, 8, 1);
    metadata.append("baunused");

    std::string value;
    value.push_back(static_cast<char>(VariantBasicType::OBJECT));
    append_unsigned(value, 2, 1);
    append_unsigned(value, 1, 1);
    append_unsigned(value, 0, 1);
    append_unsigned(value, 1, 1);
    append_unsigned(value, 0, 1);
    append_unsigned(value, 2, 1);
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));

    std::string field;
    append_unsigned(field, metadata.size(), sizeof(uint32_t));
    field.append(metadata);
    field.append(value);
    return VariantField::decode({field.data(), field.size()});
}

ColumnVariantV2::MutablePtr raw_column(StringRef metadata, StringRef value) {
    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(metadata.size)};
    const std::array<uint32_t, 2> value_offsets {0, static_cast<uint32_t>(value.size)};
    auto result = ColumnVariantV2::create();
    result->insert_encoded_rows({.metadata_bytes = metadata,
                                 .metadata_offsets = metadata_offsets,
                                 .meta_ids = {},
                                 .value_bytes = value,
                                 .value_offsets = value_offsets});
    return result;
}

} // namespace

TEST(VariantElementV2EncodedTest, InterleavedMetadataAndCacheIsPerCall) {
    auto source = ColumnVariantV2::create();
    append_json(*source, R"({"a":0,"target":1})");
    append_json(*source, R"({"other":2})");
    append_json(*source, R"({"b":0,"target":3})");
    append_json(*source, R"({"a":4,"target":5})");
    auto path = resolve({Segment::object_key(StringRef("target"))});

    ColumnPtr result = extract(*source, *path);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(nullable.get_null_map_data()[0], 0);
    EXPECT_EQ(values.get_value_ref(0).get_int(), 1);
    EXPECT_EQ(nullable.get_null_map_data()[1], 1);
    EXPECT_EQ(nullable.get_null_map_data()[2], 0);
    EXPECT_EQ(values.get_value_ref(2).get_int(), 3);
    EXPECT_EQ(values.get_value_ref(3).get_int(), 5);
    EXPECT_EQ(bytes(values.get_value_ref(0).metadata), bytes(source->get_value_ref(0).metadata));

    auto next_block = ColumnVariantV2::create();
    append_json(*next_block, R"({"different":7})");
    append_json(*next_block, R"({"target":9})");
    ColumnPtr next_result = extract(*next_block, *path);
    EXPECT_EQ(nullable_result(next_result).get_null_map_data()[0], 1);
    EXPECT_EQ(nullable_result(next_result).get_null_map_data()[1], 0);
    EXPECT_EQ(variant_result(next_result).get_value_ref(1).get_int(), 9);
}

TEST(VariantElementV2EncodedTest, ResolvedPathRejectsInvalidInputAndOwnsKeys) {
    std::string key = "target";
    auto path = resolve({Segment::object_key({key.data(), key.size()})});
    key.assign("changed");

    auto source = ColumnVariantV2::create();
    append_json(*source, R"({"target":7})");
    EXPECT_EQ(variant_result(extract(*source, *path)).get_value_ref(0).get_int(), 7);

    const ResolvedVariantElementV2Path* identity = path.get();
    Status status = resolve_variant_element_v2_path({}, &path);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(path.get(), identity);

    const std::array<Segment, 1> invalid_key {
            Segment::object_key({static_cast<const char*>(nullptr), 1})};
    status = resolve_variant_element_v2_path(invalid_key, &path);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(path.get(), identity);
}

TEST(VariantElementV2EncodedTest, ExplicitSegmentsCoverDeepDotAndArrayBounds) {
    auto source = ColumnVariantV2::create();
    append_json(*source, R"({"a.b":11,"a":{"b":{"c":{"d":22}}},"items":[0,{"v":33}]})");

    auto literal_dot = resolve({Segment::object_key(StringRef("a.b"))});
    EXPECT_EQ(variant_result(extract(*source, *literal_dot)).get_value_ref(0).get_int(), 11);

    auto deep = resolve({Segment::object_key(StringRef("a")), Segment::object_key(StringRef("b")),
                         Segment::object_key(StringRef("c")), Segment::object_key(StringRef("d"))});
    EXPECT_EQ(variant_result(extract(*source, *deep)).get_value_ref(0).get_int(), 22);

    auto first = resolve({Segment::object_key(StringRef("items")), Segment::array_index(0)});
    EXPECT_EQ(variant_result(extract(*source, *first)).get_value_ref(0).get_int(), 0);
    auto last = resolve({Segment::object_key(StringRef("items")), Segment::array_index(1),
                         Segment::object_key(StringRef("v"))});
    EXPECT_EQ(variant_result(extract(*source, *last)).get_value_ref(0).get_int(), 33);
    auto out_of_bounds =
            resolve({Segment::object_key(StringRef("items")), Segment::array_index(2)});
    EXPECT_EQ(nullable_result(extract(*source, *out_of_bounds)).get_null_map_data()[0], 1);
    auto from_end = resolve({Segment::object_key(StringRef("items")), Segment::array_index(-1),
                             Segment::object_key(StringRef("v"))});
    EXPECT_EQ(variant_result(extract(*source, *from_end)).get_value_ref(0).get_int(), 33);
    auto before_begin =
            resolve({Segment::object_key(StringRef("items")), Segment::array_index(-3)});
    EXPECT_EQ(nullable_result(extract(*source, *before_begin)).get_null_map_data()[0], 1);
}

TEST(VariantElementV2EncodedTest, OuterMissingAndPrimitiveNullAreDistinct) {
    auto source = ColumnVariantV2::create();
    append_json(*source, R"({"present":null})");
    append_json(*source, R"({"present":1})");
    append_json(*source, R"({"missing":2})");
    append_json(*source, "7");
    const std::array<uint8_t, 4> outer_nulls {0, 1, 0, 0};
    auto path = resolve({Segment::object_key(StringRef("present"))});

    ColumnPtr result = extract(*source, *path, outer_nulls);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(nullable.get_null_map_data()[0], 0);
    EXPECT_TRUE(values.get_value_ref(0).is_null());
    EXPECT_EQ(nullable.get_null_map_data()[1], 1);
    EXPECT_EQ(nullable.get_null_map_data()[2], 1);
    EXPECT_EQ(nullable.get_null_map_data()[3], 1);
}

TEST(VariantElementV2EncodedTest, LegalNoncanonicalMetadataIsCopiedWithoutCanonicalizing) {
    const VariantField field = legal_noncanonical_object();
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, field);
    auto path = resolve({Segment::object_key(StringRef("a"))});

    ColumnPtr result = extract(*source, *path);
    const VariantValueRef value = variant_result(result).get_value_ref(0);
    EXPECT_TRUE(value.get_bool());
    EXPECT_EQ(bytes(value.metadata), bytes(source->get_value_ref(0).metadata));
}

TEST(VariantElementV2EncodedTest, BadMetadataAndValueAreErrorsAndResultIsAtomic) {
    auto path = resolve({Segment::object_key(StringRef("a"))});
    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr result = sentinel->get_ptr();
    const IColumn* sentinel_identity = result.get();

    const std::string bad_metadata(1, static_cast<char>(0x11));
    const std::string null_value(1, 0);
    auto bad_metadata_column = raw_column({bad_metadata.data(), bad_metadata.size()},
                                          {null_value.data(), null_value.size()});
    Status status = extract_variant_element_v2(*bad_metadata_column, *path, {}, &result);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(result.get(), sentinel_identity);

    const VariantField valid = encode_json(R"({"a":1})");
    const std::string truncated_object(1, static_cast<char>(VariantBasicType::OBJECT));
    auto bad_value_column = raw_column({valid.ref().metadata.data, valid.ref().metadata.size},
                                       {truncated_object.data(), truncated_object.size()});
    status = extract_variant_element_v2(*bad_value_column, *path, {}, &result);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(result.get(), sentinel_identity);

    const VariantValueRef valid_ref = valid.ref();
    std::string trailing_value(valid_ref.data, valid_ref.size);
    trailing_value.push_back('\0');
    auto trailing_value_column = raw_column({valid_ref.metadata.data, valid_ref.metadata.size},
                                            {trailing_value.data(), trailing_value.size()});
    status = extract_variant_element_v2(*trailing_value_column, *path, {}, &result);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(result.get(), sentinel_identity);
}

TEST(VariantElementV2EncodedTest, SourceCowBytesRemainUnchanged) {
    auto source = ColumnVariantV2::create();
    append_json(*source, R"({"a":{"b":42}})");
    ColumnPtr shared = source->get_ptr();
    const VariantField before = VariantField::encode(source->get_value_ref(0));
    auto path = resolve({Segment::object_key(StringRef("a")), Segment::object_key(StringRef("b"))});

    ColumnPtr result = extract(*source, *path);
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 42);
    const VariantField after = VariantField::encode(source->get_value_ref(0));
    EXPECT_EQ(std::string_view(before.bytes().data, before.bytes().size),
              std::string_view(after.bytes().data, after.bytes().size));
    EXPECT_EQ(shared.get(), source.get());
}

} // namespace doris
