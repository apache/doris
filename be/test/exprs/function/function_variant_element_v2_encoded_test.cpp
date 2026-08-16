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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/value/variant/variant_canonical.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/function_variant_element_v2.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

using Segment = VariantElementV2PathSegment;

constexpr uint32_t pack_olap_date(uint32_t year, uint32_t month, uint32_t day) {
    return (year << 9) | (month << 5) | day;
}

VariantField encode_json(std::string_view json) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 1024,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder block = encoder.finish_batch();
    return VariantField::from_ref(block.value_at(0));
}

void append_json(ColumnVariantV2& column, std::string_view json) {
    const VariantField field = encode_json(json);
    insert_encoded_field(column, field);
}

ColumnVariantV2::MutablePtr encoded_shredded(std::span<const std::string_view> residual_json,
                                             std::span<const std::string_view> field_json,
                                             std::span<const uint8_t> presence,
                                             const std::vector<std::string>& field_path) {
    EXPECT_EQ(residual_json.size(), field_json.size());
    EXPECT_EQ(residual_json.size(), presence.size());
    auto residual = ColumnVariantV2::create();
    auto values = ColumnVariantV2::create();
    auto present = ColumnUInt8::create();
    for (size_t row = 0; row < residual_json.size(); ++row) {
        append_json(*residual, residual_json[row]);
        append_json(*values, field_json[row]);
        present->insert_value(presence[row]);
    }
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(field_path), std::move(values), std::move(present));
    return ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
}

ColumnVariantV2::MutablePtr wide_shredded_layout() {
    auto residual = ColumnVariantV2::create();
    append_json(*residual, "{}");
    ColumnVariantV2::ShreddedFields fields;
    auto add_field = [&](std::vector<std::string> path, int64_t value) {
        auto values = ColumnVariantV2::create();
        append_json(*values, std::to_string(value));
        auto presence = ColumnUInt8::create();
        presence->insert_value(1);
        fields.emplace_back(PathInData(path), std::move(values), std::move(presence));
    };
    add_field({"group", "a"}, 100);
    add_field({"group", "b"}, 101);
    for (size_t index = 0; index < 64; ++index) {
        add_field({"k" + std::to_string(1000 + index).substr(1)}, static_cast<int64_t>(index));
    }
    return ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
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

void expect_same_variant_result(const ColumnPtr& actual, const ColumnPtr& expected) {
    const auto& actual_nullable = nullable_result(actual);
    const auto& expected_nullable = nullable_result(expected);
    EXPECT_EQ(actual_nullable.get_null_map_data(), expected_nullable.get_null_map_data());
    const auto& actual_values = variant_result(actual);
    const auto& expected_values = variant_result(expected);
    ASSERT_TRUE(actual_values.is_encoded());
    ASSERT_TRUE(expected_values.is_encoded());
    ASSERT_EQ(actual_values.size(), expected_values.size());
    for (size_t row = 0; row < actual_values.size(); ++row) {
        if (actual_nullable.is_null_at(row)) {
            continue;
        }
        EXPECT_TRUE(canonical_equals(actual_values.get_value_ref(row),
                                     expected_values.get_value_ref(row)))
                << "row " << row;
    }
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
    return VariantField::from_bytes({field.data(), field.size()});
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
    const VariantRef value = variant_result(result).get_value_ref(0);
    EXPECT_TRUE(value.get_bool());
    EXPECT_EQ(bytes(value.metadata), bytes(source->get_value_ref(0).metadata));
}

TEST(VariantElementV2EncodedTest, SourceCowBytesRemainUnchanged) {
    auto source = ColumnVariantV2::create();
    append_json(*source, R"({"a":{"b":42}})");
    ColumnPtr shared = source->get_ptr();
    const VariantField before = VariantField::from_ref(source->get_value_ref(0));
    auto path = resolve({Segment::object_key(StringRef("a")), Segment::object_key(StringRef("b"))});

    ColumnPtr result = extract(*source, *path);
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 42);
    const VariantField after = VariantField::from_ref(source->get_value_ref(0));
    EXPECT_EQ(std::string_view(before.bytes().data, before.bytes().size),
              std::string_view(after.bytes().data, after.bytes().size));
    EXPECT_EQ(shared.get(), source.get());
}

TEST(VariantElementV2ShreddedTest, ExactPathPreservesTypeAndStructureConflicts) {
    constexpr std::array<std::string_view, 6> RESIDUAL {"{}", "{}", R"({"a":{"b":2}})",
                                                        "{}", "{}", "{}"};
    constexpr std::array<std::string_view, 6> VALUES {"1", R"("x")", "null", "null", "null", "3"};
    constexpr std::array<uint8_t, 6> PRESENCE {1, 1, 0, 0, 1, 1};
    auto source = encoded_shredded(RESIDUAL, VALUES, PRESENCE, {"a"});
    ASSERT_TRUE(source->is_shredded());
    ASSERT_TRUE(source->shredded_field_values(0).is_encoded());
    auto path = resolve({Segment::object_key(StringRef("a"))});
    constexpr std::array<uint8_t, 6> OUTER_NULLS {0, 0, 0, 0, 0, 1};

    ColumnPtr result = extract(*source, *path, OUTER_NULLS);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 0, 1, 0, 1}));
    EXPECT_EQ(values.get_value_ref(0).get_int(), 1);
    EXPECT_EQ(values.get_value_ref(1).get_string(), StringRef("x"));
    VariantRef object = values.get_value_ref(2);
    ASSERT_EQ(object.basic_type(), VariantBasicType::OBJECT);
    VariantRef b;
    ASSERT_TRUE(object.object_find(StringRef("b"), &b));
    EXPECT_EQ(b.get_int(), 2);
    EXPECT_TRUE(values.get_value_ref(4).is_null());
    EXPECT_TRUE(source->is_shredded());
    EXPECT_TRUE(source->shredded_field_values(0).is_encoded());
}

TEST(VariantElementV2ShreddedTest, ExactTopLevelTypedLeafSharesPhysicalChild) {
    auto residual = ColumnVariantV2::create();
    for (std::string_view json : {"{}", "{}", "{}", "{}"}) {
        append_json(*residual, json);
    }

    auto ints = ColumnInt64::create();
    auto typed_nulls = ColumnUInt8::create();
    for (const auto [value, is_null] :
         std::array<std::pair<int64_t, uint8_t>, 4> {{{4, 0}, {0, 1}, {0, 1}, {9, 0}}}) {
        ints->insert_value(value);
        typed_nulls->insert_value(is_null);
    }
    auto field_values = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(ints), std::move(typed_nulls)),
            std::make_shared<DataTypeInt64>());
    auto presence = ColumnUInt8::create();
    for (const auto present : std::array<uint8_t, 4> {1, 1, 0, 1}) {
        presence->insert_value(present);
    }
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a"}), std::move(field_values),
                        std::move(presence));
    auto source = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
    const ColumnVariantV2* const child_identity = &source->shredded_field_values(0);

    auto path = resolve({Segment::object_key(StringRef("a"))});
    constexpr std::array<uint8_t, 4> OUTER_NULLS {0, 0, 0, 1};
    ColumnPtr result = extract(*source, *path, OUTER_NULLS);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 1, 1}));
    ASSERT_TRUE(values.is_typed());
    EXPECT_EQ(&values, child_identity);
    const auto& typed = assert_cast<const ColumnNullable&>(values.typed_column());
    EXPECT_EQ(assert_cast<const ColumnInt64&>(typed.get_nested_column()).get_data()[0], 4);
    EXPECT_EQ(typed.get_null_map_data(), (NullMap {0, 1, 1, 0}));
}

TEST(VariantElementV2ShreddedTest, ExactEncodedLeafSharesPhysicalChildWithoutConflict) {
    constexpr std::array<std::string_view, 3> RESIDUAL {"{}", "{}", "{}"};
    constexpr std::array<std::string_view, 3> VALUES {"1", "null", "9"};
    constexpr std::array<uint8_t, 3> PRESENCE {1, 1, 0};
    auto source = encoded_shredded(RESIDUAL, VALUES, PRESENCE, {"a"});
    const ColumnVariantV2* const child_identity = &source->shredded_field_values(0);

    auto path = resolve({Segment::object_key(StringRef("a"))});
    ColumnPtr result = extract(*source, *path);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 1}));
    EXPECT_EQ(&values, child_identity);
    EXPECT_EQ(values.get_value_ref(0).get_int(), 1);
    EXPECT_TRUE(values.get_value_ref(1).is_null());
}

TEST(VariantElementV2ShreddedTest, ExactTypedLeafConflictFallsBackForWholeBatch) {
    auto residual = ColumnVariantV2::create();
    for (std::string_view json : {"{}", R"({"a":"x"})", R"({"a":{"b":2}})", "{}", "{}", "{}"}) {
        append_json(*residual, json);
    }

    auto ints = ColumnInt64::create();
    auto typed_nulls = ColumnUInt8::create();
    for (const auto [value, is_null] : std::array<std::pair<int64_t, uint8_t>, 6> {
                 {{7, 0}, {0, 1}, {0, 1}, {0, 1}, {0, 1}, {9, 0}}}) {
        ints->insert_value(value);
        typed_nulls->insert_value(is_null);
    }
    auto field_values = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(ints), std::move(typed_nulls)),
            std::make_shared<DataTypeInt64>());
    auto presence = ColumnUInt8::create();
    for (const auto present : std::array<uint8_t, 6> {1, 0, 0, 1, 0, 1}) {
        presence->insert_value(present);
    }
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a"}), std::move(field_values),
                        std::move(presence));
    auto source = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));

    auto path = resolve({Segment::object_key(StringRef("a"))});
    constexpr std::array<uint8_t, 6> OUTER_NULLS {0, 0, 0, 0, 0, 1};
    ColumnPtr result = extract(*source, *path, OUTER_NULLS);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 0, 0, 1, 1}));
    ASSERT_TRUE(values.is_encoded());
    EXPECT_EQ(values.get_value_ref(0).get_int(), 7);
    EXPECT_EQ(values.get_value_ref(1).get_string(), StringRef("x"));
    VariantRef object = values.get_value_ref(2);
    ASSERT_EQ(object.basic_type(), VariantBasicType::OBJECT);
    VariantRef b;
    ASSERT_TRUE(object.object_find(StringRef("b"), &b));
    EXPECT_EQ(b.get_int(), 2);
    EXPECT_TRUE(values.get_value_ref(3).is_null());
    EXPECT_TRUE(source->shredded_field_values(0).is_typed());
}

TEST(VariantElementV2ShreddedTest, TypedConflictSkipsInactiveInvalidDatePadding) {
    auto residual = ColumnVariantV2::create();
    append_json(*residual, "{}");
    append_json(*residual, R"({"a":"fallback"})");

    auto dates = ColumnDateV2::create();
    dates->insert_value(
            DateV2Value<DateV2ValueType>::create_from_olap_date(pack_olap_date(1970, 1, 2)));
    DateV2Value<DateV2ValueType> invalid_padding;
    dates->insert_value(invalid_padding);
    auto typed_nulls = ColumnUInt8::create();
    typed_nulls->insert_many_defaults(2);
    auto field_values = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(dates), std::move(typed_nulls)),
            std::make_shared<DataTypeDateV2>());
    auto presence = ColumnUInt8::create();
    presence->insert_value(1);
    presence->insert_value(0);
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a"}), std::move(field_values),
                        std::move(presence));
    auto source = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));

    auto path = resolve({Segment::object_key(StringRef("a"))});
    ColumnPtr result = extract(*source, *path);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0}));
    ASSERT_TRUE(values.is_encoded());
    EXPECT_EQ(values.get_value_ref(0).primitive_id(), VariantPrimitiveId::DATE);
    EXPECT_EQ(values.get_value_ref(1).get_string(), StringRef("fallback"));
    EXPECT_TRUE(source->shredded_field_values(0).is_typed());
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the ancestor-conflict matrix.
TEST(VariantElementV2ShreddedTest, NestedPathHandlesScalarAncestorAndAncestorProjection) {
    constexpr std::array<std::string_view, 4> RESIDUAL {"{}", R"({"a":9})", R"({"a":{}})", "{}"};
    auto residual = ColumnVariantV2::create();
    for (std::string_view json : RESIDUAL) {
        append_json(*residual, json);
    }

    auto ints = ColumnInt64::create();
    for (int64_t value : {7, 0, 0, 0}) {
        ints->insert_value(value);
    }
    auto typed_nulls = ColumnUInt8::create();
    for (const auto is_null : std::array<uint8_t, 4> {0, 1, 1, 1}) {
        typed_nulls->insert_value(is_null);
    }
    auto field_values = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(ints), std::move(typed_nulls)),
            std::make_shared<DataTypeInt64>());
    auto presence = ColumnUInt8::create();
    for (const auto present : std::array<uint8_t, 4> {1, 0, 0, 1}) {
        presence->insert_value(present);
    }
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a", "b"}), std::move(field_values),
                        std::move(presence));
    auto source = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
    ASSERT_TRUE(source->is_shredded());
    ASSERT_TRUE(source->shredded_field_values(0).is_typed());
    const ColumnVariantV2* const child_identity = &source->shredded_field_values(0);

    auto exact =
            resolve({Segment::object_key(StringRef("a")), Segment::object_key(StringRef("b"))});
    ColumnPtr exact_result = extract(*source, *exact);
    const auto& exact_nullable = nullable_result(exact_result);
    const auto& exact_values = variant_result(exact_result);
    EXPECT_EQ(exact_nullable.get_null_map_data(), (NullMap {0, 1, 1, 0}));
    ASSERT_TRUE(exact_values.is_typed());
    EXPECT_EQ(&exact_values, child_identity);
    const auto& exact_typed = assert_cast<const ColumnNullable&>(exact_values.typed_column());
    EXPECT_EQ(assert_cast<const ColumnInt64&>(exact_typed.get_nested_column()).get_data()[0], 7);
    EXPECT_EQ(exact_typed.get_null_map_data(), (NullMap {0, 1, 1, 1}));

    auto ancestor = resolve({Segment::object_key(StringRef("a"))});
    ColumnPtr ancestor_result = extract(*source, *ancestor);
    const auto& ancestor_nullable = nullable_result(ancestor_result);
    const auto& ancestor_values = variant_result(ancestor_result);
    EXPECT_EQ(ancestor_nullable.get_null_map_data(), (NullMap {0, 0, 0, 0}));
    ASSERT_TRUE(ancestor_values.is_shredded());
    ASSERT_EQ(ancestor_values.shredded_field_count(), 1);
    EXPECT_EQ(ancestor_values.shredded_field_path(0).get_parts(),
              PathInData(std::vector<std::string> {"b"}).get_parts());
    auto materialized_ancestor =
            ancestor_values.materialize_encoded_range(0, ancestor_values.size());
    VariantRef row0 = materialized_ancestor->get_value_ref(0);
    ASSERT_EQ(row0.basic_type(), VariantBasicType::OBJECT);
    VariantRef row0_b;
    ASSERT_TRUE(row0.object_find(StringRef("b"), &row0_b));
    EXPECT_EQ(row0_b.get_int(), 7);
    EXPECT_EQ(materialized_ancestor->get_value_ref(1).get_int(), 9);
    ASSERT_EQ(materialized_ancestor->get_value_ref(2).basic_type(), VariantBasicType::OBJECT);
    EXPECT_EQ(materialized_ancestor->get_value_ref(2).num_elements(), 0);
    VariantRef row3 = materialized_ancestor->get_value_ref(3);
    ASSERT_EQ(row3.basic_type(), VariantBasicType::OBJECT);
    VariantRef row3_b;
    ASSERT_TRUE(row3.object_find(StringRef("b"), &row3_b));
    EXPECT_TRUE(row3_b.is_null());
    EXPECT_TRUE(source->is_shredded());
    EXPECT_TRUE(source->shredded_field_values(0).is_typed());
}

TEST(VariantElementV2ShreddedTest, AncestorProjectionKeepsMultipleExactPrefixDescendants) {
    auto residual = ColumnVariantV2::create();
    append_json(*residual, R"({"a":{"base":1}})");
    append_json(*residual, R"({"ab":{"base":2}})");

    auto make_values = [](int64_t first, int64_t second) {
        auto values = ColumnVariantV2::create();
        append_json(*values, std::to_string(first));
        append_json(*values, std::to_string(second));
        return values;
    };
    auto make_presence = [](uint8_t first, uint8_t second) {
        auto presence = ColumnUInt8::create();
        presence->insert_value(first);
        presence->insert_value(second);
        return presence;
    };
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a", "b"}), make_values(7, 0),
                        make_presence(1, 0));
    fields.emplace_back(PathInData(std::vector<std::string> {"a", "c"}), make_values(8, 0),
                        make_presence(1, 0));
    fields.emplace_back(PathInData(std::vector<std::string> {"ab", "b"}), make_values(9, 10),
                        make_presence(1, 1));
    auto source = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
    const ColumnVariantV2* const first_values = &source->shredded_field_values(0);
    const ColumnUInt8* const first_presence = &source->shredded_field_presence(0);
    const ColumnVariantV2* const second_values = &source->shredded_field_values(1);
    const ColumnUInt8* const second_presence = &source->shredded_field_presence(1);

    auto ancestor = resolve({Segment::object_key(StringRef("a"))});
    ColumnPtr result = extract(*source, *ancestor);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 1}));
    ASSERT_TRUE(values.is_shredded());
    ASSERT_EQ(values.shredded_field_count(), 2);
    EXPECT_EQ(values.shredded_field_path(0).get_parts(),
              PathInData(std::vector<std::string> {"b"}).get_parts());
    EXPECT_EQ(values.shredded_field_path(1).get_parts(),
              PathInData(std::vector<std::string> {"c"}).get_parts());
    EXPECT_EQ(&values.shredded_field_values(0), first_values);
    EXPECT_EQ(&values.shredded_field_presence(0), first_presence);
    EXPECT_EQ(&values.shredded_field_values(1), second_values);
    EXPECT_EQ(&values.shredded_field_presence(1), second_presence);

    auto encoded = values.materialize_encoded_range(0, values.size());
    VariantRef row0 = encoded->get_value_ref(0);
    VariantRef field;
    ASSERT_TRUE(row0.object_find(StringRef("base"), &field));
    EXPECT_EQ(field.get_int(), 1);
    ASSERT_TRUE(row0.object_find(StringRef("b"), &field));
    EXPECT_EQ(field.get_int(), 7);
    ASSERT_TRUE(row0.object_find(StringRef("c"), &field));
    EXPECT_EQ(field.get_int(), 8);
    EXPECT_FALSE(row0.object_find(StringRef("ab"), &field));
    EXPECT_TRUE(source->is_shredded());
    EXPECT_EQ(source->shredded_field_count(), 3);

    constexpr std::array<uint8_t, 2> OUTER_NULLS {1, 0};
    ColumnPtr masked_result = extract(*source, *ancestor, OUTER_NULLS);
    const auto& masked_nullable = nullable_result(masked_result);
    EXPECT_EQ(masked_nullable.get_null_map_data(), (NullMap {1, 1}));
    const auto& masked_values = variant_result(masked_result);
    ASSERT_TRUE(masked_values.is_shredded());
    EXPECT_EQ(&masked_values.shredded_field_values(0), first_values);
    EXPECT_EQ(&masked_values.shredded_field_presence(0), first_presence);
    EXPECT_EQ(&masked_values.shredded_field_values(1), second_values);
    EXPECT_EQ(&masked_values.shredded_field_presence(1), second_presence);
}

TEST(VariantElementV2ShreddedTest, NonExactFallbackMatchesEncodedOracleAndPreservesSource) {
    constexpr std::array<std::string_view, 4> RESIDUAL {"{}", R"({"a":{"b":{"c":11}}})",
                                                        R"([{"x":12},13])", R"({"u":14})"};
    constexpr std::array<std::string_view, 4> VALUES {"7", "null", "null", "null"};
    constexpr std::array<uint8_t, 4> PRESENCE {1, 0, 0, 0};
    auto source = encoded_shredded(RESIDUAL, VALUES, PRESENCE, {"a", "b"});
    ASSERT_TRUE(source->is_shredded());

    struct PathCase {
        std::string_view name;
        std::vector<Segment> segments;
    };
    const std::array<PathCase, 3> paths {
            PathCase {"descendant",
                      {Segment::object_key(StringRef("a")), Segment::object_key(StringRef("b")),
                       Segment::object_key(StringRef("c"))}},
            PathCase {"array", {Segment::array_index(0), Segment::object_key(StringRef("x"))}},
            PathCase {"unrelated", {Segment::object_key(StringRef("u"))}}};
    for (const auto& path_case : paths) {
        SCOPED_TRACE(path_case.name);
        auto path = resolve(path_case.segments);
        auto encoded_oracle = source->materialize_encoded_range(0, source->size());
        ColumnPtr expected = extract(*encoded_oracle, *path);
        ColumnPtr actual = extract(*source, *path);
        expect_same_variant_result(actual, expected);
        EXPECT_TRUE(source->is_shredded());
        EXPECT_TRUE(source->shredded_field_values(0).is_encoded());
    }
}

TEST(VariantElementV2ShreddedTest, SortedLayoutLookupIsLogarithmicAndReturnsContiguousRange) {
    auto source = wide_shredded_layout();
    ASSERT_EQ(source->shredded_field_count(), 66);

    VariantElementV2TestAccess::reset_shredded_path_inspections();
    auto exact_path = resolve({Segment::object_key(StringRef("k063"))});
    ColumnPtr exact = extract(*source, *exact_path);
    EXPECT_EQ(nullable_result(exact).get_null_map_data(), (NullMap {0}));
    EXPECT_EQ(variant_result(exact).get_value_ref(0).get_int(), 63);
    EXPECT_LE(VariantElementV2TestAccess::shredded_path_inspections(), 9);

    VariantElementV2TestAccess::reset_shredded_path_inspections();
    auto ancestor_path = resolve({Segment::object_key(StringRef("group"))});
    ColumnPtr ancestor = extract(*source, *ancestor_path);
    EXPECT_EQ(nullable_result(ancestor).get_null_map_data(), (NullMap {0}));
    const auto& ancestor_values = variant_result(ancestor);
    ASSERT_TRUE(ancestor_values.is_shredded());
    ASSERT_EQ(ancestor_values.shredded_field_count(), 2);
    EXPECT_EQ(ancestor_values.shredded_field_path(0).get_parts(),
              PathInData(std::vector<std::string> {"a"}).get_parts());
    EXPECT_EQ(ancestor_values.shredded_field_path(1).get_parts(),
              PathInData(std::vector<std::string> {"b"}).get_parts());
    EXPECT_LE(VariantElementV2TestAccess::shredded_path_inspections(), 12);
}

TEST(VariantElementV2ShreddedTest, ExactLookupIgnoresTypedStorageMarker) {
    constexpr std::array<std::string_view, 1> RESIDUAL {R"({})"};
    constexpr std::array<std::string_view, 1> FIELD {R"(7)"};
    constexpr std::array<uint8_t, 1> PRESENCE {1};
    auto source = encoded_shredded(RESIDUAL, FIELD, PRESENCE, {"a"});

    // ColumnVariantV2 producers currently normalize this marker away. Exercise the lookup contract
    // directly so a future producer retaining storage metadata cannot violate comparator identity.
    const PathInData typed_path("a", true);
    ASSERT_TRUE(typed_path.get_is_typed());
    EXPECT_TRUE(VariantElementV2TestAccess::has_exact_shredded_path(*source, typed_path));
}

} // namespace doris
