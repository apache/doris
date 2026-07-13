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

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function_variant_native_v2.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_field.h"
#include "util/variant/variant_json.h"

namespace doris {
namespace {

VariantField encode_json(std::string_view json) {
    JsonToVariantEncoder encoder({.max_json_key_length = 1024,
                                  .throw_on_invalid_json = true,
                                  .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantEncodedBlock block = encoder.finish_block();
    return VariantField::encode(block.value_at(0));
}

ColumnVariantV2::MutablePtr encoded_column(std::span<const std::string_view> rows) {
    auto result = ColumnVariantV2::create();
    for (std::string_view row : rows) {
        result->insert_variant_field(encode_json(row));
    }
    return result;
}

ColumnVariantV2::MutablePtr typed_ints(std::span<const int32_t> rows,
                                       std::span<const uint8_t> nulls) {
    auto values = ColumnInt32::create();
    auto internal_nulls = ColumnUInt8::create();
    for (size_t row = 0; row < rows.size(); ++row) {
        values->insert_value(rows[row]);
        internal_nulls->insert_value(nulls[row]);
    }
    return ColumnVariantV2::create_typed_from_cast(
            ColumnNullable::create(std::move(values), std::move(internal_nulls)),
            std::make_shared<DataTypeInt32>());
}

ColumnPtr contains(const ColumnVariantV2& target, const ColumnVariantV2& candidate,
                   std::span<const uint8_t> target_nulls = {},
                   std::span<const uint8_t> candidate_nulls = {}) {
    ColumnPtr result;
    Status status = variant_contains_v2(target, candidate, target_nulls, candidate_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

const ColumnNullable& nullable_result(const ColumnPtr& result) {
    return assert_cast<const ColumnNullable&>(*result);
}

void expect_row(const ColumnPtr& result, size_t row, bool value, bool is_null = false) {
    const auto& nullable = nullable_result(result);
    const auto& values = assert_cast<const ColumnUInt8&>(nullable.get_nested_column());
    EXPECT_EQ(nullable.get_null_map_data()[row], is_null) << row;
    if (!is_null) {
        EXPECT_EQ(values.get_data()[row] != 0, value) << row;
    }
}

void append_unsigned(std::string& output, uint64_t value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

std::string nested_array_value(uint32_t depth) {
    std::string value(1, static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::INT8)
                                           << VARIANT_VALUE_HEADER_SHIFT));
    value.push_back(1);
    for (uint32_t level = 0; level < depth; ++level) {
        uint8_t width = 1;
        while (width < sizeof(uint32_t) &&
               value.size() >= (static_cast<size_t>(1) << (width * 8))) {
            ++width;
        }
        std::string parent;
        const auto value_header =
                static_cast<uint8_t>((width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT);
        parent.push_back(static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                           static_cast<uint8_t>(VariantBasicType::ARRAY)));
        parent.push_back(1);
        append_unsigned(parent, 0, width);
        append_unsigned(parent, value.size(), width);
        parent.append(value);
        value.swap(parent);
    }
    return value;
}

VariantField nested_array(uint32_t depth) {
    const std::string value = nested_array_value(depth);
    const std::string metadata {
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK), 0,
            0};
    std::string field;
    append_unsigned(field, metadata.size(), sizeof(uint32_t));
    field.append(metadata);
    field.append(value);
    return VariantField::decode({field.data(), field.size()});
}

ColumnVariantV2::MutablePtr raw_nested_array_column(uint32_t depth) {
    const std::string metadata {
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK), 0,
            0};
    const std::string value = nested_array_value(depth);
    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(metadata.size())};
    const std::array<uint32_t, 2> value_offsets {0, static_cast<uint32_t>(value.size())};
    auto result = ColumnVariantV2::create();
    result->insert_encoded_rows({.metadata_bytes = {metadata.data(), metadata.size()},
                                 .metadata_offsets = metadata_offsets,
                                 .meta_ids = {},
                                 .value_bytes = {value.data(), value.size()},
                                 .value_offsets = value_offsets});
    return result;
}

ColumnVariantV2::MutablePtr field_column(const VariantField& field) {
    auto result = ColumnVariantV2::create();
    result->insert_variant_field(field);
    return result;
}

ColumnVariantV2::MutablePtr truncated_object_column() {
    const VariantField valid = encode_json(R"({"a":1})");
    const std::string value(1, static_cast<char>(VariantBasicType::OBJECT));
    const std::array<uint32_t, 2> metadata_offsets {
            0, static_cast<uint32_t>(valid.ref().metadata.size)};
    const std::array<uint32_t, 2> value_offsets {0, static_cast<uint32_t>(value.size())};
    auto result = ColumnVariantV2::create();
    result->insert_encoded_rows(
            {.metadata_bytes = {valid.ref().metadata.data, valid.ref().metadata.size},
             .metadata_offsets = metadata_offsets,
             .meta_ids = {},
             .value_bytes = {value.data(), value.size()},
             .value_offsets = value_offsets});
    return result;
}

} // namespace

TEST(VariantNativeV2ContainsTest, EmptyTargetDoesNotMatchEmptyKeyCandidateAndLastKeyMatches) {
    const std::array<std::string_view, 2> targets {R"({})", R"({"":1})"};
    const std::array<std::string_view, 2> candidates {R"({"":1})", R"({"":1})"};
    auto target = encoded_column(targets);
    auto candidate = encoded_column(candidates);

    ColumnPtr result = contains(*target, *candidate);
    expect_row(result, 0, false);
    expect_row(result, 1, true);
}

TEST(VariantNativeV2ContainsTest, RecursiveObjectArrayAndCanonicalScalarSemantics) {
    const std::array<std::string_view, 9> targets {
            R"({"a":1,"b":2})", R"({"a":1})", R"([{"a":1,"b":2},[3,4]])",
            R"([1])",           R"([1,2])",   R"(1)",
            R"("1")",           R"(null)",    R"({"a":1})"};
    const std::array<std::string_view, 9> candidates {
            R"({"a":1})", R"({"a":2})", R"([{"a":1},[4]])", R"([1,1])", R"(2)",
            R"(1.0)",     R"(1)",       R"(null)",          R"(1)"};
    auto target = encoded_column(targets);
    auto candidate = encoded_column(candidates);

    ColumnPtr result = contains(*target, *candidate);
    const std::array<bool, 9> expected {true, false, true, true, true, true, false, true, false};
    for (size_t row = 0; row < expected.size(); ++row) {
        expect_row(result, row, expected[row]);
    }
}

TEST(VariantNativeV2ContainsTest, MatchesAcrossShiftedRootAndNestedDictionaryIds) {
    const std::array<std::string_view, 2> targets {R"({"a":0,"z":{"v":1}})",
                                                   R"({"a":0,"z":{"v":1}})"};
    const std::array<std::string_view, 2> candidates {R"({"z":{"v":1}})", R"({"z":{"v":2}})"};
    auto target = encoded_column(targets);
    auto candidate = encoded_column(candidates);

    ColumnPtr result = contains(*target, *candidate);
    expect_row(result, 0, true);
    expect_row(result, 1, false);
}

TEST(VariantNativeV2ContainsTest, CoversEncodedTypedAndTypedTypedPairs) {
    const std::array<std::string_view, 2> array_targets {R"([42])", R"([7])"};
    const std::array<int32_t, 2> typed_values {42, 8};
    const std::array<uint8_t, 2> no_nulls {0, 0};
    auto encoded_target = encoded_column(array_targets);
    auto typed_candidate = typed_ints(typed_values, no_nulls);
    expect_row(contains(*encoded_target, *typed_candidate), 0, true);
    expect_row(contains(*encoded_target, *typed_candidate), 1, false);

    const std::array<std::string_view, 2> encoded_values {R"(42.0)", R"(9)"};
    auto encoded_candidate = encoded_column(encoded_values);
    auto typed_target = typed_ints(typed_values, no_nulls);
    expect_row(contains(*typed_target, *encoded_candidate), 0, true);
    expect_row(contains(*typed_target, *encoded_candidate), 1, false);

    const std::array<int32_t, 2> other_values {42, 9};
    auto other_typed = typed_ints(other_values, no_nulls);
    expect_row(contains(*typed_target, *other_typed), 0, true);
    expect_row(contains(*typed_target, *other_typed), 1, false);
    EXPECT_TRUE(typed_target->is_typed());
    EXPECT_TRUE(other_typed->is_typed());
}

TEST(VariantNativeV2ContainsTest, EitherOuterNullMasksHiddenInvalidTypedRows) {
    auto invalid_dates = ColumnDateV2::create();
    DateV2Value<DateV2ValueType> invalid_date_value;
    ASSERT_FALSE(invalid_date_value.is_valid_date());
    invalid_dates->insert_value(invalid_date_value);
    invalid_dates->insert_value(invalid_date_value);
    auto internal_nulls = ColumnUInt8::create(2, 0);
    auto target = ColumnVariantV2::create_typed_from_cast(
            ColumnNullable::create(std::move(invalid_dates), std::move(internal_nulls)),
            std::make_shared<DataTypeDateV2>());
    const std::array<std::string_view, 2> candidate_rows {R"(1)", R"(2)"};
    auto candidate = encoded_column(candidate_rows);
    const std::array<uint8_t, 2> target_nulls {0, 1};
    const std::array<uint8_t, 2> candidate_nulls {1, 0};

    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr unchanged_output = sentinel->get_ptr();
    const IColumn* const unchanged_identity = unchanged_output.get();
    const Status unmasked_status =
            variant_contains_v2(*target, *candidate, {}, {}, &unchanged_output);
    EXPECT_EQ(unmasked_status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(unchanged_output.get(), unchanged_identity);
    EXPECT_TRUE(target->is_typed());

    ColumnPtr result = contains(*target, *candidate, target_nulls, candidate_nulls);
    expect_row(result, 0, false, true);
    expect_row(result, 1, false, true);
    EXPECT_TRUE(target->is_typed());
}

TEST(VariantNativeV2ContainsTest, EnforcesDepthAndKeepsOutputAtomicOnBadInput) {
    auto at_limit = field_column(nested_array(VARIANT_MAX_NESTING_DEPTH));
    expect_row(contains(*at_limit, *at_limit), 0, true);

    auto too_deep = raw_nested_array_column(VARIANT_MAX_NESTING_DEPTH + 1);
    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr output = sentinel->get_ptr();
    const IColumn* identity = output.get();
    Status status = variant_contains_v2(*too_deep, *too_deep, {}, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    auto truncated = truncated_object_column();
    auto one = encoded_column(std::array<std::string_view, 1> {R"(1)"});
    status = variant_contains_v2(*truncated, *one, {}, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const std::array<uint8_t, 1> outer_null {1};
    status = variant_contains_v2(*truncated, *one, outer_null, {}, &output);
    EXPECT_TRUE(status.ok()) << status;
    expect_row(output, 0, false, true);
}

} // namespace doris
