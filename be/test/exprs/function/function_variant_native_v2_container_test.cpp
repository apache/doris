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
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/column/column_vector.h"
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

ColumnPtr keys(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    Status status = variant_keys_v2(source, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

ColumnPtr length(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    Status status = variant_length_v2(source, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

ColumnPtr is_null(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    Status status = variant_is_null_v2(source, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

const ColumnNullable& nullable_result(const ColumnPtr& result) {
    return assert_cast<const ColumnNullable&>(*result);
}

void expect_keys(const ColumnPtr& result, size_t row, std::span<const std::string_view> expected,
                 bool is_null = false) {
    const auto& nullable = nullable_result(result);
    EXPECT_EQ(nullable.get_null_map_data()[row], is_null) << row;
    if (is_null) {
        return;
    }
    const auto& arrays = assert_cast<const ColumnArray&>(nullable.get_nested_column());
    const uint64_t begin = row == 0 ? 0 : arrays.get_offsets()[row - 1];
    const uint64_t end = arrays.get_offsets()[row];
    ASSERT_EQ(end - begin, expected.size()) << row;
    const auto& elements = assert_cast<const ColumnNullable&>(arrays.get_data());
    const auto& strings = assert_cast<const ColumnString&>(elements.get_nested_column());
    for (size_t index = 0; index < expected.size(); ++index) {
        EXPECT_FALSE(elements.is_null_at(begin + index));
        EXPECT_EQ(strings.get_data_at(begin + index),
                  StringRef(expected[index].data(), expected[index].size()))
                << index;
    }
}

template <typename ColumnType, typename ValueType>
void expect_scalar(const ColumnPtr& result, size_t row, ValueType expected,
                   bool is_sql_null = false) {
    const auto& nullable = nullable_result(result);
    EXPECT_EQ(nullable.get_null_map_data()[row], is_sql_null) << row;
    if (!is_sql_null) {
        const auto& values = assert_cast<const ColumnType&>(nullable.get_nested_column());
        EXPECT_EQ(values.get_data()[row], expected) << row;
    }
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

TEST(VariantNativeV2ContainerTest, KeysAreByteSortedAndSupportLegalNoncanonicalMetadata) {
    const std::array<std::string_view, 5> rows {R"({"b":1,"a":2})", R"([])", R"(null)", R"(7)",
                                                R"({"hidden":1})"};
    const std::array<uint8_t, 5> outer_nulls {0, 0, 0, 0, 1};
    auto source = encoded_column(rows);
    ColumnPtr result = keys(*source, outer_nulls);
    const std::array<std::string_view, 2> expected {"a", "b"};
    expect_keys(result, 0, expected);
    const std::array<std::string_view, 0> empty {};
    for (size_t row = 1; row < rows.size(); ++row) {
        expect_keys(result, row, empty, true);
    }

    auto noncanonical = ColumnVariantV2::create();
    noncanonical->insert_variant_field(legal_noncanonical_object());
    result = keys(*noncanonical);
    expect_keys(result, 0, expected);

    const std::array<int32_t, 2> typed_values {1, 2};
    const std::array<uint8_t, 2> typed_nulls {0, 1};
    auto typed = typed_ints(typed_values, typed_nulls);
    result = keys(*typed);
    expect_keys(result, 0, empty, true);
    expect_keys(result, 1, empty, true);
}

TEST(VariantNativeV2ContainerTest, LengthCountsContainersAndTreatsScalarsAsOne) {
    const std::array<std::string_view, 5> rows {R"({"a":1,"b":2})", R"([1,2,3])", R"("x")",
                                                R"(null)", R"({"hidden":1})"};
    const std::array<uint8_t, 5> outer_nulls {0, 0, 0, 0, 1};
    auto source = encoded_column(rows);
    ColumnPtr result = length(*source, outer_nulls);
    expect_scalar<ColumnInt32>(result, 0, 2);
    expect_scalar<ColumnInt32>(result, 1, 3);
    expect_scalar<ColumnInt32>(result, 2, 1);
    expect_scalar<ColumnInt32>(result, 3, 1);
    expect_scalar<ColumnInt32>(result, 4, 0, true);

    const std::array<int32_t, 3> values {1, 2, 3};
    const std::array<uint8_t, 3> internal_nulls {0, 1, 0};
    const std::array<uint8_t, 3> typed_outer_nulls {0, 0, 1};
    auto typed = typed_ints(values, internal_nulls);
    result = length(*typed, typed_outer_nulls);
    expect_scalar<ColumnInt32>(result, 0, 1);
    expect_scalar<ColumnInt32>(result, 1, 1);
    expect_scalar<ColumnInt32>(result, 2, 0, true);
}

TEST(VariantNativeV2ContainerTest, IsNullSeparatesInternalVariantNullFromOuterSqlNull) {
    const std::array<std::string_view, 4> rows {R"(null)", R"({})", R"(0)", R"(null)"};
    const std::array<uint8_t, 4> outer_nulls {0, 0, 0, 1};
    auto source = encoded_column(rows);
    ColumnPtr result = is_null(*source, outer_nulls);
    expect_scalar<ColumnUInt8>(result, 0, 1);
    expect_scalar<ColumnUInt8>(result, 1, 0);
    expect_scalar<ColumnUInt8>(result, 2, 0);
    expect_scalar<ColumnUInt8>(result, 3, 0, true);

    const std::array<int32_t, 3> values {1, 2, 3};
    const std::array<uint8_t, 3> internal_nulls {0, 1, 0};
    const std::array<uint8_t, 3> typed_outer_nulls {0, 0, 1};
    auto typed = typed_ints(values, internal_nulls);
    result = is_null(*typed, typed_outer_nulls);
    expect_scalar<ColumnUInt8>(result, 0, 0);
    expect_scalar<ColumnUInt8>(result, 1, 1);
    expect_scalar<ColumnUInt8>(result, 2, 0, true);
    EXPECT_TRUE(typed->is_typed());
}

TEST(VariantNativeV2ContainerTest, BadInputIsInvalidArgumentAndOutputIsAtomic) {
    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr output = sentinel->get_ptr();
    const IColumn* identity = output.get();

    const std::string bad_metadata(1, static_cast<char>(0x11));
    const std::string null_value(1, 0);
    auto bad_metadata_column = raw_column({bad_metadata.data(), bad_metadata.size()},
                                          {null_value.data(), null_value.size()});
    Status status = variant_keys_v2(*bad_metadata_column, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const VariantField valid = encode_json(R"({"a":1})");
    const std::string truncated_object(1, static_cast<char>(VariantBasicType::OBJECT));
    auto truncated = raw_column({valid.ref().metadata.data, valid.ref().metadata.size},
                                {truncated_object.data(), truncated_object.size()});
    status = variant_length_v2(*truncated, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    std::string trailing_value(valid.ref().data, valid.ref().size);
    trailing_value.push_back('\0');
    auto trailing = raw_column({valid.ref().metadata.data, valid.ref().metadata.size},
                               {trailing_value.data(), trailing_value.size()});
    status = variant_is_null_v2(*trailing, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const std::array<uint8_t, 1> outer_null {1};
    status = variant_keys_v2(*truncated, outer_null, &output);
    EXPECT_TRUE(status.ok()) << status;
    const std::array<std::string_view, 0> empty {};
    expect_keys(output, 0, empty, true);
}

} // namespace doris
