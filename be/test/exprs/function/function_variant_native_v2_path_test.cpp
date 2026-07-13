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
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/function_variant_native_v2.h"
#include "util/variant/variant_field.h"
#include "util/variant/variant_json.h"

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

ColumnVariantV2::MutablePtr encoded_column(std::span<const std::string_view> rows) {
    auto result = ColumnVariantV2::create();
    for (std::string_view row : rows) {
        result->insert_variant_field(encode_json(row));
    }
    return result;
}

ColumnVariantV2::MutablePtr typed_strings(std::span<const std::string_view> rows,
                                          std::span<const uint8_t> nulls) {
    auto values = ColumnString::create();
    auto internal_nulls = ColumnUInt8::create();
    for (size_t row = 0; row < rows.size(); ++row) {
        values->insert_data(rows[row].data(), rows[row].size());
        internal_nulls->insert_value(nulls[row]);
    }
    return ColumnVariantV2::create_typed_from_element_at(
            ColumnNullable::create(std::move(values), std::move(internal_nulls)),
            std::make_shared<DataTypeString>());
}

ColumnVariantV2::MutablePtr typed_ints(std::span<const int32_t> rows,
                                       std::span<const uint8_t> nulls) {
    auto values = ColumnInt32::create();
    auto internal_nulls = ColumnUInt8::create();
    for (size_t row = 0; row < rows.size(); ++row) {
        values->insert_value(rows[row]);
        internal_nulls->insert_value(nulls[row]);
    }
    return ColumnVariantV2::create_typed_from_element_at(
            ColumnNullable::create(std::move(values), std::move(internal_nulls)),
            std::make_shared<DataTypeInt32>());
}

std::unique_ptr<ResolvedVariantElementV2Path> resolve(std::vector<Segment> segments) {
    std::unique_ptr<ResolvedVariantElementV2Path> result;
    Status status = resolve_variant_element_v2_path(segments, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

ColumnPtr exists(const ColumnVariantV2& source, const ResolvedVariantElementV2Path& path,
                 std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    Status status = variant_exists_path_v2(source, path, outer_nulls, &result);
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

TEST(VariantNativeV2PathTest, EncodedExistsDistinguishesNullMissingAndTraversalMismatch) {
    const std::array<std::string_view, 6> rows {R"({"a":null})", R"({"a":[{"b":1},{"b":null}]})",
                                                R"({"x":1})",    R"(7)",
                                                R"({"a":{}})",   R"({"a":1})"};
    const std::array<uint8_t, 6> outer_nulls {0, 0, 0, 0, 0, 1};
    auto source = encoded_column(rows);

    auto top = resolve({Segment::object_key(StringRef("a"))});
    ColumnPtr result = exists(*source, *top, outer_nulls);
    expect_row(result, 0, true);
    expect_row(result, 1, true);
    expect_row(result, 2, false);
    expect_row(result, 3, false);
    expect_row(result, 4, true);
    expect_row(result, 5, false, true);

    auto nested = resolve({Segment::object_key(StringRef("a")), Segment::array_index(1),
                           Segment::object_key(StringRef("b"))});
    result = exists(*source, *nested);
    for (size_t row = 0; row < rows.size(); ++row) {
        expect_row(result, row, row == 1);
    }
}

TEST(VariantNativeV2PathTest, MetadataResolutionCacheIsCallLocal) {
    auto path = resolve({Segment::object_key(StringRef("target"))});
    const std::array<std::string_view, 3> first_rows {R"({"a":0,"target":1})", R"({"other":2})",
                                                      R"({"b":0,"target":3})"};
    auto first = encoded_column(first_rows);
    ColumnPtr result = exists(*first, *path);
    expect_row(result, 0, true);
    expect_row(result, 1, false);
    expect_row(result, 2, true);

    const std::array<std::string_view, 2> second_rows {R"({"different":7})", R"({"target":9})"};
    auto second = encoded_column(second_rows);
    result = exists(*second, *path);
    expect_row(result, 0, false);
    expect_row(result, 1, true);
}

TEST(VariantNativeV2PathTest, TypedStringsAndScalarsAreNotContainers) {
    const std::array<std::string_view, 5> documents {R"({"a":1})", "not json", R"(7)",
                                                     R"({"a":null})", R"({"a":2})"};
    const std::array<uint8_t, 5> internal_nulls {0, 0, 0, 1, 0};
    const std::array<uint8_t, 5> outer_nulls {0, 0, 0, 0, 1};
    auto strings = typed_strings(documents, internal_nulls);
    auto path = resolve({Segment::object_key(StringRef("a"))});

    ColumnPtr result = exists(*strings, *path, outer_nulls);
    expect_row(result, 0, false);
    expect_row(result, 1, false);
    expect_row(result, 2, false);
    expect_row(result, 3, false);
    expect_row(result, 4, false, true);
    EXPECT_TRUE(strings->is_typed());

    const std::array<int32_t, 3> values {1, 2, 3};
    const std::array<uint8_t, 3> typed_nulls {0, 1, 0};
    const std::array<uint8_t, 3> scalar_outer_nulls {0, 0, 1};
    auto scalars = typed_ints(values, typed_nulls);
    result = exists(*scalars, *path, scalar_outer_nulls);
    expect_row(result, 0, false);
    expect_row(result, 1, false);
    expect_row(result, 2, false, true);
    EXPECT_TRUE(scalars->is_typed());
}

TEST(VariantNativeV2PathTest, BadInputIsInvalidArgumentAndOutputIsAtomic) {
    auto path = resolve({Segment::object_key(StringRef("a"))});
    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr output = sentinel->get_ptr();
    const IColumn* identity = output.get();

    const std::string bad_metadata(1, static_cast<char>(0x11));
    const std::string null_value(1, 0);
    auto bad_metadata_column = raw_column({bad_metadata.data(), bad_metadata.size()},
                                          {null_value.data(), null_value.size()});
    Status status = variant_exists_path_v2(*bad_metadata_column, *path, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const VariantField valid = encode_json(R"({"a":1})");
    const std::string truncated_object(1, static_cast<char>(VariantBasicType::OBJECT));
    auto truncated = raw_column({valid.ref().metadata.data, valid.ref().metadata.size},
                                {truncated_object.data(), truncated_object.size()});
    status = variant_exists_path_v2(*truncated, *path, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    std::string trailing_value(valid.ref().data, valid.ref().size);
    trailing_value.push_back('\0');
    auto trailing = raw_column({valid.ref().metadata.data, valid.ref().metadata.size},
                               {trailing_value.data(), trailing_value.size()});
    status = variant_exists_path_v2(*trailing, *path, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const std::array<uint8_t, 2> wrong_sized_nulls {0, 0};
    status = variant_exists_path_v2(*truncated, *path, wrong_sized_nulls, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const std::array<uint8_t, 1> outer_null {1};
    status = variant_exists_path_v2(*truncated, *path, outer_null, &output);
    EXPECT_TRUE(status.ok()) << status;
    expect_row(output, 0, false, true);
}

} // namespace doris
