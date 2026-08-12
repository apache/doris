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

#include "core/column/variant_v2/variant_shredded_column_builder.h"

#include <gtest/gtest.h>

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

VariantField encode_json(std::string_view json) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder batch = encoder.finish_batch();
    return VariantField::from_ref(batch.value_at(0));
}

ColumnVariantV2::MutablePtr encoded_column(std::initializer_list<std::string_view> rows) {
    auto result = ColumnVariantV2::create();
    for (std::string_view row : rows) {
        insert_encoded_field(*result, encode_json(row));
    }
    return result;
}

struct JsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

std::string json(VariantRef value) {
    JsonWriter writer;
    to_json(value, writer);
    return writer.value;
}

std::string residual_json(const ColumnVariantV2& shredded, size_t row) {
    return json(shredded.read_view().residual_value_at(row));
}

std::string column_json(const ColumnVariantV2& column, size_t row) {
    if (column.is_encoded()) {
        return json(column.get_value_ref(row));
    }
    auto encoded = column.materialize_encoded_range(row, 1);
    return json(encoded->get_value_ref(0));
}

void expect_materialized_equal(const ColumnVariantV2& expected, const ColumnVariantV2& shredded) {
    ASSERT_TRUE(shredded.is_shredded());
    auto materialized = shredded.materialize_encoded_range(0, shredded.size());
    ASSERT_TRUE(materialized->is_encoded());
    ASSERT_EQ(materialized->size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        SCOPED_TRACE(testing::Message() << "row=" << row);
        EXPECT_TRUE(
                canonical_equals(materialized->get_value_ref(row), expected.get_value_ref(row)));
    }
}

DorisVector<VariantShreddedLayoutEntry> int_layout(PathInData path) {
    DorisVector<VariantShreddedLayoutEntry> layout;
    layout.emplace_back(path, std::make_shared<DataTypeInt64>());
    return layout;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the conflict-order matrix.
TEST(VariantShreddedColumnBuilderTest, ExactTypeConflictKeepsTypedChildAndConflictingResidual) {
    for (bool integer_first : {false, true}) {
        SCOPED_TRACE(testing::Message() << "integer_first=" << integer_first);
        auto source =
                integer_first
                        ? encoded_column({R"({"a":1,"keep":"x"})", R"({"a":"one","keep":"y"})"})
                        : encoded_column({R"({"a":"one","keep":"y"})", R"({"a":1,"keep":"x"})"});
        VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));

        auto shredded = builder.build(*source);

        ASSERT_TRUE(shredded->is_shredded());
        ASSERT_EQ(shredded->shredded_field_count(), 1);
        const auto& presence = shredded->shredded_field_presence(0).get_data();
        ASSERT_EQ(presence.size(), 2);
        const size_t integer_row = integer_first ? 0 : 1;
        const size_t string_row = 1 - integer_row;
        EXPECT_EQ(presence[integer_row], 1);
        EXPECT_EQ(presence[string_row], 0);
        const auto& child = shredded->shredded_field_values(0);
        ASSERT_TRUE(child.is_typed());
        EXPECT_EQ(column_json(child, integer_row), "1");
        EXPECT_EQ(column_json(child, string_row), "null");
        EXPECT_EQ(residual_json(*shredded, integer_row), R"({"keep":"x"})");
        EXPECT_EQ(residual_json(*shredded, string_row), R"({"a":"one","keep":"y"})");
        expect_materialized_equal(*source, *shredded);
    }
}

TEST(VariantShreddedColumnBuilderTest, ScalarObjectConflictIsIndependentOfInputOrder) {
    for (bool scalar_first : {false, true}) {
        SCOPED_TRACE(testing::Message() << "scalar_first=" << scalar_first);
        auto source =
                scalar_first ? encoded_column({R"({"a":7,"keep":1})", R"({"a":{"b":2},"keep":2})"})
                             : encoded_column({R"({"a":{"b":2},"keep":2})", R"({"a":7,"keep":1})"});
        VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));

        auto shredded = builder.build(*source);

        ASSERT_TRUE(shredded->is_shredded());
        const auto& presence = shredded->shredded_field_presence(0).get_data();
        ASSERT_EQ(presence.size(), 2);
        const size_t scalar_row = scalar_first ? 0 : 1;
        const size_t object_row = 1 - scalar_row;
        EXPECT_EQ(presence[scalar_row], 1);
        EXPECT_EQ(presence[object_row], 0);
        EXPECT_TRUE(shredded->shredded_field_values(0).is_typed());
        EXPECT_EQ(residual_json(*shredded, scalar_row), R"({"keep":1})");
        EXPECT_EQ(residual_json(*shredded, object_row), R"({"a":{"b":2},"keep":2})");
        expect_materialized_equal(*source, *shredded);
    }
}

TEST(VariantShreddedColumnBuilderTest, DescendantPathLeavesScalarAncestorInResidual) {
    auto source = encoded_column({R"({"a":7})", R"({"a":{"b":2,"c":3}})"});
    VariantShreddedColumnBuilder builder(
            int_layout(PathInData(std::vector<std::string> {"a", "b"})));

    auto shredded = builder.build(*source);

    ASSERT_TRUE(shredded->is_shredded());
    const auto& presence = shredded->shredded_field_presence(0).get_data();
    ASSERT_EQ(presence.size(), 2);
    EXPECT_EQ(presence[0], 0);
    EXPECT_EQ(presence[1], 1);
    EXPECT_TRUE(shredded->shredded_field_values(0).is_typed());
    EXPECT_EQ(residual_json(*shredded, 0), R"({"a":7})");
    EXPECT_EQ(residual_json(*shredded, 1), R"({"a":{"c":3}})");
    expect_materialized_equal(*source, *shredded);
}

TEST(VariantShreddedColumnBuilderTest, MissingVariantNullAndOuterNullHaveDistinctPresence) {
    auto source = encoded_column({R"({"a":1})", "{}", R"({"a":null})", R"({"a":2})"});
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));
    NullMap outer_nulls {0, 0, 0, 1};

    auto shredded = builder.build(*source, outer_nulls);

    ASSERT_TRUE(shredded->is_shredded());
    const auto& presence = shredded->shredded_field_presence(0).get_data();
    ASSERT_EQ(presence.size(), 4);
    EXPECT_EQ(presence[0], 1);
    EXPECT_EQ(presence[1], 0);
    EXPECT_EQ(presence[2], 1);
    EXPECT_EQ(presence[3], 0);
    const auto& child = shredded->shredded_field_values(0);
    ASSERT_TRUE(child.is_typed());
    const auto& typed = assert_cast<const ColumnNullable&>(child.typed_column());
    EXPECT_EQ(typed.get_null_map_data(), (NullMap {0, 1, 1, 1}));
    EXPECT_EQ(residual_json(*shredded, 0), "{}");
    EXPECT_EQ(residual_json(*shredded, 1), "{}");
    EXPECT_EQ(residual_json(*shredded, 2), "{}");
    EXPECT_EQ(residual_json(*shredded, 3), R"({"a":2})");
    expect_materialized_equal(*source, *shredded);
}

TEST(VariantShreddedColumnBuilderTest, TypedConflictKeepsMissingVariantNullAndOuterNullDistinct) {
    auto source = encoded_column({R"({"a":1,"keep":0})", R"({"a":"one","keep":1})", "{}",
                                  R"({"a":null})", R"({"a":2,"keep":4})"});
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));
    NullMap outer_nulls {0, 0, 0, 0, 1};

    auto shredded = builder.build(*source, outer_nulls);

    ASSERT_TRUE(shredded->is_shredded());
    const auto& presence = shredded->shredded_field_presence(0).get_data();
    ASSERT_EQ(presence, (PaddedPODArray<UInt8> {1, 0, 0, 1, 0}));
    const auto& child = shredded->shredded_field_values(0);
    ASSERT_TRUE(child.is_typed());
    EXPECT_EQ(column_json(child, 0), "1");
    EXPECT_EQ(column_json(child, 1), "null");
    EXPECT_EQ(column_json(child, 2), "null");
    EXPECT_EQ(column_json(child, 3), "null");
    EXPECT_EQ(column_json(child, 4), "null");
    EXPECT_EQ(residual_json(*shredded, 0), R"({"keep":0})");
    EXPECT_EQ(residual_json(*shredded, 1), R"({"a":"one","keep":1})");
    EXPECT_EQ(residual_json(*shredded, 2), "{}");
    EXPECT_EQ(residual_json(*shredded, 3), "{}");
    EXPECT_EQ(residual_json(*shredded, 4), R"({"a":2,"keep":4})");
    expect_materialized_equal(*source, *shredded);
}

TEST(VariantShreddedColumnBuilderTest,
     DirectBatchMatchesEncodedOracleAcrossScalarAndStructureConflicts) {
    auto source = encoded_column({R"({"a":1,"keep":0})", R"({"a":"one","keep":1})",
                                  R"({"a":{"b":2},"keep":2})", "{}", R"({"a":null})",
                                  R"({"a":3,"keep":5})"});
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));
    auto batch = builder.begin_batch(source->size());
    VariantBatchBuilder residual_builder({.rows = source->size()});
    for (size_t row = 0; row < source->size(); ++row) {
        auto residual = residual_builder.begin_row();
        batch.append_root(source->get_value_ref(row), residual, row != 5);
        residual.finish();
        batch.finish_row();
    }
    VariantBatchBuilder residual_batch = residual_builder.finish_batch();
    auto residual = ColumnVariantV2::create();
    residual->insert_encoded_batch(residual_batch);

    auto shredded = batch.finish(std::move(residual));

    ASSERT_TRUE(shredded->is_shredded());
    ASSERT_EQ(shredded->shredded_field_count(), 1);
    EXPECT_EQ(shredded->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0, 0, 1, 0}));
    EXPECT_TRUE(shredded->shredded_field_values(0).is_typed());
    EXPECT_EQ(residual_json(*shredded, 0), R"({"keep":0})");
    EXPECT_EQ(residual_json(*shredded, 1), R"({"a":"one","keep":1})");
    EXPECT_EQ(residual_json(*shredded, 2), R"({"a":{"b":2},"keep":2})");
    EXPECT_EQ(residual_json(*shredded, 3), "{}");
    EXPECT_EQ(residual_json(*shredded, 4), "{}");
    EXPECT_EQ(residual_json(*shredded, 5), R"({"a":3,"keep":5})");
    EXPECT_EQ(builder.test_encoded_source_builds(), 0);
    EXPECT_EQ(builder.test_direct_batches(), 1);
    expect_materialized_equal(*source, *shredded);
}

TEST(VariantShreddedColumnBuilderTest, OwnedNonNullableMaterializedPayloadTransfers) {
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));
    auto batch = builder.begin_batch(2);
    auto values = ColumnInt64::create();
    values->insert_value(10);
    values->insert_value(20);
    const IColumn* payload = values.get();
    batch.bind_materialized_source(0, *payload, nullptr, std::move(values));
    ASSERT_EQ(values.get(), nullptr);
    for (size_t row = 0; row < 2; ++row) {
        batch.append_materialized(0, row);
        batch.finish_row();
    }

    auto shredded = batch.finish(encoded_column({"{}", "{}"}));

    ASSERT_TRUE(shredded->is_shredded());
    const auto& child = shredded->shredded_field_values(0);
    ASSERT_TRUE(child.is_typed());
    const auto& nullable = assert_cast<const ColumnNullable&>(child.typed_column());
    EXPECT_EQ(&nullable.get_nested_column(), payload);
    EXPECT_EQ(shredded->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 1}));
    EXPECT_EQ(ColumnVariantV2::TestAccess::full_shredded_validations(*shredded), 0);

    auto checked_values = ColumnInt64::create();
    checked_values->insert_value(7);
    auto checked_nulls = ColumnUInt8::create();
    checked_nulls->insert_value(0);
    auto checked_presence = ColumnUInt8::create();
    checked_presence->insert_value(1);
    ColumnVariantV2::ShreddedFields checked_fields;
    checked_fields.emplace_back(
            PathInData("checked"),
            ColumnVariantV2::create_typed(
                    ColumnNullable::create(std::move(checked_values), std::move(checked_nulls)),
                    std::make_shared<DataTypeInt64>()),
            std::move(checked_presence));
    auto checked =
            ColumnVariantV2::create_shredded(encoded_column({"{}"}), std::move(checked_fields));
    EXPECT_EQ(ColumnVariantV2::TestAccess::full_shredded_validations(*checked), 1);
}

TEST(VariantShreddedColumnBuilderTest, OwnedNullableMaterializedWrapperTransfers) {
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));
    auto batch = builder.begin_batch(2);
    auto values = ColumnInt64::create();
    values->insert_value(10);
    values->insert_value(0);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    nulls->insert_value(1);
    auto owner = ColumnNullable::create(std::move(values), std::move(nulls));
    const IColumn* wrapper = owner.get();
    const ColumnNullable& nullable_source = *owner;
    const IColumn* payload = &nullable_source.get_nested_column();
    const uint8_t* nullable_nulls = nullable_source.get_null_map_data().data();
    batch.bind_materialized_source(0, *payload, nullable_nulls, std::move(owner));
    ASSERT_EQ(owner.get(), nullptr);
    batch.append_materialized(0, 0);
    batch.finish_row();
    batch.finish_row();

    auto shredded = batch.finish(encoded_column({"{}", "{}"}));

    const auto& child = shredded->shredded_field_values(0);
    ASSERT_TRUE(child.is_typed());
    EXPECT_EQ(&child.typed_column(), wrapper);
    EXPECT_EQ(shredded->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 0}));
    EXPECT_EQ(assert_cast<const ColumnNullable&>(child.typed_column()).get_null_map_data(),
              (NullMap {0, 1}));
}

TEST(VariantShreddedColumnBuilderTest, BorrowedMaterializedUsesBulkSelectionAndOwnsResult) {
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));
    auto batch = builder.begin_batch(3);
    auto values = ColumnInt64::create();
    values->insert_value(10);
    values->insert_value(20);
    values->insert_value(30);
    const IColumn* borrowed_payload = values.get();
    batch.bind_materialized_source(0, *values, nullptr);
    batch.append_materialized(0, 0);
    batch.finish_row();
    batch.finish_row();
    batch.append_materialized(0, 2);
    batch.finish_row();

    auto shredded = batch.finish(encoded_column({"{}", "{}", "{}"}));
    values->clear();

    const auto& child = shredded->shredded_field_values(0);
    const auto& nullable = assert_cast<const ColumnNullable&>(child.typed_column());
    EXPECT_NE(&nullable.get_nested_column(), borrowed_payload);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 1, 0}));
    EXPECT_EQ(shredded->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 0, 1}));
    EXPECT_EQ(column_json(child, 0), "10");
    EXPECT_EQ(column_json(child, 2), "30");
}

TEST(VariantShreddedColumnBuilderTest, InvalidOwnerIsRejectedBeforeBinding) {
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a")));
    auto batch = builder.begin_batch(1);
    auto values = ColumnInt64::create();
    values->insert_value(7);
    auto wrong_owner = ColumnInt64::create();
    wrong_owner->insert_value(8);

    EXPECT_DEATH(batch.bind_materialized_source(0, *values, nullptr, std::move(wrong_owner)),
                 "materialized owner does not contain the bound native column");

    batch.bind_materialized_source(0, *values, nullptr);
    batch.append_materialized(0, 0);
    batch.finish_row();
    auto shredded = batch.finish(encoded_column({"{}"}));
    EXPECT_EQ(column_json(shredded->shredded_field_values(0), 0), "7");
}

TEST(VariantShreddedColumnBuilderTest, LogicalPartsDistinguishLiteralDottedKeyAndRejectPrefix) {
    DorisVector<VariantShreddedLayoutEntry> overlapping;
    overlapping.push_back({PathInData("a"), std::make_shared<DataTypeInt64>()});
    overlapping.push_back(
            {PathInData(std::vector<std::string> {"a", "b"}), std::make_shared<DataTypeInt64>()});
    EXPECT_THROW(VariantShreddedColumnBuilder(std::move(overlapping)), Exception);

    PathInData::Parts literal_parts {{"a.b", false, 0}};
    DorisVector<VariantShreddedLayoutEntry> distinct;
    distinct.push_back({PathInData(literal_parts), std::make_shared<DataTypeInt64>()});
    distinct.push_back(
            {PathInData(std::vector<std::string> {"a", "b"}), std::make_shared<DataTypeInt64>()});
    auto source = encoded_column({R"({"a":{"b":2},"a.b":1})"});
    VariantShreddedColumnBuilder builder(std::move(distinct));

    auto shredded = builder.build(*source);

    ASSERT_EQ(shredded->shredded_field_count(), 2);
    EXPECT_EQ(shredded->shredded_field_path(0).get_parts().size(), 2);
    EXPECT_EQ(shredded->shredded_field_path(1).get_parts().size(), 1);
    EXPECT_EQ(column_json(shredded->shredded_field_values(0), 0), "2");
    EXPECT_EQ(column_json(shredded->shredded_field_values(1), 0), "1");
    EXPECT_EQ(residual_json(*shredded, 0), R"({"a":{}})");
    expect_materialized_equal(*source, *shredded);
}

TEST(VariantShreddedColumnBuilderTest, BatchLookupUsesLogicalAndRawPathOrder) {
    const auto int_type = std::make_shared<DataTypeInt64>();
    const PathInData ax(std::vector<std::string> {"a", "x"});
    const PathInData a_dash_x(std::vector<std::string> {"a-", "x"});
    const PathInData segmented_dot(std::vector<std::string> {"a", "b"});
    const PathInData literal_dot(PathInData::Parts {{"a.b", false, 0}});
    DorisVector<VariantShreddedLayoutEntry> layout;
    layout.push_back({a_dash_x, int_type});
    layout.push_back({literal_dot, int_type});
    layout.push_back({ax, int_type});
    layout.push_back({segmented_dot, int_type});
    VariantShreddedColumnBuilder builder(std::move(layout));
    auto batch = builder.begin_batch(0);

    const auto expect_same_logical_and_raw_index = [&](const PathInData& path, uint32_t depth) {
        const std::optional<size_t> logical = batch.find_path(path);
        const std::optional<size_t> raw = batch.find_raw_path(StringRef(path.get_path()), depth);
        ASSERT_TRUE(logical.has_value());
        ASSERT_TRUE(raw.has_value());
        EXPECT_EQ(*raw, *logical);
    };
    // Logical part order puts a.x before a-.x, while the persisted raw-byte order is reversed.
    expect_same_logical_and_raw_index(ax, 2);
    expect_same_logical_and_raw_index(a_dash_x, 2);
    // Equal raw bytes remain distinguishable by the persisted path depth.
    expect_same_logical_and_raw_index(literal_dot, 1);
    expect_same_logical_and_raw_index(segmented_dot, 2);
    EXPECT_FALSE(batch.find_raw_path(StringRef("missing"), 1).has_value());
}

TEST(VariantShreddedColumnBuilderTest, NormalizesTypedPathAndDoesNotRetainSource) {
    auto source = encoded_column({R"({"a":11,"keep":1})", R"({"a":12,"keep":2})"});
    VariantShreddedColumnBuilder builder(int_layout(PathInData("a", true)));

    auto shredded = builder.build(*source);
    source->clear();

    ASSERT_TRUE(shredded->is_shredded());
    EXPECT_FALSE(shredded->shredded_field_path(0).get_is_typed());
    EXPECT_EQ(residual_json(*shredded, 0), R"({"keep":1})");
    EXPECT_EQ(residual_json(*shredded, 1), R"({"keep":2})");
    auto materialized = shredded->materialize_encoded_range(0, 2);
    EXPECT_EQ(json(materialized->get_value_ref(0)), R"({"a":11,"keep":1})");
    EXPECT_EQ(json(materialized->get_value_ref(1)), R"({"a":12,"keep":2})");
}

} // namespace
} // namespace doris
