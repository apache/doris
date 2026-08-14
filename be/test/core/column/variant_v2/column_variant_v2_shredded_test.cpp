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

#include <array>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exec/common/sip_hash.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
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

ColumnVariantV2::MutablePtr encoded_rows(std::initializer_list<std::string_view> rows) {
    auto result = ColumnVariantV2::create();
    for (std::string_view row : rows) {
        insert_encoded_field(*result, encode_json(row));
    }
    return result;
}

ColumnUInt8::MutablePtr presence_rows(std::initializer_list<uint8_t> values) {
    auto result = ColumnUInt8::create();
    for (uint8_t value : values) {
        result->insert_value(value);
    }
    return result;
}

ColumnVariantV2::MutablePtr encoded_shredded(std::initializer_list<std::string_view> residual_rows,
                                             std::initializer_list<std::string_view> child_rows,
                                             std::initializer_list<uint8_t> presence,
                                             PathInData path = PathInData("a")) {
    auto residual = encoded_rows(residual_rows);
    auto child = encoded_rows(child_rows);
    auto present = presence_rows(presence);
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(std::move(path), std::move(child), std::move(present));
    return ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
}

ColumnVariantV2::MutablePtr typed_int_shredded(
        std::initializer_list<std::string_view> residual_rows,
        std::initializer_list<int64_t> child_rows, std::initializer_list<uint8_t> presence,
        PathInData path = PathInData("a")) {
    auto values = ColumnInt64::create();
    for (int64_t value : child_rows) {
        values->insert_value(value);
    }
    auto nulls = ColumnUInt8::create(values->size(), 0);
    auto child = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(nulls)),
            std::make_shared<DataTypeInt64>());
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(std::move(path), std::move(child), presence_rows(presence));
    return ColumnVariantV2::create_shredded(encoded_rows(residual_rows), std::move(fields));
}

ColumnVariantV2::MutablePtr typed_int_rows(std::initializer_list<int64_t> rows) {
    auto values = ColumnInt64::create();
    for (int64_t value : rows) {
        values->insert_value(value);
    }
    auto nulls = ColumnUInt8::create(values->size(), 0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(nulls)),
            std::make_shared<DataTypeInt64>());
}

ColumnVariantV2::MutablePtr typed_string_rows(std::initializer_list<std::string_view> rows) {
    auto values = ColumnString::create();
    for (std::string_view value : rows) {
        values->insert_data(value.data(), value.size());
    }
    auto nulls = ColumnUInt8::create(values->size(), 0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(nulls)),
            std::make_shared<DataTypeString>());
}

ColumnVariantV2::MutablePtr shredded_with_typed_sibling(
        std::initializer_list<std::string_view> residual_rows,
        ColumnVariantV2::MutablePtr conflict_values,
        std::initializer_list<uint8_t> conflict_presence,
        std::initializer_list<int64_t> sibling_values,
        std::initializer_list<uint8_t> sibling_presence) {
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData("a"), std::move(conflict_values),
                        presence_rows(conflict_presence));
    fields.emplace_back(PathInData("stable"), typed_int_rows(sibling_values),
                        presence_rows(sibling_presence));
    return ColumnVariantV2::create_shredded(encoded_rows(residual_rows), std::move(fields));
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

std::vector<std::string> materialized_json(const ColumnVariantV2& column) {
    auto encoded = column.materialize_encoded_range(0, column.size());
    std::vector<std::string> result;
    result.reserve(encoded->size());
    for (size_t row = 0; row < encoded->size(); ++row) {
        result.push_back(json(encoded->get_value_ref(row)));
    }
    return result;
}

void expect_json(const ColumnVariantV2& column, std::initializer_list<std::string_view> expected) {
    const std::vector<std::string> actual = materialized_json(column);
    ASSERT_EQ(actual.size(), expected.size());
    size_t row = 0;
    for (std::string_view value : expected) {
        EXPECT_EQ(actual[row], value) << "row " << row;
        ++row;
    }
}

void expect_same_layout_conflict(const ColumnVariantV2& column,
                                 std::initializer_list<uint8_t> expected_conflict_presence,
                                 std::initializer_list<uint8_t> expected_sibling_presence,
                                 std::initializer_list<std::string_view> expected_residual,
                                 std::initializer_list<std::string_view> expected_sibling,
                                 std::initializer_list<std::string_view> expected_json,
                                 bool expected_conflict_typed = false) {
    ASSERT_TRUE(column.is_shredded());
    ASSERT_EQ(column.shredded_field_count(), 2);
    EXPECT_EQ(column.shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(column.shredded_field_values(0).is_typed(), expected_conflict_typed);
    const auto& conflict_presence = column.shredded_field_presence(0).get_data();
    EXPECT_EQ(std::vector<uint8_t>(conflict_presence.begin(), conflict_presence.end()),
              std::vector<uint8_t>(expected_conflict_presence));

    EXPECT_EQ(column.shredded_field_path(1).get_path(), "stable");
    EXPECT_TRUE(column.shredded_field_values(1).is_typed());
    const auto& sibling_presence = column.shredded_field_presence(1).get_data();
    EXPECT_EQ(std::vector<uint8_t>(sibling_presence.begin(), sibling_presence.end()),
              std::vector<uint8_t>(expected_sibling_presence));
    expect_json(column.shredded_field_values(1), expected_sibling);

    ASSERT_EQ(column.size(), expected_residual.size());
    const auto view = column.read_view();
    size_t row = 0;
    for (std::string_view expected : expected_residual) {
        EXPECT_EQ(json(view.residual_value_at(row)), expected) << "residual row " << row;
        ++row;
    }
    expect_json(column, expected_json);
}

TEST(ColumnVariantV2ShreddedTest, MaterializesTypeConflictAndKeepsObjectConflictResidual) {
    auto shredded = encoded_shredded(
            {R"({"keep":0})", R"({"keep":1})", R"({"a":{"b":2},"keep":2})", R"({"keep":3})"},
            {"1", R"("one")", "{}", "{}"}, {1, 1, 0, 0});

    ASSERT_TRUE(shredded->is_shredded());
    ASSERT_TRUE(shredded->shredded_field_values(0).is_encoded());
    expect_json(*shredded, {R"({"a":1,"keep":0})", R"({"a":"one","keep":1})",
                            R"({"a":{"b":2},"keep":2})", R"({"keep":3})"});
    EXPECT_TRUE(shredded->is_shredded());
}

TEST(ColumnVariantV2ShreddedTest, InsertAndDefaultsPreserveShreddedState) {
    auto shredded = typed_int_shredded({R"({"keep":0})"}, {1}, {1});
    auto encoded_oracle = encoded_rows({R"({"a":1,"keep":0})"});
    const PathInData* const path_identity = &shredded->shredded_field_path(0);
    const ColumnVariantV2* const child_identity = &shredded->shredded_field_values(0);
    const DataTypePtr child_type = child_identity->typed_type();
    const auto expect_state = [&](std::initializer_list<uint8_t> expected_presence) {
        ASSERT_TRUE(shredded->is_shredded());
        ASSERT_TRUE(encoded_oracle->is_encoded());
        ASSERT_EQ(shredded->shredded_field_count(), 1);
        EXPECT_EQ(shredded->shredded_field_path(0).get_path(), "a");
        EXPECT_EQ(&shredded->shredded_field_path(0), path_identity);
        EXPECT_EQ(&shredded->shredded_field_values(0), child_identity);
        ASSERT_TRUE(shredded->shredded_field_values(0).is_typed());
        EXPECT_EQ(shredded->shredded_field_values(0).typed_type().get(), child_type.get());
        const auto& presence = shredded->shredded_field_presence(0).get_data();
        EXPECT_EQ(std::vector<uint8_t>(presence.begin(), presence.end()),
                  std::vector<uint8_t>(expected_presence));
        EXPECT_EQ(materialized_json(*shredded), materialized_json(*encoded_oracle));
    };

    expect_state({1});

    const Field scalar = Field::create_field<TYPE_VARIANT>(encode_json(R"({"a":1})"));
    shredded->insert(scalar);
    encoded_oracle->insert(scalar);
    expect_state({1, 0});

    const Field object = Field::create_field<TYPE_VARIANT>(encode_json(R"({"a":{"b":1}})"));
    shredded->insert(object);
    encoded_oracle->insert(object);
    expect_state({1, 0, 0});

    const Field variant_null = Field::create_field<TYPE_VARIANT>(encode_json("null"));
    shredded->insert(variant_null);
    encoded_oracle->insert(variant_null);
    expect_state({1, 0, 0, 0});

    shredded->insert_default();
    encoded_oracle->insert_default();
    expect_state({1, 0, 0, 0, 0});

    shredded->insert_many_defaults(2);
    encoded_oracle->insert_many_defaults(2);
    expect_state({1, 0, 0, 0, 0, 0, 0});
}

TEST(ColumnVariantV2ShreddedTest, FactoryRejectsResidualOverlapBeforePublishing) {
    EXPECT_DEATH(
            {
                ColumnVariantV2::ShreddedFields fields;
                fields.emplace_back(PathInData("a"), encoded_rows({"8"}), presence_rows({1}));
                static_cast<void>(ColumnVariantV2::create_shredded(encoded_rows({R"({"a":7})"}),
                                                                   std::move(fields)));
            },
            "residual overlaps a present shredded field");

    EXPECT_DEATH(
            {
                ColumnVariantV2::ShreddedFields fields;
                fields.emplace_back(PathInData(std::vector<std::string> {"a", "b"}),
                                    encoded_rows({"8"}), presence_rows({1}));
                static_cast<void>(ColumnVariantV2::create_shredded(encoded_rows({R"({"a":7})"}),
                                                                   std::move(fields)));
            },
            "residual has a scalar or array ancestor of shredded path");

    EXPECT_DEATH(
            {
                ColumnVariantV2::ShreddedFields fields;
                fields.emplace_back(PathInData("a"), encoded_rows({R"({"b":2})"}),
                                    presence_rows({1}));
                static_cast<void>(
                        ColumnVariantV2::create_shredded(encoded_rows({"{}"}), std::move(fields)));
            },
            "active shredded field must be scalar");

    auto inactive = encoded_shredded({R"({"a":7})"}, {"8"}, {0});
    expect_json(*inactive, {R"({"a":7})"});
    auto nested = encoded_shredded({R"({"a":{"c":3}})"}, {"2"}, {1},
                                   PathInData(std::vector<std::string> {"a", "b"}));
    expect_json(*nested, {R"({"a":{"b":2,"c":3}})"});
}

TEST(ColumnVariantV2ShreddedTest, FactoryEnforcesVariantNestingDepth) {
    auto maximum =
            encoded_shredded({"{}"}, {"1"}, {1},
                             PathInData(std::vector<std::string>(VARIANT_MAX_NESTING_DEPTH, "a")));
    auto materialized = maximum->materialize_encoded_range(0, 1);
    ASSERT_EQ(materialized->size(), 1);
    materialized->sanity_check();

    EXPECT_DEATH(
            {
                ColumnVariantV2::ShreddedFields fields;
                fields.emplace_back(
                        PathInData(std::vector<std::string>(VARIANT_MAX_NESTING_DEPTH + 1, "a")),
                        encoded_rows({"1"}), presence_rows({1}));
            },
            "shredded field path exceeds maximum nesting depth");
}

TEST(ColumnVariantV2ShreddedTest, FactoryRejectsInvalidUtf8Path) {
    EXPECT_DEATH(
            {
                ColumnVariantV2::ShreddedFields fields;
                fields.emplace_back(PathInData(std::vector<std::string> {
                                            std::string(1, static_cast<char>(0xFF))}),
                                    encoded_rows({"1"}), presence_rows({1}));
            },
            "invalid UTF-8");
}

TEST(ColumnVariantV2ShreddedTest, FactoryPreservesResidualAliasAndCowDetach) {
    auto residual = encoded_rows({"{}", "{}"});
    ColumnPtr residual_alias = residual->get_ptr();
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData("a"), encoded_rows({"1", "2"}), presence_rows({1, 1}));
    auto shredded = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));

    const auto& unchanged_before = assert_cast<const ColumnVariantV2&>(*residual_alias);
    ASSERT_TRUE(unchanged_before.is_encoded());
    expect_json(unchanged_before, {"{}", "{}"});
    unchanged_before.sanity_check();

    shredded->pop_back(1);

    ASSERT_TRUE(shredded->is_shredded());
    expect_json(*shredded, {R"({"a":1})"});
    const auto& unchanged_after = assert_cast<const ColumnVariantV2&>(*residual_alias);
    ASSERT_EQ(unchanged_after.size(), 2);
    expect_json(unchanged_after, {"{}", "{}"});
    unchanged_after.sanity_check();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions cover the complete state matrix.
TEST(ColumnVariantV2ShreddedTest, RangeAndIndicesCompleteStateMatrix) {
    {
        auto source = typed_int_rows({999, 7, 8});
        auto destination = typed_int_shredded({"{}"}, {9}, {1});

        destination->insert_range_from(*source, 1, 2);

        ASSERT_TRUE(destination->is_shredded());
        ASSERT_EQ(destination->shredded_field_count(), 1);
        EXPECT_EQ(destination->shredded_field_path(0).get_path(), "a");
        EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
                  (PaddedPODArray<UInt8> {1, 0, 0}));
        expect_json(*destination, {R"({"a":9})", "7", "8"});
        EXPECT_TRUE(source->is_typed());
    }

    {
        auto source = typed_int_rows({999, 7, 8});
        auto destination = typed_int_shredded({"{}"}, {9}, {1});
        constexpr std::array<uint32_t, 3> INDICES {2, 1, 2};

        destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());

        ASSERT_TRUE(destination->is_shredded());
        EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
                  (PaddedPODArray<UInt8> {1, 0, 0, 0}));
        expect_json(*destination, {R"({"a":9})", "8", "7", "8"});
        EXPECT_TRUE(source->is_typed());
    }

    auto source =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "null"}, {1, 1, 0});
    {
        auto destination = ColumnVariantV2::create();

        destination->insert_range_from(*source, 1, 2);

        ASSERT_TRUE(destination->is_shredded());
        ASSERT_EQ(destination->shredded_field_count(), 1);
        EXPECT_EQ(destination->shredded_field_path(0).get_path(), "a");
        EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
                  (PaddedPODArray<UInt8> {1, 0}));
        expect_json(*destination, {R"({"a":"two","keep":2})", R"({"a":{"nested":3},"keep":3})"});
        EXPECT_TRUE(source->is_shredded());
    }

    {
        auto destination = ColumnVariantV2::create();
        constexpr std::array<uint32_t, 2> INDICES {2, 0};

        destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());

        ASSERT_TRUE(destination->is_shredded());
        EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
                  (PaddedPODArray<UInt8> {0, 1}));
        expect_json(*destination, {R"({"a":{"nested":3},"keep":3})", R"({"a":1,"keep":1})"});
        EXPECT_TRUE(source->is_shredded());
    }

    {
        auto destination = typed_int_shredded({"{}"}, {9}, {1});
        uint32_t empty_index_sentinel = 0;
        const uint32_t* empty_indices = &empty_index_sentinel;

        destination->insert_range_from(*source, source->size(), 0);
        destination->insert_indices_from(*source, empty_indices, empty_indices);

        ASSERT_TRUE(destination->is_shredded());
        EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
        EXPECT_EQ(destination->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1}));
        expect_json(*destination, {R"({"a":9})"});
        EXPECT_TRUE(source->is_shredded());
    }

    {
        auto repeated_source = typed_int_shredded({"{}", "{}"}, {1, 2}, {1, 1});
        auto destination = typed_int_shredded({"{}"}, {9}, {1});
        constexpr std::array<uint32_t, 3> REPEATED_INDICES {1, 1, 0};
        const ColumnVariantV2* const child = &destination->shredded_field_values(0);
        const IColumn* const typed = &child->typed_column();
        const ColumnUInt8* const presence = &destination->shredded_field_presence(0);

        destination->insert_indices_from(*repeated_source, REPEATED_INDICES.begin(),
                                         REPEATED_INDICES.end());

        ASSERT_TRUE(destination->is_shredded());
        EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
        EXPECT_EQ(&destination->shredded_field_values(0), child);
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), presence);
        EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
                  (PaddedPODArray<UInt8> {1, 1, 1, 1}));
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*destination), 0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 0);
        expect_json(*destination, {R"({"a":9})", R"({"a":2})", R"({"a":2})", R"({"a":1})"});
        ASSERT_TRUE(repeated_source->is_shredded());
        EXPECT_TRUE(repeated_source->shredded_field_values(0).is_typed());
        expect_json(*repeated_source, {R"({"a":1})", R"({"a":2})"});
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions cover both conflict directions.
TEST(ColumnVariantV2ShreddedTest, SameLayoutTypedConflictFallsBackOnlyPresentRows) {
    {
        auto destination = shredded_with_typed_sibling({R"({"keep":0})"}, typed_int_rows({7}), {1},
                                                       {100}, {1});
        auto source = shredded_with_typed_sibling({R"({"keep":1})"}, typed_string_rows({"seven"}),
                                                  {1}, {200}, {1});
        const auto expected_source = materialized_json(*source);
        const ColumnVariantV2* const conflict_child = &destination->shredded_field_values(0);
        const IColumn* const conflict_typed = &conflict_child->typed_column();
        const ColumnUInt8* const conflict_presence = &destination->shredded_field_presence(0);
        ASSERT_TRUE(destination->shredded_field_values(0).is_typed());
        ASSERT_TRUE(source->shredded_field_values(0).is_typed());

        destination->insert_range_from(*source, 0, 1);

        expect_same_layout_conflict(
                *destination, {1, 0}, {1, 1}, {R"({"keep":0})", R"({"a":"seven","keep":1})"},
                {"100", "200"},
                {R"({"a":7,"keep":0,"stable":100})", R"({"a":"seven","keep":1,"stable":200})"},
                true);
        EXPECT_EQ(&destination->shredded_field_values(0), conflict_child);
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), conflict_typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), conflict_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 1);
        ASSERT_TRUE(source->is_shredded());
        EXPECT_TRUE(source->shredded_field_values(0).is_typed());
        EXPECT_TRUE(source->shredded_field_values(1).is_typed());
        EXPECT_EQ(materialized_json(*source), expected_source);
    }

    {
        auto destination = shredded_with_typed_sibling(
                {R"({"keep":0})"}, typed_string_rows({"seven"}), {1}, {100}, {1});
        auto source = shredded_with_typed_sibling({R"({"keep":1})"}, typed_int_rows({7}), {1},
                                                  {200}, {1});
        constexpr std::array<uint32_t, 1> INDICES {0};
        const auto expected_source = materialized_json(*source);
        const ColumnVariantV2* const conflict_child = &destination->shredded_field_values(0);
        const IColumn* const conflict_typed = &conflict_child->typed_column();
        const ColumnUInt8* const conflict_presence = &destination->shredded_field_presence(0);
        ASSERT_TRUE(destination->shredded_field_values(0).is_typed());
        ASSERT_TRUE(source->shredded_field_values(0).is_typed());

        destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());

        expect_same_layout_conflict(
                *destination, {1, 0}, {1, 1}, {R"({"keep":0})", R"({"a":7,"keep":1})"},
                {"100", "200"},
                {R"({"a":"seven","keep":0,"stable":100})", R"({"a":7,"keep":1,"stable":200})"},
                true);
        EXPECT_EQ(&destination->shredded_field_values(0), conflict_child);
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), conflict_typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), conflict_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 1);
        ASSERT_TRUE(source->is_shredded());
        EXPECT_TRUE(source->shredded_field_values(0).is_typed());
        EXPECT_TRUE(source->shredded_field_values(1).is_typed());
        EXPECT_EQ(materialized_json(*source), expected_source);
    }

    {
        auto destination = shredded_with_typed_sibling({R"({"keep":0})"}, typed_int_rows({7}), {1},
                                                       {100}, {1});
        auto source = shredded_with_typed_sibling({R"({"a":{"b":2},"keep":1})"},
                                                  encoded_rows({"null"}), {0}, {200}, {1});
        const auto expected_source = materialized_json(*source);
        const ColumnVariantV2* const conflict_child = &destination->shredded_field_values(0);
        const IColumn* const conflict_typed = &conflict_child->typed_column();
        const ColumnUInt8* const conflict_presence = &destination->shredded_field_presence(0);
        ASSERT_TRUE(destination->shredded_field_values(0).is_typed());
        ASSERT_TRUE(source->shredded_field_values(0).is_encoded());

        destination->insert_range_from(*source, 0, 1);

        expect_same_layout_conflict(
                *destination, {1, 0}, {1, 1}, {R"({"keep":0})", R"({"a":{"b":2},"keep":1})"},
                {"100", "200"},
                {R"({"a":7,"keep":0,"stable":100})", R"({"a":{"b":2},"keep":1,"stable":200})"},
                true);
        EXPECT_EQ(&destination->shredded_field_values(0), conflict_child);
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), conflict_typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), conflict_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 0);
        ASSERT_TRUE(source->is_shredded());
        EXPECT_TRUE(source->shredded_field_values(0).is_encoded());
        EXPECT_TRUE(source->shredded_field_values(1).is_typed());
        EXPECT_EQ(materialized_json(*source), expected_source);
    }

    {
        auto destination = shredded_with_typed_sibling({R"({"a":{"b":2},"keep":0})"},
                                                       typed_int_rows({0}), {0}, {100}, {1});
        auto source = shredded_with_typed_sibling({R"({"keep":1})"}, encoded_rows({"7"}), {1},
                                                  {200}, {1});
        constexpr std::array<uint32_t, 1> INDICES {0};
        const auto expected_source = materialized_json(*source);
        const ColumnVariantV2* const conflict_child = &destination->shredded_field_values(0);
        const IColumn* const conflict_typed = &conflict_child->typed_column();
        const ColumnUInt8* const conflict_presence = &destination->shredded_field_presence(0);
        ASSERT_TRUE(destination->shredded_field_values(0).is_typed());
        ASSERT_TRUE(source->shredded_field_values(0).is_encoded());

        destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());

        expect_same_layout_conflict(
                *destination, {0, 0}, {1, 1}, {R"({"a":{"b":2},"keep":0})", R"({"a":7,"keep":1})"},
                {"100", "200"},
                {R"({"a":{"b":2},"keep":0,"stable":100})", R"({"a":7,"keep":1,"stable":200})"},
                true);
        EXPECT_EQ(&destination->shredded_field_values(0), conflict_child);
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), conflict_typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), conflict_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 1);
        ASSERT_TRUE(source->is_shredded());
        EXPECT_TRUE(source->shredded_field_values(0).is_encoded());
        EXPECT_TRUE(source->shredded_field_values(1).is_typed());
        EXPECT_EQ(materialized_json(*source), expected_source);
    }
}

TEST(ColumnVariantV2ShreddedTest, RangeAndIndicesPreserveOrAdoptShreddedLayout) {
    auto source =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "{}"}, {1, 1, 0});

    auto same_layout = typed_int_shredded({"{}"}, {9}, {1});
    same_layout->insert_range_from(*source, 1, 2);
    ASSERT_TRUE(same_layout->is_shredded());
    EXPECT_TRUE(same_layout->shredded_field_values(0).is_typed());
    EXPECT_EQ(same_layout->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0}));
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*same_layout), 1);
    expect_json(*same_layout,
                {R"({"a":9})", R"({"a":"two","keep":2})", R"({"a":{"nested":3},"keep":3})"});

    auto different_layout = encoded_shredded({"{}"}, {"4"}, {1}, PathInData("b"));
    different_layout->insert_range_from(*source, 0, 2);
    ASSERT_TRUE(different_layout->is_shredded());
    ASSERT_EQ(different_layout->shredded_field_count(), 2);
    EXPECT_EQ(different_layout->shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(different_layout->shredded_field_path(1).get_path(), "b");
    expect_json(*different_layout,
                {R"({"b":4})", R"({"a":1,"keep":1})", R"({"a":"two","keep":2})"});

    auto encoded_source = encoded_rows({R"({"x":1})", R"({"a":"fallback"})"});
    auto shredded_destination = typed_int_shredded({"{}"}, {9}, {1});
    shredded_destination->insert_range_from(*encoded_source, 0, encoded_source->size());
    ASSERT_TRUE(shredded_destination->is_shredded());
    EXPECT_EQ(shredded_destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0}));
    expect_json(*shredded_destination, {R"({"a":9})", R"({"x":1})", R"({"a":"fallback"})"});

    constexpr std::array<uint32_t, 2> INDICES {2, 0};
    auto selected = ColumnVariantV2::create();
    selected->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(selected->is_shredded());
    expect_json(*selected, {R"({"a":{"nested":3},"keep":3})", R"({"a":1,"keep":1})"});

    auto indexed_same_layout = typed_int_shredded({"{}"}, {9}, {1});
    indexed_same_layout->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(indexed_same_layout->is_shredded());
    EXPECT_TRUE(indexed_same_layout->shredded_field_values(0).is_typed());
    EXPECT_EQ(indexed_same_layout->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0}));
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*indexed_same_layout), 1);
    expect_json(*indexed_same_layout,
                {R"({"a":9})", R"({"a":{"nested":3},"keep":3})", R"({"a":1,"keep":1})"});

    auto encoded_destination = encoded_rows({R"({"seed":0})"});
    encoded_destination->insert_range_from(*source, 0, source->size());
    ASSERT_TRUE(encoded_destination->is_shredded());
    EXPECT_EQ(encoded_destination->shredded_field_presence(0).get_data()[0], 0);
    expect_json(*encoded_destination,
                {R"({"seed":0})", R"({"a":1,"keep":1})", R"({"a":"two","keep":2})",
                 R"({"a":{"nested":3},"keep":3})"});
    EXPECT_TRUE(source->is_shredded());

    auto self_range =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "{}"}, {1, 1, 0});
    self_range->insert_range_from(*self_range, 1, 2);
    ASSERT_TRUE(self_range->is_shredded());
    expect_json(*self_range,
                {R"({"a":1,"keep":1})", R"({"a":"two","keep":2})", R"({"a":{"nested":3},"keep":3})",
                 R"({"a":"two","keep":2})", R"({"a":{"nested":3},"keep":3})"});

    auto self_indices =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "{}"}, {1, 1, 0});
    self_indices->insert_indices_from(*self_indices, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(self_indices->is_shredded());
    expect_json(*self_indices,
                {R"({"a":1,"keep":1})", R"({"a":"two","keep":2})", R"({"a":{"nested":3},"keep":3})",
                 R"({"a":{"nested":3},"keep":3})", R"({"a":1,"keep":1})"});
}

TEST(ColumnVariantV2ShreddedTest, UnionBudgetSkipsSourcePathMissingFromSelection) {
    ColumnVariantV2::ShreddedFields source_fields;
    source_fields.emplace_back(PathInData("a"), typed_int_rows({100, 200, 300, 400}),
                               presence_rows({1, 0, 0, 1}));
    source_fields.emplace_back(PathInData("b"), typed_int_rows({10, 20, 30, 40}),
                               presence_rows({1, 1, 1, 1}));
    auto source = ColumnVariantV2::create_shredded(
            encoded_rows({R"({"keep":0})", R"({"keep":1})", R"({"keep":2})", R"({"keep":3})"}),
            std::move(source_fields));
    const std::vector<std::string> expected_source {R"({"a":100,"b":10,"keep":0})",
                                                    R"({"b":20,"keep":1})", R"({"b":30,"keep":2})",
                                                    R"({"a":400,"b":40,"keep":3})"};
    constexpr std::array<uint32_t, 3> INDICES {2, 1, 2};

    struct InsertCase {
        std::string_view name;
        bool use_indices;
        std::vector<std::string> expected_json;
        std::vector<uint8_t> expected_b_presence;
    };
    const std::array<InsertCase, 2> cases {{
            {"range",
             false,
             {R"({"z":9})", R"({"b":20,"keep":1})", R"({"b":30,"keep":2})"},
             {0, 1, 1}},
            {"indices",
             true,
             {R"({"z":9})", R"({"b":30,"keep":2})", R"({"b":20,"keep":1})", R"({"b":30,"keep":2})"},
             {0, 1, 1, 1}},
    }};

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto destination = typed_int_shredded({"{}"}, {9}, {1}, PathInData("z"));

        if (test_case.use_indices) {
            destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());
        } else {
            destination->insert_range_from(*source, 1, 2);
        }

        ASSERT_TRUE(destination->is_shredded());
        ASSERT_EQ(destination->shredded_field_count(), 2);
        EXPECT_EQ(destination->shredded_field_path(0).get_path(), "b");
        EXPECT_EQ(destination->shredded_field_path(1).get_path(), "z");
        EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
        const auto& b_presence = destination->shredded_field_presence(0).get_data();
        EXPECT_EQ(std::vector<uint8_t>(b_presence.begin(), b_presence.end()),
                  test_case.expected_b_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*destination), 1);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 0);
        EXPECT_EQ(materialized_json(*destination), test_case.expected_json);

        ASSERT_TRUE(source->is_shredded());
        ASSERT_EQ(source->shredded_field_count(), 2);
        EXPECT_EQ(source->shredded_field_path(0).get_path(), "a");
        EXPECT_EQ(source->shredded_field_path(1).get_path(), "b");
        EXPECT_TRUE(source->shredded_field_values(0).is_typed());
        EXPECT_TRUE(source->shredded_field_values(1).is_typed());
        EXPECT_EQ(source->shredded_field_presence(0).get_data(),
                  (PaddedPODArray<UInt8> {1, 0, 0, 1}));
        EXPECT_EQ(materialized_json(*source), expected_source);
    }
}

TEST(ColumnVariantV2ShreddedTest, SelectedInactiveExactPathStaysTypedAcrossUnion) {
    struct InsertCase {
        std::string_view name;
        bool use_indices;
        std::vector<std::string> expected_json;
        std::vector<uint8_t> expected_a_presence;
        std::vector<uint8_t> expected_b_presence;
    };
    const std::array<InsertCase, 2> cases {{
            {"range",
             false,
             {R"({"a":1})", R"({"a":2})", R"({"b":200})", R"({"b":300})"},
             {1, 1, 0, 0},
             {0, 0, 1, 1}},
            {"indices",
             true,
             {R"({"a":1})", R"({"a":2})", R"({"b":300})", R"({"b":200})", R"({"b":300})"},
             {1, 1, 0, 0, 0},
             {0, 0, 1, 1, 1}},
    }};
    constexpr std::array<uint32_t, 3> INDICES {2, 1, 2};

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto destination = typed_int_shredded({"{}", "{}"}, {1, 2}, {1, 1});

        ColumnVariantV2::ShreddedFields source_fields;
        source_fields.emplace_back(
                PathInData("a"),
                typed_string_rows({"outside-left", "ignored-1", "ignored-2", "outside-right"}),
                presence_rows({1, 0, 0, 1}));
        source_fields.emplace_back(PathInData("b"), typed_int_rows({100, 200, 300, 400}),
                                   presence_rows({0, 1, 1, 0}));
        auto source = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}", "{}", "{}"}),
                                                       std::move(source_fields));
        const auto expected_source = materialized_json(*source);

        const ColumnVariantV2* const a_child = &destination->shredded_field_values(0);
        const IColumn* const a_typed = &a_child->typed_column();
        const ColumnUInt8* const a_presence = &destination->shredded_field_presence(0);

        if (test_case.use_indices) {
            destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());
        } else {
            destination->insert_range_from(*source, 1, 2);
        }

        ASSERT_TRUE(destination->is_shredded());
        ASSERT_EQ(destination->shredded_field_count(), 2);
        EXPECT_EQ(destination->shredded_field_path(0).get_path(), "a");
        EXPECT_EQ(destination->shredded_field_path(1).get_path(), "b");
        EXPECT_EQ(&destination->shredded_field_values(0), a_child);
        ASSERT_TRUE(destination->shredded_field_values(0).is_typed());
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), a_typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), a_presence);
        const auto& output_a_presence = destination->shredded_field_presence(0).get_data();
        EXPECT_EQ(std::vector<uint8_t>(output_a_presence.begin(), output_a_presence.end()),
                  test_case.expected_a_presence);
        EXPECT_TRUE(destination->shredded_field_values(1).is_typed());
        const auto& output_b_presence = destination->shredded_field_presence(1).get_data();
        EXPECT_EQ(std::vector<uint8_t>(output_b_presence.begin(), output_b_presence.end()),
                  test_case.expected_b_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*destination), 1);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(
                          *destination),
                  0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(
                          *destination),
                  0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 0);
        EXPECT_EQ(materialized_json(*destination), test_case.expected_json);
        EXPECT_EQ(materialized_json(*source), expected_source);
    }
}

TEST(ColumnVariantV2ShreddedTest, IndicesCoverEncodedTypedAndDifferentShreddedLayouts) {
    constexpr std::array<uint32_t, 2> INDICES {1, 0};
    auto source = encoded_shredded({R"({"keep":1})", R"({"keep":2})"}, {"1", R"("two")"}, {1, 1});

    auto encoded_destination = encoded_rows({R"({"seed":0})"});
    encoded_destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(encoded_destination->is_shredded());
    expect_json(*encoded_destination,
                {R"({"seed":0})", R"({"a":"two","keep":2})", R"({"a":1,"keep":1})"});

    auto encoded_source = encoded_rows({R"({"x":1})", R"({"a":"fallback"})"});
    auto shredded_destination = typed_int_shredded({"{}"}, {9}, {1});
    shredded_destination->insert_indices_from(*encoded_source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(shredded_destination->is_shredded());
    EXPECT_EQ(shredded_destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0}));
    expect_json(*shredded_destination, {R"({"a":9})", R"({"a":"fallback"})", R"({"x":1})"});

    auto typed_source = typed_int_rows({7, 8});
    shredded_destination->insert_indices_from(*typed_source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(shredded_destination->is_shredded());
    expect_json(*shredded_destination,
                {R"({"a":9})", R"({"a":"fallback"})", R"({"x":1})", "8", "7"});

    auto different_layout = encoded_shredded({"{}"}, {"4"}, {1}, PathInData("b"));
    different_layout->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(different_layout->is_shredded());
    ASSERT_EQ(different_layout->shredded_field_count(), 2);
    EXPECT_EQ(different_layout->shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(different_layout->shredded_field_path(1).get_path(), "b");
    expect_json(*different_layout,
                {R"({"b":4})", R"({"a":"two","keep":2})", R"({"a":1,"keep":1})"});
}

TEST(ColumnVariantV2ShreddedTest, BoundedUnionFreezesAndSubsequentLayoutsStayLinear) {
    auto destination = typed_int_shredded({"{}"}, {1}, {1}, PathInData("a"));

    ColumnVariantV2::ShreddedFields first_fields;
    first_fields.emplace_back(PathInData("b"), typed_int_rows({2}), presence_rows({1}));
    first_fields.emplace_back(PathInData("c"), typed_int_rows({3}), presence_rows({1}));
    auto first = ColumnVariantV2::create_shredded(encoded_rows({"{}"}), std::move(first_fields));
    destination->insert_range_from(*first, 0, 1);

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_EQ(destination->shredded_field_path(0).get_path(), "a");
    EXPECT_EQ(destination->shredded_field_path(1).get_path(), "b");
    EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
    EXPECT_TRUE(destination->shredded_field_values(1).is_typed());
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*destination), 1);
    expect_json(*destination, {R"({"a":1})", R"({"b":2,"c":3})"});

    ColumnVariantV2::ShreddedFields frozen_mixed_fields;
    frozen_mixed_fields.emplace_back(PathInData("b"), typed_int_rows({4}), presence_rows({1}));
    frozen_mixed_fields.emplace_back(PathInData("d"), typed_int_rows({40}), presence_rows({1}));
    auto frozen_mixed =
            ColumnVariantV2::create_shredded(encoded_rows({"{}"}), std::move(frozen_mixed_fields));
    destination->insert_range_from(*frozen_mixed, 0, 1);
    auto again_a = typed_int_shredded({"{}"}, {5}, {1}, PathInData("a"));
    destination->insert_range_from(*again_a, 0, 1);
    auto again_b = typed_int_shredded({"{}"}, {6}, {1}, PathInData("b"));
    destination->insert_range_from(*again_b, 0, 1);

    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*destination), 1);
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0, 1, 0}));
    EXPECT_EQ(destination->shredded_field_presence(1).get_data(),
              (PaddedPODArray<UInt8> {0, 1, 1, 0, 1}));
    EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
    EXPECT_TRUE(destination->shredded_field_values(1).is_typed());
    expect_json(*destination, {R"({"a":1})", R"({"b":2,"c":3})", R"({"b":4,"d":40})", R"({"a":5})",
                               R"({"b":6})"});
}

TEST(ColumnVariantV2ShreddedTest, CrossLayoutUnionReusesExclusiveExactTypedFields) {
    struct InsertCase {
        std::string_view name;
        bool use_indices;
        std::vector<std::string> expected_json;
    };
    const std::array<InsertCase, 2> cases {{
            {"range",
             false,
             {R"({"a":1,"stable":10})", R"({"a":2})", R"({"a":3,"stable":30})",
              R"({"b":200,"stable":2000})", R"({"b":300,"stable":3000})"}},
            {"indices",
             true,
             {R"({"a":1,"stable":10})", R"({"a":2})", R"({"a":3,"stable":30})",
              R"({"b":300,"stable":3000})", R"({"b":200,"stable":2000})",
              R"({"b":300,"stable":3000})"}},
    }};
    constexpr std::array<uint32_t, 3> INDICES {2, 1, 2};

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        ColumnVariantV2::ShreddedFields destination_fields;
        destination_fields.emplace_back(PathInData("a"), typed_int_rows({1, 2, 3}),
                                        presence_rows({1, 1, 1}));
        destination_fields.emplace_back(PathInData("stable"), typed_int_rows({10, 20, 30}),
                                        presence_rows({1, 0, 1}));
        auto destination = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}", "{}"}),
                                                            std::move(destination_fields));

        ColumnVariantV2::ShreddedFields source_fields;
        source_fields.emplace_back(PathInData("b"), typed_int_rows({100, 200, 300, 400}),
                                   presence_rows({1, 1, 1, 1}));
        source_fields.emplace_back(PathInData("stable"), typed_int_rows({1000, 2000, 3000, 4000}),
                                   presence_rows({1, 1, 1, 1}));
        auto source = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}", "{}", "{}"}),
                                                       std::move(source_fields));
        const auto expected_source = materialized_json(*source);

        const ColumnVariantV2* const a_child = &destination->shredded_field_values(0);
        const IColumn* const a_typed = &a_child->typed_column();
        const ColumnUInt8* const a_presence = &destination->shredded_field_presence(0);
        const ColumnVariantV2* const stable_child = &destination->shredded_field_values(1);
        const IColumn* const stable_typed = &stable_child->typed_column();
        const ColumnUInt8* const stable_presence = &destination->shredded_field_presence(1);
        const ColumnVariantV2* const source_b_child = &source->shredded_field_values(0);
        const ColumnUInt8* const source_b_presence = &source->shredded_field_presence(0);

        if (test_case.use_indices) {
            destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());
        } else {
            destination->insert_range_from(*source, 1, 2);
        }

        ASSERT_TRUE(destination->is_shredded());
        ASSERT_EQ(destination->shredded_field_count(), 3);
        EXPECT_EQ(destination->shredded_field_path(0).get_path(), "a");
        EXPECT_EQ(destination->shredded_field_path(1).get_path(), "b");
        EXPECT_EQ(destination->shredded_field_path(2).get_path(), "stable");
        EXPECT_EQ(&destination->shredded_field_values(0), a_child);
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), a_typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), a_presence);
        EXPECT_EQ(&destination->shredded_field_values(2), stable_child);
        EXPECT_EQ(&destination->shredded_field_values(2).typed_column(), stable_typed);
        EXPECT_EQ(&destination->shredded_field_presence(2), stable_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(
                          *destination),
                  0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(
                          *destination),
                  0);
        EXPECT_EQ(materialized_json(*destination), test_case.expected_json);

        EXPECT_EQ(&source->shredded_field_values(0), source_b_child);
        EXPECT_EQ(&source->shredded_field_presence(0), source_b_presence);
        EXPECT_EQ(materialized_json(*source), expected_source);
    }
}

TEST(ColumnVariantV2ShreddedTest, CrossLayoutUnionReusesExclusiveStringFieldWithIndices) {
    ColumnVariantV2::ShreddedFields destination_fields;
    destination_fields.emplace_back(PathInData("a"), typed_int_rows({1, 2}), presence_rows({1, 1}));
    destination_fields.emplace_back(PathInData("stable"), typed_string_rows({"left", "right"}),
                                    presence_rows({1, 1}));
    auto destination = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}"}),
                                                        std::move(destination_fields));

    ColumnVariantV2::ShreddedFields source_fields;
    source_fields.emplace_back(PathInData("b"), typed_int_rows({3, 4, 5}),
                               presence_rows({1, 1, 1}));
    source_fields.emplace_back(PathInData("stable"), typed_string_rows({"x", "yy", "zzz"}),
                               presence_rows({1, 1, 1}));
    auto source = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}", "{}"}),
                                                   std::move(source_fields));
    const auto expected_source = materialized_json(*source);

    const ColumnVariantV2* const stable_child = &destination->shredded_field_values(1);
    const IColumn* const stable_typed = &stable_child->typed_column();
    const auto& stable_nullable = assert_cast<const ColumnNullable&>(*stable_typed);
    const IColumn* const stable_nested = &stable_nullable.get_nested_column();
    const ColumnUInt8* const stable_presence = &destination->shredded_field_presence(1);
    constexpr std::array<uint32_t, 3> INDICES {2, 0, 2};

    destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());

    ASSERT_EQ(destination->shredded_field_count(), 3);
    EXPECT_EQ(&destination->shredded_field_values(2), stable_child);
    EXPECT_EQ(&destination->shredded_field_values(2).typed_column(), stable_typed);
    const auto& output_nullable = assert_cast<const ColumnNullable&>(*stable_typed);
    EXPECT_EQ(&output_nullable.get_nested_column(), stable_nested);
    EXPECT_EQ(&destination->shredded_field_presence(2), stable_presence);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(*destination),
              0);
    EXPECT_EQ(
            ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(*destination),
            0);
    expect_json(*destination, {R"({"a":1,"stable":"left"})", R"({"a":2,"stable":"right"})",
                               R"({"b":5,"stable":"zzz"})", R"({"b":3,"stable":"x"})",
                               R"({"b":5,"stable":"zzz"})"});
    EXPECT_EQ(materialized_json(*source), expected_source);
}

TEST(ColumnVariantV2ShreddedTest, CrossLayoutUnionDetachesSharedExactTypedFields) {
    const PathInData root_a(std::vector<std::string> {"root", "a"});
    const PathInData root_stable(std::vector<std::string> {"root", "stable"});
    ColumnVariantV2::ShreddedFields owner_fields;
    owner_fields.emplace_back(root_a, typed_int_rows({1, 2}), presence_rows({1, 1}));
    owner_fields.emplace_back(root_stable, typed_int_rows({10, 20}), presence_rows({1, 1}));
    auto owner =
            ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}"}), std::move(owner_fields));
    const auto expected_owner = materialized_json(*owner);
    auto destination = owner->project_shredded_fields(encoded_rows({"{}", "{}"}), 0, 2, 1);
    ASSERT_EQ(&destination->shredded_field_values(0), &owner->shredded_field_values(0));
    ASSERT_EQ(&destination->shredded_field_presence(0), &owner->shredded_field_presence(0));

    ColumnVariantV2::ShreddedFields source_fields;
    source_fields.emplace_back(PathInData("b"), typed_int_rows({3, 4}), presence_rows({1, 1}));
    source_fields.emplace_back(PathInData("stable"), typed_int_rows({30, 40}),
                               presence_rows({1, 1}));
    auto source =
            ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}"}), std::move(source_fields));
    const auto expected_source = materialized_json(*source);

    destination->insert_range_from(*source, 0, source->size());

    ASSERT_EQ(destination->shredded_field_count(), 3);
    EXPECT_NE(&destination->shredded_field_values(0), &owner->shredded_field_values(0));
    EXPECT_NE(&destination->shredded_field_presence(0), &owner->shredded_field_presence(0));
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(*destination),
              4);
    EXPECT_EQ(
            ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(*destination),
            4);
    expect_json(*destination, {R"({"a":1,"stable":10})", R"({"a":2,"stable":20})",
                               R"({"b":3,"stable":30})", R"({"b":4,"stable":40})"});
    EXPECT_EQ(materialized_json(*owner), expected_owner);
    EXPECT_EQ(materialized_json(*source), expected_source);
}

TEST(ColumnVariantV2ShreddedTest, CrossLayoutUnionCopiesOnlyRecursivelySharedTypedChild) {
    ColumnVariantV2::ShreddedFields destination_fields;
    destination_fields.emplace_back(PathInData("a"), typed_int_rows({1, 2}), presence_rows({1, 1}));
    destination_fields.emplace_back(PathInData("stable"), typed_int_rows({10, 20}),
                                    presence_rows({1, 1}));
    auto destination = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}"}),
                                                        std::move(destination_fields));

    ColumnVariantV2::ShreddedFields source_fields;
    source_fields.emplace_back(PathInData("b"), typed_int_rows({3, 4}), presence_rows({1, 1}));
    source_fields.emplace_back(PathInData("stable"), typed_int_rows({30, 40}),
                               presence_rows({1, 1}));
    auto source =
            ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}"}), std::move(source_fields));
    const auto expected_source = materialized_json(*source);

    const ColumnVariantV2* const a_child = &destination->shredded_field_values(0);
    const ColumnUInt8* const a_presence = &destination->shredded_field_presence(0);
    const ColumnVariantV2* const stable_child = &destination->shredded_field_values(1);
    const ColumnUInt8* const stable_presence = &destination->shredded_field_presence(1);
    const auto& stable_nullable = assert_cast<const ColumnNullable&>(stable_child->typed_column());
    ColumnPtr nested_alias = stable_nullable.get_nested_column().get_ptr();
    ASSERT_TRUE(a_child->is_exclusive());
    ASSERT_FALSE(stable_child->is_exclusive());
    constexpr std::array<uint32_t, 3> INDICES {1, 0, 1};

    destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());

    ASSERT_EQ(destination->shredded_field_count(), 3);
    EXPECT_EQ(&destination->shredded_field_values(0), a_child);
    EXPECT_EQ(&destination->shredded_field_presence(0), a_presence);
    EXPECT_NE(&destination->shredded_field_values(2), stable_child);
    EXPECT_EQ(&destination->shredded_field_presence(2), stable_presence);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(*destination),
              2);
    EXPECT_EQ(
            ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(*destination),
            0);
    expect_json(*destination,
                {R"({"a":1,"stable":10})", R"({"a":2,"stable":20})", R"({"b":4,"stable":40})",
                 R"({"b":3,"stable":30})", R"({"b":4,"stable":40})"});

    const auto& alias_values = assert_cast<const ColumnInt64&>(*nested_alias).get_data();
    ASSERT_EQ(alias_values.size(), 2);
    EXPECT_EQ(alias_values[0], 10);
    EXPECT_EQ(alias_values[1], 20);
    EXPECT_EQ(materialized_json(*source), expected_source);
}

TEST(ColumnVariantV2ShreddedTest, CrossLayoutUnionRoutesTypedMismatchRowsToResidual) {
    struct InsertCase {
        std::string_view name;
        bool use_indices;
        std::vector<uint8_t> expected_stable_presence;
        std::vector<std::string> expected_json;
        size_t expected_slow_rows;
    };
    const std::array<InsertCase, 2> cases {{
            {"range",
             false,
             {1, 1, 0, 0},
             {R"({"a":1,"stable":10})", R"({"a":2,"stable":20})", R"({"b":3,"stable":"thirty"})",
              R"({"b":4,"stable":"forty"})"},
             2},
            {"indices",
             true,
             {1, 1, 0, 0, 0},
             {R"({"a":1,"stable":10})", R"({"a":2,"stable":20})", R"({"b":4,"stable":"forty"})",
              R"({"b":3,"stable":"thirty"})", R"({"b":4,"stable":"forty"})"},
             3},
    }};
    constexpr std::array<uint32_t, 3> INDICES {1, 0, 1};

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        ColumnVariantV2::ShreddedFields destination_fields;
        destination_fields.emplace_back(PathInData("a"), typed_int_rows({1, 2}),
                                        presence_rows({1, 1}));
        destination_fields.emplace_back(PathInData("stable"), typed_int_rows({10, 20}),
                                        presence_rows({1, 1}));
        auto destination = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}"}),
                                                            std::move(destination_fields));

        ColumnVariantV2::ShreddedFields source_fields;
        source_fields.emplace_back(PathInData("b"), typed_int_rows({3, 4}), presence_rows({1, 1}));
        source_fields.emplace_back(PathInData("stable"), typed_string_rows({"thirty", "forty"}),
                                   presence_rows({1, 1}));
        auto source = ColumnVariantV2::create_shredded(encoded_rows({"{}", "{}"}),
                                                       std::move(source_fields));
        const auto expected_source = materialized_json(*source);

        const ColumnVariantV2* const a_child = &destination->shredded_field_values(0);
        const IColumn* const a_typed = &a_child->typed_column();
        const ColumnUInt8* const a_presence = &destination->shredded_field_presence(0);
        const ColumnVariantV2* const stable_child = &destination->shredded_field_values(1);
        const IColumn* const stable_typed = &stable_child->typed_column();
        const ColumnUInt8* const stable_presence = &destination->shredded_field_presence(1);

        if (test_case.use_indices) {
            destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());
        } else {
            destination->insert_range_from(*source, 0, source->size());
        }

        ASSERT_EQ(destination->shredded_field_count(), 3);
        EXPECT_EQ(&destination->shredded_field_values(0), a_child);
        EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), a_typed);
        EXPECT_EQ(&destination->shredded_field_presence(0), a_presence);
        EXPECT_EQ(&destination->shredded_field_values(2), stable_child);
        EXPECT_EQ(&destination->shredded_field_values(2).typed_column(), stable_typed);
        EXPECT_EQ(&destination->shredded_field_presence(2), stable_presence);
        EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
        EXPECT_TRUE(destination->shredded_field_values(2).is_typed());
        const auto& output_stable_presence = destination->shredded_field_presence(2).get_data();
        EXPECT_EQ(
                std::vector<uint8_t>(output_stable_presence.begin(), output_stable_presence.end()),
                test_case.expected_stable_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(
                          *destination),
                  0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(
                          *destination),
                  0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination),
                  test_case.expected_slow_rows);
        EXPECT_EQ(materialized_json(*destination), test_case.expected_json);
        EXPECT_EQ(materialized_json(*source), expected_source);
    }
}

TEST(ColumnVariantV2ShreddedTest, RowTransformsPreserveFrozenLayout) {
    enum class Transform { CONST_FILTER, INPLACE_FILTER, PERMUTE };
    struct TransformCase {
        std::string_view name;
        Transform transform;
        bool append_with_indices;
        std::vector<std::string> expected_before_append;
        std::vector<uint8_t> expected_a_presence;
        std::vector<uint8_t> expected_b_presence;
    };
    const std::array<TransformCase, 3> cases {{
            {"const filter",
             Transform::CONST_FILTER,
             false,
             {R"({"a":1})", R"({"b":3})", R"({"b":4})"},
             {1, 0, 0, 0},
             {0, 1, 1, 0}},
            {"in-place filter",
             Transform::INPLACE_FILTER,
             true,
             {R"({"a":1})", R"({"b":3})", R"({"b":4})"},
             {1, 0, 0, 0},
             {0, 1, 1, 0}},
            {"permute",
             Transform::PERMUTE,
             false,
             {R"({"b":4})", R"({"a":1})", R"({"b":3})"},
             {0, 1, 0, 0},
             {1, 0, 1, 0}},
    }};
    constexpr std::array<uint32_t, 1> C_INDEX {1};

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto frozen = typed_int_shredded({"{}", "{}"}, {1, 2}, {1, 1}, PathInData("a"));
        auto b = typed_int_shredded({"{}", "{}"}, {3, 4}, {1, 1}, PathInData("b"));
        frozen->insert_range_from(*b, 0, b->size());
        ASSERT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*frozen), 1);

        MutableColumnPtr transformed;
        if (test_case.transform == Transform::PERMUTE) {
            const IColumn::Permutation permutation {3, 0, 2};
            transformed = frozen->permute(permutation, permutation.size());
        } else {
            const IColumn::Filter filter {1, 0, 1, 1};
            if (test_case.transform == Transform::CONST_FILTER) {
                transformed = IColumn::mutate(std::as_const(*frozen).filter(filter, 3));
            } else {
                EXPECT_EQ(frozen->filter(filter), 3);
                transformed = std::move(frozen);
            }
        }
        auto& transformed_variant = assert_cast<ColumnVariantV2&>(*transformed);
        ASSERT_TRUE(transformed_variant.is_shredded());
        EXPECT_EQ(materialized_json(transformed_variant), test_case.expected_before_append);
        const size_t union_rebuilds_before =
                ColumnVariantV2::TestAccess::shredded_union_rebuilds(transformed_variant);
        const size_t slow_rows_before =
                ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(transformed_variant);

        auto c = typed_int_shredded({"{}", "{}"}, {5, 6}, {1, 1}, PathInData("c"));
        if (test_case.append_with_indices) {
            transformed_variant.insert_indices_from(*c, C_INDEX.begin(), C_INDEX.end());
        } else {
            transformed_variant.insert_range_from(*c, 1, 1);
        }

        ASSERT_EQ(transformed_variant.shredded_field_count(), 2);
        EXPECT_EQ(transformed_variant.shredded_field_path(0).get_path(), "a");
        EXPECT_EQ(transformed_variant.shredded_field_path(1).get_path(), "b");
        const auto& a_presence = transformed_variant.shredded_field_presence(0).get_data();
        EXPECT_EQ(std::vector<uint8_t>(a_presence.begin(), a_presence.end()),
                  test_case.expected_a_presence);
        const auto& b_presence = transformed_variant.shredded_field_presence(1).get_data();
        EXPECT_EQ(std::vector<uint8_t>(b_presence.begin(), b_presence.end()),
                  test_case.expected_b_presence);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(transformed_variant),
                  union_rebuilds_before);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(transformed_variant),
                  slow_rows_before + 1);
        auto expected_after_append = test_case.expected_before_append;
        expected_after_append.emplace_back(R"({"c":6})");
        EXPECT_EQ(materialized_json(transformed_variant), expected_after_append);

        ASSERT_TRUE(c->is_shredded());
        EXPECT_TRUE(c->shredded_field_values(0).is_typed());
        expect_json(*c, {R"({"c":5})", R"({"c":6})"});
    }
}

TEST(ColumnVariantV2ShreddedTest, RowConflictBitmapKeepsNonConflictRunsOnBulkPath) {
    const auto frozen_ab = [] {
        auto destination = typed_int_shredded({"{}"}, {1}, {1}, PathInData("a"));
        auto union_source = typed_int_shredded({"{}"}, {2}, {1}, PathInData("b"));
        destination->insert_range_from(*union_source, 0, 1);
        return destination;
    };
    auto destination = frozen_ab();

    ASSERT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*destination), 1);
    ASSERT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 0);
    const ColumnVariantV2* const range_a_child = &destination->shredded_field_values(0);
    const IColumn* const range_a_typed = &range_a_child->typed_column();
    const ColumnUInt8* const range_a_presence = &destination->shredded_field_presence(0);

    ColumnVariantV2::ShreddedFields mixed_fields;
    mixed_fields.emplace_back(
            PathInData("a"),
            typed_string_rows({"ignored-0", "ignored-1", "ignored-2", "ignored-3"}),
            presence_rows({0, 0, 0, 0}));
    mixed_fields.emplace_back(PathInData("b"), typed_int_rows({10, 20, 30, 40}),
                              presence_rows({1, 1, 1, 1}));
    mixed_fields.emplace_back(PathInData("d"), typed_int_rows({100, 200, 300, 400}),
                              presence_rows({0, 1, 0, 1}));
    auto mixed = ColumnVariantV2::create_shredded(
            encoded_rows({R"({"keep":0})", R"({"keep":1})", R"({"keep":2})", R"({"keep":3})"}),
            std::move(mixed_fields));

    destination->insert_range_from(*mixed, 0, mixed->size());

    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_EQ(&destination->shredded_field_values(0), range_a_child);
    ASSERT_TRUE(destination->shredded_field_values(0).is_typed());
    EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), range_a_typed);
    EXPECT_EQ(&destination->shredded_field_presence(0), range_a_presence);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*destination), 1);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 2);
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0, 0, 0, 0}));
    EXPECT_EQ(destination->shredded_field_presence(1).get_data(),
              (PaddedPODArray<UInt8> {0, 1, 1, 1, 1, 1}));
    expect_json(*destination,
                {R"({"a":1})", R"({"b":2})", R"({"b":10,"keep":0})", R"({"b":20,"d":200,"keep":1})",
                 R"({"b":30,"keep":2})", R"({"b":40,"d":400,"keep":3})"});

    auto indexed_destination = frozen_ab();
    const ColumnVariantV2* const indexed_a_child = &indexed_destination->shredded_field_values(0);
    const IColumn* const indexed_a_typed = &indexed_a_child->typed_column();
    const ColumnUInt8* const indexed_a_presence = &indexed_destination->shredded_field_presence(0);
    constexpr std::array<uint32_t, 5> INDICES {3, 0, 1, 2, 1};
    indexed_destination->insert_indices_from(*mixed, INDICES.begin(), INDICES.end());

    ASSERT_EQ(indexed_destination->shredded_field_count(), 2);
    EXPECT_EQ(&indexed_destination->shredded_field_values(0), indexed_a_child);
    ASSERT_TRUE(indexed_destination->shredded_field_values(0).is_typed());
    EXPECT_EQ(&indexed_destination->shredded_field_values(0).typed_column(), indexed_a_typed);
    EXPECT_EQ(&indexed_destination->shredded_field_presence(0), indexed_a_presence);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(*indexed_destination), 1);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*indexed_destination), 3);
    EXPECT_EQ(indexed_destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(indexed_destination->shredded_field_presence(1).get_data(),
              (PaddedPODArray<UInt8> {0, 1, 1, 1, 1, 1, 1}));
    expect_json(*indexed_destination, {R"({"a":1})", R"({"b":2})", R"({"b":40,"d":400,"keep":3})",
                                       R"({"b":10,"keep":0})", R"({"b":20,"d":200,"keep":1})",
                                       R"({"b":30,"keep":2})", R"({"b":20,"d":200,"keep":1})"});
}

TEST(ColumnVariantV2ShreddedTest, PrefixConflictFoldsOnlySourceFieldIntoResidual) {
    const PathInData child_path(std::vector<std::string> {"a", "b"});
    auto ancestor = typed_int_shredded({R"({"keep":1})"}, {1}, {1}, PathInData("a"));
    auto descendant = typed_int_shredded({R"({"keep":2})"}, {2}, {1}, child_path);

    ancestor->insert_range_from(*descendant, 0, 1);

    ASSERT_TRUE(ancestor->is_shredded());
    ASSERT_EQ(ancestor->shredded_field_count(), 1);
    EXPECT_EQ(ancestor->shredded_field_path(0).get_parts().size(), 1);
    EXPECT_EQ(ancestor->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 0}));
    expect_json(*ancestor, {R"({"a":1,"keep":1})", R"({"a":{"b":2},"keep":2})"});

    auto descendant_destination = typed_int_shredded({R"({"keep":3})"}, {3}, {1}, child_path);
    auto ancestor_source = typed_int_shredded({R"({"keep":4})"}, {4}, {1}, PathInData("a"));
    constexpr std::array<uint32_t, 1> INDICES {0};
    descendant_destination->insert_indices_from(*ancestor_source, INDICES.begin(), INDICES.end());

    ASSERT_TRUE(descendant_destination->is_shredded());
    ASSERT_EQ(descendant_destination->shredded_field_count(), 1);
    EXPECT_EQ(descendant_destination->shredded_field_path(0).get_parts().size(), 2);
    EXPECT_EQ(descendant_destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0}));
    expect_json(*descendant_destination, {R"({"a":{"b":3},"keep":3})", R"({"a":4,"keep":4})"});
}

TEST(ColumnVariantV2ShreddedTest, UnionUsesLogicalPathPartsForDottedKeys) {
    const PathInData segmented(std::vector<std::string> {"a", "b"});
    const PathInData literal(PathInData::Parts {{"a.b", false, 0}});
    auto destination = typed_int_shredded({"{}"}, {1}, {1}, segmented);
    auto source = typed_int_shredded({"{}"}, {2}, {1}, literal);

    destination->insert_range_from(*source, 0, 1);

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_EQ(destination->shredded_field_path(0).get_parts().size(), 2);
    EXPECT_EQ(destination->shredded_field_path(1).get_parts().size(), 1);
    expect_json(*destination, {R"({"a":{"b":1}})", R"({"a.b":2})"});
}

TEST(ColumnVariantV2ShreddedTest, ExpandingUnionRoutesMismatchedExactChildToResidual) {
    auto destination = typed_int_shredded({"{}"}, {7}, {1}, PathInData("a"));
    ColumnVariantV2::ShreddedFields source_fields;
    source_fields.emplace_back(PathInData("a"), typed_string_rows({"seven"}), presence_rows({1}));
    source_fields.emplace_back(PathInData("b"), typed_int_rows({8}), presence_rows({1}));
    auto source = ColumnVariantV2::create_shredded(encoded_rows({"{}"}), std::move(source_fields));
    const auto expected_source = materialized_json(*source);
    const ColumnVariantV2* const a_child = &destination->shredded_field_values(0);
    const IColumn* const a_typed = &a_child->typed_column();
    const ColumnUInt8* const a_presence = &destination->shredded_field_presence(0);

    destination->insert_range_from(*source, 0, 1);

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_EQ(&destination->shredded_field_values(0), a_child);
    EXPECT_EQ(&destination->shredded_field_values(0).typed_column(), a_typed);
    EXPECT_EQ(&destination->shredded_field_presence(0), a_presence);
    EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
    EXPECT_TRUE(destination->shredded_field_values(1).is_typed());
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 0}));
    EXPECT_EQ(destination->shredded_field_presence(1).get_data(), (PaddedPODArray<UInt8> {0, 1}));
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_existing_child_rows_copied(*destination),
              0);
    EXPECT_EQ(
            ColumnVariantV2::TestAccess::shredded_union_existing_presence_rows_copied(*destination),
            0);
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 1);
    expect_json(*destination, {R"({"a":7})", R"({"a":"seven","b":8})"});
    EXPECT_EQ(materialized_json(*source), expected_source);
}

TEST(ColumnVariantV2ShreddedTest, EmptyAndNonEmptyTypedDestinationsAdoptShreddedLayout) {
    auto source =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "{}"}, {1, 1, 0});
    constexpr std::array<uint32_t, 2> INDICES {2, 0};

    struct InsertCase {
        std::string_view name;
        bool has_existing_typed_row;
        bool use_indices;
        std::vector<std::string> expected_json;
        std::vector<uint8_t> expected_presence;
    };
    const std::array<InsertCase, 4> cases {{
            {"empty range",
             false,
             false,
             {R"({"a":"two","keep":2})", R"({"a":{"nested":3},"keep":3})"},
             {1, 0}},
            {"nonempty range",
             true,
             false,
             {"9", R"({"a":"two","keep":2})", R"({"a":{"nested":3},"keep":3})"},
             {0, 1, 0}},
            {"empty indices",
             false,
             true,
             {R"({"a":{"nested":3},"keep":3})", R"({"a":1,"keep":1})"},
             {0, 1}},
            {"nonempty indices",
             true,
             true,
             {"9", R"({"a":{"nested":3},"keep":3})", R"({"a":1,"keep":1})"},
             {0, 0, 1}},
    }};

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto destination =
                test_case.has_existing_typed_row ? typed_int_rows({9}) : typed_int_rows({});
        ASSERT_TRUE(destination->is_typed());
        EXPECT_EQ(destination->empty(), !test_case.has_existing_typed_row);

        if (test_case.use_indices) {
            destination->insert_indices_from(*source, INDICES.begin(), INDICES.end());
        } else {
            destination->insert_range_from(*source, 1, 2);
        }

        ASSERT_TRUE(destination->is_shredded());
        EXPECT_EQ(materialized_json(*destination), test_case.expected_json);
        const auto& presence = destination->shredded_field_presence(0).get_data();
        EXPECT_EQ(std::vector<uint8_t>(presence.begin(), presence.end()),
                  test_case.expected_presence);
    }
    EXPECT_TRUE(source->is_shredded());
}

TEST(ColumnVariantV2ShreddedTest, RowTransformsAndHashesDoNotMaterializeSource) {
    auto source =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "{}"}, {1, 1, 0});

    IColumn::Filter filter {1, 0, 1};
    ColumnPtr filtered = std::as_const(*source).filter(filter, 2);
    const auto& filtered_variant = assert_cast<const ColumnVariantV2&>(*filtered);
    ASSERT_TRUE(filtered_variant.is_shredded());
    expect_json(filtered_variant, {R"({"a":1,"keep":1})", R"({"a":{"nested":3},"keep":3})"});

    auto inplace =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "{}"}, {1, 1, 0});
    EXPECT_EQ(inplace->filter(filter), 2);
    ASSERT_TRUE(inplace->is_shredded());
    expect_json(*inplace, {R"({"a":1,"keep":1})", R"({"a":{"nested":3},"keep":3})"});

    IColumn::Permutation permutation {2, 0, 1};
    MutableColumnPtr permuted = source->permute(permutation, 2);
    const auto& permuted_variant = assert_cast<const ColumnVariantV2&>(*permuted);
    ASSERT_TRUE(permuted_variant.is_shredded());
    expect_json(permuted_variant, {R"({"a":{"nested":3},"keep":3})", R"({"a":1,"keep":1})"});

    ColumnPtr cloned = source->clone();
    MutableColumnPtr detached_clone = IColumn::mutate(std::move(cloned));
    auto& detached_variant = assert_cast<ColumnVariantV2&>(*detached_clone);
    ASSERT_TRUE(detached_variant.is_shredded());
    detached_variant.pop_back(1);
    expect_json(detached_variant, {R"({"a":1,"keep":1})", R"({"a":"two","keep":2})"});
    expect_json(*source, {R"({"a":1,"keep":1})", R"({"a":"two","keep":2})",
                          R"({"a":{"nested":3},"keep":3})"});
    MutableColumnPtr shrunk = source->clone_resized(2);
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*shrunk).is_shredded());
    MutableColumnPtr grown = source->clone_resized(4);
    const auto& grown_variant = assert_cast<const ColumnVariantV2&>(*grown);
    ASSERT_TRUE(grown_variant.is_shredded());
    expect_json(grown_variant, {R"({"a":1,"keep":1})", R"({"a":"two","keep":2})",
                                R"({"a":{"nested":3},"keep":3})", "{}"});

    auto resized =
            encoded_shredded({R"({"keep":1})", R"({"keep":2})", R"({"a":{"nested":3},"keep":3})"},
                             {"1", R"("two")", "{}"}, {1, 1, 0});
    resized->resize(2);
    ASSERT_TRUE(resized->is_shredded());
    resized->resize(4);
    ASSERT_TRUE(resized->is_shredded());
    expect_json(*resized, {R"({"a":1,"keep":1})", R"({"a":"two","keep":2})", "{}", "{}"});

    auto cleared = encoded_shredded({R"({"keep":1})"}, {"1"}, {1});
    cleared->clear();
    ASSERT_TRUE(cleared->is_shredded());
    cleared->insert_default();
    ASSERT_TRUE(cleared->is_shredded());
    expect_json(*cleared, {"{}"});

    auto encoded = source->materialize_encoded_range(0, source->size());
    std::array<uint64_t, 3> source_xx {11, 22, 33};
    std::array<uint64_t, 3> encoded_xx = source_xx;
    source->update_hashes_with_value(source_xx.data(), nullptr);
    encoded->update_hashes_with_value(encoded_xx.data(), nullptr);
    EXPECT_EQ(source_xx, encoded_xx);
    std::array<uint32_t, 3> source_crc {44, 55, 66};
    std::array<uint32_t, 3> encoded_crc = source_crc;
    source->update_crc32c_batch(source_crc.data(), nullptr);
    encoded->update_crc32c_batch(encoded_crc.data(), nullptr);
    EXPECT_EQ(source_crc, encoded_crc);
    EXPECT_TRUE(source->is_shredded());
}

TEST(ColumnVariantV2ShreddedTest, BatchHashFamiliesMatchEncodedOracleWithThreeNullStates) {
    constexpr size_t MISSING_ROW = 1;
    constexpr size_t VARIANT_NULL_ROW = 2;
    constexpr size_t SQL_NULL_ROW = 3;
    auto shredded = encoded_shredded({R"({"pad":0})", "{}", "{}", "{}", "{}"},
                                     {"1", "null", "null", "null", "7"}, {1, 0, 1, 1, 1});
    auto encoded = encoded_rows(
            {R"({"a":1,"pad":0})", "{}", R"({"a":null})", R"({"a":null})", R"({"a":7})"});
    auto outer_null_column = presence_rows({0, 0, 0, 1, 0});
    const auto& outer_nulls = outer_null_column->get_data();
    ColumnPtr outer_null_column_ptr = outer_null_column->get_ptr();
    auto shredded_nullable = ColumnNullable::create(shredded->get_ptr(), outer_null_column_ptr);
    auto encoded_nullable = ColumnNullable::create(encoded->get_ptr(), outer_null_column_ptr);

    ASSERT_TRUE(shredded->is_shredded());
    EXPECT_EQ(shredded->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 1, 1, 1}));
    expect_json(*shredded,
                {R"({"a":1,"pad":0})", "{}", R"({"a":null})", R"({"a":null})", R"({"a":7})"});

    std::array<uint64_t, 5> shredded_sip {};
    std::array<uint64_t, 5> encoded_sip {};
    for (size_t row = 0; row < shredded->size(); ++row) {
        SipHash shredded_hash;
        SipHash encoded_hash;
        shredded_nullable->update_hash_with_value(row, shredded_hash);
        encoded_nullable->update_hash_with_value(row, encoded_hash);
        shredded_sip[row] = shredded_hash.get64();
        encoded_sip[row] = encoded_hash.get64();
    }
    EXPECT_EQ(shredded_sip, encoded_sip);
    EXPECT_NE(shredded_sip[MISSING_ROW], shredded_sip[VARIANT_NULL_ROW]);
    EXPECT_NE(shredded_sip[VARIANT_NULL_ROW], shredded_sip[SQL_NULL_ROW]);

    constexpr std::array<uint64_t, 5> XX_SEEDS {11, 22, 33, 44, 55};
    auto shredded_xx = XX_SEEDS;
    auto encoded_xx = XX_SEEDS;
    shredded->update_hashes_with_value(shredded_xx.data(), outer_nulls.data());
    encoded->update_hashes_with_value(encoded_xx.data(), outer_nulls.data());
    EXPECT_EQ(shredded_xx, encoded_xx);
    EXPECT_EQ(shredded_xx[SQL_NULL_ROW], XX_SEEDS[SQL_NULL_ROW]);

    constexpr std::array<uint32_t, 5> CRC_SEEDS {66, 77, 88, 99, 111};
    auto shredded_crc = CRC_SEEDS;
    auto encoded_crc = CRC_SEEDS;
    shredded->update_crcs_with_value(shredded_crc.data(), PrimitiveType::TYPE_VARIANT,
                                     shredded->size(), 0, outer_nulls.data());
    encoded->update_crcs_with_value(encoded_crc.data(), PrimitiveType::TYPE_VARIANT,
                                    encoded->size(), 0, outer_nulls.data());
    EXPECT_EQ(shredded_crc, encoded_crc);
    EXPECT_EQ(shredded_crc[SQL_NULL_ROW], CRC_SEEDS[SQL_NULL_ROW]);

    auto shredded_crc32c = CRC_SEEDS;
    auto encoded_crc32c = CRC_SEEDS;
    shredded->update_crc32c_batch(shredded_crc32c.data(), outer_nulls.data());
    encoded->update_crc32c_batch(encoded_crc32c.data(), outer_nulls.data());
    EXPECT_EQ(shredded_crc32c, encoded_crc32c);
    EXPECT_EQ(shredded_crc32c[SQL_NULL_ROW], CRC_SEEDS[SQL_NULL_ROW]);

    EXPECT_TRUE(shredded->is_shredded());
}

TEST(ColumnVariantV2ShreddedTest, RangeHashFamiliesMatchEncodedOracleWithThreeNullStates) {
    auto shredded = encoded_shredded({R"({"pad":0})", "{}", "{}", "{}", "{}"},
                                     {"1", "null", "null", "null", "7"}, {1, 0, 1, 1, 1});
    auto encoded = encoded_rows(
            {R"({"a":1,"pad":0})", "{}", R"({"a":null})", R"({"a":null})", R"({"a":7})"});
    auto outer_null_column = presence_rows({0, 0, 0, 1, 0});
    const auto& outer_nulls = outer_null_column->get_data();

    // This range contains missing, Variant null, and outer SQL NULL. The S path materializes
    // [start, end) at row zero, so it must shift the full-column null map by RANGE_START.
    constexpr size_t RANGE_START = 1;
    constexpr size_t RANGE_END = 4;
    uint64_t shredded_xx_range = 123;
    uint64_t encoded_xx_range = 123;
    shredded->update_xxHash_with_value(RANGE_START, RANGE_END, shredded_xx_range,
                                       outer_nulls.data());
    encoded->update_xxHash_with_value(RANGE_START, RANGE_END, encoded_xx_range, outer_nulls.data());
    EXPECT_EQ(shredded_xx_range, encoded_xx_range);

    uint32_t shredded_crc_range = 234;
    uint32_t encoded_crc_range = 234;
    shredded->update_crc_with_value(RANGE_START, RANGE_END, shredded_crc_range, outer_nulls.data());
    encoded->update_crc_with_value(RANGE_START, RANGE_END, encoded_crc_range, outer_nulls.data());
    EXPECT_EQ(shredded_crc_range, encoded_crc_range);

    uint32_t shredded_crc32c_range = 345;
    uint32_t encoded_crc32c_range = 345;
    shredded->update_crc32c_single(RANGE_START, RANGE_END, shredded_crc32c_range,
                                   outer_nulls.data());
    encoded->update_crc32c_single(RANGE_START, RANGE_END, encoded_crc32c_range, outer_nulls.data());
    EXPECT_EQ(shredded_crc32c_range, encoded_crc32c_range);
    EXPECT_TRUE(shredded->is_shredded());
}

TEST(ColumnVariantV2ShreddedTest, EncodedChildTransformsOwnMetadataAcrossMemTrackers) {
    enum class Transform { CLONE, PERMUTE, INDICES };
    for (Transform transform : {Transform::CLONE, Transform::PERMUTE, Transform::INDICES}) {
        SCOPED_TRACE(testing::Message() << "transform=" << static_cast<int>(transform));
        const auto source_tracker = std::make_shared<MemTracker>("shredded-source");
        const auto destination_tracker = std::make_shared<MemTracker>("shredded-destination");
        ColumnVariantV2::MutablePtr source;
        MutableColumnPtr transformed;

        {
            SCOPED_CONSUME_MEM_TRACKER(source_tracker);
            source = encoded_shredded({"{}", "{}"}, {"1", R"("one")"}, {1, 1});
            ASSERT_TRUE(source->shredded_field_values(0).is_encoded());
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            EXPECT_GT(source_tracker->consumption(), 0);
        }
        {
            SCOPED_CONSUME_MEM_TRACKER(destination_tracker);
            if (transform == Transform::PERMUTE) {
                const IColumn::Permutation permutation {1, 0};
                transformed = source->permute(permutation, 2);
            } else if (transform == Transform::CLONE) {
                transformed = source->clone_resized(1);
            } else {
                constexpr std::array<uint32_t, 2> INDICES {1, 0};
                auto selected = ColumnVariantV2::create();
                selected->insert_indices_from(*source, INDICES.begin(), INDICES.end());
                transformed = std::move(selected);
            }
            ASSERT_TRUE(assert_cast<const ColumnVariantV2&>(*transformed).is_shredded());
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            EXPECT_GT(destination_tracker->consumption(), 0);
        }
        {
            SCOPED_CONSUME_MEM_TRACKER(source_tracker);
            source.reset();
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        }
        {
            SCOPED_CONSUME_MEM_TRACKER(destination_tracker);
            transformed.reset();
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        }
        EXPECT_EQ(source_tracker->consumption(), 0);
        EXPECT_EQ(destination_tracker->consumption(), 0);
    }
}

} // namespace
} // namespace doris
