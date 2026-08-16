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

ColumnVariantV2::MutablePtr typed_nullable_int_rows(std::initializer_list<int64_t> rows,
                                                    std::initializer_list<uint8_t> nulls) {
    auto values = ColumnInt64::create();
    for (int64_t value : rows) {
        values->insert_value(value);
    }
    auto null_map = ColumnUInt8::create();
    for (uint8_t value : nulls) {
        null_map->insert_value(value);
    }
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(null_map)),
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

ColumnVariantV2::MutablePtr selection_sensitive_shredded_source() {
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData("a"), typed_int_rows({10, 20, 30, 40}),
                        presence_rows({1, 0, 0, 1}));
    fields.emplace_back(PathInData("b"),
                        typed_nullable_int_rows({100, 200, 300, 400}, {0, 1, 1, 0}),
                        presence_rows({0, 1, 0, 0}));
    return ColumnVariantV2::create_shredded(
            encoded_rows({R"({"keep":0})", R"({"keep":1})", R"({"keep":2})", R"({"keep":3})"}),
            std::move(fields));
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

TEST(ColumnVariantV2ShreddedTest, FixedLayoutMergeKeepsDestinationAndPromotesMappedChild) {
    ColumnVariantV2::ShreddedFields destination_fields;
    destination_fields.emplace_back(PathInData("a"), typed_int_rows({1}), presence_rows({1}));
    destination_fields.emplace_back(PathInData("b"), typed_int_rows({2}), presence_rows({1}));
    auto destination =
            ColumnVariantV2::create_shredded(encoded_rows({"{}"}), std::move(destination_fields));

    ColumnVariantV2::ShreddedFields source_fields;
    source_fields.emplace_back(PathInData("a"), typed_string_rows({"one"}), presence_rows({1}));
    source_fields.emplace_back(PathInData("c"), typed_int_rows({3}), presence_rows({1}));
    auto source = ColumnVariantV2::create_shredded(encoded_rows({"{}"}), std::move(source_fields));

    destination->insert_range_from(*source, 0, source->size());

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_EQ(destination->shredded_field_path(0), PathInData("a"));
    EXPECT_TRUE(destination->shredded_field_values(0).is_encoded());
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 1}));
    EXPECT_EQ(destination->shredded_field_path(1), PathInData("b"));
    EXPECT_TRUE(destination->shredded_field_values(1).is_typed());
    EXPECT_EQ(destination->shredded_field_presence(1).get_data(), (PaddedPODArray<UInt8> {1, 0}));
    EXPECT_EQ(json(destination->read_view().residual_value_at(1)), R"({"c":3})");
    expect_json(*destination, {R"({"a":1,"b":2})", R"({"a":"one","c":3})"});
}

TEST(ColumnVariantV2ShreddedTest, FixedLayoutProjectsNestedResidualForRangeAndIndices) {
    const auto make_destination = [] {
        return typed_int_shredded({"{}"}, {7}, {1},
                                  PathInData(std::vector<std::string> {"a", "b"}));
    };
    auto source = encoded_rows({R"({"a":{"b":1,"c":2}})", R"({"a":3})", R"({"a":{"b":"one"}})"});

    auto ranged = make_destination();
    ranged->insert_range_from(*source, 0, source->size());

    ASSERT_TRUE(ranged->is_shredded());
    ASSERT_EQ(ranged->shredded_field_count(), 1);
    EXPECT_TRUE(ranged->shredded_field_values(0).is_encoded());
    EXPECT_EQ(ranged->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 1, 0, 1}));
    EXPECT_EQ(json(ranged->read_view().residual_value_at(1)), R"({"a":{"c":2}})");
    EXPECT_EQ(json(ranged->read_view().residual_value_at(2)), R"({"a":3})");
    EXPECT_EQ(json(ranged->read_view().residual_value_at(3)), R"({"a":{}})");
    expect_json(*ranged, {R"({"a":{"b":7}})", R"({"a":{"b":1,"c":2}})", R"({"a":3})",
                          R"({"a":{"b":"one"}})"});

    constexpr std::array<uint32_t, 2> INDICES {2, 0};
    auto indexed = make_destination();
    indexed->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(indexed->shredded_field_values(0).is_encoded());
    EXPECT_EQ(indexed->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 1, 1}));
    EXPECT_EQ(json(indexed->read_view().residual_value_at(1)), R"({"a":{}})");
    EXPECT_EQ(json(indexed->read_view().residual_value_at(2)), R"({"a":{"c":2}})");
    expect_json(*indexed, {R"({"a":{"b":7}})", R"({"a":{"b":"one"}})", R"({"a":{"b":1,"c":2}})"});
}

TEST(ColumnVariantV2ShreddedTest, FixedLayoutProjectsSiblingPathsThroughMetadataPlan) {
    ColumnVariantV2::ShreddedFields destination_fields;
    destination_fields.emplace_back(PathInData(std::vector<std::string> {"a", "b"}),
                                    typed_int_rows({7}), presence_rows({1}));
    destination_fields.emplace_back(PathInData(std::vector<std::string> {"a", "c"}),
                                    typed_int_rows({8}), presence_rows({1}));
    auto destination =
            ColumnVariantV2::create_shredded(encoded_rows({"{}"}), std::move(destination_fields));
    auto source = encoded_rows(
            {R"({"a":{"b":1,"c":"x","keep":9}})", R"({"a":{"b":2,"c":"y","keep":10}})",
             R"({"a":{"b":{"nested":2},"c":3}})", R"({"a":{"b":4},"other":5})", R"({"a":6})"});

    destination->insert_range_from(*source, 0, source->size());

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 1, 1, 0, 1, 0}));
    EXPECT_EQ(destination->shredded_field_presence(1).get_data(),
              (PaddedPODArray<UInt8> {1, 1, 1, 1, 0, 0}));
    EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
    EXPECT_TRUE(destination->shredded_field_values(1).is_encoded());
    EXPECT_EQ(json(destination->read_view().residual_value_at(1)), R"({"a":{"keep":9}})");
    EXPECT_EQ(json(destination->read_view().residual_value_at(2)), R"({"a":{"keep":10}})");
    EXPECT_EQ(json(destination->read_view().residual_value_at(3)), R"({"a":{"b":{"nested":2}}})");
    EXPECT_EQ(json(destination->read_view().residual_value_at(4)), R"({"a":{},"other":5})");
    EXPECT_EQ(json(destination->read_view().residual_value_at(5)), R"({"a":6})");
    expect_json(*destination,
                {R"({"a":{"b":7,"c":8}})", R"({"a":{"b":1,"c":"x","keep":9}})",
                 R"({"a":{"b":2,"c":"y","keep":10}})", R"({"a":{"b":{"nested":2},"c":3}})",
                 R"({"a":{"b":4},"other":5})", R"({"a":6})"});
}

TEST(ColumnVariantV2ShreddedTest, FixedLayoutProjectsAndOverlaysOneDirtyRow) {
    auto destination =
            typed_int_shredded({"{}"}, {7}, {1}, PathInData(std::vector<std::string> {"a", "b"}));
    auto source = typed_int_shredded({R"({"a":{"b":1,"d":2}})"}, {9}, {1},
                                     PathInData(std::vector<std::string> {"a", "c"}));

    destination->insert_range_from(*source, 0, 1);

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 1);
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 1}));
    EXPECT_EQ(json(destination->read_view().residual_value_at(1)), R"({"a":{"c":9,"d":2}})");
    expect_json(*destination, {R"({"a":{"b":7}})", R"({"a":{"b":1,"c":9,"d":2}})"});
}

TEST(ColumnVariantV2ShreddedTest, FixedLayoutKeepsAncestorDescendantConflictsInResidual) {
    auto ancestor_destination = typed_int_shredded({R"({"keep":0})"}, {1}, {1});
    auto descendant_source = typed_int_shredded({R"({"keep":1})"}, {2}, {1},
                                                PathInData(std::vector<std::string> {"a", "b"}));

    ancestor_destination->insert_range_from(*descendant_source, 0, 1);

    ASSERT_TRUE(ancestor_destination->is_shredded());
    ASSERT_EQ(ancestor_destination->shredded_field_count(), 1);
    EXPECT_EQ(ancestor_destination->shredded_field_path(0), PathInData("a"));
    EXPECT_EQ(ancestor_destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0}));
    EXPECT_EQ(json(ancestor_destination->read_view().residual_value_at(1)),
              R"({"a":{"b":2},"keep":1})");
    expect_json(*ancestor_destination, {R"({"a":1,"keep":0})", R"({"a":{"b":2},"keep":1})"});

    auto descendant_destination = typed_int_shredded(
            {R"({"keep":0})"}, {1}, {1}, PathInData(std::vector<std::string> {"a", "b"}));
    auto ancestor_source = typed_int_shredded({R"({"keep":1})"}, {2}, {1});

    descendant_destination->insert_range_from(*ancestor_source, 0, 1);

    ASSERT_TRUE(descendant_destination->is_shredded());
    ASSERT_EQ(descendant_destination->shredded_field_count(), 1);
    EXPECT_EQ(descendant_destination->shredded_field_path(0),
              PathInData(std::vector<std::string> {"a", "b"}));
    EXPECT_EQ(descendant_destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0}));
    EXPECT_EQ(json(descendant_destination->read_view().residual_value_at(1)),
              R"({"a":2,"keep":1})");
    expect_json(*descendant_destination, {R"({"a":{"b":1},"keep":0})", R"({"a":2,"keep":1})"});
}

TEST(ColumnVariantV2ShreddedTest, ReplaceNullPayloadsMasksOnlyResidualAndPresence) {
    auto shredded = typed_int_shredded({R"({"keep":0})", R"({"keep":1})", R"({"keep":2})"},
                                       {1, 2, 3}, {1, 1, 1});
    const ColumnVariantV2* const child_identity = &shredded->shredded_field_values(0);
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};

    shredded->replace_column_null_data(NULLS.data());

    ASSERT_TRUE(shredded->is_shredded());
    EXPECT_EQ(&shredded->shredded_field_values(0), child_identity);
    EXPECT_EQ(shredded->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 0, 1}));
    EXPECT_EQ(json(shredded->read_view().residual_value_at(0)), R"({"keep":0})");
    EXPECT_EQ(json(shredded->read_view().residual_value_at(1)), "{}");
    EXPECT_EQ(json(shredded->read_view().residual_value_at(2)), R"({"keep":2})");
    expect_json(*shredded, {R"({"a":1,"keep":0})", "{}", R"({"a":3,"keep":2})"});
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
    expect_state({1, 1});

    const Field object = Field::create_field<TYPE_VARIANT>(encode_json(R"({"a":{"b":1}})"));
    shredded->insert(object);
    encoded_oracle->insert(object);
    expect_state({1, 1, 0});

    const Field variant_null = Field::create_field<TYPE_VARIANT>(encode_json("null"));
    shredded->insert(variant_null);
    encoded_oracle->insert(variant_null);
    expect_state({1, 1, 0, 0});

    shredded->insert_default();
    encoded_oracle->insert_default();
    expect_state({1, 1, 0, 0, 0});

    shredded->insert_many_defaults(2);
    encoded_oracle->insert_many_defaults(2);
    expect_state({1, 1, 0, 0, 0, 0, 0});
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

    EXPECT_DEATH(
            {
                ColumnVariantV2::ShreddedFields fields;
                fields.emplace_back(PathInData("a"), encoded_rows({"8"}), presence_rows({0}));
                static_cast<void>(ColumnVariantV2::create_shredded(encoded_rows({R"({"a":7})"}),
                                                                   std::move(fields)));
            },
            "residual owns an exact scalar for an absent shredded field");

    auto inactive_structure = encoded_shredded({R"({"a":{"c":3}})"}, {"8"}, {0});
    expect_json(*inactive_structure, {R"({"a":{"c":3}})"});
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
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(*destination), 0);
        EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(
                          destination->shredded_field_values(0)),
                  0);
        expect_json(*destination, {R"({"a":9})", R"({"a":2})", R"({"a":2})", R"({"a":1})"});
        ASSERT_TRUE(repeated_source->is_shredded());
        EXPECT_TRUE(repeated_source->shredded_field_values(0).is_typed());
        expect_json(*repeated_source, {R"({"a":1})", R"({"a":2})"});
    }
}

TEST(ColumnVariantV2ShreddedTest, EmptyEncodedAdoptsTheFullSourceLayoutOnce) {
    auto source = selection_sensitive_shredded_source();

    auto inactive = ColumnVariantV2::create();
    inactive->insert_range_from(*source, 2, 1);
    ASSERT_TRUE(inactive->is_shredded());
    ASSERT_EQ(inactive->shredded_field_count(), 2);
    EXPECT_EQ(inactive->shredded_field_path(0), PathInData("a"));
    EXPECT_EQ(inactive->shredded_field_path(1), PathInData("b"));
    EXPECT_EQ(inactive->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {0}));
    EXPECT_EQ(inactive->shredded_field_presence(1).get_data(), (PaddedPODArray<UInt8> {0}));
    expect_json(*inactive, {R"({"keep":2})"});

    constexpr std::array<uint32_t, 2> INDICES {1, 2};
    auto selected = ColumnVariantV2::create();
    selected->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(selected->is_shredded());
    ASSERT_EQ(selected->shredded_field_count(), 2);
    EXPECT_EQ(selected->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {0, 0}));
    EXPECT_EQ(selected->shredded_field_presence(1).get_data(), (PaddedPODArray<UInt8> {1, 0}));
    expect_json(*selected, {R"({"b":null,"keep":1})", R"({"keep":2})"});

    auto nonempty = encoded_rows({R"({"seed":9})"});
    nonempty->insert_range_from(*source, 1, 2);
    EXPECT_TRUE(nonempty->is_encoded());
    expect_json(*nonempty, {R"({"seed":9})", R"({"b":null,"keep":1})", R"({"keep":2})"});

    const size_t source_materializations =
            ColumnVariantV2::TestAccess::encoded_range_materializations(*source);
    constexpr std::array<uint32_t, 3> NONEMPTY_INDICES {2, 0, 2};
    auto nonempty_indexed = encoded_rows({R"({"seed":10})"});
    nonempty_indexed->insert_indices_from(*source, NONEMPTY_INDICES.begin(),
                                          NONEMPTY_INDICES.end());
    EXPECT_TRUE(nonempty_indexed->is_encoded());
    EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(*source),
              source_materializations);
    expect_json(*nonempty_indexed,
                {R"({"seed":10})", R"({"keep":2})", R"({"a":10,"keep":0})", R"({"keep":2})"});
}

TEST(ColumnVariantV2ShreddedTest, SamePathConflictPromotesOnlyThatChild) {
    auto destination =
            shredded_with_typed_sibling({R"({"keep":0})"}, typed_int_rows({7}), {1}, {100}, {1});
    auto source = shredded_with_typed_sibling({R"({"keep":1})"}, typed_string_rows({"seven"}), {1},
                                              {200}, {1});
    const ColumnVariantV2* const conflict_child = &destination->shredded_field_values(0);
    const ColumnUInt8* const conflict_presence = &destination->shredded_field_presence(0);
    const ColumnVariantV2* const stable_child = &destination->shredded_field_values(1);
    const IColumn* const stable_typed_column = &stable_child->typed_column();
    const ColumnUInt8* const stable_presence = &destination->shredded_field_presence(1);

    destination->insert_range_from(*source, 0, 1);

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 2);
    EXPECT_TRUE(destination->shredded_field_values(0).is_encoded());
    EXPECT_TRUE(destination->shredded_field_values(1).is_typed());
    EXPECT_EQ(&destination->shredded_field_values(0), conflict_child);
    EXPECT_EQ(&destination->shredded_field_presence(0), conflict_presence);
    EXPECT_EQ(&destination->shredded_field_values(1), stable_child);
    EXPECT_EQ(&destination->shredded_field_values(1).typed_column(), stable_typed_column);
    EXPECT_EQ(&destination->shredded_field_presence(1), stable_presence);
    EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(*destination), 0);
    EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(
                      destination->shredded_field_values(0)),
              1);
    EXPECT_EQ(ColumnVariantV2::TestAccess::encoded_range_materializations(
                      destination->shredded_field_values(1)),
              0);
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(), (PaddedPODArray<UInt8> {1, 1}));
    EXPECT_EQ(destination->shredded_field_presence(1).get_data(), (PaddedPODArray<UInt8> {1, 1}));
    EXPECT_EQ(json(destination->read_view().residual_value_at(1)), R"({"keep":1})");
    expect_json(*destination,
                {R"({"a":7,"keep":0,"stable":100})", R"({"a":"seven","keep":1,"stable":200})"});

    constexpr std::array<uint32_t, 2> INDICES {0, 0};
    auto indexed = shredded_with_typed_sibling({"{}"}, typed_int_rows({8}), {1}, {101}, {1});
    indexed->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(indexed->shredded_field_values(0).is_encoded());
    EXPECT_TRUE(indexed->shredded_field_values(1).is_typed());
    expect_json(*indexed, {R"({"a":8,"stable":101})", R"({"a":"seven","keep":1,"stable":200})",
                           R"({"a":"seven","keep":1,"stable":200})"});
}

TEST(ColumnVariantV2ShreddedTest, SelectedAllMissingMappedChildDoesNotPromote) {
    auto destination = typed_int_shredded({"{}"}, {7}, {1});
    ColumnVariantV2::ShreddedFields source_fields;
    source_fields.emplace_back(PathInData("a"), encoded_rows({R"("unused")", R"("unused")"}),
                               presence_rows({0, 0}));
    auto source = ColumnVariantV2::create_shredded(
            encoded_rows({R"({"keep":1})", R"({"a":{"nested":2},"keep":2})"}),
            std::move(source_fields));

    destination->insert_range_from(*source, 0, 2);

    ASSERT_TRUE(destination->is_shredded());
    EXPECT_TRUE(destination->shredded_field_values(0).is_typed());
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0}));
    expect_json(*destination, {R"({"a":7})", R"({"keep":1})", R"({"a":{"nested":2},"keep":2})"});
}

TEST(ColumnVariantV2ShreddedTest, SourceOnlyOverlayRebuildsOnlyPresentRows) {
    auto destination = typed_int_shredded({"{}"}, {7}, {1}, PathInData("b"));
    auto source = typed_int_shredded({R"({"keep":0})", R"({"keep":1})", R"({"keep":2})"},
                                     {10, 20, 30}, {0, 1, 0}, PathInData("a"));

    destination->insert_range_from(*source, 0, 3);

    ASSERT_TRUE(destination->is_shredded());
    ASSERT_EQ(destination->shredded_field_count(), 1);
    EXPECT_EQ(destination->shredded_field_path(0), PathInData("b"));
    EXPECT_EQ(destination->shredded_field_presence(0).get_data(),
              (PaddedPODArray<UInt8> {1, 0, 0, 0}));
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*destination), 1);
    expect_json(*destination,
                {R"({"b":7})", R"({"keep":0})", R"({"a":20,"keep":1})", R"({"keep":2})"});

    constexpr std::array<uint32_t, 3> INDICES {2, 1, 2};
    auto indexed = typed_int_shredded({"{}"}, {8}, {1}, PathInData("b"));
    indexed->insert_indices_from(*source, INDICES.begin(), INDICES.end());
    EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_conflict_slow_rows(*indexed), 1);
    expect_json(*indexed, {R"({"b":8})", R"({"keep":2})", R"({"a":20,"keep":1})", R"({"keep":2})"});
}

TEST(ColumnVariantV2ShreddedTest, SelfRangeAndIndicesKeepFixedLayout) {
    auto ranged = typed_int_shredded({"{}", "{}"}, {1, 2}, {1, 1});
    ranged->insert_range_from(*ranged, 1, 1);
    ASSERT_TRUE(ranged->is_shredded());
    ASSERT_EQ(ranged->shredded_field_count(), 1);
    EXPECT_TRUE(ranged->shredded_field_values(0).is_typed());
    expect_json(*ranged, {R"({"a":1})", R"({"a":2})", R"({"a":2})"});

    constexpr std::array<uint32_t, 2> INDICES {1, 0};
    auto indexed = typed_int_shredded({"{}", "{}"}, {1, 2}, {1, 1});
    indexed->insert_indices_from(*indexed, INDICES.begin(), INDICES.end());
    ASSERT_TRUE(indexed->is_shredded());
    ASSERT_EQ(indexed->shredded_field_count(), 1);
    EXPECT_TRUE(indexed->shredded_field_values(0).is_typed());
    expect_json(*indexed, {R"({"a":1})", R"({"a":2})", R"({"a":2})", R"({"a":1})"});
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
