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

#include "exprs/function/function_variant_element.cpp"

#include <gtest/gtest.h>

#include <array>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {
namespace {

ColumnVariantV2::MutablePtr nested_shredded_variant_v2() {
    constexpr std::array<std::string_view, 6> RESIDUAL {"{}", R"({"a":9})", R"({"a":{"other":2}})",
                                                        "{}", "{}",         R"({"a":null})"};
    JsonStringToVariantEncoder encoder;
    for (std::string_view json : RESIDUAL) {
        encoder.add_json({json.data(), json.size()});
    }
    auto residual = ColumnVariantV2::create();
    residual->insert_encoded_batch(encoder.finish_batch());

    auto values = ColumnInt64::create();
    auto child_nulls = ColumnUInt8::create();
    for (const auto [value, is_null] : std::array<std::pair<int64_t, uint8_t>, 6> {
                 {{7, 0}, {0, 1}, {8, 0}, {0, 1}, {0, 1}, {0, 1}}}) {
        values->insert_value(value);
        child_nulls->insert_value(is_null);
    }
    auto child = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(child_nulls)),
            std::make_shared<DataTypeInt64>());
    auto presence = ColumnUInt8::create();
    for (const auto present : std::array<uint8_t, 6> {1, 0, 1, 0, 0, 0}) {
        presence->insert_value(present);
    }
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a", "b"}), std::move(child),
                        std::move(presence));
    return ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
}

ColumnPtr execute_variant_v2_element(ColumnPtr source, const DataTypePtr& source_type,
                                     std::string_view key) {
    auto index_values = ColumnString::create();
    index_values->insert_data(key.data(), key.size());
    ColumnPtr index = ColumnConst::create(std::move(index_values), source->size());
    const auto index_type = std::make_shared<DataTypeString>();
    const DataTypePtr result_type = make_nullable(remove_nullable(source_type));
    Block block {{std::move(source), source_type, "source"},
                 {std::move(index), index_type, "index"},
                 {result_type->create_column(), result_type, "result"}};
    const Status status =
            FunctionVariantElement::create()->execute(nullptr, block, {0, 1}, 2, block.rows());
    EXPECT_TRUE(status.ok()) << status;
    return block.get_by_position(2).column;
}

} // namespace

TEST(function_variant_element_test, extract_from_sparse_column) {
    auto variant_column = ColumnVariant::create(1 /*max_subcolumns_count*/, false);
    auto* variant_ptr = variant_column.get();

    ColumnVariant::Subcolumn subcolumn(0, true, false);
    Field field = Field::create_field<TYPE_STRING>("John");
    subcolumn.insert(field);

    auto [sparse_column_keys, sparse_column_values] =
            variant_ptr->get_sparse_data_paths_and_values();
    auto& sparse_column_offsets = variant_ptr->serialized_sparse_column_offsets();
    subcolumn.serialize_to_binary_column(sparse_column_keys, "profile.age", sparse_column_values,
                                         0);
    subcolumn.serialize_to_binary_column(sparse_column_keys, "profile.name", sparse_column_values,
                                         0);
    subcolumn.serialize_to_binary_column(sparse_column_keys, "profile_id", sparse_column_values, 0);
    sparse_column_offsets.push_back(sparse_column_keys->size());
    variant_ptr->get_subcolumn({})->insert_default();
    variant_ptr->set_num_rows(1);
    variant_ptr->get_doc_value_column_mutable().resize(1);

    ColumnPtr result;
    ColumnPtr index_column_ptr = ColumnString::create();
    auto* index_column_ptr_mutable =
            assert_cast<ColumnString*>(index_column_ptr->assert_mutable().get());
    index_column_ptr_mutable->insert_data("profile", 7);
    ColumnPtr index_column = ColumnConst::create(index_column_ptr, 1);
    auto status =
            FunctionVariantElement::get_element_column(*variant_column, index_column, &result);
    EXPECT_TRUE(status.ok());

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    auto result_ptr = assert_cast<const ColumnVariant&>(*result.get());
    std::string result_string;
    result_ptr.serialize_one_row_to_string(0, &result_string, options);
    EXPECT_EQ(result_string, "{\"age\":\"John\",\"name\":\"John\"}");
}

// CIR-20498: extracting a string property from a scalar-string-root variant
// (the shape produced by `cast(text as variant)`) must return the raw string,
// not its JSON token with surrounding double quotes.
TEST(function_variant_element_test, extract_string_from_scalar_root) {
    auto variant_column = ColumnVariant::create(0 /*max_subcolumns_count*/, false);
    auto root_column = ColumnString::create();
    std::string doc = R"({"wsn":"SRFSPXFDVY","uploadTimeValue":"2026-05-20 18:40:02","n":49.98})";
    root_column->insert_data(doc.data(), doc.size());
    variant_column->create_root(std::make_shared<DataTypeString>(), std::move(root_column));
    variant_column->set_num_rows(1);
    ASSERT_TRUE(variant_column->is_scalar_variant());

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    auto extract = [&](const std::string& key) {
        ColumnPtr index_inner = ColumnString::create();
        assert_cast<ColumnString*>(index_inner->assert_mutable().get())
                ->insert_data(key.data(), key.size());
        ColumnPtr index_column = ColumnConst::create(index_inner, 1);
        ColumnPtr result;
        auto status =
                FunctionVariantElement::get_element_column(*variant_column, index_column, &result);
        EXPECT_TRUE(status.ok());
        std::string out;
        assert_cast<const ColumnVariant&>(*result.get())
                .serialize_one_row_to_string(0, &out, options);
        return out;
    };

    // string values: no surrounding quotes
    EXPECT_EQ(extract("wsn"), "SRFSPXFDVY");
    EXPECT_EQ(extract("uploadTimeValue"), "2026-05-20 18:40:02");
    // non-string scalars keep their JSON representation
    EXPECT_EQ(extract("n"), "49.98");
}

TEST(function_variant_element_test, exact_storage_json_null_remains_variant_null) {
    Slice null_json("null", 4);
    JsonBinaryValue null_value;
    ASSERT_TRUE(null_value.from_json_string(null_json.data, null_json.size).ok());
    ColumnVariant::Subcolumn null_subcolumn(0, true, false);
    null_subcolumn.insert(
            Field::create_field<TYPE_JSONB>(JsonbField(null_value.value(), null_value.size())));

    for (const bool use_doc_value : {false, true}) {
        SCOPED_TRACE(use_doc_value ? "doc" : "sparse");
        auto source = ColumnVariant::create(1, use_doc_value);
        auto [paths, values] = use_doc_value ? source->get_doc_value_data_paths_and_values()
                                             : source->get_sparse_data_paths_and_values();
        auto& offsets = use_doc_value ? source->serialized_doc_value_column_offsets()
                                      : source->serialized_sparse_column_offsets();
        null_subcolumn.serialize_to_binary_column(paths, "a", values, 0);
        offsets.push_back(paths->size());
        source->get_subcolumn({})->insert_default();
        source->set_num_rows(1);
        if (use_doc_value) {
            source->get_sparse_column_mutable().resize(1);
        } else {
            source->get_doc_value_column_mutable().resize(1);
        }

        ColumnPtr index_data = ColumnString::create();
        assert_cast<ColumnString*>(index_data->assert_mutable().get())->insert_data("a", 1);
        ColumnPtr index = ColumnConst::create(index_data, 1);
        const auto variant_type = std::make_shared<DataTypeVariant>();
        const auto index_type = std::make_shared<DataTypeString>();
        const auto result_type = make_nullable(variant_type);
        Block block {{std::move(source), variant_type, "source"},
                     {std::move(index), index_type, "index"},
                     {result_type->create_column(), result_type, "result"}};
        ASSERT_TRUE(
                FunctionVariantElement::create()->execute_impl(nullptr, block, {0, 1}, 2, 1).ok());

        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        ASSERT_EQ(nullable.size(), 1);
        EXPECT_FALSE(nullable.is_null_at(0));

        DataTypeSerDe::FormatOptions options;
        auto timezone = cctz::utc_time_zone();
        options.timezone = &timezone;
        std::string logical_value;
        assert_cast<const ColumnVariant&>(nullable.get_nested_column())
                .serialize_one_row_to_string(0, &logical_value, options);
        EXPECT_EQ(logical_value, "null");
    }
}

TEST(function_variant_element_test, v2_nested_element_at_preserves_shredded_intermediate) {
    auto source_values = nested_shredded_variant_v2();
    const ColumnVariantV2* source_identity = source_values.get();
    auto source_nulls = ColumnUInt8::create();
    for (const auto is_null : std::array<uint8_t, 6> {0, 0, 0, 0, 1, 0}) {
        source_nulls->insert_value(is_null);
    }
    const DataTypePtr source_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    ColumnPtr source = ColumnNullable::create(std::move(source_values), std::move(source_nulls));

    ColumnPtr ancestor = execute_variant_v2_element(source, source_type, "a");
    const auto& ancestor_nullable = assert_cast<const ColumnNullable&>(*ancestor);
    EXPECT_EQ(ancestor_nullable.get_null_map_data(), (NullMap {0, 0, 0, 1, 1, 0}));
    const auto& ancestor_values =
            assert_cast<const ColumnVariantV2&>(ancestor_nullable.get_nested_column());
    ASSERT_TRUE(ancestor_values.is_shredded());
    ASSERT_EQ(ancestor_values.shredded_field_count(), 1);
    EXPECT_EQ(ancestor_values.shredded_field_path(0).get_parts(),
              PathInData(std::vector<std::string> {"b"}).get_parts());
    EXPECT_TRUE(source_identity->is_shredded());

    const ColumnVariantV2* ancestor_identity = &ancestor_values;
    const ColumnVariantV2* child_identity = &ancestor_values.shredded_field_values(0);
    ColumnPtr leaf = execute_variant_v2_element(ancestor, source_type, "b");
    const auto& leaf_nullable = assert_cast<const ColumnNullable&>(*leaf);
    EXPECT_EQ(leaf_nullable.get_null_map_data(), (NullMap {0, 1, 0, 1, 1, 1}));
    const auto& leaf_values =
            assert_cast<const ColumnVariantV2&>(leaf_nullable.get_nested_column());
    ASSERT_TRUE(leaf_values.is_typed());
    EXPECT_EQ(&leaf_values, child_identity);
    EXPECT_TRUE(ancestor_identity->is_shredded());
}

TEST(function_variant_element_test, v2_const_shredded_source_stays_const_and_shares_child) {
    auto residual = ColumnVariantV2::create();
    JsonStringToVariantEncoder encoder;
    encoder.add_json(StringRef("{}"));
    residual->insert_encoded_batch(encoder.finish_batch());
    auto values = ColumnInt64::create();
    values->insert_value(7);
    auto child = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), ColumnUInt8::create(1, 0)),
            std::make_shared<DataTypeInt64>());
    auto presence = ColumnUInt8::create(1, 1);
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a"}), std::move(child),
                        std::move(presence));
    auto source_values = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
    const ColumnVariantV2* child_identity = &source_values->shredded_field_values(0);
    constexpr size_t ROWS = 128;
    ColumnPtr source = ColumnConst::create(std::move(source_values), ROWS);

    ColumnPtr result = execute_variant_v2_element(std::move(source),
                                                  std::make_shared<DataTypeVariantV2>(), "a");
    const auto* constant = check_and_get_column<ColumnConst>(result.get());
    ASSERT_NE(constant, nullptr);
    EXPECT_EQ(constant->size(), ROWS);
    const auto& physical = assert_cast<const ColumnNullable&>(constant->get_data_column());
    const auto& values_result = assert_cast<const ColumnVariantV2&>(physical.get_nested_column());
    EXPECT_EQ(&values_result, child_identity);
    const auto& typed = assert_cast<const ColumnNullable&>(values_result.typed_column());
    EXPECT_EQ(assert_cast<const ColumnInt64&>(typed.get_nested_column()).get_data()[0], 7);
}

} // namespace doris
