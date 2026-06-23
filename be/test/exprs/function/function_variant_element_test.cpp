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

namespace doris {

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

} // namespace doris
