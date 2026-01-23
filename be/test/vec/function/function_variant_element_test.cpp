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

#include "vec/functions/function_variant_element.cpp"

#include <gtest/gtest.h>

namespace doris::vectorized {

TEST(function_variant_element_test, extract_from_sparse_column) {
    auto variant_column = ColumnVariant::create(1 /*max_subcolumns_count*/);
    auto* variant_ptr = assert_cast<ColumnVariant*>(variant_column.get());

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
    variant_ptr->get_doc_value_column()->assume_mutable()->resize(1);

    ColumnPtr result;
    ColumnPtr index_column_ptr = ColumnString::create();
    auto* index_column_ptr_mutable =
            assert_cast<ColumnString*>(index_column_ptr->assume_mutable().get());
    index_column_ptr_mutable->insert_data("profile", 7);
    ColumnPtr index_column = ColumnConst::create(index_column_ptr, 1);
    auto status =
            FunctionVariantElement::get_element_column(*variant_column, index_column, &result);
    EXPECT_TRUE(status.ok());

    vectorized::DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    auto result_ptr = assert_cast<const ColumnVariant&>(*result.get());
    std::string result_string;
    result_ptr.serialize_one_row_to_string(0, &result_string, options);
    EXPECT_EQ(result_string, "{\"age\":\"John\",\"name\":\"John\"}");
}

} // namespace doris::vectorized