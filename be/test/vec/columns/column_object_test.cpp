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

#include "vec/columns/column_object.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "vec/columns/common_column_test.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {

class ColumnObjectTest : public ::testing::Test {};

auto construct_dst_varint_column() {
    // 1. create an empty variant column
    vectorized::ColumnObject::Subcolumns dynamic_subcolumns;
    dynamic_subcolumns.create_root(vectorized::ColumnObject::Subcolumn(0, true, true /*root*/));
    dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.e"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.b.d"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                           vectorized::ColumnObject::Subcolumn {0, true});
    return ColumnObject::create(std::move(dynamic_subcolumns), true);
}

TEST_F(ColumnObjectTest, permute) {
    auto column_variant = construct_dst_varint_column();
    {
        // test empty column and limit == 0
        IColumn::Permutation permutation(0);
        auto col = column_variant->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }

    MutableColumns columns;
    columns.push_back(column_variant->get_ptr());
    assert_column_vector_permute(columns, 0);
    assert_column_vector_permute(columns, 1);
    assert_column_vector_permute(columns, column_variant->size());
    assert_column_vector_permute(columns, UINT64_MAX);
}

// TEST
TEST_F(ColumnObjectTest, test_pop_back) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int(123);
    Field field_string("hello");

    subcolumn.insert(field_int);
    subcolumn.insert(field_string);

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int8)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");
}

TEST_F(ColumnObjectTest, test_pop_back_multiple_types) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int8(42);
    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int8)");

    Field field_int16(12345);
    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(Int16)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int16)");

    Field field_int32(1234567);
    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 3);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(Int16)");
    EXPECT_EQ(subcolumn.data_types[2]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(Int16)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int16)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int8)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int8)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");

    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int32)");

    Field field_string("hello");
    subcolumn.insert(field_string);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(Int32)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(JSONB)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(JSONB)");

    subcolumn.pop_back(3);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");
}

TEST_F(ColumnObjectTest, test_nested_array_of_jsonb_get) {
    // Test case: Create a ColumnObject with subcolumn type Array<JSONB>

    // Create a ColumnObject with subcolumns
    auto variant_column = ColumnObject::create(true);

    // Add subcolumn with path "nested.array"
    variant_column->add_sub_column(PathInData("nested.array"), 0);

    // Get the subcolumn and manually set its type to Array<JSONB>
    auto* subcolumn = variant_column->get_subcolumn(PathInData("nested.array"));
    ASSERT_NE(subcolumn, nullptr);

    // Create test data: Array of strings
    Field array_of_strings = Array();

    // Add string elements to the array
    std::string test_data1 = R"("a")";
    std::string test_data2 = R"(b)";

    array_of_strings.get<Array&>().emplace_back(test_data1);
    array_of_strings.get<Array&>().emplace_back(test_data2);

    // Insert the array field into the subcolumn
    subcolumn->insert(array_of_strings);

    // Test 1:  the column and test get method
    {
        EXPECT_TRUE(variant_column->is_finalized());
        // check the subcolumn get method
        Field result;
        EXPECT_NO_THROW(subcolumn->get(0, result));

        // Verify the result is still an array
        EXPECT_EQ(result.get_type(), doris::vectorized::Field::Types::Array);

        const auto& result_array = result.get<const Array&>();
        EXPECT_EQ(result_array.size(), 2);

        // Check that all elements are JSONB fields
        for (const auto& item : result_array) {
            EXPECT_EQ(item.get_type(), doris::vectorized::Field::Types::String);
        }

        // Verify string content is preserved
        const auto& string1 = result_array[0].get<const String&>();
        const auto& string2 = result_array[1].get<const String&>();

        EXPECT_EQ(string1, R"("a")"); // "\"a\""
        EXPECT_EQ(string2, R"(b)");   // "b"
    }

    // Test 2: Test with a row of different type of array to test the subcolumn get method
    {
        // Add another row with different int array
        Field int_array = Array();
        int_array.get<Array&>().push_back(1);
        int_array.get<Array&>().push_back(2);
        int_array.get<Array&>().push_back(3);

        // and we should add more data to the subcolumn column
        subcolumn->insert(int_array);

        EXPECT_FALSE(variant_column->is_finalized());
        // check the subcolumn get method
        Field result;
        EXPECT_NO_THROW(subcolumn->get(1, result));
        EXPECT_EQ(result.get_type(), doris::vectorized::Field::Types::Array);
        const auto& result_array = result.get<const Array&>();
        EXPECT_EQ(result_array.size(), 3);
        EXPECT_EQ(result_array[0].get_type(), doris::vectorized::Field::Types::JSONB);
        EXPECT_EQ(result_array[1].get_type(), doris::vectorized::Field::Types::JSONB);
        EXPECT_EQ(result_array[2].get_type(), doris::vectorized::Field::Types::JSONB);

        // check the first row Field is a string
        Field result_string;
        EXPECT_NO_THROW(subcolumn->get(0, result_string));
        EXPECT_EQ(result_string.get_type(), doris::vectorized::Field::Types::Array);
        const auto& result_string_array = result_string.get<const Array&>();
        EXPECT_EQ(result_string_array.size(), 2);
        EXPECT_EQ(result_string_array[0].get_type(), doris::vectorized::Field::Types::JSONB);
        EXPECT_EQ(result_string_array[1].get_type(), doris::vectorized::Field::Types::JSONB);

        // Finalize -> we should get the least common type of the subcolumn
        variant_column->finalize();
        EXPECT_TRUE(variant_column->is_finalized());
        // we should get another subcolumn from the variant column
        auto* subcolumn_finalized = variant_column->get_subcolumn(PathInData("nested.array"));
        ASSERT_NE(subcolumn_finalized, nullptr);
        // check the subcolumn_finalized get method
        Field result1, result2;
        EXPECT_NO_THROW(subcolumn_finalized->get(0, result1));
        EXPECT_NO_THROW(subcolumn_finalized->get(1, result2));

        // Verify both results are arrays
        EXPECT_EQ(result1.get_type(), doris::vectorized::Field::Types::Array);
        EXPECT_EQ(result2.get_type(), doris::vectorized::Field::Types::Array);

        const auto& array1 = result1.get<const Array&>();
        const auto& array2 = result2.get<const Array&>();

        EXPECT_EQ(array1.size(), 2);
        EXPECT_EQ(array2.size(), 3);

        // Verify all elements are JSONB
        for (const auto& item : array1) {
            EXPECT_EQ(item.get_type(), doris::vectorized::Field::Types::JSONB);
        }
        for (const auto& item : array2) {
            EXPECT_EQ(item.get_type(), doris::vectorized::Field::Types::JSONB);
        }
    }

    // Test 4: Test with empty array
    {
        auto* subcolumn = variant_column->get_subcolumn(PathInData("nested.array"));
        ASSERT_NE(subcolumn, nullptr);
        Field empty_array_field = Array();
        subcolumn->insert(empty_array_field);

        EXPECT_TRUE(variant_column->is_finalized());
        // check the subcolumn get method
        Field result;
        EXPECT_NO_THROW(subcolumn->get(2, result));
        EXPECT_EQ(result.get_type(), doris::vectorized::Field::Types::Array);
        const auto& result_array = result.get<const Array&>();
        EXPECT_EQ(result_array.size(), 0);
    }
}

} // namespace doris::vectorized
