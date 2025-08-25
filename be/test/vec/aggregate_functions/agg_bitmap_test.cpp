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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

const int agg_test_batch_size = 10;

namespace doris::vectorized {
// declare function
void register_aggregate_function_bitmap(AggregateFunctionSimpleFactory& factory);

TEST(AggBitmapTest, bitmap_union_test) {
    Arena arena;
    std::string function_name = "bitmap_union";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();
    for (int i = 0; i < agg_test_batch_size; i++) {
        BitmapValue bitmap_value(i);
        assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value);
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(function_name, data_types, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnBitmap ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.size(), 1);
    EXPECT_EQ(ans.get_element(0).cardinality(), agg_test_batch_size);
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    agg_function->streaming_agg_serialize_to_column(column, dst, agg_test_batch_size, arena);

    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        EXPECT_EQ(std::to_string(i), assert_cast<ColumnBitmap&>(*dst).get_element(i).to_string());
    }
}

TEST(AggBitmapTest, group_bitmap_xor_test) {
    Arena arena;
    std::string function_name = "group_bitmap_xor";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();

    // Insert first bitmap: {4, 7, 8}
    std::vector<int> values_1 = {4, 7, 8};
    BitmapValue bitmap_value_1;
    for (int value : values_1) {
        bitmap_value_1.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_1);

    // Insert second bitmap: {1, 3, 6, 15}
    std::vector<int> values_2 = {1, 3, 6, 15};
    BitmapValue bitmap_value_2;
    for (int value : values_2) {
        bitmap_value_2.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_2);

    // Insert third bitmap: {4, 7}
    std::vector<int> values_3 = {4, 7};
    BitmapValue bitmap_value_3;
    for (int value : values_3) {
        bitmap_value_3.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_3);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(function_name, data_types, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < 3; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnBitmap ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.size(), 1);
    EXPECT_EQ(ans.get_element(0).cardinality(), 5); // Expected intersection: {1, 3, 6, 8, 15}
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    BitmapValue intersection_result = ans.get_element(0);
    assert_cast<ColumnBitmap&>(*dst).insert_value(intersection_result);

    std::vector<int> expected_values = {1, 3, 6, 8, 15};
    BitmapValue expected_intersection;
    for (int value : expected_values) {
        expected_intersection.add(value);
    }

    EXPECT_EQ(expected_intersection.to_string(),
              assert_cast<ColumnBitmap&>(*dst).get_element(0).to_string());
}

TEST(AggBitmapTest, bitmap_intersect_test) {
    Arena arena;
    std::string function_name = "bitmap_intersect";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();

    // Insert first bitmap: {0, 1, 2, 3, 4, 5}
    std::vector<int> values_1 = {0, 1, 2, 3, 4, 5};
    BitmapValue bitmap_value_1;
    for (int value : values_1) {
        bitmap_value_1.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_1);

    // Insert second bitmap: {0, 1, 2, 3, 100}
    std::vector<int> values_2 = {0, 1, 2, 3, 100};
    BitmapValue bitmap_value_2;
    for (int value : values_2) {
        bitmap_value_2.add(value);
    }
    assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value_2);

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(function_name, data_types, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < 2; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnBitmap ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.size(), 1);
    EXPECT_EQ(ans.get_element(0).cardinality(), 4); // Expected intersection: {0, 1, 2, 3}
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    BitmapValue intersection_result = ans.get_element(0);
    assert_cast<ColumnBitmap&>(*dst).insert_value(intersection_result);

    BitmapValue expected_intersection;
    for (int i = 0; i < 4; i++) {
        expected_intersection.add(i);
    }

    EXPECT_EQ(expected_intersection.to_string(),
              assert_cast<ColumnBitmap&>(*dst).get_element(0).to_string());
}

TEST(AggBitmapTest, bitmap_union_count_test) {
    Arena arena;
    std::string function_name = "bitmap_union_count";
    auto data_type = std::make_shared<DataTypeBitMap>();
    // Prepare test data.
    auto column_bitmap = data_type->create_column();

    std::vector<int> values = {1, 2, 3, 4, 5, 1, 2, 3, 100};
    auto values_size = values.size();
    for (int value : values) {
        BitmapValue bitmap_value(value);
        assert_cast<ColumnBitmap&>(*column_bitmap).insert_value(bitmap_value);
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    auto agg_function = factory.get(function_name, {data_type}, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_bitmap.get()};
    for (int i = 0; i < values_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnInt64 ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.get_element(0), 6);
    agg_function->destroy(place);
}

template <PrimitiveType T>
void validate_bitmap_union_int_test() {
    Arena arena;
    std::string function_name = "bitmap_union_int";
    DataTypePtr data_type;
    if constexpr (T == TYPE_TINYINT) {
        data_type = std::make_shared<DataTypeInt8>();
    } else if constexpr (T == TYPE_SMALLINT) {
        data_type = std::make_shared<DataTypeInt16>();
    } else if constexpr (T == TYPE_INT) {
        data_type = std::make_shared<DataTypeInt32>();
    } else if constexpr (T == TYPE_BIGINT) {
        data_type = std::make_shared<DataTypeInt64>();
    } else {
        LOG(FATAL) << "unsupported type";
    }

    // Prepare test data.
    auto column_int = data_type->create_column();

    std::vector<typename PrimitiveTypeTraits<T>::ColumnItemType> values = {1, 2, 3, 4,  5,
                                                                           1, 2, 3, 100};
    auto values_size = values.size();
    for (int value : values) {
        (*column_int).insert(Field::create_field<T>(value));
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_bitmap(factory);
    auto agg_function = factory.get(function_name, {data_type}, false, -1);
    agg_function->set_version(3);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_int.get()};
    for (int i = 0; i < values_size; i++) {
        agg_function->add(place, column, i, arena);
    }

    // Check result.
    ColumnInt64 ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(ans.get_element(0), 6);
    agg_function->destroy(place);
}

TEST(AggBitmapTest, bitmap_union_int_test) {
    validate_bitmap_union_int_test<TYPE_TINYINT>();
    validate_bitmap_union_int_test<TYPE_SMALLINT>();
    validate_bitmap_union_int_test<TYPE_INT>();
    validate_bitmap_union_int_test<TYPE_BIGINT>();
}

} // namespace doris::vectorized
