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

#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

const int agg_test_batch_size = 4096;

namespace doris::vectorized {
// declare function
void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory);

class AggMinMaxTest : public ::testing::TestWithParam<std::string> {};

TEST_P(AggMinMaxTest, min_max_test) {
    std::string min_max_type = GetParam();
    // Prepare test data.
    auto column_vector_int32 = ColumnVector<Int32>::create();
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_int32->insert(cast_to_nearest_field_type(i));
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {std::make_shared<DataTypeInt32>()};
    auto agg_function = factory.get(min_max_type, data_types);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_vector_int32.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, nullptr);
    }

    // Check result.
    ColumnInt32 ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(min_max_type == "min" ? 0 : agg_test_batch_size - 1, ans.get_element(0));
    agg_function->destroy(place);
}

TEST_P(AggMinMaxTest, min_max_decimal_test) {
    std::string min_max_type = GetParam();
    auto data_type = std::make_shared<DataTypeDecimal<Decimal128V2>>();
    // Prepare test data.
    auto column_vector_decimal128 = data_type->create_column();
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_decimal128->insert(
                cast_to_nearest_field_type(DecimalField<Decimal128V2>(Decimal128V2(i), 9)));
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {data_type};
    auto agg_function = factory.get(min_max_type, data_types);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_vector_decimal128.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, nullptr);
    }

    // Check result.
    ColumnDecimal128V2 ans(0, 9);
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(min_max_type == "min" ? 0 : agg_test_batch_size - 1, ans.get_element(0).value);
    agg_function->destroy(place);

    auto dst = agg_function->create_serialize_column();
    agg_function->streaming_agg_serialize_to_column(column, dst, agg_test_batch_size, nullptr);

    std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data() * agg_test_batch_size]);
    AggregateDataPtr places = memory2.get();
    agg_function->deserialize_from_column(places, *dst, nullptr, agg_test_batch_size);

    ColumnDecimal128V2 result(0, 9);
    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        agg_function->insert_result_into(places + agg_function->size_of_data() * i, result);
    }

    for (size_t i = 0; i != agg_test_batch_size; ++i) {
        EXPECT_EQ(i, result.get_element(i).value);
    }
}

TEST_P(AggMinMaxTest, min_max_string_test) {
    std::string min_max_type = GetParam();
    // Prepare test data.
    auto column_vector_str = ColumnString::create();
    std::vector<std::string> str_data = {"", "xyz", "z", "zzz", "abc", "foo", "bar"};
    for (const auto& s : str_data) {
        column_vector_str->insert_data(s.c_str(), s.length());
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_minmax(factory);
    DataTypes data_types = {std::make_shared<DataTypeString>()};
    auto agg_function = factory.get(min_max_type, data_types);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    // Do aggregation.
    const IColumn* column[1] = {column_vector_str.get()};
    for (int i = 0; i < str_data.size(); i++) {
        agg_function->add(place, column, i, nullptr);
    }

    // Check result.
    ColumnString ans;
    agg_function->insert_result_into(place, ans);
    EXPECT_EQ(min_max_type == "min" ? StringRef("") : StringRef("zzz"), ans.get_data_at(0));
    agg_function->destroy(place);
}

INSTANTIATE_TEST_SUITE_P(Params, AggMinMaxTest,
                         ::testing::ValuesIn(std::vector<std::string> {"min", "max"}));

} // namespace doris::vectorized
