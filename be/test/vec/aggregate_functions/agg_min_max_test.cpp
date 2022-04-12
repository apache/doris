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

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_min_max.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
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
    Array array;
    auto agg_function = factory.get(min_max_type, data_types, array);
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
    Array array;
    auto agg_function = factory.get(min_max_type, data_types, array);
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
