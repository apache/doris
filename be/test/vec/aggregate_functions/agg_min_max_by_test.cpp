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
#include "vec/aggregate_functions/aggregate_function_min_max_by.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

const int agg_test_batch_size = 4096;

namespace doris::vectorized {
// declare function
void register_aggregate_function_min_max_by(AggregateFunctionSimpleFactory& factory);

class AggMinMaxByTest : public ::testing::TestWithParam<std::string> {};

TEST_P(AggMinMaxByTest, min_max_by_test) {
    std::string min_max_by_type = GetParam();
    // Prepare test data.
    auto column_vector_value = ColumnInt32::create();
    auto column_vector_key_int32 = ColumnInt32::create();
    auto column_vector_key_str = ColumnString::create();
    auto max_pair = std::make_pair<std::string, int32_t>("foo_0", 0);
    auto min_pair = max_pair;
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_value->insert(cast_to_nearest_field_type(i));
        column_vector_key_int32->insert(cast_to_nearest_field_type(agg_test_batch_size - i));
        std::string str_val = fmt::format("foo_{}", i);
        if (max_pair.first < str_val) {
            max_pair.first = str_val;
            max_pair.second = i;
        }
        if (min_pair.first > str_val) {
            min_pair.first = str_val;
            min_pair.second = i;
        }
        column_vector_key_str->insert(cast_to_nearest_field_type(str_val));
    }

    // Prepare test function and parameters.
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_min_max_by(factory);

    // Test on 2 kind of key types (int32, string).
    for (int i = 0; i < 2; i++) {
        DataTypes data_types = {std::make_shared<DataTypeInt32>(),
                                i == 0 ? (DataTypePtr)std::make_shared<DataTypeInt32>()
                                       : (DataTypePtr)std::make_shared<DataTypeString>()};
        Array array;
        auto agg_function = factory.get(min_max_by_type, data_types, array);
        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        // Do aggregation.
        const IColumn* columns[2] = {column_vector_value.get(),
                                     i == 0 ? (IColumn*)column_vector_key_int32.get()
                                            : (IColumn*)column_vector_key_str.get()};
        for (int j = 0; j < agg_test_batch_size; j++) {
            agg_function->add(place, columns, j, nullptr);
        }

        // Check result.
        ColumnInt32 ans;
        agg_function->insert_result_into(place, ans);
        if (i == 0) {
            // Key type is int32.
            EXPECT_EQ(min_max_by_type == "max_by" ? 0 : agg_test_batch_size - 1,
                      ans.get_element(0));
        } else {
            // Key type is string.
            EXPECT_EQ(min_max_by_type == "max_by" ? max_pair.second : min_pair.second,
                      ans.get_element(0));
        }
        agg_function->destroy(place);
    }
}

INSTANTIATE_TEST_SUITE_P(Params, AggMinMaxByTest,
                         ::testing::ValuesIn(std::vector<std::string> {"min_by", "max_by"}));
} // namespace doris::vectorized
