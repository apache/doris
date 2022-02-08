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

#include <string>

#include "gtest/gtest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
// declare function
void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);

TEST(AggTest, basic_test) {
    auto column_vector_int32 = ColumnVector<Int32>::create();
    for (int i = 0; i < 4096; i++) {
        column_vector_int32->insert(cast_to_nearest_field_type(i));
    }
    // test implement interface
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    DataTypePtr data_type(std::make_shared<DataTypeInt32>());
    DataTypes data_types = {data_type};
    Array array;
    auto agg_function = factory.get("sum", data_types, array);
    AggregateDataPtr place = (char*)malloc(sizeof(uint64_t) * 4096);
    agg_function->create(place);
    const IColumn* column[1] = {column_vector_int32.get()};
    for (int i = 0; i < 4096; i++) {
        agg_function->add(place, column, i, nullptr);
    }
    int ans = 0;
    for (int i = 0; i < 4096; i++) {
        ans += i;
    }
    ASSERT_EQ(ans, *(int32_t*)place);
    agg_function->destroy(place);
    if(place) {
        free(place);
    }
}
} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
